/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package podlistprocessor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/extender/occupancy"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// This test deliberately exercises the existing-node filter and the estimator,
// rather than constructing a SchedulingError by hand.
func TestExtenderFailureDoesNotOfferScaleUp(t *testing.T) {
	for _, mode := range []string{"http-503", "connection-close", "invalid-ack", "no-capacity", "existing-fits", "ignorable-http-503"} {
		t.Run(mode, func(t *testing.T) {
			var existingCalls, templateCalls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request: %v", err)
					return
				}
				var request occupancy.Request
				if err := json.Unmarshal(body, &request); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				if request.Nodes == nil || len(request.Nodes.Items) != 1 {
					t.Errorf("want exactly one candidate node, got %+v", request.Nodes)
					return
				}
				node := request.Nodes.Items[0]
				if node.Name == "existing" {
					existingCalls.Add(1)
					switch mode {
					case "http-503", "ignorable-http-503":
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					case "connection-close":
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Errorf("hijack: %v", err)
							return
						}
						_ = conn.Close()
						return
					}
				} else {
					templateCalls.Add(1)
				}
				result := occupancy.Result{
					ExtenderFilterResult: extenderv1.ExtenderFilterResult{Nodes: &corev1.NodeList{Items: []corev1.Node{node}}},
					Simulation:           &occupancy.Ack{Version: occupancy.Version, Digest: fmt.Sprintf("%x", sha256.Sum256(body))},
				}
				if node.Name == "existing" {
					switch mode {
					case "invalid-ack":
						result.Simulation.Digest = "invalid"
					case "no-capacity":
						result.Nodes.Items = nil
						result.FailedNodes = extenderv1.FailedNodesMap{node.Name: "full"}
					}
				}
				if err := json.NewEncoder(w).Encode(result); err != nil {
					t.Errorf("encode result: %v", err)
				}
			}))
			t.Cleanup(srv.Close)

			cfg, err := latest.Default()
			require.NoError(t, err)
			ignorable := mode == "ignorable-http-503"
			cfg.Extenders = []schedulerconfig.Extender{{
				URLPrefix: srv.URL, FilterVerb: "filter", Ignorable: ignorable,
				ManagedResources: []schedulerconfig.ExtenderManagedResource{{Name: "example.com/slots", IgnoredByScheduler: true}},
			}}
			handle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cfg, false, false)
			require.NoError(t, err)
			if ignorable {
				require.Error(t, handle.ConfigureExtenderOccupancy(cfg, []string{srv.URL}), "occupancy must reject ignorable extenders")
			} else {
				require.NoError(t, handle.ConfigureExtenderOccupancy(cfg, []string{srv.URL}))
			}
			snapshot := predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), handle, false, 1, false, 0)
			require.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(testutils.BuildTestNode("existing", 10000, 1000000))))
			candidate := testutils.BuildTestPod("task30", 100, 100)
			candidate.UID = "task30"
			candidate.Spec.Containers[0].Resources.Requests["example.com/slots"] = resource.MustParse("1")
			processor := NewFilterOutSchedulablePodListProcessor(scheduling.ScheduleAnywhere)
			pending, err := processor.filterOutSchedulableByPacking(context.Background(), &ca_context.AutoscalingContext{}, []*corev1.Pod{candidate}, snapshot)
			if err == nil && len(pending) != 0 {
				limiter := estimator.NewThresholdBasedEstimationLimiter([]estimator.Threshold{estimator.NewStaticThreshold(1, 0)})
				binpacker := estimator.NewBinpackingNodeEstimator(snapshot, limiter, estimator.NewDecreasingPodOrderer(), nil, nil, false)
				template := framework.NewTestNodeInfo(testutils.BuildTestNode("template", 10000, 1000000))
				count, fitted := binpacker.Estimate(context.Background(), []estimator.PodEquivalenceGroup{{Pods: pending}}, template, nil)
				t.Logf("existing-node check returned pending Pod; template estimated %d new node(s), %d fitted Pod(s)", count, len(fitted))
				require.Equal(t, int32(1), templateCalls.Load(), "template node must be evaluated")
				if mode == "no-capacity" {
					require.Equal(t, 1, count)
					require.Len(t, fitted, 1)
				} else {
					t.Errorf("extender failure yielded a scale-up candidate: %d node(s)", count)
				}
			}
			require.Positive(t, existingCalls.Load())
			switch mode {
			case "http-503", "connection-close", "invalid-ack":
				var schedulingErr clustersnapshot.SchedulingError
				require.ErrorAs(t, err, &schedulingErr)
				require.Equal(t, clustersnapshot.SchedulingInternalError, schedulingErr.Type())
				require.Empty(t, pending)
			case "no-capacity":
				require.NoError(t, err)
				require.Len(t, pending, 1)
			case "existing-fits", "ignorable-http-503":
				require.NoError(t, err)
				require.Empty(t, pending)
				require.Zero(t, templateCalls.Load())
			default:
				t.Fatalf("unexpected test mode: %s", mode)
			}
		})
	}
}
