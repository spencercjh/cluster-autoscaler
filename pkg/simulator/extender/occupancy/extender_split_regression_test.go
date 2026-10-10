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

package occupancy_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/extender/occupancy"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type splitCall struct {
	size  int
	names []string
}

func splitFixture(t *testing.T, url string, names []string, residentsPerNode int) ([]fwk.NodeInfo, *framework.Handle) {
	t.Helper()
	snapshot, handle := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, url)
	infos := make([]fwk.NodeInfo, 0, len(names))
	for _, name := range names {
		nd := testutils.BuildTestNode(name, 10000, 1000000)
		require.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(nd)))
		for i := 0; i < residentsPerNode; i++ {
			resident := pod(fmt.Sprintf("%s-resident-%03d", name, i))
			resident.Spec.NodeName = name
			resident.Annotations = map[string]string{"example.com/allocation": strings.Repeat("x", 8<<10)}
			require.NoError(t, snapshot.ForceAddPod(resident, name))
		}
		info, err := snapshot.GetNodeInfo(name)
		require.NoError(t, err)
		infos = append(infos, info)
	}
	return infos, handle
}

// The request is made large by many ordinary resident Pods, not by putting an
// invalid >256 KiB annotation on a Node or Pod.
func TestRealisticOccupancySplitAndAtomicFailure(t *testing.T) {
	names := []string{"n0", "n1", "n2", "n3", "n4"}
	for _, mode := range []string{"success", "second-batch-http-503", "second-batch-invalid-ack"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var calls []splitCall
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
				if err := occupancy.Validate(request); err != nil {
					t.Errorf("invalid occupancy request: %v", err)
					return
				}
				call := splitCall{size: len(body)}
				result := occupancy.Result{
					ExtenderFilterResult: extenderv1.ExtenderFilterResult{
						Nodes: &v1.NodeList{}, FailedNodes: extenderv1.FailedNodesMap{},
						FailedAndUnresolvableNodes: extenderv1.FailedNodesMap{},
					},
					Simulation: &occupancy.Ack{Version: occupancy.Version, Digest: fmt.Sprintf("%x", sha256.Sum256(body))},
				}
				for _, node := range request.Nodes.Items {
					call.names = append(call.names, node.Name)
					if got := len(request.Simulation.Nodes[node.Name]); got != 32 {
						t.Errorf("node %s: want all 32 residents, got %d", node.Name, got)
					}
					switch node.Name {
					case "n1", "n4":
						result.FailedNodes[node.Name] = "full"
					case "n2":
						result.FailedAndUnresolvableNodes[node.Name] = "forbidden"
					default:
						result.Nodes.Items = append(result.Nodes.Items, node)
					}
				}
				mu.Lock()
				calls = append(calls, call)
				mu.Unlock()
				if strings.Contains(strings.Join(call.names, ","), "n4") {
					if mode == "second-batch-http-503" {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					}
					if mode == "second-batch-invalid-ack" {
						result.Simulation.Digest = "invalid"
					}
				}
				if err := json.NewEncoder(w).Encode(result); err != nil {
					t.Errorf("encode response: %v", err)
				}
			}))
			t.Cleanup(srv.Close)
			infos, handle := splitFixture(t, srv.URL, names, 32)
			originalNodes := make([]*v1.Node, len(infos))
			originalPods := make([][]*v1.Pod, len(infos))
			for i, info := range infos {
				originalNodes[i] = info.Node().DeepCopy()
				for _, resident := range info.GetPods() {
					originalPods[i] = append(originalPods[i], resident.GetPod().DeepCopy())
				}
			}
			candidate := pod("candidate")
			originalCandidate := candidate.DeepCopy()
			got, failed, unresolvable, err := handle.Extenders[0].Filter(candidate, infos)
			mu.Lock()
			observed := append([]splitCall(nil), calls...)
			mu.Unlock()
			t.Logf("mode=%s HTTP calls=%d batch sizes=%v", mode, len(observed), observed)
			require.Greater(t, len(observed), 1, "oversized multi-node request must be split")
			seen := map[string]int{}
			for _, call := range observed {
				require.LessOrEqual(t, call.size, 1<<20)
				for _, name := range call.names {
					seen[name]++
				}
			}
			require.Equal(t, map[string]int{"n0": 1, "n1": 1, "n2": 1, "n3": 1, "n4": 1}, seen)
			if mode == "success" {
				require.NoError(t, err)
				var fittedNames []string
				for _, info := range got {
					fittedNames = append(fittedNames, info.Node().Name)
				}
				require.Equal(t, []string{"n0", "n3"}, fittedNames)
				require.Equal(t, extenderv1.FailedNodesMap{"n1": "full", "n4": "full"}, failed)
				require.Equal(t, extenderv1.FailedNodesMap{"n2": "forbidden"}, unresolvable)
			} else {
				require.Error(t, err)
				require.Nil(t, got, "no earlier batch's success can escape on later batch failure")
				require.Nil(t, failed)
				require.Nil(t, unresolvable)
			}
			require.True(t, reflect.DeepEqual(originalCandidate, candidate), "candidate Pod was mutated")
			for i, info := range infos {
				require.True(t, reflect.DeepEqual(originalNodes[i], info.Node()), "node %d was mutated", i)
				for j, resident := range info.GetPods() {
					require.True(t, reflect.DeepEqual(originalPods[i][j], resident.GetPod()), "resident %d/%d was mutated", i, j)
				}
			}
		})
	}
}

func TestOversizedSingleNodeFailsWithoutSending(t *testing.T) {
	var mu sync.Mutex
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	infos, handle := splitFixture(t, srv.URL, []string{"huge"}, 140)
	before := len(infos[0].GetPods())
	got, failed, unresolvable, err := handle.Extenders[0].Filter(pod("candidate"), infos)
	require.ErrorContains(t, err, "size limit")
	require.Nil(t, got)
	require.Nil(t, failed)
	require.Nil(t, unresolvable)
	mu.Lock()
	require.Zero(t, calls, "oversized node must not be silently truncated or sent")
	mu.Unlock()
	require.Equal(t, before, len(infos[0].GetPods()))
}
