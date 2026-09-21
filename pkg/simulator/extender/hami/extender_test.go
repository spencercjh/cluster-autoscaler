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

package hami_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	config "k8s.io/kubernetes/pkg/scheduler/apis/config"
	latest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/extender/hami"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func pod(name string) *v1.Pod {
	p := testutils.BuildTestPod(name, 100, 100)
	p.UID = types.UID(name)
	p.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
	return p
}
func newSnapshot(t *testing.T, factory func() clustersnapshot.ClusterSnapshotStore, url string) (clustersnapshot.ClusterSnapshot, *framework.Handle) {
	cfg, err := latest.Default()
	require.NoError(t, err)
	cfg.Extenders = []config.Extender{{URLPrefix: url, FilterVerb: "filter", ManagedResources: []config.ExtenderManagedResource{{Name: "nvidia.com/gpu", IgnoredByScheduler: true}}}}
	handle, err := framework.NewHandle(context.Background(), informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cfg, false, false)
	require.NoError(t, err)
	require.NoError(t, handle.ConfigureHAMiFeasibility(cfg, []string{url}))
	s := predicate.NewPredicateSnapshot(factory(), handle, false, 1, false, 0)
	require.NoError(t, s.AddNodeInfo(framework.NewTestNodeInfo(testutils.BuildTestNode("node", 10000, 1000000))))
	return s, handle
}
func server(t *testing.T, alter func(*hami.Result)) (*httptest.Server, chan hami.Request) {
	requests := make(chan hami.Request, 32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request hami.Request
		if err := json.Unmarshal(body, &request); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", 400)
			return
		}
		requests <- request
		result := hami.Result{ExtenderFilterResult: extenderv1.ExtenderFilterResult{Nodes: request.Nodes}, Simulation: &hami.Ack{Version: hami.Version, Digest: fmt.Sprintf("%x", sha256.Sum256(body))}}
		if alter != nil {
			alter(&result)
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	t.Cleanup(srv.Close)
	return srv, requests
}
func TestPlacementFollowsSnapshotLifecycle(t *testing.T) {
	for name, factory := range map[string]func() clustersnapshot.ClusterSnapshotStore{"basic": func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, "delta": func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() }} {
		t.Run(name, func(t *testing.T) {
			srv, requests := server(t, nil)
			s, _ := newSnapshot(t, factory, srv.URL)
			resident := pod("resident")
			resident.Spec.NodeName = "node"
			resident.Annotations = map[string]string{"hami.io/vgpu-devices-allocated": "GPU-0,NVIDIA,18432,0:;"}
			require.NoError(t, s.ForceAddPod(resident, "node"))
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Equal(t, hami.Preserve, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
			s.Fork()
			require.NoError(t, s.UnschedulePod(resident.Namespace, resident.Name, "node"))
			moved := resident.DeepCopy()
			moved.Spec.NodeName = ""
			require.NoError(t, s.SchedulePod(moved, "node"))
			<-requests
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Equal(t, hami.Allocate, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
			copy, err := s.GetNodeInfo("node")
			require.NoError(t, err)
			require.True(t, copy.DeepCopy().Pods()[0].SimulatedPlacement)
			s.Revert()
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Equal(t, hami.Preserve, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
			s.Fork()
			require.NoError(t, s.ForceRemovePod(resident.Namespace, resident.Name, "node"))
			require.NoError(t, s.Commit())
			require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
			require.Empty(t, (<-requests).Simulation.Nodes["node"])
			require.Equal(t, "node", resident.Spec.NodeName)
		})
	}
}
func TestInvalidOrLegacyResponseCannotPassIgnoredResources(t *testing.T) {
	cases := map[string]func(*hami.Result){"legacy": func(r *hami.Result) { r.Simulation = nil }, "digest": func(r *hami.Result) { r.Simulation.Digest = "wrong" }, "missing node": func(r *hami.Result) { r.Nodes.Items = nil }, "unknown node": func(r *hami.Result) { r.Nodes.Items[0].Name = "other" }, "contradictory": func(r *hami.Result) { r.FailedNodes = extenderv1.FailedNodesMap{"node": "failed"} }}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			srv, _ := server(t, alter)
			s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, srv.URL)
			require.Error(t, s.SchedulePod(pod("probe"), "node"))
			n, err := s.GetNodeInfo("node")
			require.NoError(t, err)
			require.Empty(t, n.Pods())
		})
	}
}
func TestUnknownPlacementFailsBeforeHTTP(t *testing.T) {
	srv, requests := server(t, nil)
	s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewBasicSnapshotStore() }, srv.URL)
	require.NoError(t, s.ForceAddPod(pod("unknown"), "node"))
	require.Error(t, s.CheckPredicates(pod("probe"), "node"))
	require.Empty(t, requests)
}

func TestNominatedPodIsReallocated(t *testing.T) {
	srv, requests := server(t, nil)
	s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() }, srv.URL)
	nominated := pod("nominated")
	nominated.Status.NominatedNodeName = "node"
	node := testutils.BuildTestNode("node", 10000, 1000000)
	require.NoError(t, s.SetClusterState(context.Background(), []*v1.Node{node}, []*v1.Pod{nominated}, nil, nil))
	require.NoError(t, s.CheckPredicates(pod("probe"), "node"))
	require.Equal(t, hami.Allocate, (<-requests).Simulation.Nodes["node"][0].Allocation.Mode)
}

func TestTemplateCopyDoesNotPreserveRealAllocations(t *testing.T) {
	srv, requests := server(t, nil)
	s, _ := newSnapshot(t, func() clustersnapshot.ClusterSnapshotStore { return store.NewDeltaSnapshotStore() }, srv.URL)
	resident := pod("daemon")
	resident.Spec.NodeName = "node"
	resident.Annotations = map[string]string{"hami.io/vgpu-devices-allocated": "source-device,NVIDIA,1024,0:;"}
	require.NoError(t, s.ForceAddPod(resident, "node"))
	original, err := s.GetNodeInfo("node")
	require.NoError(t, err)
	template, err := simulator.SanitizedNodeInfo(context.Background(), original, "copy")
	require.NoError(t, err)
	require.NoError(t, s.AddNodeInfo(template))
	require.NoError(t, s.CheckPredicates(pod("probe"), template.Node().Name))
	request := <-requests
	require.Equal(t, hami.Allocate, request.Simulation.Nodes[template.Node().Name][0].Allocation.Mode)
	require.Equal(t, "source-device,NVIDIA,1024,0:;", resident.Annotations["hami.io/vgpu-devices-allocated"])
}
