// Copyright 2020 Envoyproxy Authors
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package linear

import (
	"testing"

	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

type resourceInfo struct {
	name    string
	version string
}

func verifyDeltaResponse(t *testing.T, ch <-chan cache.DeltaResponse, resources []resourceInfo) {
	t.Helper()
	r := <-ch
	if r.GetDeltaRequest().TypeUrl != testType {
		t.Errorf("unexpected empty request type URL: %q", r.GetDeltaRequest().TypeUrl)
	}
	out, err := r.GetDeltaDiscoveryResponse()
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Resources) != len(resources) {
		t.Errorf("unexpected number of responses: got %d, want %d", len(out.Resources), len(resources))
	}
	for _, r := range resources {
		found := false
		for _, r1 := range out.Resources {
			if r1.Name == r.name && r1.Version == r.version {
				found = true
				break
			} else if r1.Name == r.name {
				t.Errorf("unexpected version for resource %q: got %q, want %q", r.name, r1.Version, r.version)
				found = true
				break
			}
		}
		if !found {
			t.Errorf("resource with name %q not found in response", r.name)
		}
	}
	if out.TypeUrl != testType {
		t.Errorf("unexpected type URL: %q", out.TypeUrl)
	}
}

func checkDeltaWatchCount(t *testing.T, c *LinearCache, name string, count int) {
	t.Helper()
	if i := c.NumDeltaWatches(name); i != count {
		t.Errorf("unexpected number of delta watches: got %d, want %d", i, count)
	}
}

func mustBlockDelta(t *testing.T, w <-chan cache.DeltaResponse) {
	select {
	case <-w:
		t.Error("watch must block")
	default:
	}
}

func TestLinearDeltaBasic(t *testing.T) {
	c := NewLinearCache(testType)
	state := stream.StreamState{Wildcard: false, ResourceVersions: map[string]string{"a": "", "b": ""}}

	w, _ := c.CreateDeltaWatch(&cache.DeltaRequest{TypeUrl: testType}, state)
	mustBlockDelta(t, w)
	checkDeltaWatchCount(t, c, "a", 1)
	a := &endpoint.ClusterLoadAssignment{ClusterName: "a"}
	hash := hashResource(a)
	c.UpdateResource("a", a)
	checkDeltaWatchCount(t, c, "a", 0)
	verifyDeltaResponse(t, w, []resourceInfo{{"a", hash}}) // b shouldn't be included in the response
}

func TestLinearDeltaExistingResources(t *testing.T) {
	c := NewLinearCache(testType)
	a := &endpoint.ClusterLoadAssignment{ClusterName: "a"}
	hashA := hashResource(a)
	c.UpdateResource("a", a)
	b := &endpoint.ClusterLoadAssignment{ClusterName: "b"}
	hashB := hashResource(b)
	c.UpdateResource("b", b)

	state := stream.StreamState{Wildcard: false, ResourceVersions: map[string]string{"b": "", "c": ""}} // watching b and c - not interested in a
	w, _ := c.CreateDeltaWatch(&cache.DeltaRequest{TypeUrl: testType}, state)
	checkDeltaWatchCount(t, c, "a", 0)
	verifyDeltaResponse(t, w, []resourceInfo{{"b", hashB}})

	state = stream.StreamState{Wildcard: false, ResourceVersions: map[string]string{"a": "", "b": ""}}
	w, _ = c.CreateDeltaWatch(&cache.DeltaRequest{TypeUrl: testType}, state)
	checkDeltaWatchCount(t, c, "b", 0)
	verifyDeltaResponse(t, w, []resourceInfo{{"b", hashB}, {"a", hashA}})
}

func TestLinearDeltaInitialResourcesVersionSet(t *testing.T) {
	c := NewLinearCache(testType)
	a := &endpoint.ClusterLoadAssignment{ClusterName: "a"}
	hashA := hashResource(a)
	c.UpdateResource("a", a)
	b := &endpoint.ClusterLoadAssignment{ClusterName: "b"}
	hashB := hashResource(b)
	c.UpdateResource("b", b)

	state := stream.StreamState{Wildcard: false, ResourceVersions: map[string]string{"a": "", "b": hashB}}
	w, _ := c.CreateDeltaWatch(&cache.DeltaRequest{TypeUrl: testType}, state)
	checkDeltaWatchCount(t, c, "a", 0)
	verifyDeltaResponse(t, w, []resourceInfo{{"a", hashA}}) // b is up to date and shouldn't be returned

	state = stream.StreamState{Wildcard: false, ResourceVersions: map[string]string{"a": hashA, "b": hashB}}
	w, _ = c.CreateDeltaWatch(&cache.DeltaRequest{TypeUrl: testType}, state)
	mustBlockDelta(t, w)
	checkDeltaWatchCount(t, c, "b", 1)
	b = &endpoint.ClusterLoadAssignment{ClusterName: "b", Endpoints: []*endpoint.LocalityLbEndpoints{{Priority: 10}}} // new version of b
	hashB = hashResource(b)
	c.UpdateResource("b", b)
	checkDeltaWatchCount(t, c, "b", 0)
	verifyDeltaResponse(t, w, []resourceInfo{{"b", hashB}})
}

func hashResource(r types.Resource) string {
	// hash our verison in here and build the version map
	marshaledResource, err := cache.MarshalResource(r)
	if err != nil {
		return ""
	}

	v := cache.HashResource(marshaledResource)
	if v == "" {
		return ""
	}

	return v
}
