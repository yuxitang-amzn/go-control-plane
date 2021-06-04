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
	"strconv"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

func (c *LinearCache) CreateDeltaWatch(request *cache.DeltaRequest, state stream.StreamState) (chan cache.DeltaResponse, func()) {
	value := make(chan cache.DeltaResponse, 1)

	if request.TypeUrl != c.typeURL {
		close(value)
		return value, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	stale := false
	staleResources := []string{}

	for name, version := range state.ResourceVersions {
		if c.deltaVersionVector[name] != version {
			stale = true
			staleResources = append(staleResources, name)
		}
	}

	// If we've detected change, respond
	if stale {
		c.respondDelta(value, state.Wildcard, staleResources)
		return value, nil
	}

	if state.Wildcard || len(state.ResourceVersions) == 0 {
		c.deltaWatchAll[value] = struct{}{}
		return value, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			delete(c.deltaWatchAll, value)
		}
	}
	// We use our stream state here so we can accurately respond to what the server knows about
	for name := range state.ResourceVersions {
		set, exists := c.deltaWatches[name]
		if !exists {
			set = make(deltaWatches)
			c.deltaWatches[name] = set
		}
		set[value] = struct{}{}
	}

	return value, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		for name := range state.ResourceVersions {
			set, exists := c.deltaWatches[name]
			if exists {
				delete(set, value)
			}
			if len(set) == 0 {
				delete(c.deltaWatches, name)
			}
		}
	}
}

func (c *LinearCache) respondDelta(value chan cache.DeltaResponse, wildcard bool, staleResources []string) {
	var resources []types.Resource

	// TODO: optimize the resources slice creations across different clients
	if len(staleResources) == 0 {
		resources = make([]types.Resource, 0, len(c.resources))
		for _, resource := range c.resources {
			resources = append(resources, resource)
		}
	} else {
		resources = make([]types.Resource, 0, len(staleResources))
		for _, name := range staleResources {
			resource := c.resources[name]
			if resource != nil {
				resources = append(resources, resource)
			}
		}
	}
	value <- &cache.RawDeltaResponse{
		DeltaRequest:      &cache.DeltaRequest{TypeUrl: c.typeURL},
		Resources:         resources,
		SystemVersionInfo: c.versionPrefix + strconv.FormatUint(c.version, 10),
	}
}

func (c *LinearCache) notifyAllDelta(modified map[string]struct{}) {
	// de-duplicate watches that need to be responded
	notifyList := make(map[chan cache.DeltaResponse][]string)
	for name := range modified {
		for watch := range c.deltaWatches[name] {
			notifyList[watch] = append(notifyList[watch], name)
		}
		delete(c.watches, name)
	}
	for value, stale := range notifyList {
		c.respondDelta(value, false, stale)
	}
	for value := range c.deltaWatchAll {
		c.respondDelta(value, false, nil)
	}
	c.watchAll = make(watches)
}

func (c *LinearCache) NumDeltaWatches(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.deltaWatches[name]) + len(c.deltaWatchAll)
}
