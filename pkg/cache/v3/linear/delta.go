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

	return value, nil
}

func (c *LinearCache) respondDelta(value chan cache.DeltaResponse, staleResources []string) {
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
	notifyList := make(map[chan cache.Response][]string)
	for name := range modified {
		for watch := range c.watches[name] {
			notifyList[watch] = append(notifyList[watch], name)
		}
		delete(c.watches, name)
	}
	for value, stale := range notifyList {
		c.respond(value, stale)
	}
	for value := range c.watchAll {
		c.respond(value, nil)
	}
	c.watchAll = make(watches)
}
