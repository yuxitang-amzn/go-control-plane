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
	"errors"
	"strconv"
	"strings"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
)

func (c *LinearCache) respond(value chan cache.Response, staleResources []string) {
	var resources []types.ResourceWithTtl
	// TODO: optimize the resources slice creations across different clients
	if len(staleResources) == 0 {
		resources = make([]types.ResourceWithTtl, 0, len(c.resources))
		for _, resource := range c.resources {
			resources = append(resources, types.ResourceWithTtl{Resource: resource})
		}
	} else {
		resources = make([]types.ResourceWithTtl, 0, len(staleResources))
		for _, name := range staleResources {
			resource := c.resources[name]
			if resource != nil {
				resources = append(resources, types.ResourceWithTtl{Resource: resource})
			}
		}
	}
	value <- &cache.RawResponse{
		Request:   &cache.Request{TypeUrl: c.typeURL},
		Resources: resources,
		Version:   c.versionPrefix + strconv.FormatUint(c.version, 10),
	}
}

func (c *LinearCache) notifyAll(modified map[string]struct{}) {
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

func (c *LinearCache) CreateWatch(request *cache.Request) (chan cache.Response, func()) {
	value := make(chan cache.Response, 1)
	if request.TypeUrl != c.typeURL {
		close(value)
		return value, nil
	}
	// If the version is not up to date, check whether any requested resource has
	// been updated between the last version and the current version. This avoids the problem
	// of sending empty updates whenever an irrelevant resource changes.
	stale := false
	staleResources := []string{} // empty means all

	// strip version prefix if it is present
	var lastVersion uint64
	var err error
	if strings.HasPrefix(request.VersionInfo, c.versionPrefix) {
		lastVersion, err = strconv.ParseUint(request.VersionInfo[len(c.versionPrefix):], 0, 64)
	} else {
		err = errors.New("mis-matched version prefix")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err != nil {
		stale = true
		staleResources = request.ResourceNames
	} else if len(request.ResourceNames) == 0 {
		stale = lastVersion != c.version
	} else {
		for _, name := range request.ResourceNames {
			// When a resource is removed, its version defaults 0 and it is not considered stale.
			if lastVersion < c.versionVector[name] {
				stale = true
				staleResources = append(staleResources, name)
			}
		}
	}
	if stale {
		c.respond(value, staleResources)
		return value, nil
	}
	// Create open watches since versions are up to date.
	if len(request.ResourceNames) == 0 {
		c.watchAll[value] = struct{}{}
		return value, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			delete(c.watchAll, value)
		}
	}
	for _, name := range request.ResourceNames {
		set, exists := c.watches[name]
		if !exists {
			set = make(watches)
			c.watches[name] = set
		}
		set[value] = struct{}{}
	}
	return value, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, name := range request.ResourceNames {
			set, exists := c.watches[name]
			if exists {
				delete(set, value)
			}
			if len(set) == 0 {
				delete(c.watches, name)
			}
		}
	}
}

// Number of active watches for a resource name.
func (c *LinearCache) NumWatches(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.watches[name]) + len(c.watchAll)
}
