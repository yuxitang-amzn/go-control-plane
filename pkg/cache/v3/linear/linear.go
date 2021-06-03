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
	"context"
	"errors"
	"sync"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/log"
)

type watches = map[chan cache.Response]struct{}
type deltaWatches = map[chan cache.DeltaResponse]struct{}

// LinearCache supports collectons of opaque resources. This cache has a
// single collection indexed by resource names and manages resource versions
// internally. It implements the cache interface for a single type URL and
// should be combined with other caches via type URL muxing. It can be used to
// supply EDS entries, for example, uniformly across a fleet of proxies.
type LinearCache struct {
	// Type URL specific to the cache.
	typeURL string
	// Collection of resources indexed by name.
	resources map[string]types.Resource
	// Watches open by clients, indexed by resource name. Whenever resources
	// are changed, the watch is triggered.
	watches map[string]watches
	// DeltaWatches open by clients, indexed by resource name.
	// Whenever resouces are changed, we trigger the watch
	deltaWatches map[string]deltaWatches
	// Set of watches for all resources in the collection
	watchAll      watches
	deltaWatchAll deltaWatches
	// Continously incremented version
	version uint64
	// Version prefix to be sent to the clients
	versionPrefix string
	// Versions for each resource by name.
	versionVector map[string]uint64
	mu            sync.Mutex
	log           log.Logger
}

var _ cache.Cache = &LinearCache{}

// Options for modifying the behavior of the linear cache.
type LinearCacheOption func(*LinearCache)

// WithVersionPrefix sets a version prefix of the form "prefixN" in the version info.
// Version prefix can be used to distinguish replicated instances of the cache, in case
// a client re-connects to another instance.
func WithVersionPrefix(prefix string) LinearCacheOption {
	return func(cache *LinearCache) {
		cache.versionPrefix = prefix
	}
}

// WithInitialResources initializes the initial set of resources.
func WithInitialResources(resources map[string]types.Resource) LinearCacheOption {
	return func(cache *LinearCache) {
		cache.resources = resources
		for name := range resources {
			cache.versionVector[name] = 0
		}
	}
}

// WithLogger initializes the internal cach logger with whatever is passed in
func WithLogger(log log.Logger) LinearCacheOption {
	return func(cache *LinearCache) {
		cache.log = log
	}
}

// NewLinearCache creates a new cache. See the comments on the struct definition.
func NewLinearCache(typeURL string, opts ...LinearCacheOption) *LinearCache {
	out := &LinearCache{
		typeURL:       typeURL,
		resources:     make(map[string]types.Resource),
		watches:       make(map[string]watches),
		deltaWatches:  make(map[string]deltaWatches),
		watchAll:      make(watches),
		deltaWatchAll: make(deltaWatches),
		version:       0,
		versionVector: make(map[string]uint64),
	}
	for _, opt := range opts {
		opt(out)
	}
	return out
}

// UpdateResource updates a resource in the collection.
func (cache *LinearCache) UpdateResource(name string, res types.Resource) error {
	if res == nil {
		return errors.New("nil resource")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.version += 1
	cache.versionVector[name] = cache.version
	cache.resources[name] = res

	// TODO: batch watch closures to prevent rapid updates
	cache.notifyAll(map[string]struct{}{name: {}})

	return nil
}

// DeleteResource removes a resource in the collection.
func (cache *LinearCache) DeleteResource(name string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.version += 1
	delete(cache.versionVector, name)
	delete(cache.resources, name)

	// TODO: batch watch closures to prevent rapid updates
	cache.notifyAll(map[string]struct{}{name: {}})
	return nil
}

func (c *LinearCache) Fetch(ctx context.Context, request *cache.Request) (cache.Response, error) {
	return nil, errors.New("not implemented")
}
