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

package mux

import (
	"context"
	"errors"

	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

// MuxCache multiplexes across several caches using a classification function.
// If there is no matching cache for a classification result, the cache
// responds with an empty closed channel, which effectively terminates the
// stream on the server. It might be preferred to respond with a "nil" channel
// instead which will leave the stream open in case the stream is aggregated by
// making sure there is always a matching cache.
type MuxCache struct {
	// Classification functions.
	Classify      func(*cache.Request) string
	ClassifyDelta func(*cache.DeltaRequest) string

	// Muxed caches.
	Caches map[string]cache.Cache
}

var _ cache.Cache = &MuxCache{}

func (mux *MuxCache) CreateWatch(request *cache.Request) (chan cache.Response, func()) {
	key := mux.Classify(request)
	c, exists := mux.Caches[key]
	if !exists {
		value := make(chan cache.Response)
		close(value)
		return value, nil
	}
	return c.CreateWatch(request)
}

func (mux *MuxCache) CreateDeltaWatch(request *cache.DeltaRequest, state stream.StreamState) (chan cache.DeltaResponse, func()) {
	key := mux.ClassifyDelta(request)
	c, exists := mux.Caches[key]
	if !exists {
		value := make(chan cache.DeltaResponse)
		close(value)
		return value, nil
	}
	return c.CreateDeltaWatch(request, state)
}

func (mux *MuxCache) Fetch(ctx context.Context, request *cache.Request) (cache.Response, error) {
	return nil, errors.New("not implemented")
}
