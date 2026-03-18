// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"fmt"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/test"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
)

func TestMakeCircuitBreakerResponse_NoPanicWhenSubscriptionNil(t *testing.T) {
	config.Current = test.BuildTestConfig()

	subCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subCache

	// Subscription does not exist (deleted/orphaned)
	subCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "orphaned-sub").
		Return((*resource.SubscriptionResource)(nil), nil)

	cbMsg := &message.CircuitBreakerMessage{
		SubscriptionId: "orphaned-sub",
		Status:         enum.CircuitBreakerStatusClosed,
		EventType:      "test.event.v1",
		Environment:    "playground",
	}

	// After fix, this should return gracefully with empty SubscriberId (no panic).
	assert.NotPanics(t, func() {
		resp := makeCircuitBreakerResponse(cbMsg)
		assert.Empty(t, resp.SubscriberId, "subscriberId should be empty when subscription not found")
		assert.Empty(t, resp.PublisherId, "publisherId should be empty when subscription not found")
	})
}

func TestMakeCircuitBreakerResponse_WithSubscriptionError(t *testing.T) {
	config.Current = test.BuildTestConfig()

	subCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subCache

	// Subscription cache returns an error
	subCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "error-sub").
		Return((*resource.SubscriptionResource)(nil), fmt.Errorf("hazelcast connection error"))

	cbMsg := &message.CircuitBreakerMessage{
		SubscriptionId: "error-sub",
		Status:         enum.CircuitBreakerStatusOpen,
		EventType:      "test.event.v1",
		Environment:    "playground",
	}

	// Should return gracefully even when subscription cache errors
	assert.NotPanics(t, func() {
		resp := makeCircuitBreakerResponse(cbMsg)
		assert.Empty(t, resp.SubscriberId, "subscriberId should be empty on error")
	})
}
