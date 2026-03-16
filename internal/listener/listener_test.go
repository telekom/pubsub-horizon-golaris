// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"context"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/republish"
	"pubsub-horizon-golaris/internal/test"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
)

func createSubscriptionResource(subscriptionId, deliveryType string, circuitBreaker bool, callbackUrl string, redeliveriesPerSecond int) *resource.SubscriptionResource {
	return &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:        subscriptionId,
				DeliveryType:          enum.DeliveryType(deliveryType),
				CircuitBreakerOptOut:  circuitBreaker,
				Callback:              callbackUrl,
				RedeliveriesPerSecond: redeliveriesPerSecond,
			},
		},
	}
}

func setupMocks() (*test.RepublishingMockMap, *test.CircuitBreakerMockCache) {
	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap

	circuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = circuitBreakerCache
	config.Current.Hazelcast.Caches.CircuitBreakerCache = "test-circuit-breaker-cache"

	handlerCache := new(test.MockHandlerCache)
	handlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	handlerCache.On("TryLockWithLeaseAndTimeout", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	handlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)
	cache.HandlerCache = handlerCache

	return republishMockMap, circuitBreakerCache

}

func Test_InitializeListener(t *testing.T) {
	listener := &SubscriptionListener{}

	mockSubscriptionCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = mockSubscriptionCache
	config.Current.Hazelcast.Caches.SubscriptionCache = "test-subscription-cache"

	mockSubscriptionCache.On("AddListener", "test-subscription-cache", listener).Return(nil)

	Initialize()

	mockSubscriptionCache.AssertCalled(t, "AddListener", "test-subscription-cache", listener)
}

func TestSubscriptionListener_OnUpdate_DeliveryTypeToSSE(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "", 0)
	newSubscription := createSubscriptionResource(subscriptionId, "sse", false, "", 0)

	republishMockMap, circuitBreakerCache := setupMocks()

	// Adjust the mock setup
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("NewLockContext", mock.Anything).Return(context.Background())

	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == string(oldSubscription.Spec.Subscription.DeliveryType) &&
			entry.SubscriptionChange == false
	})).Return(nil)

	openCBMessage := &message.CircuitBreakerMessage{
		SubscriptionId: subscriptionId,
		Status:         enum.CircuitBreakerStatusOpen,
		LoopCounter:    0,
	}
	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(openCBMessage, nil)
	circuitBreakerCache.On("Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == string(oldSubscription.Spec.Subscription.DeliveryType) &&
			entry.SubscriptionChange == false
	}))
	circuitBreakerCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	circuitBreakerCache.AssertCalled(t, "Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything)
}

func TestSubscriptionListener_OnUpdate_DeliveryTypeToCallback(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "sse", false, "", 0)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false, "", 0)

	republishMockMap, circuitBreakerCache := setupMocks()
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == string(oldSubscription.Spec.Subscription.DeliveryType) &&
			entry.SubscriptionChange == false
	})).Return(nil)
	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(nil, nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == string(oldSubscription.Spec.Subscription.DeliveryType) &&
			entry.SubscriptionChange == false
	}))
}

func TestSubscriptionListener_OnUpdate_CallbackUrl(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "http://old-callback-url", 0)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false, "http://new-callback-url", 0)

	republishMockMap, circuitBreakerCache := setupMocks()
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("NewLockContext", mock.Anything).Return(context.Background())
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == "" &&
			entry.SubscriptionChange == false
	})).Return(nil)
	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(nil, nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == "" &&
			entry.SubscriptionChange == false
	}))
}

func TestSubscriptionListener_OnUpdate_CircuitBreakerOptOut(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "", 0)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", true, "", 0)

	republishMockMap, circuitBreakerCache := setupMocks()
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("NewLockContext", mock.Anything).Return(context.Background())
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.SubscriptionChange == false
	})).Return(nil)

	openCBMessage := &message.CircuitBreakerMessage{
		SubscriptionId: subscriptionId,
		Status:         enum.CircuitBreakerStatusOpen,
		LoopCounter:    0,
	}
	circuitBreakerCache.On("Get", "test-circuit-breaker-cache", subscriptionId).Return(openCBMessage, nil)
	circuitBreakerCache.On("Put", "test-circuit-breaker-cache", subscriptionId, mock.Anything).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	circuitBreakerCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	circuitBreakerCache.AssertCalled(t, "Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.SubscriptionChange == false
	}))
}

func TestSubscriptionListener_OnUpdate_RedeliveriesPerSecond(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "", 1)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false, "", 0)

	republishMockMap, circuitBreakerCache := setupMocks()
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("NewLockContext", mock.Anything).Return(context.Background())
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.SubscriptionChange == false
	})).Return(nil)
	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(nil, nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.SubscriptionChange == false
	}))
}

func TestHandleDeliveryTypeChangeFromSSEToCallback_NoRepublishingEntry(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "sse", false, "", 0)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false, "", 0)

	republishMockMap, circuitBreakerCache := setupMocks()

	// No existing republishing cache entry for this subscription
	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(nil, nil)
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == string(oldSubscription.Spec.Subscription.DeliveryType) &&
			entry.SubscriptionChange == false
	})).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, mock.MatchedBy(func(entry republish.RepublishingCacheEntry) bool {
		return entry.SubscriptionId == subscriptionId &&
			entry.OldDeliveryType == string(oldSubscription.Spec.Subscription.DeliveryType) &&
			entry.SubscriptionChange == false
	}))
	republishMockMap.AssertNotCalled(t, "Get", mock.Anything, subscriptionId)
}

func TestSubscriptionListener_OnDelete(t *testing.T) {
	subscriptionId := "test-subscription-id"

	republishMockMap, _ := setupMocks()
	mockEntry := &republish.RepublishingCacheEntry{SubscriptionId: subscriptionId}
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(mockEntry, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("NewLockContext", mock.Anything).Return(context.Background())

	event := &hazelcast.EntryNotified{Key: subscriptionId}
	listener := &SubscriptionListener{}
	listener.OnDelete(event)

	republishMockMap.AssertCalled(t, "Get", mock.Anything, subscriptionId)
	republishMockMap.AssertCalled(t, "Delete", mock.Anything, subscriptionId)
}
