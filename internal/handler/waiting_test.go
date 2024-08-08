// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/republish"
	"pubsub-horizon-golaris/internal/test"
	"testing"
	"time"
)

func TestCheckWaitingEvents(t *testing.T) {
	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	subscriptionMockCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subscriptionMockCache

	republishMockCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockCache

	circuitBreakerMockCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = circuitBreakerMockCache

	waitingHandler := new(test.WaitingMockHandler)
	cache.WaitingHandler = waitingHandler

	waitingHandler.On("NewLockContext", mock.Anything).Return(context.Background())
	waitingHandler.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	waitingHandler.On("Unlock", mock.Anything, mock.Anything).Return(nil)

	dbMessages := []message.StatusMessage{
		{
			Topic:          "test-topic",
			Status:         enum.StatusWaiting,
			SubscriptionId: "sub123",
			DeliveryType:   enum.DeliveryTypeCallback,
		},
		{
			Topic:          "test-topic",
			Status:         enum.StatusWaiting,
			SubscriptionId: "sub123",
			DeliveryType:   enum.DeliveryTypeCallback,
		},
	}

	mockMongo.On("FindUniqueWaitingMessages", mock.Anything, mock.Anything).Return(dbMessages, nil, nil)

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId: "sub123",
				DeliveryType:   enum.DeliveryTypeCallback,
			},
		},
	}

	subscriptionMockCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").Return(subscription, nil)
	republishMockCache.On("Get", mock.Anything, "sub123").Return(nil, nil)
	republishMockCache.On("Set", mock.Anything, "sub123", republish.RepublishingCacheEntry{SubscriptionId: "sub123"}).Return(nil)
	circuitBreakerMockCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, "sub123").Return(&message.CircuitBreakerMessage{}, nil)

	CheckWaitingEvents()

	mockMongo.AssertExpectations(t)

	waitingHandler.AssertCalled(t, "NewLockContext", mock.Anything)
	waitingHandler.AssertCalled(t, "TryLockWithTimeout", mock.Anything, cache.WaitingLockKey, 10*time.Millisecond)
	waitingHandler.AssertCalled(t, "Unlock", mock.Anything, cache.WaitingLockKey)

	subscriptionMockCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123")
	republishMockCache.AssertCalled(t, "Get", mock.Anything, "sub123")
	republishMockCache.AssertCalled(t, "Set", mock.Anything, "sub123", republish.RepublishingCacheEntry{SubscriptionId: "sub123"})
	circuitBreakerMockCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, "sub123")
}
