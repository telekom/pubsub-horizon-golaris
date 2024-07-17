// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"os"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/republish"
	"pubsub-horizon-golaris/internal/test"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	test.SetupDocker(&test.Options{
		MongoDb:   false,
		Hazelcast: true,
	})
	config.Current = test.BuildTestConfig()
	cache.Initialize()
	code := m.Run()

	test.TeardownDocker()
	os.Exit(code)
}

func TestCheckOpenCircuitBreakers_SubscriptionExists(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Mock HandleOpenCircuitBreaker function
	HandleOpenCircuitBreakerFunc = func(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
		cbMessage.Status = enum.CircuitBreakerStatusClosed
		cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage)
	}

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)
	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// Set mocked circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Set mocked subscription in the cache
	cache.SubscriptionCache.Put(config.Current.Hazelcast.Caches.SubscriptionCache, testSubscriptionId, *testSubscriptionResource)

	// Call the function under test
	checkOpenCircuitBreakers()

	// Assertions
	cbEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusClosed, cbEntry.Status)
}

func TestCheckOpenCircuitBreakers_NoSubscription(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Mock HandleOpenCircuitBreaker function
	HandleOpenCircuitBreakerFunc = func(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {}

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)

	// Set mocked circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	checkOpenCircuitBreakers()

	// Assertions
	cbEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusClosed, cbEntry.Status)
}

func TestCheckRepublishingEntries_SubscriptionExists(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	ctx := context.Background()

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	// Mock HandleRepublishingEntry function
	HandleRepublishingEntryFunc = func(subscription *resource.SubscriptionResource) {
		cache.RepublishingCache.Delete(ctx, testSubscriptionId)
	}

	republishingCacheEntry := republish.RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// Set mocked republishing cache entry in the cache
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	// Set mocked subscription in the cache
	cache.SubscriptionCache.Put(config.Current.Hazelcast.Caches.SubscriptionCache, testSubscriptionId, *testSubscriptionResource)

	// Call the function under test
	checkRepublishingEntries()

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

func TestCheckRepublishingEntries_NoSubscription(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	ctx := context.Background()

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"

	// Mock HandleRepublishingEntry function
	HandleRepublishingEntryFunc = func(subscription *resource.SubscriptionResource) {}

	republishingCacheEntry := republish.RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}

	// Set mocked republishing cache entry in the cache
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)

	// Call the function under test
	checkRepublishingEntries()

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

func TestGetSubscription(t *testing.T) {
	var assertions = assert.New(t)

	mockCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = mockCache
	config.Current.Hazelcast.Caches.SubscriptionCache = "testMap"

	expectedSubscription := &resource.SubscriptionResource{}
	mockCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").Return(expectedSubscription, nil)

	subscription := getSubscription("sub123")
	assertions.NotNil(subscription)
	assertions.Equal(expectedSubscription, subscription)
}
