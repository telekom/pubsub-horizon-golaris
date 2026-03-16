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
	"sync"
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

	republishingCacheEntry := republish.RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
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

	republishingCacheEntry := republish.RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}

	// Set mocked republishing cache entry in the cache
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)

	// Call the function under test
	checkRepublishingEntries()

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

func TestCheckOpenCircuitBreakers_ContinuesAfterNilSubscription(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	var mu sync.Mutex
	handledSubscriptionIds := make([]string, 0)

	// Mock HandleOpenCircuitBreaker to track which entries are handled
	HandleOpenCircuitBreakerFunc = func(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
		mu.Lock()
		defer mu.Unlock()
		handledSubscriptionIds = append(handledSubscriptionIds, cbMessage.SubscriptionId)
	}

	// Prepare test data: three CB entries, only first and third have subscriptions
	subId1 := "sub-with-subscription-1"
	subId2 := "sub-without-subscription"
	subId3 := "sub-with-subscription-3"

	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	cbMsg1 := test.NewTestCbMessage(subId1)
	cbMsg2 := test.NewTestCbMessage(subId2)
	cbMsg3 := test.NewTestCbMessage(subId3)

	subResource1 := test.NewTestSubscriptionResource(subId1, testCallbackUrl, testEnvironment)
	subResource3 := test.NewTestSubscriptionResource(subId3, testCallbackUrl, testEnvironment)

	// Set CB entries in cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subId1, cbMsg1)
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subId2, cbMsg2)
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subId3, cbMsg3)

	// Use mock SubscriptionCache because Hazelcast JSON round-trip fails for SubscriptionResource
	origSubCache := cache.SubscriptionCache
	mockSubCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = mockSubCache
	defer func() { cache.SubscriptionCache = origSubCache }()

	mockSubCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId1).Return(subResource1, nil)
	mockSubCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId2).Return((*resource.SubscriptionResource)(nil), nil)
	mockSubCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId3).Return(subResource3, nil)

	// Call the function under test
	checkOpenCircuitBreakers()

	// Wait briefly for async goroutines to complete
	time.Sleep(500 * time.Millisecond)

	// Assertions: sub2 should be closed (nil subscription path) and sub1/sub3 should be handled
	mu.Lock()
	defer mu.Unlock()

	// sub2 has no subscription, so its CB should be closed
	cbEntry2, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subId2)
	assertions.Equal(enum.CircuitBreakerStatusClosed, cbEntry2.Status)

	// sub1 and sub3 should have been handled (goroutine was launched)
	assertions.Contains(handledSubscriptionIds, subId1)
	assertions.Contains(handledSubscriptionIds, subId3)
}

func TestCheckRepublishingEntries_ContinuesAfterNilSubscription(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	ctx := context.Background()

	var mu sync.Mutex
	handledSubscriptionIds := make([]string, 0)

	// Mock HandleRepublishingEntry to track which entries are handled
	HandleRepublishingEntryFunc = func(subscription *resource.SubscriptionResource) {
		mu.Lock()
		defer mu.Unlock()
		handledSubscriptionIds = append(handledSubscriptionIds, subscription.Spec.Subscription.SubscriptionId)
	}

	// Prepare test data: three republishing entries, only first and third have subscriptions
	subId1 := "sub-with-subscription-1"
	subId2 := "sub-without-subscription"
	subId3 := "sub-with-subscription-3"

	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	entry1 := republish.RepublishingCacheEntry{SubscriptionId: subId1, RepublishingUpTo: time.Now()}
	entry2 := republish.RepublishingCacheEntry{SubscriptionId: subId2, RepublishingUpTo: time.Now()}
	entry3 := republish.RepublishingCacheEntry{SubscriptionId: subId3, RepublishingUpTo: time.Now()}

	subResource1 := test.NewTestSubscriptionResource(subId1, testCallbackUrl, testEnvironment)
	subResource3 := test.NewTestSubscriptionResource(subId3, testCallbackUrl, testEnvironment)

	// Set republishing entries in cache
	cache.RepublishingCache.Set(ctx, subId1, entry1)
	cache.RepublishingCache.Set(ctx, subId2, entry2)
	cache.RepublishingCache.Set(ctx, subId3, entry3)

	// Use mock SubscriptionCache because Hazelcast JSON round-trip fails for SubscriptionResource
	origSubCache := cache.SubscriptionCache
	mockSubCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = mockSubCache
	defer func() { cache.SubscriptionCache = origSubCache }()

	mockSubCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId1).Return(subResource1, nil)
	mockSubCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId2).Return((*resource.SubscriptionResource)(nil), nil)
	mockSubCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId3).Return(subResource3, nil)

	// Call the function under test
	checkRepublishingEntries()

	// Wait briefly for async goroutines to complete
	time.Sleep(500 * time.Millisecond)

	// Assertions
	mu.Lock()
	defer mu.Unlock()

	// sub2 should have been deleted from republishing cache (nil subscription path)
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, subId2))

	// sub1 and sub3 should have been handled (goroutine was launched)
	assertions.Contains(handledSubscriptionIds, subId1)
	assertions.Contains(handledSubscriptionIds, subId3)

	// sub1 and sub3 should still exist in republishing cache (mock doesn't delete them)
	assertions.True(cache.RepublishingCache.ContainsKey(ctx, subId1))
	assertions.True(cache.RepublishingCache.ContainsKey(ctx, subId3))
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
