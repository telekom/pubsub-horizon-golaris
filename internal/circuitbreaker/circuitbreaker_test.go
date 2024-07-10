// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuitbreaker

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/healthcheck"
	"golaris/internal/republish"
	"golaris/internal/test"
	"os"
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

func TestIncreaseRepublishingCount_Success(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)

	// set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	result, err := IncreaseRepublishingCount(testSubscriptionId)

	assertions.NoError(err)
	assertions.Equal(1, result.RepublishingCount)
}

func TestHandleOpenCircuitBreaker_Success(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testHealthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, healthcheck.GetHttpMethod(testSubscriptionResource), testCallbackUrl)

	// Mock health check function
	healthCheckFunc = func(hcData *healthcheck.PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
		hcData.HealthCheckEntry.LastCheckedStatus = 200
		return nil
	}

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if cb entry is closed
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusClosed, circuitBreakerCacheEntry.Status)

	// Check if there is a new republishing entry
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.Equal(testSubscriptionId, republishingCacheEntry.(republish.RepublishingCache).SubscriptionId)

	// Check if health check cache entry is not locked
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestHandleOpenCircuitBreaker_AlreadyLocked(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testHealthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, healthcheck.GetHttpMethod(testSubscriptionResource), testCallbackUrl)

	// Create  locked health check entry
	healthcheck.PrepareHealthCheck(testSubscriptionResource)

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if cb entry is open furthermore
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusOpen, circuitBreakerCacheEntry.Status)

	// Check if there  was no republishing entry created
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.Nil(republishingCacheEntry)

	// Check if health check cache entry is locked furthermore
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.True(healthCheckCacheLocked)

	//Cleanup
	defer cache.HealthCheckCache.ForceUnlock(context.Background(), testHealthCheckKey)
}

func TestHandleOpenCircuitBreaker_CoolDownWithoutRepublishing(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testHealthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, healthcheck.GetHttpMethod(testSubscriptionResource), testCallbackUrl)

	// Create  health check entry that  provokes a cool down and no republishing because of lst http status
	hcData, _ := healthcheck.PrepareHealthCheck(testSubscriptionResource)
	cache.HealthCheckCache.Unlock(hcData.Ctx, testHealthCheckKey)
	hcData.HealthCheckEntry.LastChecked = time.Now()
	hcData.HealthCheckEntry.LastCheckedStatus = 500
	cache.HealthCheckCache.Set(context.Background(), testHealthCheckKey, hcData.HealthCheckEntry)

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if cb entry is open
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusOpen, circuitBreakerCacheEntry.Status)

	// Check if there is no republishing entry
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.Nil(republishingCacheEntry)

	// Check if health check cache entry is not locked
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestHandleOpenCircuitBreaker_CoolDownWithRepublishing(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testHealthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, healthcheck.GetHttpMethod(testSubscriptionResource), testCallbackUrl)

	// Create  health check entry that  provokes a cool down and no republishing because of lst http status
	hcData, _ := healthcheck.PrepareHealthCheck(testSubscriptionResource)
	cache.HealthCheckCache.Unlock(hcData.Ctx, testHealthCheckKey)
	hcData.HealthCheckEntry.LastChecked = time.Now()
	hcData.HealthCheckEntry.LastCheckedStatus = 200
	cache.HealthCheckCache.Set(context.Background(), testHealthCheckKey, hcData.HealthCheckEntry)

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if cb entry is  closed
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusClosed, circuitBreakerCacheEntry.Status)

	// Check if there is a new republishing entry
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.Equal(testSubscriptionId, republishingCacheEntry.(republish.RepublishingCache).SubscriptionId)

	// Check if health check cache entry is not locked
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestDeleteRepubEntryAndIncreaseRepubCount_NoEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCbMessage := test.NewTestCbMessage(testSubscriptionId)
	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// call the function under test
	preparedHealthCheck, err := healthcheck.PrepareHealthCheck(testSubscriptionResource)

	cbMessageAfterDeletion, err := deleteRepubEntryAndIncreaseRepubCount(testCbMessage, preparedHealthCheck)

	// assert the result
	assertions.NoError(err)
	assertions.NotNil(cbMessageAfterDeletion)
	assertions.Equal(0, cbMessageAfterDeletion.RepublishingCount)
}

func TestDeleteRepubEntryAndIncreaseRepubCount_ExistingEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCbMessage := test.NewTestCbMessage(testSubscriptionId)
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCbMessage)

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	republishingCacheEntry := republish.RepublishingCache{SubscriptionId: testCbMessage.SubscriptionId, RepublishingUpTo: time.Now()}
	err := cache.RepublishingCache.Set(context.Background(), testCbMessage.SubscriptionId, republishingCacheEntry)

	preparedHealthCheck, err := healthcheck.PrepareHealthCheck(testSubscriptionResource)

	// call the function under test
	cbMessageAfterRepubEntryDeletion, err := deleteRepubEntryAndIncreaseRepubCount(testCbMessage, preparedHealthCheck)

	// assert the result
	assertions.NoError(err)
	assertions.Nil(cache.RepublishingCache.Get(context.Background(), testSubscriptionId))
	assertions.Equal(1, cbMessageAfterRepubEntryDeletion.RepublishingCount)
	assertions.NotNil(cbMessageAfterRepubEntryDeletion)
}

func TestCloseCircuitBreaker(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testCbMessage := test.NewTestCbMessage(testSubscriptionId)

	// call the function under test
	CloseCircuitBreaker(testCbMessage)

	// assert the result
	cbMessage, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.NotNil(cbMessage)
	assertions.Equal(enum.CircuitBreakerStatusClosed, cbMessage.Status)
}
