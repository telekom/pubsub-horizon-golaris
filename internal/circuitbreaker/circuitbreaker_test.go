// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuitbreaker

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/types"
	"os"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/healthcheck"
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

func TestHandleOpenCircuitBreaker_WithoutHealthCheckEntry(t *testing.T) {
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

func TestHandleOpenCircuitBreaker_WithExistingHealthCheckEntry(t *testing.T) {
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
	hcData.HealthCheckEntry.LastChecked = time.Now().Add(-time.Duration(config.Current.HealthCheck.CoolDownTime.Seconds() * float64(time.Second)))
	cache.HealthCheckCache.Set(context.Background(), testHealthCheckKey, hcData.HealthCheckEntry)

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

func TestHandleOpenCircuitBreaker_HealthCheckEntryAlreadyLocked(t *testing.T) {
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
	hcData, _ := healthcheck.PrepareHealthCheck(testSubscriptionResource)

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
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(hcData.Ctx, testHealthCheckKey)
	assertions.True(healthCheckCacheLocked)

	//Cleanup
	defer cache.HealthCheckCache.Unlock(hcData.Ctx, testHealthCheckKey)
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
	CloseCircuitBreaker(&testCbMessage)

	// assert the result
	cbMessage, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.NotNil(cbMessage)
	assertions.Equal(enum.CircuitBreakerStatusClosed, cbMessage.Status)
}

func TestCheckAndHandleCircuitBreakerLoop_WithinLoopDetectionPeriod(t *testing.T) {
	// Prepare test data
	config.Current.CircuitBreaker.OpenCbLoopDetectionPeriod = 10 * time.Second
	initialLastOpened := time.Now().Add(-5 * time.Second) // Within loop detection period

	testCbMessage := test.NewTestCbMessage("testSubscriptionId")
	testCbMessage.LastRepublished = types.NewTimestamp(initialLastOpened)
	testCbMessage.RepublishingCount = 0

	// call the function under test
	checkAndHandleCircuitBreakerLoop(&testCbMessage)

	// assert the result
	assert.Equal(t, 1, testCbMessage.RepublishingCount, "Republishing count should be incremented")
	assert.True(t, testCbMessage.LastRepublished.ToTime().After(initialLastOpened), "Last opened time should be updated")
}

func TestCheckAndHandleCircuitBreakerLoop_OutsideLoopDetectionPeriod(t *testing.T) {
	// Prepare test data
	config.Current.CircuitBreaker.OpenCbLoopDetectionPeriod = 10 * time.Second
	initialLastOpened := time.Now().Add(-15 * time.Second) // Outside loop detection period

	testCbMessage := test.NewTestCbMessage("testSubscriptionId")
	testCbMessage.LastRepublished = types.NewTimestamp(initialLastOpened)
	testCbMessage.RepublishingCount = 0

	// call the function under test
	checkAndHandleCircuitBreakerLoop(&testCbMessage)

	// assert the result
	assert.Equal(t, 0, testCbMessage.RepublishingCount, "Republishing count should be reset to 0")
	assert.True(t, testCbMessage.LastRepublished.ToTime().After(initialLastOpened), "Last opened time should be updated")
}

func TestCalculateExponentialBackoff(t *testing.T) {
	// Mock the configuration values for the test
	config.Current.CircuitBreaker.ExponentialBackoffBase = 1000 * time.Millisecond
	config.Current.CircuitBreaker.ExponentialBackoffMax = 60 * time.Minute

	backoffBase := config.Current.CircuitBreaker.ExponentialBackoffBase
	backoffMax := config.Current.CircuitBreaker.ExponentialBackoffMax

	tests := []struct {
		name            string
		cbLoopCounter   int
		expectedBackoff time.Duration
	}{
		{
			name:            "First open, no backoff",
			cbLoopCounter:   1,
			expectedBackoff: 0,
		},
		{
			name:            "First retry, base backoff = 2^1*backoffBase",
			cbLoopCounter:   2,
			expectedBackoff: 2 * backoffBase,
		},
		{
			name:            "Second retry, exponential backoff = 2^2*backoffBase",
			cbLoopCounter:   3,
			expectedBackoff: 4 * backoffBase,
		},
		{
			name:            "Third retry, exponential backoff = 2^3*backoffBase",
			cbLoopCounter:   4,
			expectedBackoff: 8 * backoffBase,
		},
		{
			name:            "Max backoff reached exponential backoff  = 2^12*backoffBase",
			cbLoopCounter:   13,
			expectedBackoff: backoffMax,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cbMessage := message.CircuitBreakerMessage{
				RepublishingCount: tc.cbLoopCounter,
			}
			backoff := calculateExponentialBackoff(cbMessage)
			log.Debug().Msgf("Backoff: %d", backoff)
			assert.True(t, tc.expectedBackoff == backoff)
		})
	}
}
