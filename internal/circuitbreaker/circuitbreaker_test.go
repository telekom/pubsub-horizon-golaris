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

func TestHandleOpenCircuitBreaker_SuccessWithoutHealthCheckEntry(t *testing.T) {
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
	assertions.Equal(testSubscriptionId, republishingCacheEntry.(republish.RepublishingCacheEntry).SubscriptionId)

	// Check if health check cache entry is not locked
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestHandleOpenCircuitBreaker_SuccessWithExistingHealthCheckEntry(t *testing.T) {
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
	assertions.Equal(testSubscriptionId, republishingCacheEntry.(republish.RepublishingCacheEntry).SubscriptionId)

	// Check if health check cache entry is not locked
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestHandleOpenCircuitBreaker_WithConsumerUnhealthy(t *testing.T) {
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
		hcData.HealthCheckEntry.LastCheckedStatus = 500
		return nil
	}

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if cb entry is still open
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusOpen, circuitBreakerCacheEntry.Status)

	// Check if there  was no republishing entry created
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.Nil(republishingCacheEntry)

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

	// Create  health check entry that  provokes a cool down and no republishing because of the last http status
	hcData, _ := healthcheck.PrepareHealthCheck(testSubscriptionResource)
	cache.HealthCheckCache.Unlock(hcData.Ctx, testHealthCheckKey)
	hcData.HealthCheckEntry.LastChecked = time.Now() // provokes cool down
	hcData.HealthCheckEntry.LastCheckedStatus = 500  // last http status unhealthy, no republishing
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

	// Create  health check entry that  provokes a cool down and a republishing because of  the last http status
	hcData, _ := healthcheck.PrepareHealthCheck(testSubscriptionResource)
	cache.HealthCheckCache.Unlock(hcData.Ctx, testHealthCheckKey)
	hcData.HealthCheckEntry.LastChecked = time.Now() // provokes cool down
	hcData.HealthCheckEntry.LastCheckedStatus = 200  // last http status healthy, provokes republishing
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
	assertions.Equal(testSubscriptionId, republishingCacheEntry.(republish.RepublishingCacheEntry).SubscriptionId)

	// Check if health check cache entry is not locked
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestHandleOpenCircuitBreaker_IncreaseLoopCounterWithinLoopDetectionPeriod(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	config.Current.CircuitBreaker.OpenLoopDetectionPeriod = 300 * time.Second

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"
	testLoopCounter := 0
	testLastOpened := types.NewTimestamp(time.Now()) // Within loop detection period, provokes loop counter increment

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)
	testCircuitBreakerMessage.LoopCounter = testLoopCounter
	testCircuitBreakerMessage.LastOpened = testLastOpened

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// Mock health check function
	healthCheckFunc = func(hcData *healthcheck.PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
		hcData.HealthCheckEntry.LastCheckedStatus = 200
		return nil
	}

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if loopCounter is incremented
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(testLoopCounter+1, circuitBreakerCacheEntry.LoopCounter)

	// Check if there is no postponed republishing entry
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.True(republishingCacheEntry.(republish.RepublishingCacheEntry).PostponedUntil.Before(time.Now()))
}

func TestHandleOpenCircuitBreaker_ResetLoopCounterOutsideLoopDetectionPeriod(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	config.Current.CircuitBreaker.OpenLoopDetectionPeriod = 0 * time.Second

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"
	testLoopCounter := 1
	testLastOpened := types.NewTimestamp(time.Now().Add(-60 * time.Second)) // Outside loop detection period, provokes loop counter reset

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)
	testCircuitBreakerMessage.LoopCounter = testLoopCounter
	testCircuitBreakerMessage.LastOpened = testLastOpened

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// Mock health check function
	healthCheckFunc = func(hcData *healthcheck.PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
		hcData.HealthCheckEntry.LastCheckedStatus = 200
		return nil
	}

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if loopCounter is reset
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(0, circuitBreakerCacheEntry.LoopCounter)
}

func TestHandleOpenCircuitBreaker_UnchangedLoopCounterWhenUnhealthy(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	config.Current.CircuitBreaker.OpenLoopDetectionPeriod = 600 * time.Second

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"
	testLoopCounter := 1
	testLastOpened := types.NewTimestamp(time.Now())

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)
	testCircuitBreakerMessage.LoopCounter = testLoopCounter
	testCircuitBreakerMessage.LastOpened = testLastOpened

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// Mock health check function
	healthCheckFunc = func(hcData *healthcheck.PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
		hcData.HealthCheckEntry.LastCheckedStatus = 500 // Unhealthy, provokes unchanged loop counter
		return nil
	}

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if loopCounter is not changed
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(1, circuitBreakerCacheEntry.LoopCounter)
}

func TestHandleOpenCircuitBreaker_RepublishingPostponedDueToBackoff(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	config.Current.CircuitBreaker.OpenLoopDetectionPeriod = 600 * time.Second

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"
	testLoopCounter := 13 // Provokes a postponed republishing entry
	testLastOpened := types.NewTimestamp(time.Now())

	testCircuitBreakerMessage := test.NewTestCbMessage(testSubscriptionId)
	testCircuitBreakerMessage.LoopCounter = testLoopCounter
	testCircuitBreakerMessage.LastOpened = testLastOpened

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// Mock health check function
	healthCheckFunc = func(hcData *healthcheck.PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
		hcData.HealthCheckEntry.LastCheckedStatus = 200
		return nil
	}

	// Set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// Call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// Check if there is a postponed republishing entry
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.True(republishingCacheEntry.(republish.RepublishingCacheEntry).PostponedUntil.After(time.Now()))
}

func TestForceDeleteRepublishingEntry_WithoutEntryToDelete(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCbMessage := test.NewTestCbMessage(testSubscriptionId)
	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	preparedHealthCheck, err := healthcheck.PrepareHealthCheck(testSubscriptionResource)

	err = forceDeleteRepublishingEntry(testCbMessage, preparedHealthCheck)

	// assert the result
	assertions.NoError(err)
}

func TestForceDeleteRepublishingEntry_WithEntryToDelete(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCbMessage := test.NewTestCbMessage(testSubscriptionId)
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCbMessage)

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	republishingCacheEntry := republish.RepublishingCacheEntry{SubscriptionId: testCbMessage.SubscriptionId, RepublishingUpTo: time.Now()}
	err := cache.RepublishingCache.Set(context.Background(), testCbMessage.SubscriptionId, republishingCacheEntry)

	preparedHealthCheck, err := healthcheck.PrepareHealthCheck(testSubscriptionResource)

	// call the function under test
	err = forceDeleteRepublishingEntry(testCbMessage, preparedHealthCheck)

	// assert the result
	assertions.NoError(err)
	assertions.Nil(cache.RepublishingCache.Get(context.Background(), testSubscriptionId))
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

func TestCheckForCircuitBreakerLoop_WithinLoopDetectionPeriod(t *testing.T) {
	// Prepare test data
	config.Current.CircuitBreaker.OpenLoopDetectionPeriod = 10 * time.Second
	initialLastOpened := time.Now().Add(-5 * time.Second) // Within loop detection period
	initialLastModified := initialLastOpened

	testCbMessage := test.NewTestCbMessage("testSubscriptionId")
	testCbMessage.LastOpened = types.NewTimestamp(initialLastOpened)
	testCbMessage.LastModified = types.NewTimestamp(initialLastModified)
	testCbMessage.LoopCounter = 0

	// call the function under test
	err := checkForCircuitBreakerLoop(&testCbMessage)

	// assert the result
	assert.NoError(t, err)
	assert.Equal(t, 1, testCbMessage.LoopCounter, "LoopCounter should be incremented")
	assert.True(t, testCbMessage.LastModified.ToTime().After(initialLastModified), "LastOpened should be updated to LastModified")
	assert.Equal(t, testCbMessage.LastOpened, types.NewTimestamp(initialLastModified), "LastModified should be updated afterwards")

}

func TestCheckForCircuitBreakerLoop_OutsideLoopDetectionPeriod(t *testing.T) {
	// Prepare test data
	config.Current.CircuitBreaker.OpenLoopDetectionPeriod = 10 * time.Second
	initialLastOpened := time.Now().Add(-15 * time.Second) // Outside loop detection period
	initialLastModified := initialLastOpened

	testCbMessage := test.NewTestCbMessage("testSubscriptionId")
	testCbMessage.LastOpened = types.NewTimestamp(initialLastOpened)
	testCbMessage.LastModified = types.NewTimestamp(initialLastModified)
	testCbMessage.LoopCounter = 0

	// call the function under test
	err := checkForCircuitBreakerLoop(&testCbMessage)

	// assert the result
	assert.NoError(t, err)
	assert.Equal(t, 0, testCbMessage.LoopCounter, "LoopCounter should be reset to 0")
	assert.Equal(t, testCbMessage.LastOpened, types.NewTimestamp(initialLastModified), "LastOpened should be updated to LastModified")
	assert.True(t, testCbMessage.LastModified.ToTime().After(initialLastModified), "LastModified should be updated afterwards")
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
			name:            "First open, because loop counter is incremented before, no backoff",
			cbLoopCounter:   1,
			expectedBackoff: 0,
		},
		{
			name:            "First retry, base backoff = 2^1 * 1000ms",
			cbLoopCounter:   2,
			expectedBackoff: 2 * backoffBase,
		},
		{
			name:            "Second retry, exponential backoff = 2^2 * 1000ms",
			cbLoopCounter:   3,
			expectedBackoff: 4 * backoffBase,
		},
		{
			name:            "Third retry, exponential backoff = 2^3 * 1000ms",
			cbLoopCounter:   4,
			expectedBackoff: 8 * backoffBase,
		},
		{
			name:            "Max backoff reached exponential backoff  = 2^12 * 1000ms",
			cbLoopCounter:   13,
			expectedBackoff: backoffMax,
		},
		{
			name:            "Max backoff 44 reached exponential backoff  = 2^43 * 1000ms",
			cbLoopCounter:   44,
			expectedBackoff: backoffMax,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cbMessage := message.CircuitBreakerMessage{
				LoopCounter: tc.cbLoopCounter,
			}
			backoff := calculateExponentialBackoff(cbMessage)
			log.Debug().Msgf("Backoff: %d", backoff)
			assert.True(t, tc.expectedBackoff == backoff)
		})
	}
}
