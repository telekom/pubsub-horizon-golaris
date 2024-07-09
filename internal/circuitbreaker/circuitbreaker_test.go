// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuitbreaker

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
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
	config.Current = buildTestConfig()
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

	testCircuitBreakerMessage := newTestCbMessage(testSubscriptionId)

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

	testCircuitBreakerMessage := newTestCbMessage(testSubscriptionId)

	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testHealthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, getHttpMethod(testSubscriptionResource), testCallbackUrl)

	// mock health check function
	healthCheckFunc = func(hcData *healthcheck.PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
		hcData.HealthCheckEntry.LastCheckedStatus = 200
		return nil
	}

	// set mocked  circuit breaker message in the cache
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCircuitBreakerMessage)

	// call the function under test
	HandleOpenCircuitBreaker(testCircuitBreakerMessage, testSubscriptionResource)

	// assert the result
	circuitBreakerCacheEntry, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.Equal(enum.CircuitBreakerStatusClosed, circuitBreakerCacheEntry.Status)

	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assertions.Equal(testSubscriptionId, republishingCacheEntry.(republish.RepublishingCache).SubscriptionId)

	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assertions.False(healthCheckCacheLocked)
}

func TestPrepareHealthCheck_NewEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	healthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, getHttpMethod(testSubscriptionResource), testCallbackUrl)

	// call the function under test
	preparedHealthCheck, err := prepareHealthCheck(testSubscriptionResource)

	// assert the result
	assertions.NoError(err)
	assertions.NotNil(preparedHealthCheck)
	assertions.NotNil(preparedHealthCheck.Ctx)
	assertions.Equal(healthCheckKey, preparedHealthCheck.HealthCheckKey)
	assertions.True(preparedHealthCheck.IsAcquired)
	assertions.True(cache.HealthCheckCache.IsLocked(preparedHealthCheck.Ctx, preparedHealthCheck.HealthCheckKey))
}

func TestPrepareHealthCheck_ExistingEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test2.com"

	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	healthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, getHttpMethod(testSubscriptionResource), testCallbackUrl)

	healthCheckEntry := healthcheck.NewHealthCheckEntry(testSubscriptionResource, getHttpMethod(testSubscriptionResource))
	healthCheckEntry.LastChecked = time.Now()
	err := cache.HealthCheckCache.Set(context.Background(), healthCheckKey, healthCheckEntry)

	assertions.NoError(err)

	// call the function under test
	preparedHealthCheck, err := prepareHealthCheck(testSubscriptionResource)

	// assert the result
	assertions.NoError(err)
	assertions.NotNil(preparedHealthCheck)
	assertions.NotNil(preparedHealthCheck.Ctx)
	assertions.NotNil(preparedHealthCheck.HealthCheckEntry.LastChecked)
	assertions.Equal(healthCheckKey, preparedHealthCheck.HealthCheckKey)
	//assertions.True(preparedHealthCheck.IsAcquired)
	//assertions.True(cache.HealthCheckCache.IsLocked(preparedHealthCheck.Ctx, preparedHealthCheck.HealthCheckKey))
}

func TestDeleteRepubEntryAndIncreaseRepubCount_NoEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCbMessage := newTestCbMessage(testSubscriptionId)
	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	// call the function under test
	preparedHealthCheck, err := prepareHealthCheck(testSubscriptionResource)

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

	testCbMessage := newTestCbMessage(testSubscriptionId)
	cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId, testCbMessage)

	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	republishingCacheEntry := republish.RepublishingCache{SubscriptionId: testCbMessage.SubscriptionId, RepublishingUpTo: time.Now()}
	err := cache.RepublishingCache.Set(context.Background(), testCbMessage.SubscriptionId, republishingCacheEntry)

	preparedHealthCheck, err := prepareHealthCheck(testSubscriptionResource)

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
	testCbMessage := newTestCbMessage(testSubscriptionId)

	// call the function under test
	CloseCircuitBreaker(testCbMessage)

	// assert the result
	cbMessage, _ := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, testSubscriptionId)
	assertions.NotNil(cbMessage)
	assertions.Equal(enum.CircuitBreakerStatusClosed, cbMessage.Status)
}

func TestGetHttpMethod_GetMethod(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testSubscriptionResource.Spec.Subscription.EnforceGetHealthCheck = true

	// call the function under test
	httpMethod := getHttpMethod(testSubscriptionResource)

	// assert the result
	assertions.Equal("GET", httpMethod)
}

func TestGetHttpMethod_HeadMethod(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := newTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testSubscriptionResource.Spec.Subscription.EnforceGetHealthCheck = false

	// call the function under test
	httpMethod := getHttpMethod(testSubscriptionResource)

	// assert the result
	assertions.Equal("HEAD", httpMethod)
}

func newTestSubscriptionResource(testSubscriptionId string, testCallbackUrl string, testEnvironment string) *resource.SubscriptionResource {
	testSubscriptionResource := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:        testSubscriptionId,
				Callback:              testCallbackUrl,
				EnforceGetHealthCheck: false,
			},
			Environment: testEnvironment,
		},
	}
	return testSubscriptionResource
}

func newTestCbMessage(testSubscriptionId string) message.CircuitBreakerMessage {
	testCircuitBreakerMessage := message.CircuitBreakerMessage{
		SubscriptionId:    testSubscriptionId,
		Status:            enum.CircuitBreakerStatusOpen,
		RepublishingCount: 0,
		LastRepublished:   time.Now(),
		LastModified:      time.Now(),
	}
	return testCircuitBreakerMessage
}

func buildTestConfig() config.Configuration {

	return config.Configuration{
		LogLevel: "debug",
		Port:     8080,
		CircuitBreaker: config.CircuitBreaker{
			OpenCbCheckInterval: 30 * time.Second,
		},
		HealthCheck: config.HealthCheck{
			SuccessfulResponseCodes: []int{200, 201, 202},
			CoolDownTime:            30 * time.Second,
		},
		Republishing: config.Republishing{
			CheckInterval: 10 * time.Second,
			BatchSize:     100,
		},
		Hazelcast: config.Hazelcast{
			ServiceDNS:  test.EnvOrDefault("HAZELCAST_HOST", "localhost"),
			ClusterName: "dev",
			Caches: config.Caches{
				SubscriptionCache:   "subCache",
				CircuitBreakerCache: "cbCache",
				HealthCheckCache:    "hcCache",
				RepublishingCache:   "repCache",
			},
			CustomLoggerEnabled: false,
		},
		Kafka: config.Kafka{
			Brokers: []string{"broker1:9092", "broker2:9092"},
			Topics:  []string{"testTopic"},
		},
		Mongo: config.Mongo{
			Url:        "mongodb://localhost:27017",
			Database:   "mydatabase",
			Collection: "mycollection",
			BulkSize:   10,
		},
		Security: config.Security{
			Enabled:      true,
			Url:          "https://security.local",
			ClientId:     "my-client-id",
			ClientSecret: "my-client-secret",
		},
		Tracing: config.Tracing{
			CollectorEndpoint: "http://tracing.local/collect",
			Https:             true,
			DebugEnabled:      false,
			Enabled:           true,
		},
		Kubernetes: config.Kubernetes{
			Namespace: "default",
		},
		MockCbSubscriptionId: "mock-sub-id-123",
	}
}
