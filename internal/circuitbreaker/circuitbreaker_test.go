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
	mockCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = mockCache
	config.Current.Hazelcast.Caches.CircuitBreakerCache = "circuit_breaker_test_cache"

	subscriptionId := "sub123"
	initialMessage := &message.CircuitBreakerMessage{RepublishingCount: 1}
	updatedMessage := &message.CircuitBreakerMessage{RepublishingCount: 2}

	mockCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(initialMessage, nil)
	mockCache.On("Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, *updatedMessage).Return(nil)

	result, err := IncreaseRepublishingCount(subscriptionId)

	assert.NoError(t, err)
	assert.Equal(t, updatedMessage, result)
	assert.Equal(t, 2, result.RepublishingCount)
}

func TestHandleOpenCircuitBreaker_Success(t *testing.T) {

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testCircuitBreakerMessage := message.CircuitBreakerMessage{
		SubscriptionId:    testSubscriptionId,
		Status:            "OPEN",
		RepublishingCount: 0,
		LastRepublished:   time.Now(),
		LastModified:      time.Now(),
	}

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
	assert.True(t, circuitBreakerCacheEntry.Status == enum.CircuitBreakerStatusCooldown)
	republishingCacheEntry, _ := cache.RepublishingCache.Get(context.Background(), testSubscriptionId)
	assert.True(t, republishingCacheEntry.(republish.RepublishingCache).SubscriptionId == testSubscriptionId)
	healthCheckCacheLocked, _ := cache.HealthCheckCache.IsLocked(context.Background(), testHealthCheckKey)
	assert.False(t, healthCheckCacheLocked)
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
			ServiceDNS:  "localhost",
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
