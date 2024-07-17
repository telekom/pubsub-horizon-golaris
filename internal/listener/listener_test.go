package listener

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/republish"
	"pubsub-horizon-golaris/internal/test"
	"testing"
	"time"
)

func createSubscriptionResource(subscriptionId, deliveryType string, circuitBreaker bool, callbackUrl string) *resource.SubscriptionResource {
	return &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:       subscriptionId,
				DeliveryType:         enum.DeliveryType(deliveryType),
				CircuitBreakerOptOut: circuitBreaker,
				Callback:             callbackUrl,
			},
		},
	}
}

func setupMocks() (*test.RepublishingMockMap, *test.HealthCheckMockMap, *test.CircuitBreakerMockCache) {
	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap

	healthMockMap := new(test.HealthCheckMockMap)
	cache.HealthCheckCache = healthMockMap

	circuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = circuitBreakerCache
	config.Current.Hazelcast.Caches.CircuitBreakerCache = "test-circuit-breaker-cache"

	return republishMockMap, healthMockMap, circuitBreakerCache

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
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "")
	newSubscription := createSubscriptionResource(subscriptionId, "sse", false, "")

	republishMockMap, healthMockMap, circuitBreakerCache := setupMocks()
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Set", mock.Anything, subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId}).Return(nil)

	healthMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)

	openCBMessage := &message.CircuitBreakerMessage{
		SubscriptionId:    subscriptionId,
		Status:            enum.CircuitBreakerStatusOpen,
		RepublishingCount: 0,
	}
	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(openCBMessage, nil)
	circuitBreakerCache.On("Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything).Return(nil)

	// Channel to signal that the goroutine has finished
	done := make(chan struct{})
	iterations := 0
	go func() {
		// Close the channel when the goroutine is finished
		defer close(done)
		// Simulate a long-running goroutine ( for example, publishing events)
		for i := 1; i <= 100000; i++ {
			time.Sleep(1 * time.Nanosecond)

			cache.CancelMapMutex.Lock()
			if cache.SubscriptionCancelMap[subscriptionId] {
				cache.CancelMapMutex.Unlock()
				return
			}
			cache.CancelMapMutex.Unlock()

			iterations++
		}
	}()

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	assert.True(t, cache.SubscriptionCancelMap[subscriptionId])

	select {
	case <-done:
		t.Logf("Number of iterations completed: %d", iterations)
		assert.NotEqual(t, 0, iterations)
		assert.NotEqual(t, 10000, iterations)
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Goroutine did not exit within expected time")
	}

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId})
	healthMockMap.AssertCalled(t, "Delete", mock.Anything, subscriptionId)
	circuitBreakerCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	circuitBreakerCache.AssertCalled(t, "Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything)
}

func TestSubscriptionListener_OnUpdate_DeliveryTypeToCallback(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "sse", false, "")
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false, "")

	republishMockMap, _, _ := setupMocks()
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.Anything).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, republish.RepublishingCache{
		SubscriptionId:  subscriptionId,
		OldDeliveryType: string(oldSubscription.Spec.Subscription.DeliveryType),
	})
}

func TestSubscriptionListener_OnUpdate_CallbackUrl(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "http://old-callback-url")
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false, "http://new-callback-url")

	republishMockMap, _, _ := setupMocks()
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Set", mock.Anything, subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId}).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	assert.True(t, cache.SubscriptionCancelMap[subscriptionId])

	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId})
}

func TestSubscriptionListener_OnUpdate_CircuitBreakerOptOut(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false, "")
	newSubscription := createSubscriptionResource(subscriptionId, "callback", true, "")

	republishMockMap, healthMockMap, circuitBreakerCache := setupMocks()

	openCBMessage := &message.CircuitBreakerMessage{
		SubscriptionId:    subscriptionId,
		Status:            enum.CircuitBreakerStatusOpen,
		RepublishingCount: 0,
	}
	circuitBreakerCache.On("Get", "test-circuit-breaker-cache", subscriptionId).Return(openCBMessage, nil)
	circuitBreakerCache.On("Put", "test-circuit-breaker-cache", subscriptionId, mock.Anything).Return(nil)

	republishMockMap.On("Set", mock.Anything, subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId}).Return(nil)

	healthMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	circuitBreakerCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	circuitBreakerCache.AssertCalled(t, "Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything)
	republishMockMap.AssertCalled(t, "Set", mock.Anything, subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId})
	healthMockMap.AssertCalled(t, "Delete", mock.Anything, subscriptionId)
}

func TestSubscriptionListener_OnDelete(t *testing.T) {
	subscriptionId := "test-subscription-id"

	republishMockMap, _, _ := setupMocks()
	mockEntry := &republish.RepublishingCache{SubscriptionId: subscriptionId}
	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(mockEntry, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)

	event := &hazelcast.EntryNotified{Key: subscriptionId}
	listener := &SubscriptionListener{}
	listener.OnDelete(event)

	assert.True(t, cache.SubscriptionCancelMap[subscriptionId])

	republishMockMap.AssertCalled(t, "Get", mock.Anything, subscriptionId)
	republishMockMap.AssertCalled(t, "Delete", mock.Anything, subscriptionId)
}
