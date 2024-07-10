package listener

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/republish"
	"golaris/internal/test"
	"testing"
	"time"
)

func setupMocks(subscriptionId string, oldSubscription *resource.SubscriptionResource) {
	// Mock of RepublishingMockMap
	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap

	republishMockMap.On("Get", mock.Anything, subscriptionId).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Set", mock.Anything, subscriptionId, mock.Anything).Return(nil)

	// Mock of HealthCheckMap
	healthMockMap := new(test.HealthCheckMockMap)
	cache.HealthChecks = healthMockMap
	healthMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)

	// Mock of CircuitBreakerCache
	circuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakers = circuitBreakerCache
	config.Current.Hazelcast.Caches.CircuitBreakerCache = "test-circuit-breaker-cache"

	openCBMessage := &message.CircuitBreakerMessage{
		SubscriptionId:    subscriptionId,
		Status:            enum.CircuitBreakerStatusOpen,
		RepublishingCount: 0,
	}

	circuitBreakerCache.On("Get", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId).Return(openCBMessage, nil)
	circuitBreakerCache.On("Put", config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, mock.Anything).Return(nil)
}

func createSubscriptionResource(subscriptionId, deliveryType string, circuitBreaker bool) *resource.SubscriptionResource {
	return &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:       subscriptionId,
				DeliveryType:         enum.DeliveryType(deliveryType),
				CircuitBreakerOptOut: circuitBreaker,
			},
		},
	}
}

func Test_InitializeListener(t *testing.T) {
	listener := &SubscriptionListener{}

	mockSubscriptionCache := new(test.SubscriptionMockCache)
	cache.Subscriptions = mockSubscriptionCache
	config.Current.Hazelcast.Caches.SubscriptionCache = "test-subscription-cache"

	mockSubscriptionCache.On("AddListener", "test-subscription-cache", listener).Return(nil)

	Initialize()
}

// TestSubscriptionListener_OnUpdate tests the OnUpdate method of the SubscriptionListener
// It tests if the SubscriptionListener correctly sets the SubscriptionCancelMap and cancels the goroutine
// The SubscriptionCancelMap should be called, if the deliveryType change!
func TestSubscriptionListener_OnUpdate_DeliveryTypeToSSE(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false)
	newSubscription := createSubscriptionResource(subscriptionId, "sse", false)

	setupMocks(subscriptionId, oldSubscription)

	// Channel to signal that the goroutine has finished
	done := make(chan struct{})

	iterations := 0
	go func() {
		// Close the channel when the goroutine is finished
		defer close(done)
		// Simulate a long-running goroutine ( for example, publishing events)
		for i := 1; i <= 50; i++ {
			time.Sleep(1 * time.Millisecond)

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
		assert.NotEqual(t, 50, iterations)
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Goroutine did not exit within expected time")
	}
}

func TestSubscriptionListener_OnUpdate_DeliveryTypeToCallback(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "sse", false)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false)

	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap
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
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", false)
	oldSubscription.Spec.Subscription.Callback = "http://old-callback-url"
	newSubscription.Spec.Subscription.Callback = "http://new-callback-url"

	// Channel to signal that the goroutine has finished
	done := make(chan struct{})

	// Simulate a long-running goroutine
	go func() {
		// Close the channel when the goroutine is finished
		defer close(done)

		// Simulate processing (e.g., publishing events)
		for i := 1; i <= 50; i++ {
			time.Sleep(1 * time.Millisecond)

			cache.CancelMapMutex.Lock()
			if cache.SubscriptionCancelMap[subscriptionId] {
				cache.CancelMapMutex.Unlock()
				return
			}
			cache.CancelMapMutex.Unlock()
		}
	}()

	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap
	republishMockMap.On("Get", mock.Anything, subscriptionId, mock.Anything).Return(oldSubscription, nil)
	republishMockMap.On("IsLocked", mock.Anything, subscriptionId).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, subscriptionId).Return(nil)
	republishMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)

	republishMockMap.On("Set", mock.Anything, subscriptionId, republish.RepublishingCache{
		SubscriptionId: subscriptionId,
	}).Return(nil)

	listener := &SubscriptionListener{}
	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	assert.True(t, cache.SubscriptionCancelMap[subscriptionId])
	republishMockMap.AssertNumberOfCalls(t, "Set", 1)
}

func TestSubscriptionListener_OnUpdate_CircuitBreakerOptOut(t *testing.T) {
	subscriptionId := "test-subscription-id"
	oldSubscription := createSubscriptionResource(subscriptionId, "callback", false)
	newSubscription := createSubscriptionResource(subscriptionId, "callback", true)

	circuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakers = circuitBreakerCache
	config.Current.Hazelcast.Caches.CircuitBreakerCache = "test-circuit-breaker-cache"

	openCBMessage := &message.CircuitBreakerMessage{
		SubscriptionId:    subscriptionId,
		Status:            enum.CircuitBreakerStatusOpen,
		RepublishingCount: 0,
	}
	circuitBreakerCache.On("Get", "test-circuit-breaker-cache", subscriptionId).Return(openCBMessage, nil)
	circuitBreakerCache.On("Put", "test-circuit-breaker-cache", subscriptionId, mock.Anything).Return(nil)

	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap

	republishMockMap.On("Set", mock.Anything, subscriptionId, republish.RepublishingCache{
		SubscriptionId: subscriptionId,
	}).Return(nil)

	healthMockMap := new(test.HealthCheckMockMap)
	cache.HealthChecks = healthMockMap

	healthMockMap.On("Delete", mock.Anything, subscriptionId).Return(nil)

	listener := &SubscriptionListener{}

	listener.OnUpdate(&hazelcast.EntryNotified{}, *newSubscription, *oldSubscription)

	circuitBreakerCache.AssertNumberOfCalls(t, "Get", 1)
	circuitBreakerCache.AssertNumberOfCalls(t, "Put", 1)
	republishMockMap.AssertNumberOfCalls(t, "Set", 1)
	healthMockMap.AssertNumberOfCalls(t, "Delete", 1)
}

func TestSubscriptionListener_OnDelete(t *testing.T) {
	subscriptionId := "test-subscription-id"
	key := subscriptionId

	done := make(chan struct{})

	iterations := 0
	go func() {
		// Close the channel when the goroutine is finished
		defer close(done)
		// Simulate a long-running goroutine ( for example, publishing events)
		for i := 1; i <= 50; i++ {
			time.Sleep(1 * time.Millisecond)

			cache.CancelMapMutex.Lock()
			if cache.SubscriptionCancelMap[subscriptionId] {
				cache.CancelMapMutex.Unlock()
				return
			}
			cache.CancelMapMutex.Unlock()

			iterations++
		}
	}()

	republishMockMap := new(test.RepublishingMockMap)
	cache.RepublishingCache = republishMockMap

	mockEntry := &republish.RepublishingCache{SubscriptionId: key}

	republishMockMap.On("Get", mock.Anything, key).Return(mockEntry, nil)
	republishMockMap.On("IsLocked", mock.Anything, key).Return(true, nil)
	republishMockMap.On("ForceUnlock", mock.Anything, key).Return(nil)
	republishMockMap.On("Delete", mock.Anything, key).Return(nil)

	cache.SubscriptionCancelMap = make(map[string]bool)

	event := &hazelcast.EntryNotified{Key: key}

	listener := &SubscriptionListener{}
	listener.OnDelete(event)

	assert.True(t, cache.SubscriptionCancelMap[key])
}
