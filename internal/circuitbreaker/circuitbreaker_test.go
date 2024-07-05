package circuitbreaker

import (
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/message"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/test"
	"testing"
)

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
