package scheduler

import (
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/test"
	"testing"
)

func TestGetSubscription(t *testing.T) {
	mockCache := new(test.SubscriptionMockCache)
	cache.Subscriptions = mockCache
	config.Current.Hazelcast.Caches.SubscriptionCache = "testMap"

	expectedSubscription := &resource.SubscriptionResource{}
	mockCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").Return(expectedSubscription, nil)

	subscription := getSubscription("sub123")
	assert.NotNil(t, subscription)
	assert.Equal(t, expectedSubscription, subscription)
}
