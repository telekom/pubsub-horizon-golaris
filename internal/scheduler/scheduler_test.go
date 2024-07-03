package scheduler

import (
	"eni.telekom.de/horizon2go/pkg/resource"
	"github.com/stretchr/testify/assert"
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
