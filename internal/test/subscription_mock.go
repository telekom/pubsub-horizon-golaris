package test

import (
	"eni.telekom.de/horizon2go/pkg/resource"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/stretchr/testify/mock"
)

type SubscriptionMockCache struct {
	mock.Mock
}

func (m *SubscriptionMockCache) Get(cacheName string, key string) (*resource.SubscriptionResource, error) {
	args := m.Called(cacheName, key)
	return args.Get(0).(*resource.SubscriptionResource), args.Error(1)
}

func (m *SubscriptionMockCache) Put(cacheName string, key string, value resource.SubscriptionResource) error {
	args := m.Called(cacheName, key, value)
	return args.Error(0)
}

func (m *SubscriptionMockCache) Delete(cacheName string, key string) error {
	args := m.Called(cacheName, key)
	return args.Error(0)
}

func (m *SubscriptionMockCache) GetQuery(cacheName string, query predicate.Predicate) ([]resource.SubscriptionResource, error) {
	args := m.Called(cacheName, query)
	return args.Get(0).([]resource.SubscriptionResource), args.Error(1)
}
