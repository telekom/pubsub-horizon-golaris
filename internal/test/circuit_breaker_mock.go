package test

import (
	"eni.telekom.de/horizon2go/pkg/message"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/stretchr/testify/mock"
)

type CircuitBreakerMockCache struct {
	mock.Mock
}

func (m *CircuitBreakerMockCache) Get(cacheName string, key string) (*message.CircuitBreakerMessage, error) {
	args := m.Called(cacheName, key)
	return args.Get(0).(*message.CircuitBreakerMessage), args.Error(1)
}

func (m *CircuitBreakerMockCache) Put(cacheName string, key string, value message.CircuitBreakerMessage) error {
	args := m.Called(cacheName, key, value)
	return args.Error(0)
}

func (m *CircuitBreakerMockCache) Delete(cacheName string, key string) error {
	args := m.Called(cacheName, key)
	return args.Error(0)
}

func (m *CircuitBreakerMockCache) GetQuery(cacheName string, query predicate.Predicate) ([]message.CircuitBreakerMessage, error) {
	args := m.Called(cacheName, query)
	return args.Get(0).([]message.CircuitBreakerMessage), args.Error(1)
}
