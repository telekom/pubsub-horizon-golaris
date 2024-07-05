package test

import (
	"encoding/json"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/mock"
	c "github.com/telekom/pubsub-horizon-go/cache"
	"github.com/telekom/pubsub-horizon-go/message"
)

type CircuitBreakerMockCache struct {
	mock.Mock
}

func (m *CircuitBreakerMockCache) Get(mapName string, key string) (*message.CircuitBreakerMessage, error) {
	args := m.Called(mapName, key)
	return args.Get(0).(*message.CircuitBreakerMessage), args.Error(1)
}

func (m *CircuitBreakerMockCache) GetQuery(cacheName string, query predicate.Predicate) ([]message.CircuitBreakerMessage, error) {
	args := m.Called(cacheName, query)
	return args.Get(0).([]message.CircuitBreakerMessage), args.Error(1)
}

func (m *CircuitBreakerMockCache) GetClient() *hazelcast.Client {
	args := m.Called()
	return args.Get(0).(*hazelcast.Client)
}

func (m *CircuitBreakerMockCache) GetMap(mapKey string) (*hazelcast.Map, error) {
	args := m.Called(mapKey)
	return args.Get(0).(*hazelcast.Map), args.Error(1)
}

func (m *CircuitBreakerMockCache) Put(cacheName string, key string, value message.CircuitBreakerMessage) error {
	args := m.Called(cacheName, key, value)
	return args.Error(0)
}

func (m *CircuitBreakerMockCache) Delete(cacheName string, key string) error {
	args := m.Called(cacheName, key)
	return args.Error(0)
}

func (m *CircuitBreakerMockCache) AddListener(mapName string, listener c.Listener[message.CircuitBreakerMessage]) error {
	args := m.Called(mapName, listener)
	return args.Error(0)
}

func (m *CircuitBreakerMockCache) unmarshalHazelcastJson(key any, value any, obj any) error {
	hzJsonValue, ok := value.(serialization.JSON)
	if !ok {
		return fmt.Errorf("value of cached object with key '%s' is not a HazelcastJsonValue", key)
	}
	return json.Unmarshal(hzJsonValue, obj)
}
