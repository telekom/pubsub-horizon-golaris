// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/stretchr/testify/mock"
	c "github.com/telekom/pubsub-horizon-go/cache"
	"github.com/telekom/pubsub-horizon-go/resource"
)

type SubscriptionMockCache struct {
	mock.Mock
}

func (m *SubscriptionMockCache) GetClient() *hazelcast.Client {
	args := m.Called()
	return args.Get(0).(*hazelcast.Client)
}

func (m *SubscriptionMockCache) GetMap(mapKey string) (*hazelcast.Map, error) {
	args := m.Called(mapKey)
	return args.Get(0).(*hazelcast.Map), args.Error(1)
}

func (m *SubscriptionMockCache) AddListener(mapName string, listener c.Listener[resource.SubscriptionResource]) error {
	args := m.Called(mapName, listener)
	return args.Error(0)
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
