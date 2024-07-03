// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	c "eni.telekom.de/horizon2go/pkg/cache"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"eni.telekom.de/horizon2go/pkg/util"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"golaris/internal/config"
)

type SubscriptionCache interface {
	Get(cacheName string, key string) (*resource.SubscriptionResource, error)
	Put(cacheName string, key string, value resource.SubscriptionResource) error
	Delete(cacheName string, key string) error
	GetQuery(cacheName string, query predicate.Predicate) ([]resource.SubscriptionResource, error)
}

type CircuitBreakerCache interface {
	Get(cacheName string, key string) (*message.CircuitBreakerMessage, error)
	Put(cacheName string, key string, value message.CircuitBreakerMessage) error
	Delete(cacheName string, key string) error
	GetQuery(cacheName string, query predicate.Predicate) ([]message.CircuitBreakerMessage, error)
}

var Subscriptions SubscriptionCache
var CircuitBreakers CircuitBreakerCache
var HealthChecks *hazelcast.Map
var RepublishingCache *hazelcast.Map

func Initialize() {
	c := createNewHazelcastConfig()
	err := initializeCaches(c)
	if err != nil {
		log.Panic().Err(err).Msg("error while initializing caches")
	}
}

func createNewHazelcastConfig() hazelcast.Config {
	cacheConfig := hazelcast.NewConfig()

	cacheConfig.Cluster.Name = config.Current.Hazelcast.ClusterName
	cacheConfig.Cluster.Network.SetAddresses(config.Current.Hazelcast.ServiceDNS)

	if config.Current.Hazelcast.CustomLoggerEnabled {
		cacheConfig.Logger.CustomLogger = new(util.HazelcastZerologLogger)
	}

	return cacheConfig
}

func initializeCaches(config hazelcast.Config) error {
	var err error

	Subscriptions, err = c.NewCache[resource.SubscriptionResource](config)
	if err != nil {
		return fmt.Errorf("error initializing Hazelcast subscription health cache: %v", err)
	}

	CircuitBreakers, err = c.NewCache[message.CircuitBreakerMessage](config)
	if err != nil {
		return fmt.Errorf("error initializing CircuitBreakerCache: %v", err)
	}

	// TODO:
	// We should initialize the healthcheck cache similar to the other caches
	// For this we need to update the parent, as the interface currently does not support locking operations
	//HealthChecks, err = c.NewCache[health_check.HealthCheck](config)
	HealthChecks, err = newHealthCheckCache(config)
	if err != nil {
		return fmt.Errorf("error initializing HealthCheckCache: %v", err)
	}

	RepublishingCache, err = newRepublishingCache(config)
	if err != nil {
		return fmt.Errorf("error initializing RebublishingCache: %v", err)
	}

	return nil
}

func newHealthCheckCache(hzConfig hazelcast.Config) (*hazelcast.Map, error) {
	hazelcastClient, err := hazelcast.StartNewClientWithConfig(context.Background(), hzConfig)
	if err != nil {
		return nil, err
	}

	m, err := hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.HealthCheckCache)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func newRepublishingCache(hzConfig hazelcast.Config) (*hazelcast.Map, error) {
	hazelcastClient, err := hazelcast.StartNewClientWithConfig(context.Background(), hzConfig)
	if err != nil {
		return nil, err
	}

	m, err := hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.RepublishingCache)
	if err != nil {
		return nil, err
	}

	return m, nil
}
