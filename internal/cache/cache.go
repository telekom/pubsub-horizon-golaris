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
	"github.com/rs/zerolog/log"
	"golaris/internal/config"
)

var SubscriptionCache *c.Cache[resource.SubscriptionResource]
var CircuitBreakerCache *c.Cache[message.CircuitBreakerMessage]
var HealthCheckCache *hazelcast.Map
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

// initializeCaches sets up the Hazelcast caches used in the application.
// It takes a Hazelcast configuration object as a parameter.
// The function initializes the SubscriptionCache, CircuitBreakerCache, HealthCheckCache, and RepublishingCache.
func initializeCaches(config hazelcast.Config) error {
	var err error

	SubscriptionCache, err = c.NewCache[resource.SubscriptionResource](config)
	if err != nil {
		return fmt.Errorf("error initializing Hazelcast subscription health cache: %v", err)
	}

	CircuitBreakerCache, err = c.NewCache[message.CircuitBreakerMessage](config)
	if err != nil {
		return fmt.Errorf("error initializing CircuitBreakerCache: %v", err)
	}

	// TODO:
	// We should initialize the healthcheck cache similar to the other caches
	// For this we need to update the parent, as the interface currently does not support locking operations
	//HealthCheckCache, err = c.NewCache[healthcheck.HealthCheck](config)
	HealthCheckCache, err = newHealthCheckCache(config)
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
