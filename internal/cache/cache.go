// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	c "github.com/telekom/pubsub-horizon-go/cache"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/util"
	"golaris/internal/config"
)

var SubscriptionCache c.HazelcastBasedCache[resource.SubscriptionResource]
var CircuitBreakerCache c.HazelcastBasedCache[message.CircuitBreakerMessage]
var HealthCheckCache *hazelcast.Map
var RepublishingCache *hazelcast.Map
var hazelcastClient *hazelcast.Client

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
func initializeCaches(hzConfig hazelcast.Config) error {
	var err error

	hazelcastClient, err = hazelcast.StartNewClientWithConfig(context.Background(), hzConfig)
	if err != nil {
		return fmt.Errorf("error initializing Hazelcast client: %v", err)
	}

	SubscriptionCache = c.NewHazelcastCacheWithClient[resource.SubscriptionResource](hazelcastClient)
	CircuitBreakerCache = c.NewHazelcastCacheWithClient[message.CircuitBreakerMessage](hazelcastClient)

	HealthCheckCache, err = hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.HealthCheckCache)
	if err != nil {
		return fmt.Errorf("error initializing HealthCheckCache: %v", err)
	}

	RepublishingCache, err = hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.RepublishingCache)
	if err != nil {
		return fmt.Errorf("error initializing RebublishingCache: %v", err)
	}

	return nil
}
