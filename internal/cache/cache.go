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

var Subscriptions c.HazelcastBasedCache[resource.SubscriptionResource]
var CircuitBreakers c.HazelcastBasedCache[message.CircuitBreakerMessage]
var HealthChecks *hazelcast.Map
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

func initializeCaches(hzConfig hazelcast.Config) error {
	var err error

	Subscriptions, err = c.NewCache[resource.SubscriptionResource](hzConfig)
	if err != nil {
		return fmt.Errorf("error initializing Hazelcast subscription health cache: %v", err)
	}

	CircuitBreakers, err = c.NewCache[message.CircuitBreakerMessage](hzConfig)
	if err != nil {
		return fmt.Errorf("error initializing CircuitBreakerCache: %v", err)
	}

	hazelcastClient, err = hazelcast.StartNewClientWithConfig(context.Background(), hzConfig)
	if err != nil {
		return fmt.Errorf("error initializing Hazelcast client: %v", err)
	}

	HealthChecks, err = hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.HealthCheckCache)
	if err != nil {
		return fmt.Errorf("error initializing HealthCheckCache: %v", err)
	}

	RepublishingCache, err = hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.RepublishingCache)
	if err != nil {
		return fmt.Errorf("error initializing RebublishingCache: %v", err)
	}

	return nil
}
