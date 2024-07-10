// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/rs/zerolog/log"
	c "github.com/telekom/pubsub-horizon-go/cache"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/util"
	"golaris/internal/config"
	"sync"
	"time"
)

type HazelcastMapInterface interface {
	Get(ctx context.Context, key interface{}) (interface{}, error)
	Set(ctx context.Context, key interface{}, value interface{}) error
	TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error)
	GetEntrySet(ctx context.Context) ([]types.Entry, error)
	NewLockContext(ctx context.Context) context.Context
	Delete(ctx context.Context, key interface{}) error
	Unlock(ctx context.Context, key interface{}) error
	IsLocked(ctx context.Context, key interface{}) (bool, error)
	ForceUnlock(ctx context.Context, key interface{}) error
}

var Subscriptions c.HazelcastBasedCache[resource.SubscriptionResource]
var CircuitBreakers c.HazelcastBasedCache[message.CircuitBreakerMessage]
var HealthChecks HazelcastMapInterface
var RepublishingCache HazelcastMapInterface
var hazelcastClient *hazelcast.Client

var SubscriptionCancelMap = make(map[string]bool)
var CancelMapMutex sync.Mutex

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

	Subscriptions, err = c.NewHazelcastCache[resource.SubscriptionResource](hzConfig)
	if err != nil {
		return fmt.Errorf("error initializing Hazelcast subscription health cache: %v", err)
	}

	CircuitBreakers, err = c.NewHazelcastCache[message.CircuitBreakerMessage](hzConfig)
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
