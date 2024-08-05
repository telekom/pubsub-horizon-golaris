// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/rs/zerolog/log"
	c "github.com/telekom/pubsub-horizon-go/cache"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/util"
	"pubsub-horizon-golaris/internal/config"
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
	ContainsKey(ctx context.Context, key interface{}) (bool, error)
	Clear(ctx context.Context) error
	Lock(ctx context.Context, key interface{}) error
}

var SubscriptionCache c.HazelcastBasedCache[resource.SubscriptionResource]
var CircuitBreakerCache c.HazelcastBasedCache[message.CircuitBreakerMessage]
var HealthCheckCache HazelcastMapInterface
var RepublishingCache HazelcastMapInterface
var hazelcastClient *hazelcast.Client

var subscriptionCancelMap = make(map[string]bool)
var cancelMapMutex sync.RWMutex

var DeliveringHandler HazelcastMapInterface
var FailedHandler HazelcastMapInterface

var DeliveringLockKey string
var FailedLockKey string

func SetCancelStatus(subscriptionId string, status bool) {
	cancelMapMutex.Lock()
	defer cancelMapMutex.Unlock()
	subscriptionCancelMap[subscriptionId] = status
}

func GetCancelStatus(subscriptionId string) bool {
	cancelMapMutex.RLock()
	defer cancelMapMutex.RUnlock()
	return subscriptionCancelMap[subscriptionId]
}

func Initialize() {
	c := createNewHazelcastConfig()
	err := initializeCaches(c)
	if err != nil {
		log.Panic().Err(err).Msg("error while initializing caches")
	}

	DeliveringLockKey = "delivering_" + uuid.New().String()
	FailedLockKey = "failed_" + uuid.New().String()
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

	DeliveringHandler, err = hazelcastClient.GetMap(context.Background(), config.Current.Handler.Delivering)
	if err != nil {
		return fmt.Errorf("error initializing DeliveringHandler: %v", err)
	}

	FailedHandler, err = hazelcastClient.GetMap(context.Background(), config.Current.Handler.Failed)
	if err != nil {
		return fmt.Errorf("error initializing FailedHandler: %v", err)
	}

	return nil
}
