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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	c "github.com/telekom/pubsub-horizon-go/cache"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/util"
	"os"
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
	TryLockWithLeaseAndTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error)
	TryLockWithLease(ctx context.Context, key interface{}, duration time.Duration) (bool, error)
}

var SubscriptionCache c.HazelcastBasedCache[resource.SubscriptionResource]
var CircuitBreakerCache c.HazelcastBasedCache[message.CircuitBreakerMessage]
var HealthCheckCache HazelcastMapInterface
var RepublishingCache HazelcastMapInterface
var hazelcastClient *hazelcast.Client

var subscriptionCancelMap = make(map[string]bool)
var cancelMapMutex sync.RWMutex

var NotificationSender HazelcastMapInterface
var HandlerCache HazelcastMapInterface

var DeliveringLockKey string
var FailedLockKey string
var WaitingLockKey string

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

	DeliveringLockKey = "deliveringHandlerLockEntry"
	FailedLockKey = "failedHandlerLockEntry"
	WaitingLockKey = "waitingHandlerLockEntry"
}

func createNewHazelcastConfig() hazelcast.Config {
	cacheConfig := hazelcast.NewConfig()

	instanceName := os.Getenv("POD_NAME")
	if instanceName == "" {
		instanceName = "golaris-" + uuid.New().String()
	}
	cacheConfig.Cluster.Name = config.Current.Hazelcast.ClusterName
	cacheConfig.Cluster.Network.SetAddresses(config.Current.Hazelcast.ServiceDNS)
	cacheConfig.Logger.CustomLogger = util.NewHazelcastZerologLogger(parseHazelcastLogLevel(config.Current.Hazelcast.LogLevel))
	cacheConfig.ClientName = instanceName

	return cacheConfig
}

func parseHazelcastLogLevel(logLevel string) zerolog.Level {
	var hazelcastLogLevel, err = zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Error().Err(err).Fields(map[string]any{
			"logLevel": config.Current.Hazelcast.LogLevel,
		}).Msg("Could not parse log-level for hazelcast logger. Falling back to INFO...")
		return zerolog.InfoLevel
	}
	return hazelcastLogLevel
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

	HandlerCache, err = hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.HandlerCache)
	if err != nil {
		return fmt.Errorf("error initializing DeliveringHandler: %v", err)
	}

	NotificationSender, err = hazelcastClient.GetMap(context.Background(), "notificationSender")
	if err != nil {
		return fmt.Errorf("error initializing lock-cache for notification-sender")
	}

	return nil
}
