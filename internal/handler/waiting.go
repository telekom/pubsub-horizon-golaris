// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/republish"
	"time"
)

type (
	WaitingHandlerInterface interface {
		CheckWaitingEvents()
		GetCircuitBreakerSubscriptionsMap() (map[string]struct{}, error)
		GetRepublishingSubscriptionsMap() (map[string]struct{}, error)
	}
	waitingHandler struct{}
)

var WaitingHandlerService WaitingHandlerInterface = new(waitingHandler)

func (waitingHandler *waitingHandler) CheckWaitingEvents() {
	log.Info().Msgf("WaitingHandler: Republish messages stucked in state WAITING")

	minMessageAge := config.Current.WaitingHandler.MinMessageAge
	maxMessageAge := config.Current.WaitingHandler.MaxMessageAge

	// Create a WaitingHandler entry and lock it
	var ctx = cache.HandlerCache.NewLockContext(context.Background())

	if acquired, _ := cache.HandlerCache.TryLockWithTimeout(ctx, cache.WaitingLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("WaitingHandler: Could not acquire lock for WaitingHandler entry: %s", cache.WaitingLockKey)
		return
	}

	defer func() {
		if err := cache.HandlerCache.Unlock(ctx, cache.WaitingLockKey); err != nil {
			log.Error().Err(err).Msg("WaitingHandler: Error unlocking WaitingHandler")
		}
	}()

	// Get all subscriptions (distinct) for messages in state WAITING from db
	dbSubscriptionsForWaitingEvents, err := mongo.CurrentConnection.FindDistinctSubscriptionsForWaitingEvents(time.Now().Add(-maxMessageAge), time.Now().Add(-minMessageAge))
	if err != nil {
		log.Error().Err(err).Msgf("WaitingHandler: Error while fetching distinct subscriptions for events stucked in state WAITING from db")
		return
	}

	// If no subscriptions found, return
	log.Debug().Msgf("WaitingHandler: Found %d subscriptions with waiting messages: %v", len(dbSubscriptionsForWaitingEvents), dbSubscriptionsForWaitingEvents)
	if len(dbSubscriptionsForWaitingEvents) == 0 {
		return
	}

	// Get all republishing cache entries
	republishingSubscriptionsMap, err := WaitingHandlerService.GetRepublishingSubscriptionsMap()
	if err != nil {
		log.Error().Err(err).Msgf("WaitingHandler: Error while fetching rebublishing cache entries for events stucked in state WAITING")
		return
	}
	log.Debug().Msgf("WaitingHandler: Found %d rebublishing entries: %v", len(republishingSubscriptionsMap), republishingSubscriptionsMap)

	// Get all circuit-breaker entries with status OPEN
	circuitBreakerSubscriptionsMap, err := WaitingHandlerService.GetCircuitBreakerSubscriptionsMap()
	if err != nil {
		log.Error().Err(err).Msgf("WaitingHandler: Error while fetching circuit breaker cache entries for events stucked in state WAITING")
		return
	}
	log.Debug().Msgf("WaitingHandler: Found %d circuitbreaker entries in state OPEN: %v", len(circuitBreakerSubscriptionsMap), circuitBreakerSubscriptionsMap)

	// Check if subscription is in republishing cache or in circuit breaker cache. If not create a republishing cache entry
	for _, subscriptionId := range dbSubscriptionsForWaitingEvents {
		log.Debug().Msgf("WaitingHandler: Checking subscription for events stucked in state WAITING. subscription: %v", subscriptionId)
		_, inRepublishing := republishingSubscriptionsMap[subscriptionId]
		_, inCircuitBreaker := circuitBreakerSubscriptionsMap[subscriptionId]
		if !inRepublishing && !inCircuitBreaker {
			log.Warn().Msgf("WaitingHandler: Subscription %v has waiting messages and no circuitbreaker entry and no republishing entry!. Creating republishing entry for events stucked in state WAITING", subscriptionId)

			// Create republishing cache entry for subscription with stuck waiting events
			republishingCacheEntry := republish.RepublishingCacheEntry{
				SubscriptionId:   subscriptionId,
				RepublishingUpTo: time.Now(),
				PostponedUntil:   time.Now(),
			}
			if err := cache.RepublishingCache.Set(context.Background(), subscriptionId, republishingCacheEntry); err != nil {
				log.Error().Err(err).Msgf("WaitingHandler: Error while creating RepublishingCacheEntry entry for events stucked in state WAITING. subscriptionId: %s", subscriptionId)
				continue
			}
			log.Debug().Msgf("WaitingHandler: Successfully created RepublishingCacheEntry entry for for events stucked in state WAITING. subscriptionId: %s republishingEntry: %+v", subscriptionId, republishingCacheEntry)
		}
	}
	log.Info().Msgf("WaitingHandler: Finished republishing messages stucked in state WAITING")
}

func (waitingHandler *waitingHandler) GetCircuitBreakerSubscriptionsMap() (map[string]struct{}, error) {

	statusQuery := predicate.Equal("status", string(enum.CircuitBreakerStatusOpen))
	circuitBreakerEntries, err := cache.CircuitBreakerCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		return nil, err
	}

	circuitBreakerMap := make(map[string]struct{})
	for _, entry := range circuitBreakerEntries {
		circuitBreakerMap[entry.SubscriptionId] = struct{}{}
	}
	return circuitBreakerMap, nil
}

func (waitingHandler *waitingHandler) GetRepublishingSubscriptionsMap() (map[string]struct{}, error) {

	cacheRepublishingEntries, err := cache.RepublishingCache.GetEntrySet(context.Background())
	if err != nil {
		return nil, err
	}

	republishingMap := make(map[string]struct{})
	for _, entry := range cacheRepublishingEntries {
		if castedEntry, ok := entry.Value.(republish.RepublishingCacheEntry); ok {
			republishingMap[castedEntry.SubscriptionId] = struct{}{}
		} else {
			log.Error().Msgf("Error casting republishing entry: %v", entry)
		}
	}
	return republishingMap, nil
}
