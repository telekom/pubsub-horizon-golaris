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

func CheckWaitingEvents() {
	log.Info().Msgf("Republish messages stucked in state WAITING")

	minMessageAge := config.Current.WaitingHandler.MinMessageAge
	maxMessageAge := config.Current.WaitingHandler.MaxMessageAge

	// Create a WaitingHandler entry and lock it
	var ctx = cache.HandlerCache.NewLockContext(context.Background())

	if acquired, _ := cache.HandlerCache.TryLockWithTimeout(ctx, cache.WaitingLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for WaitingHandler entry: %s", cache.WaitingLockKey)
		return
	}

	defer func() {
		if err := cache.HandlerCache.Unlock(ctx, cache.WaitingLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking WaitingHandler")
		}
	}()

	// Get all subscriptions (distinct) for messages in state WAITING
	subscriptionIdsForWaitingMsgs, err := mongo.CurrentConnection.FindDistinctSubscriptionsForWaitingEvents(time.Now().Add(-maxMessageAge), time.Now().Add(-minMessageAge))
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching distinct subscriptions for events stucked in state WAITING from db")
		return
	}

	// Get all republishing cache entries
	republishingMap, err := getRepublishingCacheMap()
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching rebublishing cache entries for events stucked in state WAITING")
		return
	}

	// Get all CircuitBreaker entries with status OPEN
	circuitBreakerMap, err := getCircuitBreakerCacheMap()
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching circuit breaker cache entries for events stucked in state WAITING")
		return
	}

	// Check if subscription is in republishing cache or circuit breaker cache, if not create a republishing cache entry
	for _, subscriptionId := range subscriptionIdsForWaitingMsgs {
		log.Debug().Msgf("Checking subscription for events stucked in state WAITING. subscription: %v", subscriptionId)
		if _, contains := republishingMap[subscriptionId]; !contains {
			if _, contains := circuitBreakerMap[subscriptionId]; !contains {
				log.Warn().Msgf("Subscription %v has waiting messages and no circuitbreaker entry or republishing entry. Events stucked in state WAITING", subscriptionId)

				// Create republishing cache entry for subscription with stuck waiting events
				republishingCacheEntry := republish.RepublishingCacheEntry{
					SubscriptionId:   subscriptionId,
					RepublishingUpTo: time.Now(),
					PostponedUntil:   time.Now(),
				}
				if err := cache.RepublishingCache.Set(context.Background(), subscriptionId, republishingCacheEntry); err != nil {
					log.Error().Err(err).Msgf("Error while creating RepublishingCacheEntry entry for events stucked in state WAITING. subscriptionId: %s", subscriptionId)
					continue
				}
				log.Debug().Msgf("Successfully created RepublishingCacheEntry entry for for events stucked in state WAITING. subscriptionId: %s republishingEntry: %+v", subscriptionId, republishingCacheEntry)
			}
		}
	}

	// ToDo Only for testing
	log.Info().Msgf("Found republishing entries: %v", republishingMap)
	log.Info().Msgf("Found circuitbreaker entries: %v", circuitBreakerMap)
	log.Info().Msgf("Found waiting messages: %v", subscriptionIdsForWaitingMsgs)
}

func getCircuitBreakerCacheMap() (map[string]struct{}, error) {

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

func getRepublishingCacheMap() (map[string]struct{}, error) {

	cacheRepublishingEntries, err := cache.RepublishingCache.GetEntrySet(context.Background())
	if err != nil {
		return nil, err
	}

	republishingMap := make(map[string]struct{})
	for _, entry := range cacheRepublishingEntries {
		castedEntry, ok := entry.Value.(republish.RepublishingCacheEntry)
		if !ok {
			log.Error().Msgf("Error casting republishing entry: %v", entry)
			continue
		}
		republishingMap[castedEntry.SubscriptionId] = struct{}{}
	}
	return republishingMap, nil
}
