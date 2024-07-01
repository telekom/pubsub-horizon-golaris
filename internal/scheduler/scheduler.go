// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/resource"
	"github.com/go-co-op/gocron"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"golaris/internal/cache"
	"golaris/internal/circuit_breaker"
	"golaris/internal/config"
	"golaris/internal/republish"
	"time"
)

var scheduler *gocron.Scheduler

func StartScheduler() {
	scheduler = gocron.NewScheduler(time.UTC)

	if _, err := scheduler.Every(config.Current.CircuitBreaker.OpenCbCheckInterval).Do(func() {
		CheckCircuitBreakersByStatus(enum.CircuitBreakerStatusOpen)
	}); err != nil {
		log.Error().Msgf("Error while scheduling for OPEN CircuitBreakers: %v", err)
	}

	if _, err := scheduler.Every(config.Current.Republishing.CheckInterval).Do(func() {
		CheckRepublishingEntries()
	}); err != nil {
		log.Error().Msgf("Error while scheduling for republishing entries: %v", err)
	}

	scheduler.StartAsync()
}

// TODO why public?
func CheckCircuitBreakersByStatus(status enum.CircuitBreakerStatus) {
	statusQuery := predicate.Equal("status", string(status))
	cbEntries, err := cache.CircuitBreakers.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		log.Debug().Msgf("Error while getting CircuitBreaker messages: %v", err)
		return
	}

	for _, entry := range cbEntries {
		log.Info().Msgf("Checking CircuitBreaker with id %s", entry.SubscriptionId)

		subscription := GetSubscriptionForCbMessage(entry.SubscriptionId)
		if subscription == nil {
			log.Info().Msgf("Subscripton with id: %s does not exist. Delete cbMessage", entry.SubscriptionId)
			return
		} else {
			log.Debug().Msgf("Subscription with id %s found: %v", entry.SubscriptionId, subscription)
		}

		// ToDo: Check whether the subscription has changed
		go circuit_breaker.HandleOpenCircuitBreaker(entry, subscription)

	}
}

func CheckRepublishingEntries() {
	republishingEntries, err := cache.RepublishingCache.GetEntrySet(context.Background())
	if err != nil {
		log.Debug().Msgf("Error while getting republishing entries: %v", err)
		return
	}

	for _, entry := range republishingEntries {
		subscriptionId := entry.Value.(republish.RepublishingCache).SubscriptionId
		log.Info().Msgf("Checking republishing entry with id %s", subscriptionId)

		subscription := GetSubscriptionForCbMessage(subscriptionId)
		if subscription == nil {
			log.Info().Msgf("Subscription with id %s for republishing entry does not exist.", subscriptionId)
			return
		}
		log.Debug().Msgf("Subscription with id %s for republishing entry found: %v", subscriptionId, subscription)

		// ToDo: Check whether the subscription has changed
		go republish.HandleRepublishingEntry(subscription)

	}

}

// TODO why public?
func GetSubscriptionForCbMessage(subscriptionId string) *resource.SubscriptionResource {
	subscription, err := cache.Subscriptions.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Could not read subscription with id %s", subscriptionId)
		return nil
	}

	return subscription
}
