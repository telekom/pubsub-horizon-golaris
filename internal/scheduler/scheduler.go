// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"github.com/go-co-op/gocron"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/circuit_breaker"
	"golaris/internal/config"
	"golaris/internal/republish"
	"time"
)

var scheduler *gocron.Scheduler

// StartScheduler initializes and starts the task scheduler. It schedules periodic tasks
// for checking open circuit breakers and republishing entries based on the configured intervals.
func StartScheduler() {
	scheduler = gocron.NewScheduler(time.UTC)

	// Schedule the task for checking open circuit breakers
	if _, err := scheduler.Every(config.Current.CircuitBreaker.OpenCbCheckInterval).Do(func() {
		checkOpenCircuitBreakers()
	}); err != nil {
		log.Error().Msgf("Error while scheduling for OPEN CircuitBreakers: %v", err)
	}

	// Schedule the task for checking republishing entries
	if _, err := scheduler.Every(config.Current.Republishing.CheckInterval).Do(func() {
		checkRepublishingEntries()
	}); err != nil {
		log.Error().Msgf("Error while scheduling for republishing entries: %v", err)
	}

	// Start the scheduler asynchronously
	scheduler.StartAsync()
}

// checkOpenCircuitBreakers queries the circuit breaker cache for entries with the specified status
// and processes each entry asynchronously. It checks if the corresponding subscription exists
// and handles the open circuit breaker entry if the subscription is found.
func checkOpenCircuitBreakers() {
	// Get all CircuitBreaker entries with status OPEN
	statusQuery := predicate.Equal("status", enum.CircuitBreakerStatusOpen)
	cbEntries, err := cache.CircuitBreakers.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		log.Debug().Msgf("Error while getting CircuitBreaker messages: %v", err)
		return
	}

	// Iterate over all circuit breaker entries and handle them
	for _, entry := range cbEntries {
		log.Info().Msgf("Checking CircuitBreaker with id %s", entry.SubscriptionId)

		subscription := getSubscription(entry.SubscriptionId)
		if subscription == nil {
			log.Info().Msgf("Subscription with id %s for circuit breaker entry doesn't exist.", entry.SubscriptionId)
			return
		}

		if subscription.Spec.Subscription.CircuitBreakerOptOut {
			log.Info().Msgf("Subscription with id %s has opted out of circuit breaker.", entry.SubscriptionId)

			err = cache.RepublishingCache.Set(context.Background(), entry.SubscriptionId, republish.RepublishingCache{
				SubscriptionId: entry.SubscriptionId,
			})
			if err != nil {
				log.Info().Msgf("Error while setting republishing entry for subscriptionId %s", entry.SubscriptionId)
			}

			_ = cache.HealthChecks.Delete(context.Background(), entry.SubscriptionId)
			if err != nil {
				log.Error().Msgf("Error while deleting health check for subscriptionId %s", entry.SubscriptionId)
			}

			circuit_breaker.CloseCircuitBreaker(entry)

			return
		}

		// Handle each circuit breaker entry asynchronously
		go circuit_breaker.HandleOpenCircuitBreaker(entry, subscription)
	}
}

// checkRepublishingEntries queries the republishing cache for entries and processes each entry asynchronously.
// It checks if the corresponding subscription exists and handles the republishing entry if the subscription is found.
func checkRepublishingEntries() {
	// Get all republishing entries
	republishingEntries, err := cache.RepublishingCache.GetEntrySet(context.Background())
	if err != nil {
		log.Debug().Msgf("Error while getting republishing entries: %v", err)
		return
	}

	// Iterate over all republishing entries and handle them
	for _, republishEntry := range republishingEntries {
		subscriptionId := republishEntry.Value.(republish.RepublishingCache).SubscriptionId
		log.Info().Msgf("Checking republishing entry for subscriptionId %s", subscriptionId)

		// ToDo: Check whether the subscription has changed or was deleted and handle it
		subscription := getSubscription(subscriptionId)
		if subscription == nil {
			log.Info().Msgf("Subscription with id %s for republishing entry doesn't exist.", subscriptionId)
			return
		}

		log.Debug().Msgf("Subscription with id %s for republishing entry found: %v", subscriptionId, subscription)
		// Handle each republishing entry asynchronously
		go republish.HandleRepublishingEntry(subscription)
	}
}

func getSubscription(subscriptionId string) *resource.SubscriptionResource {
	subscription, err := cache.Subscriptions.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		return nil
	}
	return subscription
}
