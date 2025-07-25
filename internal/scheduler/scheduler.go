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
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/circuitbreaker"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/handler"
	"pubsub-horizon-golaris/internal/republish"
	"time"
)

var scheduler *gocron.Scheduler
var HandleOpenCircuitBreakerFunc = circuitbreaker.HandleOpenCircuitBreaker
var HandleRepublishingEntryFunc = republish.HandleRepublishingEntry

// StartScheduler initializes and starts the task scheduler. It schedules periodic tasks
// for checking open circuit breakers and republishing entries based on the configured intervals.
func StartScheduler() {
	scheduler = gocron.NewScheduler(time.UTC)

	// Schedule the task for checking open circuit breakers
	if _, err := scheduler.Every(config.Current.CircuitBreaker.OpenCheckInterval).Do(func() {
		checkOpenCircuitBreakers()
	}); err != nil {
		log.Error().Err(err).Msgf("Error while scheduling for OPEN CircuitBreakerCache: %v", err)
	}

	// Schedule the task for checking republishing entries
	if _, err := scheduler.Every(config.Current.Republishing.CheckInterval).Do(func() {
		checkRepublishingEntries()
	}); err != nil {
		log.Error().Err(err).Msgf("Error while scheduling: %v", err)
	}

	if deliveringHandler := config.Current.Handlers.Delivering; deliveringHandler.Enabled {
		initialDelay := time.Now().Add(deliveringHandler.InitialDelay)
		if _, err := scheduler.Every(deliveringHandler.Interval).StartAt(initialDelay).Do(handler.CheckDeliveringEvents); err != nil {
			log.Error().Err(err).Msg("Unable to schedule delivering handler task")
		}
	}

	if failedHandler := config.Current.Handlers.Failed; failedHandler.Enabled {
		initialDelay := time.Now().Add(failedHandler.InitialDelay)
		if _, err := scheduler.Every(failedHandler.Interval).StartAt(initialDelay).Do(handler.CheckFailedEvents); err != nil {
			log.Error().Err(err).Msg("Unable to schedule failed handler task")
		}
	}

	if waitingHandler := config.Current.Handlers.Waiting; waitingHandler.Enabled {
		initialDelay := time.Now().Add(waitingHandler.InitialDelay)
		if _, err := scheduler.Every(waitingHandler.Interval).StartAt(initialDelay).Do(handler.WaitingHandlerService.CheckWaitingEvents); err != nil {
			log.Error().Err(err).Msg("Unable to schedule waiting handler task")
		}
	}

	// Start the scheduler asynchronously
	scheduler.StartAsync()
}

// checkOpenCircuitBreakers queries the circuit breaker cache for entries with the specified status
// and processes each entry asynchronously. It checks if the corresponding subscription exists
// and handles the open circuit breaker entry if the subscription is found.
func checkOpenCircuitBreakers() {
	// Get all CircuitBreaker entries with status OPEN
	statusQuery := predicate.Equal("status", string(enum.CircuitBreakerStatusOpen))
	cbEntries, err := cache.CircuitBreakerCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		log.Debug().Msgf("Error while getting CircuitBreaker messages: %v", err)
		return
	}

	//Iterate over all circuit breaker entries and handle them
	for _, entry := range cbEntries {
		log.Debug().Msgf("Checking CircuitBreaker with id %s", entry.SubscriptionId)

		subscription := getSubscription(entry.SubscriptionId)
		if subscription == nil {
			log.Debug().Msgf("Subscripton with id %s for circuit breaker entry doesn't exist.", entry.SubscriptionId)
			circuitbreaker.CloseCircuitBreaker(&entry)
			return
		} else {
			log.Debug().Msgf("Subscription with id %s for circuit breaker entry found: %v", entry.SubscriptionId, subscription)
		}
		//Handle each circuit breaker entry asynchronously
		go HandleOpenCircuitBreakerFunc(entry, subscription)
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
	for _, entry := range republishingEntries {
		subscriptionId := entry.Value.(republish.RepublishingCacheEntry).SubscriptionId
		log.Debug().Msgf("Checking republishing entry for subscriptionId %s", subscriptionId)

		subscription := getSubscription(subscriptionId)
		if subscription == nil {
			log.Debug().Msgf("Subscription with id %s for republishing entry doesn't exist.", subscriptionId)
			err := cache.RepublishingCache.Delete(context.Background(), subscriptionId)
			if err != nil {
				return
			}
			return
		}
		log.Debug().Msgf("Subscription with id %s for republishing entry found: %v", subscriptionId, subscription)
		// Handle each republishing entry asynchronously
		go HandleRepublishingEntryFunc(subscription)
	}
}

func getSubscription(subscriptionId string) *resource.SubscriptionResource {
	subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		return nil
	}
	return subscription
}
