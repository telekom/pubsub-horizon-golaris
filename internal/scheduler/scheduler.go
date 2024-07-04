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
	"time"

	"github.com/rs/zerolog/log"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/republish"
)

var scheduler *gocron.Scheduler

//func StartScheduler() {
//	log.Debug().Msg("#TEST StartScheduler")
//
//ticker := time.NewTicker(config.Current.CircuitBreaker.OpenCbCheckInterval)
//done := make(chan bool)
//
//go func() {
//	for {
//		log.Debug().Msg("#TEST start checkOpenCircuitBreakers")
//		select {
//		case <-done:
//			return
//		case <-ticker.C:
//			checkOpenCircuitBreakers()
//		}
//	}
//}()

// To stop the ticker, send a signal on the "done" channel
// done <- true
//}

// StartScheduler initializes and starts the task scheduler. It schedules periodic tasks
// for checking open circuit breakers and republishing entries based on the configured intervals.
func StartScheduler() {
	log.Debug().Msg("#TEST StartScheduler")

	scheduler = gocron.NewScheduler(time.UTC)

	log.Debug().Msgf("#TEST new scheduler created: %v", scheduler)

	// Schedule the task for checking open circuit breakers
	if _, err := scheduler.Every(config.Current.CircuitBreaker.OpenCbCheckInterval).Do(func() {
		checkOpenCircuitBreakers()
	}); err != nil {
		log.Error().Err(err).Msgf("Error while scheduling for OPEN CircuitBreakerCache: %v", err)
	}

	log.Debug().Msgf("#TEST scheduled checkOpenCircuitBreakers")

	// Schedule the task for checking republishing entries
	//if _, err := scheduler.Every(config.Current.Republishing.CheckInterval).Do(func() {
	//	checkRepublishingEntries()
	//}); err != nil {
	//	log.Error().Err(err).Msgf("Error while scheduling for republishing entries: %v", err)
	//}

	// Start the scheduler asynchronously
	scheduler.StartAsync()
}

// checkOpenCircuitBreakers queries the circuit breaker cache for entries with the specified status
// and processes each entry asynchronously. It checks if the corresponding subscription exists
// and handles the open circuit breaker entry if the subscription is found.
func checkOpenCircuitBreakers() {
	log.Debug().Msg("#TEST execute checkOpenCircuitBreakers")
	// Get all CircuitBreaker entries with status OPEN
	statusQuery := predicate.Equal("status", string(enum.CircuitBreakerStatusOpen))
	cbEntries, err := cache.CircuitBreakerCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		log.Debug().Msgf("Error while getting CircuitBreaker messages: %v", err)
		return
	}

	log.Debug().Msgf("#TEST cbEntries: %v", cbEntries)

	//Iterate over all circuit breaker entries and handle them
	for _, entry := range cbEntries {
		log.Debug().Msgf("Checking CircuitBreaker with id %s", entry.SubscriptionId)

		//// ToDo: Check whether the subscription has changed or was deleted and handle it
		//subscription := getSubscription(entry.SubscriptionId)
		//if subscription == nil {
		//	log.Debug().Msgf("Subscripton with id %s for circuit breaker entry doesn't exist.", entry.SubscriptionId)
		//	return
		//} else {
		//	log.Debug().Msgf("Subscription with id %s for circuit breaker entry found: %v", entry.SubscriptionId, subscription)
		//}
		// Handle each circuit breaker entry asynchronously
		//go circuitbreaker.HandleOpenCircuitBreaker(entry, subscription)
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
		subscriptionId := entry.Value.(republish.RepublishingCache).SubscriptionId
		log.Debug().Msgf("Checking republishing entry for subscriptionId %s", subscriptionId)

		// ToDo: Check whether the subscription has changed or was deleted and handle it
		subscription := getSubscription(subscriptionId)
		if subscription == nil {
			log.Debug().Msgf("Subscription with id %s for republishing entry doesn't exist.", subscriptionId)
			return
		}
		log.Debug().Msgf("Subscription with id %s for republishing entry found: %v", subscriptionId, subscription)
		// Handle each republishing entry asynchronously
		go republish.HandleRepublishingEntry(subscription)
	}
}

func getSubscription(subscriptionId string) *resource.SubscriptionResource {
	log.Debug().Msgf("#TEST Getting subscription with subscriptionId %s", subscriptionId)
	subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("#TEST Error while getting subscription with subscriptionId %s", subscriptionId)
		return nil
	}
	return subscription
}
