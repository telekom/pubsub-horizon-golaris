// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/circuitbreaker"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/republish"
	"reflect"
)

type SubscriptionListener struct{}

func Initialize() {
	subscriptionListener := &SubscriptionListener{}
	err := cache.SubscriptionCache.AddListener(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionListener)
	if err != nil {
		panic(err)
	}

	log.Info().Msgf("SubscriptionLister initialized")
}

// OnAdd is not implemented for OnAdd event handling.
func (sl *SubscriptionListener) OnAdd(event *hazelcast.EntryNotified, obj resource.SubscriptionResource) {
}

// OnUpdate handles the subscription resource update event.
func (sl *SubscriptionListener) OnUpdate(event *hazelcast.EntryNotified, obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	if reflect.DeepEqual(obj, oldObj) {
		return
	}

	if obj.Spec.Subscription.DeliveryType == "callback" && (oldObj.Spec.Subscription.DeliveryType == "sse" || oldObj.Spec.Subscription.DeliveryType == "server_sent_event") {
		handleDeliveryTypeChangeFromSSEToCallback(obj, oldObj)
	}

	if (obj.Spec.Subscription.DeliveryType == "sse" || obj.Spec.Subscription.DeliveryType == "server_sent_event") && oldObj.Spec.Subscription.DeliveryType == "callback" {
		handleDeliveryTypeChangeFromCallbackToSSE(obj, oldObj)
	}

	if obj.Spec.Subscription.Callback != "" && oldObj.Spec.Subscription.Callback != "" {
		if obj.Spec.Subscription.Callback != oldObj.Spec.Subscription.Callback {
			handleCallbackUrlChange(obj, oldObj)
		}
	}

	if obj.Spec.Subscription.CircuitBreakerOptOut == true && oldObj.Spec.Subscription.CircuitBreakerOptOut != true {
		handleCircuitBreakerOptOutChange(obj, oldObj)
	}

	if obj.Spec.Subscription.RedeliveriesPerSecond != oldObj.Spec.Subscription.RedeliveriesPerSecond {
		handleRedeliveriesPerSecondChange(obj, oldObj)
	}
}

// OnDelete handles the deletion of a subscription if a RepublishingCacheEntry exists for the subscription.
func (sl *SubscriptionListener) OnDelete(event *hazelcast.EntryNotified) {
	key, ok := event.Key.(string)
	if !ok {
		log.Error().Msg("event.Key is not of type string")
		return
	}

	optionalEntry, err := cache.RepublishingCache.Get(context.Background(), key)
	if err != nil {
		log.Error().Msgf("failed with err: %v to get republishing cache", err)
		return
	}

	if optionalEntry != nil {
		republish.ForceDelete(context.Background(), key)

		cache.SubscriptionCancelMap[key] = true
	}
}

// OnError handles any errors encountered by SubscriptionListener.
func (sl *SubscriptionListener) OnError(event *hazelcast.EntryNotified, err error) {
	log.Error().Msgf("Error in SubscriptionListener: %v", err)
}

// handleDeliveryTypeChange reacts to changes for the deliveryType of subscriptions.
// If delivery type changes from sse to callback, sets a new entry in RepublishingCache and store the old delivery type.
// When republishing, the old deliveryType is used to check whether old SSE events that are set to PROCESSED still need to be republished.
// If delivery type changes from callback to sse, deletes existing entry in RepublishingCache if present and sets a new entry without storing the old delivery type.
// Delete the HealthCheck entry and close the circuitBreaker, because it is no longer needed for sse.
func handleDeliveryTypeChangeFromSSEToCallback(obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	log.Debug().Msgf("Delivery type changed from sse to callback for subscription %s", obj.Spec.Subscription.SubscriptionId)
	setNewEntryToRepublishingCache(obj.Spec.Subscription.SubscriptionId, string(oldObj.Spec.Subscription.DeliveryType))
}

func handleDeliveryTypeChangeFromCallbackToSSE(obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	log.Debug().Msgf("Delivery type changed from callback to sse for subscription %s", obj.Spec.Subscription.SubscriptionId)
	optionalEntry, err := cache.RepublishingCache.Get(context.Background(), obj.Spec.Subscription.SubscriptionId)
	if err != nil {
		log.Error().Msgf("Failed to get republishing cache entry for subscription %s: %v", obj.Spec.Subscription.SubscriptionId, err)
		return
	}

	if optionalEntry != nil {
		republish.ForceDelete(context.Background(), obj.Spec.Subscription.SubscriptionId)
		log.Debug().Msgf("Setting cancel map for subscription %s", obj.Spec.Subscription.SubscriptionId)
		cache.SubscriptionCancelMap[obj.Spec.Subscription.SubscriptionId] = true
	}

	setNewEntryToRepublishingCache(obj.Spec.Subscription.SubscriptionId, string(oldObj.Spec.Subscription.DeliveryType))

	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, obj.Spec.Subscription.SubscriptionId)
	if err != nil {
		log.Error().Msgf("failed with err: %v to get circuit breaker", err)
		return
	}

	if cbMessage != nil {
		circuitbreaker.CloseCircuitBreaker(cbMessage)
	}
}

// handleCallbackUrlChange reacts to changes for the callback URL of subscriptions.
// If callback URL changes and an entry exists in RepublishingCache, deletes the existing entry and sets a new one.
func handleCallbackUrlChange(obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	log.Debug().Msgf("Callback URL changed from %s to %s for subscription %s", oldObj.Spec.Subscription.Callback, obj.Spec.Subscription.Callback, obj.Spec.Subscription.SubscriptionId)
	optionalEntry, err := cache.RepublishingCache.Get(context.Background(), obj.Spec.Subscription.SubscriptionId)
	if err != nil {
		log.Error().Msgf("Failed to get republishing cache entry for subscription %s: %v", obj.Spec.Subscription.SubscriptionId, err)
		return
	}

	if optionalEntry != nil {
		republish.ForceDelete(context.Background(), obj.Spec.Subscription.SubscriptionId)
		log.Debug().Msgf("Setting cancel map for subscription %s", obj.Spec.Subscription.SubscriptionId)
		cache.SubscriptionCancelMap[obj.Spec.Subscription.SubscriptionId] = true
	}

	setNewEntryToRepublishingCache(obj.Spec.Subscription.SubscriptionId, "")
}

// handleCircuitBreakerOptOutChange reacts to changes for CircuitBreakerOptOut flag of subscriptions.
// If the flag is set to true and an entry exists in RepublishingCache, close circuitBreaker,
// add new entry in the republishingCache and deletes health checks.
func handleCircuitBreakerOptOutChange(obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	log.Debug().Msgf("CircuitBreakerOptOut changed from %v to %v for subscription %s", oldObj.Spec.Subscription.CircuitBreakerOptOut, obj.Spec.Subscription.CircuitBreakerOptOut, obj.Spec.Subscription.SubscriptionId)
	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, obj.Spec.Subscription.SubscriptionId)
	if err != nil {
		log.Error().Msgf("failed with err: %v to get circuit breaker", err)
		return
	}

	if cbMessage != nil {
		circuitbreaker.CloseCircuitBreaker(cbMessage)
	}

	setNewEntryToRepublishingCache(obj.Spec.Subscription.SubscriptionId, "")
}

func handleRedeliveriesPerSecondChange(obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	log.Debug().Msgf("RedeliveriesPerSecond changed from %v to %v for subscription %s", oldObj.Spec.Subscription.RedeliveriesPerSecond, obj.Spec.Subscription.RedeliveriesPerSecond, obj.Spec.Subscription.SubscriptionId)
	optionalEntry, err := cache.RepublishingCache.Get(context.Background(), obj.Spec.Subscription.SubscriptionId)
	if err != nil {
		log.Error().Msgf("Failed to get republishing cache entry for subscription %s: %v", obj.Spec.Subscription.SubscriptionId, err)
		return
	}

	if optionalEntry != nil {
		republish.ForceDelete(context.Background(), obj.Spec.Subscription.SubscriptionId)
		log.Debug().Msgf("Setting cancel map for subscription %s", obj.Spec.Subscription.SubscriptionId)
		cache.SubscriptionCancelMap[obj.Spec.Subscription.SubscriptionId] = true
	}

	setNewEntryToRepublishingCache(obj.Spec.Subscription.SubscriptionId, "")
}

func setNewEntryToRepublishingCache(subscriptionID string, oldDeliveryType string) {
	err := cache.RepublishingCache.Set(context.Background(), subscriptionID, republish.RepublishingCache{
		SubscriptionId:  subscriptionID,
		OldDeliveryType: oldDeliveryType,
	})
	if err != nil {
		log.Error().Msgf("Failed to set republishing cache for subscription %s: %v", subscriptionID, err)
		return
	}
}
