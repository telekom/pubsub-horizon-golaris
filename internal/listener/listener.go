package listener

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/circuit_breaker"
	"golaris/internal/config"
	"golaris/internal/republish"
)

type SubscriptionListener struct{}

func Initialize() {
	subscriptionListener := &SubscriptionListener{}
	err := cache.Subscriptions.AddListener(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionListener)
	if err != nil {
		panic(err)
	}
}

func (sl *SubscriptionListener) OnAdd(event *hazelcast.EntryNotified, obj resource.SubscriptionResource) {
}

func (sl *SubscriptionListener) OnUpdate(event *hazelcast.EntryNotified, obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {

	// So in the normal case, there are no WAITING events so which events should be redelivered here?
	// Maybe old PROCESSED events with deliveryType SSE?
	if oldObj.Spec.Subscription.DeliveryType == "sse" || oldObj.Spec.Subscription.DeliveryType == "server_sent_event" && obj.Spec.Subscription.DeliveryType == "callback" {
		if cache.RepublishingCache == nil {
			log.Error().Msg("RepublishingCache is not initialized")
			return
		}
		err := cache.RepublishingCache.Set(context.Background(), obj.Spec.Subscription.SubscriptionId, republish.RepublishingCache{
			SubscriptionId: obj.Spec.Subscription.SubscriptionId,
		})
		if err != nil {
			log.Error().Msgf("failed with err: %v to set republishing cache", err)
			return
		}
	}

	// Get and maybe delete RepublishingCache Entry if DeliveryType change from callback to sse.
	// 	If republishing is running for a SubscriptionId since to 10 hours (throttling) and the deliveryType change,
	//	we have to delete the current entry in Republishing process because we don't check the subscription again in the republishing cache,
	//	after the locking. Because of that, we have to delete the republishing cache entry if the deliveryType change to get the subscription again with new deliveryType.

	// If no RepublishingEntry exist, republish all WAITING events with new deliveryType SSE.
	if oldObj.Spec.Subscription.DeliveryType == "callback" && obj.Spec.Subscription.DeliveryType == "sse" || obj.Spec.Subscription.DeliveryType == "server_sent_event" {
		optionalEntry, err := cache.RepublishingCache.Get(context.Background(), obj.Spec.Subscription.SubscriptionId)
		if err != nil {
			log.Error().Msgf("failed with err: %v to get republishing cache", err)
			return
		}

		if optionalEntry != nil {
			republish.ForceDelete(obj.Spec.Subscription.SubscriptionId, context.Background())
		}

		err = cache.RepublishingCache.Set(context.Background(), obj.Spec.Subscription.SubscriptionId, republish.RepublishingCache{
			SubscriptionId: obj.Spec.Subscription.SubscriptionId,
		})
		if err != nil {
			log.Error().Msgf("failed with err: %v to set republishing cache", err)
			return
		}

		err = cache.HealthChecks.Delete(context.Background(), obj.Spec.Subscription.SubscriptionId)
		// How can we delete the CB here?
	}

	// Get and maybe delete RepublishingCache Entry if CallbackUrl change.
	// If republishing is running for a SubscriptionId since to 10 hours (throttling) and the callbackUrl change,
	// we would write the old callbackUrl to kafka because we don't check the subscription again in the republishing cache,
	// after the locking. Because of that, we have to delete the republishing cache entry if the callbackUrl change to get the subscription again with new callbackUrl.

	// If no republishing entry exists, we actually have to do nothing.
	if oldObj.Spec.Subscription.Callback != obj.Spec.Subscription.Callback {
		optionalEntry, err := cache.RepublishingCache.Get(context.Background(), obj.Spec.Subscription.SubscriptionId)
		if err != nil {
			log.Error().Msgf("failed with err: %v to get republishing cache", err)
			return
		}

		if optionalEntry != nil {
			republish.ForceDelete(obj.Spec.Subscription.SubscriptionId, context.Background())

			err = cache.RepublishingCache.Set(context.Background(), obj.Spec.Subscription.SubscriptionId, republish.RepublishingCache{
				SubscriptionId: obj.Spec.Subscription.SubscriptionId,
			})
			if err != nil {
				log.Error().Msgf("failed with err: %v to set republishing cache", err)
				return
			}
		}
	}

	// Create entry in RepublishingCache if CircuitBreakerOptOut Change.
	// If republishing is running for a SubscriptionId since to 10 hours (throttling) and the CircuitBreaker goes optOut during the republishing period,
	// we do not need to delete the current RepublishingEntry because the Comet should write new events directly to FAILED.
	// If no republishing entry exists, we need to create one to record the last WAITING events.
	if oldObj.Spec.Subscription.CircuitBreakerOptOut != obj.Spec.Subscription.CircuitBreakerOptOut {
		err := cache.RepublishingCache.Set(context.Background(), obj.Spec.Subscription.SubscriptionId, republish.RepublishingCache{
			SubscriptionId: obj.Spec.Subscription.SubscriptionId,
		})
		if err != nil {
			log.Error().Msgf("failed with err: %v to set republishing cache", err)
			return
		}

		// Maybe delete HealthCheckData here?

		cbMessage, err := cache.CircuitBreakers.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, obj.Spec.Subscription.SubscriptionId)
		if err != nil {
			log.Error().Msgf("failed with err: %v to get circuit breaker", err)
			return
		}

		if cbMessage != nil {
			circuit_breaker.CloseCircuitBreaker(*cbMessage)
		}
	}
}

func (sl *SubscriptionListener) OnDelete(event *hazelcast.EntryNotified) {}

func (sl *SubscriptionListener) OnError(event *hazelcast.EntryNotified, err error) {}
