package listener

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
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
	// If the delivery type of the subscription has changed from SSE to Callback, add the subscription to the republishing cache
	if oldObj.Spec.Subscription.DeliveryType == "SSE" && obj.Spec.Subscription.DeliveryType == "Callback" {
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

	if oldObj.Spec.Subscription.DeliveryType == "Callback" && obj.Spec.Subscription.DeliveryType == "SSE" {
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
}

func (sl *SubscriptionListener) OnDelete(event *hazelcast.EntryNotified) {}

func (sl *SubscriptionListener) OnError(event *hazelcast.EntryNotified, err error) {}
