package listener

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/republish"
)

type SubscriptionListener struct{}

func (sl *SubscriptionListener) OnAdd(event *hazelcast.EntryNotified, obj resource.SubscriptionResource) {
}

func (sl *SubscriptionListener) OnUpdate(event *hazelcast.EntryNotified, obj resource.SubscriptionResource, oldObj resource.SubscriptionResource) {
	// If the delivery type of the subscription has changed from SSE to Callback, add the subscription to the republishing cache
	if oldObj.Spec.Subscription.DeliveryType == "SSE" && obj.Spec.Subscription.DeliveryType == "Callback" {
		err := cache.RepublishingCache.Set(context.Background(), obj.Spec.Subscription.SubscriptionId, republish.RepublishingCache{
			SubscriptionId: obj.Spec.Subscription.SubscriptionId,
		})
		if err != nil {
			return
		}
	}
}

func (sl *SubscriptionListener) OnDelete(event *hazelcast.EntryNotified) {}

func (sl *SubscriptionListener) OnError(event *hazelcast.EntryNotified, err error) {}
