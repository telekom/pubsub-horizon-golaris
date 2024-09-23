package throttling

import (
	"context"
	"github.com/1pkg/gohalt"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"time"
)

type SubscriptionAwareThrottler struct {
	throttler    gohalt.Throttler
	subscription *resource.SubscriptionResource
}

func CreateSubscriptionAwareThrottler(subscription *resource.SubscriptionResource) SubscriptionAwareThrottler {
	deliveryType := string(subscription.Spec.Subscription.DeliveryType)
	redeliveriesPerSecond := subscription.Spec.Subscription.RedeliveriesPerSecond

	var throttler gohalt.Throttler

	if deliveryType == "sse" || deliveryType == "server_sent_event" || redeliveriesPerSecond <= 0 {
		throttler = gohalt.NewThrottlerEcho(nil)
	} else {
		log.Info().Msgf("Creating throttler with %d redeliveries", redeliveriesPerSecond)
		throttler = gohalt.NewThrottlerTimed(uint64(redeliveriesPerSecond), config.Current.Republishing.ThrottlingIntervalTime, 0)
	}

	return SubscriptionAwareThrottler{
		throttler:    throttler,
		subscription: subscription,
	}
}

func (t SubscriptionAwareThrottler) Throttle() {
	subscriptionId := t.subscription.Spec.Subscription.SubscriptionId

	for canceled := cache.GetCancelStatus(subscriptionId); !canceled; {
		if err := t.throttler.Acquire(context.Background()); err != nil {
			// throttling quota is drained
			sleepInterval := time.Millisecond * 10
			totalSleepTime := config.Current.Republishing.ThrottlingIntervalTime
			for slept := time.Duration(0); slept < totalSleepTime; slept += sleepInterval {
				if cache.GetCancelStatus(subscriptionId) {
					break
				}

				time.Sleep(sleepInterval)
			}
		} else {
			return // free throttling quota
		}
	}
}

func (t SubscriptionAwareThrottler) Release() {
	t.throttler.Release(context.Background())
}
