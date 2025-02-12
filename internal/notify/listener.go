package notify

import (
	"context"
	"eni.telekom.de/galileo/client/options"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/utils"
	"strings"
	"time"
)

type NotificationListener struct{}

func (n NotificationListener) OnAdd(event *hazelcast.EntryNotified, obj message.CircuitBreakerMessage) {
	if obj.LoopCounter == 0 {
		if err := notifyConsumer(&obj); err != nil {
			log.Error().
				Str("subscriptionId", obj.SubscriptionId).
				Err(err).
				Msg("Failed to send notification when circuit-breaker was created")
		}
	}
}

func (n NotificationListener) OnUpdate(event *hazelcast.EntryNotified, obj message.CircuitBreakerMessage, oldObj message.CircuitBreakerMessage) {
	if obj.LoopCounter == 0 && oldObj.LoopCounter > 0 {
		if err := notifyConsumer(&obj); err != nil {
			log.Error().
				Str("subscriptionId", obj.SubscriptionId).
				Err(err).
				Msg("Failed to send notification when circuit-breaker was updated")
		}
	}
}

func (n NotificationListener) OnDelete(event *hazelcast.EntryNotified) {
	// Nothing to do here
}

func (n NotificationListener) OnError(event *hazelcast.EntryNotified, err error) {
	log.Error().
		Err(err).
		Msg("Could not listen for circuit breaker changes")
}

func notifyConsumer(cbMessage *message.CircuitBreakerMessage) error {
	log.Debug().
		Str("subscriptionId", cbMessage.SubscriptionId).
		Msg("Sending notification for open circuit-breaker")
	subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, cbMessage.SubscriptionId)
	if err != nil {
		return err
	}

	if subscription != nil {
		label, ok := subscription.Metadata.Annotations["ei.telekom.de/origin.namespace"].(string)

		if ok {
			mailConfig := config.Current.Notifications.Mail

			callbackUrlParts := strings.SplitN(subscription.Spec.Subscription.Callback, "url=", 2)
			callbackUrl := utils.IfThenElse(len(callbackUrlParts) > 1, callbackUrlParts[1], callbackUrlParts[0])

			subject := utils.ReplaceWithMap(mailConfig.Subject, map[string]string{
				"$environment": cbMessage.Environment,
				"$application": strings.Replace(subscription.Spec.Subscription.SubscriberId, label+"--", "", 1),
			})

			notifyOpts := options.Notify().
				SetParameters([]string{label}).
				SetData(map[string]any{
					"display": map[string]any{
						"english": true,
						"german":  true,
					},
					"cb": map[string]any{
						"environment": cbMessage.Environment,
						"callbackUrl": callbackUrl,
						"eventType":   subscription.Spec.Subscription.Type,
						"lastOpened":  cbMessage.LastOpened.ToTime().Format(time.RFC3339),
						"status":      cbMessage.Status.String(),
					},
				}).
				SetSender(mailConfig.Sender).
				SetSenderName(mailConfig.SenderName).
				SetTemplate(mailConfig.Template).
				SetSubject(subject)

			if err := CurrentHandler.SendNotification(context.Background(), notifyOpts); err != nil {
				log.Error().Err(err).Msg("Could not send notification")
			}
		}
	}

	return nil
}
