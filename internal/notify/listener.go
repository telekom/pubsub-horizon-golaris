// Copyright 2025 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"eni.telekom.de/galileo/client/options"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/utils"
	"strings"
	"time"
)

type NotificationListener struct{}

func (n NotificationListener) OnAdd(event *hazelcast.EntryNotified, obj message.CircuitBreakerMessage) {
	notificationConfig := config.Current.Notifications
	circuitBreakerOpen := obj.Status == enum.CircuitBreakerStatusOpen

	// Circuit breaker initialized
	if obj.LoopCounter == 0 && circuitBreakerOpen {
		template := notificationConfig.Mail.Templates.OpenCircuitBreaker
		if err := notifyConsumer(&obj, notificationConfig.Mail.Subject.OpenCircuitBreaker, template); err != nil {
			log.Error().
				Str("subscriptionId", obj.SubscriptionId).
				Err(err).
				Msg("Failed to send notification when circuit-breaker was created")
		}
	}
}

func (n NotificationListener) OnUpdate(event *hazelcast.EntryNotified, obj message.CircuitBreakerMessage, oldObj message.CircuitBreakerMessage) {
	notificationConfig := config.Current.Notifications
	circuitBreakerOpen := obj.Status == enum.CircuitBreakerStatusOpen

	// Circuit-breaker reset
	if circuitBreakerOpen && (obj.LoopCounter == 0 && oldObj.LoopCounter > 0) {
		template := notificationConfig.Mail.Templates.OpenCircuitBreaker
		if err := notifyConsumer(&obj, notificationConfig.Mail.Subject.OpenCircuitBreaker, template); err != nil {
			log.Error().
				Str("event", "openCircuitBreaker").
				Str("subscriptionId", obj.SubscriptionId).
				Err(err).
				Msg("Failed to send notification when circuit-breaker was updated")
		}
	}

	// Loop detected
	if circuitBreakerOpen && (obj.LoopCounter > oldObj.LoopCounter && obj.LoopCounter%notificationConfig.LoopModulo == 0) {
		template := notificationConfig.Mail.Templates.LoopDetected
		if err := notifyConsumer(&obj, notificationConfig.Mail.Subject.LoopDetected, template); err != nil {
			log.Error().
				Str("event", "loop").
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

func notifyConsumer(cbMessage *message.CircuitBreakerMessage, subject string, template string) error {
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

			subject := utils.ReplaceWithMap(subject, map[string]string{
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
						"loopCounter": cbMessage.LoopCounter,
						"status":      cbMessage.Status.String(),
					},
				}).
				SetSender(mailConfig.Sender).
				SetSenderName(mailConfig.SenderName).
				SetTemplate(template).
				SetSubject(subject)

			if err := CurrentSender.SendNotification(context.Background(), notifyOpts); err != nil {
				log.Error().Err(err).Msg("Could not send notification")
			}
		}
	}

	return nil
}
