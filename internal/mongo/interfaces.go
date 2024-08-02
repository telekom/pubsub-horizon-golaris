// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"github.com/telekom/pubsub-horizon-go/message"
	"time"
)

type HandlerInterface interface {
	FindWaitingMessages(timestamp time.Time, subscriptionId string) ([]message.StatusMessage, error)
	FindDeliveringMessagesByDeliveryType(timestamp time.Time) ([]message.StatusMessage, error)
	FindProcessedMessagesByDeliveryTypeSSE(timestamp time.Time, subscriptionId string) ([]message.StatusMessage, error)
	FindFailedMessagesWithCallbackUrlNotFoundException(timestamp time.Time) ([]message.StatusMessage, error)
}
