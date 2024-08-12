// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"github.com/telekom/pubsub-horizon-go/message"
	"time"
)

type HandlerInterface interface {
	FindWaitingMessages(timestamp time.Time, lastCursor any, subscriptionId string) ([]message.StatusMessage, any, error)
	FindDeliveringMessagesByDeliveryType(timestamp time.Time, lastCursor any) ([]message.StatusMessage, any, error)
	FindProcessedMessagesByDeliveryTypeSSE(timestamp time.Time, lastCursor any, subscriptionId string) ([]message.StatusMessage, any, error)
	FindFailedMessagesWithCallbackUrlNotFoundException(timestamp time.Time, lastCursor any) ([]message.StatusMessage, any, error)
}
