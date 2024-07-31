// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"github.com/telekom/pubsub-horizon-go/message"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type HandlerInterface interface {
	FindWaitingMessages(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error)
	FindDeliveringMessagesByDeliveryType(status string, timestamp time.Time, pageable options.FindOptions, deliveryType string) ([]message.StatusMessage, error)
	FindProcessedMessagesByDeliveryTypeSSE(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error)
	FindFailedMessagesWithCallbackUrlNotFoundException(timestamp time.Time, pageable *options.FindOptions) ([]message.StatusMessage, error)
}
