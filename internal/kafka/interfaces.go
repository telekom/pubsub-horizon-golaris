// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/telekom/pubsub-horizon-go/message"
)

type HandlerInterface interface {
	PickMessage(message message.StatusMessage) (*sarama.ConsumerMessage, error)
	RepublishMessage(message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string, errorParams bool) error
}
