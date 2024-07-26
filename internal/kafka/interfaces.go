// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/telekom/pubsub-horizon-go/tracing"
)

type HandlerInterface interface {
	PickMessage(topic string, partition *int32, offset *int64) (*sarama.ConsumerMessage, error)
	RepublishMessage(traceCtx *tracing.TraceContext, message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string) error
}
