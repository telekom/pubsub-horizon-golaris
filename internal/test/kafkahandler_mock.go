// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/tracing"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"pubsub-horizon-golaris/internal/config"
)

type MockKafkaHandler struct {
	client *mongodrv.Client
	config *config.Mongo
	mock.Mock
}

func (m *MockKafkaHandler) PickMessage(status message.StatusMessage) (*sarama.ConsumerMessage, error) {
	args := m.Called(status)
	return args.Get(0).(*sarama.ConsumerMessage), args.Error(1)
}

func (m *MockKafkaHandler) RepublishMessage(traceCtx *tracing.TraceContext, message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string, errorParams bool) error {
	args := m.Called(message, newDeliveryType, newCallbackUrl)
	return args.Error(0)
}
