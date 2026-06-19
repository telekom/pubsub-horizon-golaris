// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/tracing"
)

type MockKafkaHandler struct {
	mock.Mock
}

func (m *MockKafkaHandler) RepublishMessage(
	traceCtx *tracing.TraceContext,
	message *sarama.ConsumerMessage,
	newDeliveryType, newCallbackUrl string,
	errorParams bool,
) error {
	args := m.Called(message, newDeliveryType, newCallbackUrl)
	return args.Error(0)
}
