// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/kafka"
)

type MockPicker struct {
	mock.Mock
}

func (m *MockPicker) Close() {
	// Nothing to do here
}

func (m *MockPicker) Pick(status *message.StatusMessage) (*sarama.ConsumerMessage, error) {
	var args = m.Called(status)
	return args.Get(0).(*sarama.ConsumerMessage), args.Error(1)
}

func InjectMockPicker(mockPicker *MockPicker) {
	kafka.NewPicker = func() (kafka.MessagePicker, error) {
		return mockPicker, nil
	}
}
