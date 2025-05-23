// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

var mockHandler *Handler

func GetMockHandler(t *testing.T, shouldFail bool) *Handler {
	mockConfig := mocks.NewTestConfig()

	mockConfig.Net.MaxOpenRequests = 1
	mockConfig.Version = sarama.V0_11_0_0

	mockProducer := mocks.NewSyncProducer(t, mockConfig)
	if shouldFail {
		mockProducer.ExpectSendMessageAndFail(errors.New("Could not send message with id"))
		mockProducer.ExpectSendMessageAndFail(errors.New("Could not send message with id"))
	} else {
		mockProducer.ExpectSendMessageAndSucceed()
		mockProducer.ExpectSendMessageAndSucceed()
	}

	mockConsumer := mocks.NewConsumer(t, mockConfig)
	mockConsumer.ExpectConsumePartition("test-topic", 0, 0).YieldMessage(&sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	})

	mockHandler = &Handler{
		Consumer: mockConsumer,
		Producer: mockProducer,
	}

	return mockHandler
}

func TestHandler_RepublishMessage_NoError(t *testing.T) {
	mockHandler = GetMockHandler(t, false)

	message := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	}

	err := mockHandler.RepublishMessage(nil, message, "", "", false)
	assert.NoError(t, err)
}

func TestHandler_RepublishMessage_Error(t *testing.T) {
	mockHandler = GetMockHandler(t, true)

	message := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	}

	err := mockHandler.RepublishMessage(nil, message, "", "", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Could not send message with id")
}
