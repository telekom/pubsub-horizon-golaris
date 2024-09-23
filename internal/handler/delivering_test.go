// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/test"
	"testing"
	"time"
)

func TestCheckDeliveringEvents_Success(t *testing.T) {
	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockKafka := new(test.MockKafkaHandler)
	kafka.CurrentHandler = mockKafka

	mockCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = mockCache

	mockPicker := new(test.MockPicker)
	test.InjectMockPicker(mockPicker)

	deliveringHandler := new(test.DeliveringMockHandler)
	cache.DeliveringHandler = deliveringHandler

	deliveringHandler.On("NewLockContext", mock.Anything).Return(context.Background())
	deliveringHandler.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	deliveringHandler.On("Unlock", mock.Anything, mock.Anything).Return(nil)

	config.Current.Republishing.BatchSize = 5
	config.Current.Republishing.DeliveringStatesOffset = 30 * time.Minute

	partitionValue1 := int32(1)
	offsetValue1 := int64(100)
	partitionValue2 := int32(1)
	offsetValue2 := int64(101)

	dbMessages := []message.StatusMessage{
		{
			Topic:          "test-topic",
			Status:         "DELIVERING",
			SubscriptionId: "sub123",
			DeliveryType:   "callback",
			Coordinates: &message.Coordinates{
				Partition: &partitionValue1,
				Offset:    &offsetValue1,
			}},
		{
			Topic:          "test-topic",
			Status:         "DELIVERING",
			SubscriptionId: "sub123",
			DeliveryType:   enum.DeliveryTypeCallback,
			Coordinates: &message.Coordinates{
				Partition: &partitionValue2,
				Offset:    &offsetValue2,
			}},
	}

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId: "sub123",
				DeliveryType:   enum.DeliveryTypeCallback,
			},
		},
	}

	mockMongo.On("FindDeliveringMessagesByDeliveryType", mock.Anything, mock.Anything).Return(dbMessages, nil, nil)
	mockCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").Return(subscription, nil)

	expectedKafkaMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	}

	mockPicker.On("Pick", mock.AnythingOfType("*message.StatusMessage")).Return(expectedKafkaMessage, nil)
	mockKafka.On("RepublishMessage", expectedKafkaMessage, "", "").Return(nil)

	CheckDeliveringEvents()

	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindDeliveringMessagesByDeliveryType", mock.Anything, mock.Anything)

	mockKafka.AssertExpectations(t)
	mockKafka.AssertCalled(t, "RepublishMessage", expectedKafkaMessage, "", "")
	mockPicker.AssertCalled(t, "Pick", mock.AnythingOfType("*message.StatusMessage"))
}

func TestCheckDeliveringEvents_NoEvents(t *testing.T) {
	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockKafka := new(test.MockKafkaHandler)
	kafka.CurrentHandler = mockKafka

	deliveringHandler := new(test.DeliveringMockHandler)
	cache.DeliveringHandler = deliveringHandler

	mockPicker := new(test.MockPicker)
	test.InjectMockPicker(mockPicker)

	deliveringHandler.On("NewLockContext", mock.Anything).Return(context.Background())
	deliveringHandler.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	deliveringHandler.On("Unlock", mock.Anything, mock.Anything).Return(nil)

	config.Current.Republishing.BatchSize = 5
	config.Current.Republishing.DeliveringStatesOffset = 30 * time.Minute

	mockMongo.On("FindDeliveringMessagesByDeliveryType", mock.Anything, mock.Anything).Return([]message.StatusMessage{}, nil, nil)

	CheckDeliveringEvents()

	mockKafka.AssertNotCalled(t, "RepublishMessage", mock.Anything, "", "")
	mockPicker.AssertNotCalled(t, "Pick", mock.AnythingOfType("*message.StatusMessage"))

	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindDeliveringMessagesByDeliveryType", mock.Anything, mock.Anything)

	mockKafka.AssertExpectations(t)
}
