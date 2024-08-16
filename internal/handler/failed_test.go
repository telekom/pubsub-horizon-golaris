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
)

func TestCheckFailedEvents(t *testing.T) {
	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockKafka := new(test.MockKafkaHandler)
	kafka.CurrentHandler = mockKafka

	mockCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = mockCache

	failedHandler := new(test.FailedMockHandler)
	cache.FailedHandler = failedHandler

	mockPicker := new(test.MockPicker)
	test.InjectMockPicker(mockPicker)

	failedHandler.On("NewLockContext", mock.Anything).Return(context.Background())
	failedHandler.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	failedHandler.On("Unlock", mock.Anything, mock.Anything).Return(nil)

	config.Current.Republishing.BatchSize = 5

	partitionValue := int32(1)
	offsetValue := int64(100)

	dbMessage := []message.StatusMessage{
		{
			Topic:          "test-topic",
			Status:         "FAILED",
			SubscriptionId: "sub123",
			DeliveryType:   enum.DeliveryTypeSse,
			Coordinates: &message.Coordinates{
				Partition: &partitionValue,
				Offset:    &offsetValue,
			}},
	}

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId: "sub123",
				DeliveryType:   enum.DeliveryTypeSse,
			},
		},
	}

	mockMongo.On("FindFailedMessagesWithCallbackUrlNotFoundException", mock.Anything, mock.Anything).Return(dbMessage, nil, nil)
	mockCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").Return(subscription, nil)

	expectedKafkaMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 1,
		Offset:    100,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	}

	mockPicker.On("Pick", mock.AnythingOfType("*message.StatusMessage")).Return(expectedKafkaMessage, nil)
	mockKafka.On("RepublishMessage", mock.Anything, "SERVER_SENT_EVENT", "").Return(nil)

	CheckFailedEvents()

	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindFailedMessagesWithCallbackUrlNotFoundException", mock.Anything, mock.Anything)

	mockCache.AssertExpectations(t)
	mockCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123")

	mockKafka.AssertExpectations(t)
	mockKafka.AssertCalled(t, "RepublishMessage", expectedKafkaMessage, "SERVER_SENT_EVENT", "")
	mockPicker.AssertCalled(t, "Pick", mock.AnythingOfType("*message.StatusMessage"))
}
