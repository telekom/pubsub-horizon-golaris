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

// failedTestCase holds the data for table-driven tests of CheckFailedEvents.
type failedTestCase struct {
	name            string
	mongoSteps      []mockStep
	dbMessages      []message.StatusMessage
	kafkaMessages   []sarama.ConsumerMessage
	republishErrors []error
	subscription    *resource.SubscriptionResource
}

func TestCheckFailedEvents_TableDriven(t *testing.T) {
	// Global republishing settings.
	config.Current.Republishing.BatchSize = 5

	// Table of test cases.
	testCases := []failedTestCase{
		{
			name: "No events -> no republish",
			// Only one step returning an empty slice to simulate no messages.
			mongoSteps: []mockStep{
				{
					OutMessages:   []message.StatusMessage{},
					OutNextCursor: nil,
					OutError:      nil,
				},
			},
			// No messages, so no Kafka or subscription data needed.
			dbMessages:      []message.StatusMessage{},
			kafkaMessages:   []sarama.ConsumerMessage{},
			republishErrors: []error{},
			subscription:    nil, // No subscription needed for zero messages
		},
		{
			name: "One FAILED SSE event -> republish",
			// Two steps: first returns one message, second returns an empty slice.
			mongoSteps: []mockStep{
				{
					OutMessages: []message.StatusMessage{
						{
							Topic:          "test-topic",
							Status:         "FAILED",
							SubscriptionId: "sub123",
							DeliveryType:   enum.DeliveryTypeCallback,
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(100),
							},
						},
					},
					OutNextCursor: nil,
					OutError:      nil,
				},
				{
					// Second step returns zero messages, causing the loop to exit.
					OutMessages:   []message.StatusMessage{},
					OutNextCursor: nil,
					OutError:      nil,
				},
			},
			dbMessages: []message.StatusMessage{
				{
					Topic:          "test-topic",
					Status:         "FAILED",
					SubscriptionId: "sub123",
					DeliveryType:   enum.DeliveryTypeCallback,
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(100),
					},
				},
			},
			kafkaMessages: []sarama.ConsumerMessage{
				{
					Topic:     "test-topic",
					Partition: 1,
					Offset:    100,
					Key:       []byte("test-key"),
					Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
				},
			},
			republishErrors: []error{nil},
			subscription: &resource.SubscriptionResource{
				Spec: struct {
					Subscription resource.Subscription `json:"subscription"`
					Environment  string                `json:"environment"`
				}{
					Subscription: resource.Subscription{
						SubscriptionId: "sub123",
						DeliveryType:   enum.DeliveryTypeSse,
					},
				},
			},
		},
		// Additional scenarios can be added here if desired.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Mocks for locking.
			failedHandler := new(test.FailedMockHandler)
			cache.FailedHandler = failedHandler

			failedHandler.
				On("NewLockContext", mock.Anything).
				Return(context.Background()).
				Once()
			failedHandler.
				On("TryLockWithTimeout", mock.Anything, cache.FailedLockKey, mock.Anything).
				Return(true, nil).
				Once()
			failedHandler.
				On("Unlock", mock.Anything, cache.FailedLockKey).
				Return(nil).
				Once()

			// Mongo mock to simulate multiple paging steps.
			mockMongo := new(test.MockMongoHandler)
			mongo.CurrentConnection = mockMongo

			for _, step := range tc.mongoSteps {
				mockMongo.
					On("FindFailedMessagesWithCallbackUrlNotFoundException",
						mock.Anything, // time.Time
						mock.Anything, // cursor
					).
					Return(step.OutMessages, step.OutNextCursor, step.OutError).
					Once()
			}

			// Subscription cache mock for SSE subscriptions.
			mockCache := new(test.SubscriptionMockCache)
			cache.SubscriptionCache = mockCache

			if tc.subscription != nil && len(tc.dbMessages) > 0 {
				subId := tc.subscription.Spec.Subscription.SubscriptionId
				mockCache.
					On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, subId).
					Return(tc.subscription, nil).
					Maybe()
			}

			// Picker mock and Kafka mock for republishing messages.
			mockPicker := new(test.MockPicker)
			test.InjectMockPicker(mockPicker)

			mockKafka := new(test.MockKafkaHandler)
			kafka.CurrentHandler = mockKafka

			for i, dbMsg := range tc.dbMessages {
				mockPicker.
					On("Pick", &dbMsg).
					Return(&tc.kafkaMessages[i], nil).
					Once()

				errVal := tc.republishErrors[i]
				// SSE is republished as "SERVER_SENT_EVENT" with an empty callback URL
				// according to the original unit test expectations. Adjust if needed.
				mockKafka.
					On("RepublishMessage",
						mock.Anything,
						&tc.kafkaMessages[i],
						"SERVER_SENT_EVENT",
						"",
						false,
					).
					Return(errVal).
					Once()
			}

			// Calls the function under test.
			CheckFailedEvents()

			// Assertions to ensure all mocks were triggered as expected.
			failedHandler.AssertExpectations(t)
			mockMongo.AssertExpectations(t)
			mockPicker.AssertExpectations(t)
			mockKafka.AssertExpectations(t)
			mockCache.AssertExpectations(t)

			// If no messages were given, verifies that none of the Picker/Kafka methods were called.
			if len(tc.dbMessages) == 0 {
				mockPicker.AssertNotCalled(t, "Pick", mock.Anything)
				mockKafka.AssertNotCalled(t, "RepublishMessage", mock.Anything)
			}
		})
	}
}
