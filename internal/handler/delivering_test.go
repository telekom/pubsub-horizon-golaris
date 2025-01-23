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

// mockStep represents a single paging result returned by FindDeliveringMessagesByDeliveryType.
type mockStep struct {
	OutMessages   []message.StatusMessage
	OutNextCursor any
	OutError      error
}

// deliveringTestCase holds the data for table-driven tests of CheckDeliveringEvents.
type deliveringTestCase struct {
	name            string
	mongoSteps      []mockStep
	dbMessages      []message.StatusMessage
	kafkaMessages   []sarama.ConsumerMessage
	republishErrors []error
}

// intPtr and int64Ptr are helper functions for pointer fields in message coordinates.
func intPtr(i int32) *int32   { return &i }
func int64Ptr(i int64) *int64 { return &i }

func TestCheckDeliveringEvents_TableDriven(t *testing.T) {
	// Configure global parameters for the republishing logic.
	config.Current.Republishing.BatchSize = 5
	config.Current.Republishing.DeliveringStatesOffset = 30 * time.Minute

	// Define test cases in a table-driven style.
	testCases := []deliveringTestCase{
		{
			name: "No events -> no republishing",
			// Only one step returning an empty slice of messages.
			mongoSteps: []mockStep{
				{
					OutMessages:   []message.StatusMessage{},
					OutNextCursor: nil,
					OutError:      nil,
				},
			},
			dbMessages:      []message.StatusMessage{},
			kafkaMessages:   []sarama.ConsumerMessage{},
			republishErrors: []error{},
		},
		{
			name: "Multi-page fetch -> all republished successfully",
			/*
			 * Three steps total:
			 *   1) Two messages
			 *   2) Two messages
			 *   3) Zero messages -> loop exits
			 */
			mongoSteps: []mockStep{
				{
					OutMessages: []message.StatusMessage{
						{
							Topic:          "test-topic",
							Status:         "DELIVERING",
							SubscriptionId: "sub123",
							DeliveryType:   "callback",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(100),
							},
						},
						{
							Topic:          "test-topic",
							Status:         "DELIVERING",
							SubscriptionId: "sub123",
							DeliveryType:   "callback",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(101),
							},
						},
					},
					OutNextCursor: "cursor_page_1",
					OutError:      nil,
				},
				{
					OutMessages: []message.StatusMessage{
						{
							Topic:          "test-topic",
							Status:         "DELIVERING",
							SubscriptionId: "sub123",
							DeliveryType:   "callback",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(102),
							},
						},
						{
							Topic:          "test-topic",
							Status:         "DELIVERING",
							SubscriptionId: "sub123",
							DeliveryType:   "callback",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(103),
							},
						},
					},
					OutNextCursor: nil,
					OutError:      nil,
				},
				{
					// Zero messages -> loop exits
					OutMessages:   []message.StatusMessage{},
					OutNextCursor: nil,
					OutError:      nil,
				},
			},
			dbMessages: []message.StatusMessage{
				{
					Topic:          "test-topic",
					Status:         "DELIVERING",
					SubscriptionId: "sub123",
					DeliveryType:   "callback",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(100),
					},
				},
				{
					Topic:          "test-topic",
					Status:         "DELIVERING",
					SubscriptionId: "sub123",
					DeliveryType:   "callback",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(101),
					},
				},
				{
					Topic:          "test-topic",
					Status:         "DELIVERING",
					SubscriptionId: "sub123",
					DeliveryType:   "callback",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(102),
					},
				},
				{
					Topic:          "test-topic",
					Status:         "DELIVERING",
					SubscriptionId: "sub123",
					DeliveryType:   "callback",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(103),
					},
				},
			},
			kafkaMessages: []sarama.ConsumerMessage{
				{Topic: "test-topic", Partition: 1, Offset: 100, Value: []byte("test-content-1")},
				{Topic: "test-topic", Partition: 1, Offset: 101, Value: []byte("test-content-2")},
				{Topic: "test-topic", Partition: 1, Offset: 102, Value: []byte("test-content-3")},
				{Topic: "test-topic", Partition: 1, Offset: 103, Value: []byte("test-content-4")},
			},
			republishErrors: []error{nil, nil, nil, nil},
		},
		// Additional scenarios can be added here as needed.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Sets up all mocks.
			mockMongo := new(test.MockMongoHandler)
			mongo.CurrentConnection = mockMongo

			mockKafka := new(test.MockKafkaHandler)
			kafka.CurrentHandler = mockKafka

			mockPicker := new(test.MockPicker)
			test.InjectMockPicker(mockPicker)

			mockCache := new(test.SubscriptionMockCache)
			cache.SubscriptionCache = mockCache

			deliveringHandler := new(test.DeliveringMockHandler)
			cache.DeliveringHandler = deliveringHandler

			// Mocks for lock mechanism.
			deliveringHandler.
				On("NewLockContext", mock.Anything).
				Return(context.Background()).
				Once()
			deliveringHandler.
				On("TryLockWithTimeout", mock.Anything, cache.DeliveringLockKey, mock.Anything).
				Return(true, nil).
				Once()
			deliveringHandler.
				On("Unlock", mock.Anything, cache.DeliveringLockKey).
				Return(nil).
				Once()

			// Mocks for multi-page Mongo fetch.
			for _, step := range tc.mongoSteps {
				mockMongo.
					On("FindDeliveringMessagesByDeliveryType",
						mock.Anything, // time.Time
						mock.Anything, // lastCursor
					).
					Return(step.OutMessages, step.OutNextCursor, step.OutError).
					Once()
			}

			// Picker and Kafka mocks are only configured if dbMessages are present.
			for i, dbMsg := range tc.dbMessages {
				mockPicker.
					On("Pick", &dbMsg).
					Return(&tc.kafkaMessages[i], nil).
					Once()

				errVal := tc.republishErrors[i]
				mockKafka.
					On("RepublishMessage",
						mock.Anything,
						&tc.kafkaMessages[i],
						mock.AnythingOfType("string"),
						mock.AnythingOfType("string"),
						false,
					).
					Return(errVal).Once()
			}

			// Subscription cache mock if relevant for the scenario.
			if len(tc.dbMessages) > 0 {
				subscription := &resource.SubscriptionResource{
					Spec: struct {
						Subscription resource.Subscription `json:"subscription"`
						Environment  string                `json:"environment"`
					}{
						Subscription: resource.Subscription{
							SubscriptionId: "sub123",
							DeliveryType:   enum.DeliveryTypeCallback,
							Callback:       "http://test-callback.com",
						},
					},
				}
				mockCache.
					On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").
					Return(subscription, nil).
					Maybe()
			}

			// Calls the function under test.
			CheckDeliveringEvents()

			// Asserts that all expected mock calls were executed.
			mockMongo.AssertExpectations(t)
			mockPicker.AssertExpectations(t)
			mockKafka.AssertExpectations(t)
			mockCache.AssertExpectations(t)
			deliveringHandler.AssertExpectations(t)

			// Additional verification can be performed based on scenario requirements.
			// For "no events," asserts that neither RepublishMessage nor Pick was called.
			if len(tc.dbMessages) == 0 {
				mockPicker.AssertNotCalled(t, "Pick", mock.Anything)
				mockKafka.AssertNotCalled(t, "RepublishMessage", mock.Anything)
			}
		})
	}
}
