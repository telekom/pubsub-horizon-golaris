// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"os"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/test"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	test.SetupDocker(&test.Options{
		MongoDb:   false,
		Hazelcast: true,
	})
	config.Current = test.BuildTestConfig()
	cache.Initialize()

	code := m.Run()

	test.TeardownDocker()
	os.Exit(code)
}

func TestHandleRepublishingEntry_Acquired(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Mock republishPendingEventsFunc
	republishPendingEventsFunc = func(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
		return nil
	}

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	ctx := context.Background()
	republishingCacheEntry := RepublishingCacheEntry{
		SubscriptionId:   testSubscriptionId,
		RepublishingUpTo: time.Now(),
	}

	err := cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	assertions.NoError(err, "error setting up republishing cache entry")

	// Call the function under test
	HandleRepublishingEntry(testSubscriptionResource)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

func TestHandleRepublishingEntry_NotAcquired(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	republishPendingEventsFunc = func(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
		return nil
	}

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	ctx := context.Background()

	republishingCacheEntry := RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	cache.RepublishingCache.Lock(ctx, testSubscriptionId)

	// Call the function under test
	HandleRepublishingEntry(testSubscriptionResource)

	// Assertions
	assertions.True(cache.RepublishingCache.IsLocked(ctx, testSubscriptionId))

	// Unlock the cache entry
	defer cache.RepublishingCache.Unlock(ctx, testSubscriptionId)
}

func Test_Unlock_RepublishingEntryLocked(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	ctx := context.Background()
	republishingCacheEntry := RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	cache.RepublishingCache.Lock(ctx, testSubscriptionId)

	// call the function under test
	err := Unlock(ctx, testSubscriptionId)

	// Assertions
	assertions.Nil(err)
	assertions.False(cache.RepublishingCache.IsLocked(ctx, testSubscriptionId))
}

func Test_Unlock_RepublishingEntryUnlocked(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	ctx := context.Background()
	republishingCacheEntry := RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)

	// call the function under test
	err := Unlock(ctx, testSubscriptionId)

	// Assertions
	assertions.Nil(err)
	assertions.False(cache.RepublishingCache.IsLocked(ctx, testSubscriptionId))
}

func Test_ForceDelete_RepublishingEntryLocked(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	ctx := context.Background()
	republishingCacheEntry := RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	cache.RepublishingCache.Lock(ctx, testSubscriptionId)

	// call the function under test
	ForceDelete(ctx, testSubscriptionId)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

func Test_ForceDelete_RepublishingEntryUnlocked(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	ctx := context.Background()
	republishingCacheEntry := RepublishingCacheEntry{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)

	// call the function under test
	ForceDelete(ctx, testSubscriptionId)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

// mockStep beschreibt ein "Paging-Ergebnis" für FindWaitingMessages.
type mockStep struct {
	OutMessages   []message.StatusMessage
	OutNextCursor any
	OutError      error
}

// republishTestCase enthält die Test-Daten für unser Table-Driven-Testverfahren.
type republishTestCase struct {
	name            string
	subscriptionId  string
	mongoSteps      []mockStep
	dbMessages      []message.StatusMessage
	kafkaMessages   []sarama.ConsumerMessage
	republishErrors []error
	expectedError   bool
}

// Helper
func intPtr(i int32) *int32   { return &i }
func int64Ptr(i int64) *int64 { return &i }

func TestRepublishPendingEvents_TableDriven(t *testing.T) {
	config.Current.Republishing.BatchSize = 10
	config.Current.Tracing.Enabled = true

	cache.Initialize()
	defer test.ClearCaches()

	// Test Case
	testCases := []republishTestCase{
		{
			name:           "Successful republish with multi-page fetch",
			subscriptionId: "success_multi_page",
			/*
			 * Important: We need 4 calls (the 4th call returns 0 messages => break).
			 *   1) 2 messages
			 *   2) 2 messages
			 *   3) 1 message
			 *   4) 0 messages => break
			 */
			mongoSteps: []mockStep{
				{
					OutMessages: []message.StatusMessage{
						{
							Topic: "test-topic",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(100),
							},
						},
						{
							Topic: "test-topic",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(101),
							},
						},
					},
					OutNextCursor: "success_multi_page_cursor_1",
					OutError:      nil,
				},
				{
					OutMessages: []message.StatusMessage{
						{
							Topic: "test-topic",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(102),
							},
						},
						{
							Topic: "test-topic",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(103),
							},
						},
					},
					OutNextCursor: "success_multi_page_cursor_2",
					OutError:      nil,
				},
				{
					OutMessages: []message.StatusMessage{
						{
							Topic: "test-topic",
							Coordinates: &message.Coordinates{
								Partition: intPtr(1),
								Offset:    int64Ptr(104),
							},
						},
					},
					OutNextCursor: nil,
					OutError:      nil,
				},
				{
					// 4th call: no messages => len(...) == 0 => break
					OutMessages:   []message.StatusMessage{},
					OutNextCursor: nil,
					OutError:      nil,
				},
			},

			// 2+2+1 = 5 messages in MongoDB
			dbMessages: []message.StatusMessage{
				{
					Topic: "test-topic",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(100),
					},
				},
				{
					Topic: "test-topic",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(101),
					},
				},
				{
					Topic: "test-topic",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(102),
					},
				},
				{
					Topic: "test-topic",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(103),
					},
				},
				{
					Topic: "test-topic",
					Coordinates: &message.Coordinates{
						Partition: intPtr(1),
						Offset:    int64Ptr(104),
					},
				},
			},

			// 5 corresponding Kafka messages
			kafkaMessages: []sarama.ConsumerMessage{
				{Value: []byte("test-content-1")},
				{Value: []byte("test-content-2")},
				{Value: []byte("test-content-3")},
				{Value: []byte("test-content-4")},
				{Value: []byte("test-content-5")},
			},

			// No republishing errors in this testcase
			republishErrors: []error{nil, nil, nil, nil, nil},
			expectedError:   false,
		},
	}

	// Test execution
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1) Prepare mocks
			mockMongo := new(test.MockMongoHandler)
			mockKafka := new(test.MockKafkaHandler)
			mockPicker := new(test.MockPicker)

			mongo.CurrentConnection = mockMongo
			kafka.CurrentHandler = mockKafka
			test.InjectMockPicker(mockPicker)

			// 2) Configure Mongo mock: as many "On()" calls as in tc.mongoSteps
			for _, step := range tc.mongoSteps {
				mockMongo.
					On("FindWaitingMessages", mock.Anything, mock.Anything, tc.subscriptionId).
					Return(step.OutMessages, step.OutNextCursor, step.OutError).
					Once()
			}

			// 3) Picker mock: For each dbMessage, there is one "Pick" call
			for i, dbMsg := range tc.dbMessages {
				mockPicker.
					On("Pick", &dbMsg).
					Return(&tc.kafkaMessages[i], nil).
					Once()
			}

			// 4) Kafka mock: RepublishMessage with the new interface signature,
			// expecting one "On()" call per Kafka message.
			for i, kafkaMsg := range tc.kafkaMessages {
				errVal := tc.republishErrors[i]

				mockKafka.On(
					"RepublishMessage",
					mock.Anything,                 // context.Context
					&kafkaMsg,                     // *sarama.ConsumerMessage
					mock.AnythingOfType("string"), // newDeliveryType
					mock.AnythingOfType("string"), // newCallbackUrl
					false,                         // skipTopicSuffix
				).Return(errVal).Once()

			}

			// 5) Subscription Resource
			subscription := &resource.SubscriptionResource{
				Spec: struct {
					Subscription resource.Subscription `json:"subscription"`
					Environment  string                `json:"environment"`
				}{
					Subscription: resource.Subscription{
						SubscriptionId: tc.subscriptionId,
						DeliveryType:   enum.DeliveryTypeCallback,
						Callback:       "http://test-callback.com",
					},
				},
			}

			// 6) Entry
			entry := RepublishingCacheEntry{
				SubscriptionId: tc.subscriptionId,
			}

			// 7) Run test
			err := republishPendingEvents(subscription, entry)

			// 8) Assertions
			if tc.expectedError {
				assert.Error(t, err, "Expected an error, but got none for test %s", tc.name)
			} else {
				assert.NoError(t, err, "Did not expect an error, but got one for test %s", tc.name)
			}

			mockMongo.AssertExpectations(t)
			mockKafka.AssertExpectations(t)
			mockPicker.AssertExpectations(t)
		})
	}
}
