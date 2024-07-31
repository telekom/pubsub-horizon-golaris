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
	republishPendingEventsFunc = func(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) {}

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

	republishPendingEventsFunc = func(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) {}

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

func TestRepublishEvents(t *testing.T) {
	// Initialize mocks
	mockMongo := new(test.MockMongoHandler)
	mockKafka := new(test.MockKafkaHandler)

	// Replace real handlers with mocks
	mongo.CurrentConnection = mockMongo
	kafka.CurrentHandler = mockKafka

	// Set configurations for the test
	config.Current.Republishing.BatchSize = 10

	// Mock data
	subscriptionId := "sub123"

	partitionValue1 := int32(1)
	offsetValue1 := int64(100)
	partitionValue2 := int32(1)
	offsetValue2 := int64(101)
	dbMessages := []message.StatusMessage{
		{Topic: "test-topic", Coordinates: &message.Coordinates{Partition: &partitionValue1, Offset: &offsetValue1}},
		{Topic: "test-topic", Coordinates: &message.Coordinates{Partition: &partitionValue2, Offset: &offsetValue2}},
	}

	kafkaMessage := sarama.ConsumerMessage{Value: []byte("test-content")}

	// Expectations for the batch
	mockMongo.On("FindWaitingMessages", mock.Anything, mock.Anything, subscriptionId).Return(dbMessages, nil).Once()

	mockKafka.On("PickMessage", mock.AnythingOfType("message.StatusMessage")).Return(&kafkaMessage, nil).Twice()
	mockKafka.On("RepublishMessage", mock.AnythingOfType("*sarama.ConsumerMessage"), "CALLBACK", "http://new-callbackUrl/callback").Return(nil).Twice()

	// Call the function under test
	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId: "sub123",
				DeliveryType:   enum.DeliveryTypeCallback,
				Callback:       "http://new-callbackUrl/callback",
			},
		},
	}

	RepublishPendingEvents(subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

	// Assertions
	mockMongo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
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
