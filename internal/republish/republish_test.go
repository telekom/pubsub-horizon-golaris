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
	republishPendingEventsFunc = func(ctx context.Context, subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
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

	republishPendingEventsFunc = func(ctx context.Context, subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
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

func TestRepublishEvents(t *testing.T) {
	defer test.ClearCaches()

	// Initialize mocks
	mockMongo := new(test.MockMongoHandler)
	mockKafka := new(test.MockKafkaHandler)

	mockPicker := new(test.MockPicker)
	test.InjectMockPicker(mockPicker)

	// Replace real handlers with mocks
	mongo.CurrentConnection = mockMongo
	kafka.CurrentHandler = mockKafka

	// Set configurations for the test
	config.Current.Republishing.BatchSize = 10

	// Mock data
	subscriptionId := "sub123"
	ctx := context.Background()

	// Set up RepublishingCache entry and lock it so cancellation check passes
	cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})
	lockCtx := cache.RepublishingCache.NewLockContext(ctx)
	cache.RepublishingCache.TryLockWithTimeout(lockCtx, subscriptionId, 100*time.Millisecond)
	defer cache.RepublishingCache.Unlock(lockCtx, subscriptionId)

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
	mockMongo.On("FindWaitingMessages", mock.Anything, mock.Anything, subscriptionId).Return(dbMessages, nil, nil).Once()

	mockPicker.On("Pick", mock.AnythingOfType("*message.StatusMessage")).Return(&kafkaMessage, nil).Twice()
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

	RepublishPendingEvents(lockCtx, subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

	// Assertions
	mockMongo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
	mockPicker.AssertExpectations(t)
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
	err := ForceDelete(ctx, testSubscriptionId)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
	assertions.NoError(err, "error should be nil when deleting a locked republishing entry")
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
	err := ForceDelete(ctx, testSubscriptionId)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
	assertions.NoError(err, "error should be nil when deleting an unlocked republishing entry")
}

func TestRepublishEventsThrottled(t *testing.T) {
	defer test.ClearCaches()

	// Initialize mocks
	mockMongo := new(test.MockMongoHandler)
	mockKafka := new(test.MockKafkaHandler)

	mockPicker := new(test.MockPicker)
	test.InjectMockPicker(mockPicker)

	// Replace real handlers with mocks
	mongo.CurrentConnection = mockMongo
	kafka.CurrentHandler = mockKafka

	// Mock data
	subscriptionId := "sub123"
	numDbMessages := 50
	redeliveriesPerSecond := 10
	ctx := context.Background()

	// Set up RepublishingCache entry and lock it so cancellation check passes
	cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})
	lockCtx := cache.RepublishingCache.NewLockContext(ctx)
	cache.RepublishingCache.TryLockWithTimeout(lockCtx, subscriptionId, 100*time.Millisecond)
	defer cache.RepublishingCache.Unlock(lockCtx, subscriptionId)

	// Set configurations for the test
	config.Current.Republishing.BatchSize = int64(numDbMessages + 1)
	config.Current.Republishing.ThrottlingIntervalTime = 1 * time.Second

	dbMessages := test.GenerateStatusMessages("test-topic", 1, 100, numDbMessages)

	kafkaMessage := sarama.ConsumerMessage{Value: []byte("test-content")}

	// Expectations for the batch
	mockMongo.On("FindWaitingMessages", mock.Anything, mock.Anything, subscriptionId).Return(dbMessages, nil, nil).Once()

	mockPicker.On("Pick", mock.AnythingOfType("*message.StatusMessage")).Return(&kafkaMessage, nil).Times(len(dbMessages))
	mockKafka.On("RepublishMessage", mock.AnythingOfType("*sarama.ConsumerMessage"), "CALLBACK", "http://new-callbackUrl/callback").Return(nil).Times(len(dbMessages))

	// Call the function under test
	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:        "sub123",
				DeliveryType:          enum.DeliveryTypeCallback,
				Callback:              "http://new-callbackUrl/callback",
				RedeliveriesPerSecond: redeliveriesPerSecond,
			},
		},
	}

	RepublishPendingEvents(lockCtx, subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

	// Assertions
	mockMongo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
	mockPicker.AssertExpectations(t)
}

func TestRepublishEventsUnThrottled(t *testing.T) {
	defer test.ClearCaches()

	// Initialize mocks
	mockMongo := new(test.MockMongoHandler)
	mockKafka := new(test.MockKafkaHandler)

	mockPicker := new(test.MockPicker)
	test.InjectMockPicker(mockPicker)

	// Replace real handlers with mocks
	mongo.CurrentConnection = mockMongo
	kafka.CurrentHandler = mockKafka

	// Mock data
	subscriptionId := "sub124"
	numDbMessages := 50
	redeliveriesPerSecond := 0
	ctx := context.Background()

	// Set up RepublishingCache entry and lock it so cancellation check passes
	cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})
	lockCtx := cache.RepublishingCache.NewLockContext(ctx)
	cache.RepublishingCache.TryLockWithTimeout(lockCtx, subscriptionId, 100*time.Millisecond)
	defer cache.RepublishingCache.Unlock(lockCtx, subscriptionId)

	// Set configurations for the test
	config.Current.Republishing.BatchSize = int64(numDbMessages + 1)
	config.Current.Republishing.ThrottlingIntervalTime = 1 * time.Second

	dbMessages := test.GenerateStatusMessages("test-topic", 1, 100, numDbMessages)

	kafkaMessage := sarama.ConsumerMessage{Value: []byte("test-content")}

	// Expectations for the batch
	mockMongo.On("FindWaitingMessages", mock.Anything, mock.Anything, subscriptionId).Return(dbMessages, nil, nil).Once()

	mockPicker.On("Pick", mock.AnythingOfType("*message.StatusMessage")).Return(&kafkaMessage, nil).Times(len(dbMessages))
	mockKafka.On("RepublishMessage", mock.AnythingOfType("*sarama.ConsumerMessage"), "CALLBACK", "http://new-callbackUrl/callback").Return(nil).Times(len(dbMessages))

	// Call the function under test
	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:        "sub124",
				DeliveryType:          enum.DeliveryTypeCallback,
				Callback:              "http://new-callbackUrl/callback",
				RedeliveriesPerSecond: redeliveriesPerSecond,
			},
		},
	}

	RepublishPendingEvents(lockCtx, subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

	// Assertions
	mockMongo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
	mockPicker.AssertExpectations(t)
}

func TestForceDeleteStopsConcurrentGoroutinesViaContainsKey(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	subscriptionId := "test-concurrent-cancel"
	ctx := context.Background()

	// Set up RepublishingCache entry
	republishingCacheEntry := RepublishingCacheEntry{
		SubscriptionId:   subscriptionId,
		RepublishingUpTo: time.Now(),
	}
	err := cache.RepublishingCache.Set(ctx, subscriptionId, republishingCacheEntry)
	assertions.NoError(err)

	// Channels to track goroutine cancellation detection
	goroutine1Cancelled := make(chan bool, 1)
	goroutine2Cancelled := make(chan bool, 1)

	// Simulate long-running processing that checks ContainsKey per batch
	simulateRepublishing := func(cancelledCh chan bool) {
		for {
			exists, err := cache.RepublishingCache.ContainsKey(ctx, subscriptionId)
			if err != nil {
				cancelledCh <- false
				return
			}
			if !exists {
				cancelledCh <- true
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Start two goroutines simulating two replicas processing the same subscription
	go simulateRepublishing(goroutine1Cancelled)
	go simulateRepublishing(goroutine2Cancelled)

	// Give goroutines time to start and perform initial ContainsKey checks
	time.Sleep(200 * time.Millisecond)

	// ForceDelete the entry (simulating cancellation from another replica)
	err = ForceDelete(ctx, subscriptionId)
	assertions.NoError(err)

	// Verify both goroutines detect cancellation via ContainsKey returning false
	select {
	case cancelled := <-goroutine1Cancelled:
		assertions.True(cancelled, "goroutine 1 should detect cancellation via ContainsKey")
	case <-time.After(5 * time.Second):
		assertions.Fail("goroutine 1 did not detect cancellation within timeout")
	}

	select {
	case cancelled := <-goroutine2Cancelled:
		assertions.True(cancelled, "goroutine 2 should detect cancellation via ContainsKey")
	case <-time.After(5 * time.Second):
		assertions.Fail("goroutine 2 did not detect cancellation within timeout")
	}

	// Verify entry is gone
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, subscriptionId))
}

func TestForceDeleteCancelsViaLockCheck(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	subscriptionId := "test-lock-cancel"
	ctx := cache.RepublishingCache.NewLockContext(context.Background())

	// Set up entry and acquire lock (simulating HandleRepublishingEntry)
	err := cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})
	assertions.NoError(err)
	acquired, err := cache.RepublishingCache.TryLockWithTimeout(ctx, subscriptionId, 100*time.Millisecond)
	assertions.True(acquired)
	assertions.NoError(err)

	// Entry exists and is locked — the check in RepublishPendingEvents should pass
	exists, err := cache.RepublishingCache.ContainsKey(ctx, subscriptionId)
	assertions.NoError(err)
	assertions.True(exists)
	locked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	assertions.NoError(err)
	assertions.True(locked)

	// ForceDelete removes the lock and entry
	err = ForceDelete(ctx, subscriptionId)
	assertions.NoError(err)

	// After ForceDelete: entry gone AND lock gone
	exists, err = cache.RepublishingCache.ContainsKey(ctx, subscriptionId)
	assertions.NoError(err)
	assertions.False(exists, "entry should be deleted")
	locked, err = cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	assertions.NoError(err)
	assertions.False(locked, "lock should be force-unlocked")
}
