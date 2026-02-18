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

func TestHandleRepublishingEntry_CancelledSkipsDelete(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	testSubscriptionId := "testCancelledSubscription"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	// Mock republishPendingEventsFunc to return ErrCancelled
	republishPendingEventsFunc = func(ctx context.Context, subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
		// Simulate what happens in putCloseCircuitBreakerById:
		// ForceDelete removes the old entry, then Set creates a new one.
		// The goroutine detects the absence via ContainsKey and returns ErrCancelled.
		// By the time we return, a new entry already exists.
		cache.RepublishingCache.Delete(ctx, testSubscriptionId)
		cache.RepublishingCache.Set(ctx, testSubscriptionId, RepublishingCacheEntry{SubscriptionId: testSubscriptionId})
		return ErrCancelled
	}

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	ctx := context.Background()
	republishingCacheEntry := RepublishingCacheEntry{
		SubscriptionId:   testSubscriptionId,
		RepublishingUpTo: time.Now(),
	}

	err := cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	assertions.NoError(err)

	// Call the function under test
	HandleRepublishingEntry(testSubscriptionResource)

	// The new entry set by the mock should still exist (not deleted by HandleRepublishingEntry)
	exists, checkErr := cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId)
	assertions.NoError(checkErr)
	assertions.True(exists,
		"entry should still exist because HandleRepublishingEntry must not delete on ErrCancelled")
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

	// Set up RepublishingCache entry so ContainsKey returns true
	cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})

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

	RepublishPendingEvents(ctx, subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

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

	// Set up RepublishingCache entry so ContainsKey returns true
	cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})

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

	RepublishPendingEvents(ctx, subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

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

	// Set up RepublishingCache entry so ContainsKey returns true
	cache.RepublishingCache.Set(ctx, subscriptionId, RepublishingCacheEntry{SubscriptionId: subscriptionId})

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

	RepublishPendingEvents(ctx, subscription, RepublishingCacheEntry{SubscriptionId: subscriptionId})

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

// Integration tests for multi-replica parallelization behavior using real Hazelcast.

func TestLeaseBasedLock_AutoReleasesWithoutUnlock(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	lockKey := "test-lease-auto-release"
	lease := 2 * time.Second

	ctx := cache.HandlerCache.NewLockContext(context.Background())

	// Acquire lock with short lease, do NOT unlock (simulating process crash)
	acquired, err := cache.HandlerCache.TryLockWithLeaseAndTimeout(ctx, lockKey, lease, 100*time.Millisecond)
	assertions.True(acquired, "should acquire lock")
	assertions.NoError(err)

	// Immediately after acquiring, lock should be held
	isLocked, err := cache.HandlerCache.IsLocked(context.Background(), lockKey)
	assertions.NoError(err)
	assertions.True(isLocked, "lock should be held immediately after acquisition")

	// Wait for lease to expire
	time.Sleep(lease + 1*time.Second)

	// Lock should have auto-released
	isLocked, err = cache.HandlerCache.IsLocked(context.Background(), lockKey)
	assertions.NoError(err)
	assertions.False(isLocked, "lock should auto-release after lease expiry without explicit Unlock")
}

func TestLeaseBasedLock_SecondReplicaAcquiresAfterLeaseExpiry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	lockKey := "test-replica-takeover"
	lease := 2 * time.Second

	// Replica A acquires lock, then "crashes" (no Unlock)
	ctxA := cache.HandlerCache.NewLockContext(context.Background())
	acquired, err := cache.HandlerCache.TryLockWithLeaseAndTimeout(ctxA, lockKey, lease, 100*time.Millisecond)
	assertions.True(acquired, "replica A should acquire lock")
	assertions.NoError(err)

	// Replica B tries to acquire immediately -- should fail (lock is held by A)
	ctxB := cache.HandlerCache.NewLockContext(context.Background())
	acquired, err = cache.HandlerCache.TryLockWithLeaseAndTimeout(ctxB, lockKey, lease, 100*time.Millisecond)
	assertions.False(acquired, "replica B should NOT acquire lock while A holds it")
	assertions.NoError(err)

	// Wait for A's lease to expire
	time.Sleep(lease + 1*time.Second)

	// Replica B tries again -- should succeed now
	acquired, err = cache.HandlerCache.TryLockWithLeaseAndTimeout(ctxB, lockKey, lease, 100*time.Millisecond)
	assertions.True(acquired, "replica B should acquire lock after A's lease expired")
	assertions.NoError(err)

	// Clean up: B unlocks
	defer cache.HandlerCache.Unlock(ctxB, lockKey)
}

func TestParallelHandlers_MutualExclusion(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	lockKey := "test-parallel-exclusion"
	lease := 5 * time.Second

	executionOrder := make(chan string, 2)

	// Simulate two replicas trying to process the same handler concurrently
	runHandler := func(replicaName string) {
		ctx := cache.HandlerCache.NewLockContext(context.Background())
		acquired, err := cache.HandlerCache.TryLockWithLeaseAndTimeout(ctx, lockKey, lease, 2*time.Second)
		if err != nil {
			return
		}
		if !acquired {
			return
		}
		defer cache.HandlerCache.Unlock(ctx, lockKey)

		// Simulate work
		time.Sleep(500 * time.Millisecond)
		executionOrder <- replicaName
	}

	// Launch both replicas concurrently
	go runHandler("replica-A")
	go runHandler("replica-B")

	// Collect results -- at least one should execute
	var executed []string
	timeout := time.After(10 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case name := <-executionOrder:
			executed = append(executed, name)
		case <-timeout:
			break
		}
	}

	// Both replicas should eventually execute (one waits for the other's lock release)
	assertions.Len(executed, 2, "both replicas should execute sequentially via distributed lock")
	assertions.NotEqual(executed[0], executed[1], "replicas should be different")
}
