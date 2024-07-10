package republish

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/test"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	test.DockerMutex.Lock()
	test.SetupDocker(&test.Options{
		MongoDb:   false,
		Hazelcast: true,
	})
	config.Current = test.BuildTestConfig()
	cache.Initialize()
	code := m.Run()

	test.TeardownDocker()
	time.Sleep(500 * time.Millisecond)
	test.DockerMutex.Unlock()
	os.Exit(code)
}

// Mock structures for Mongo and Kafka handlers
type MockMongoHandler struct {
	mock.Mock
}

type MockKafkaHandler struct {
	mock.Mock
}

// Mock methods for MongoHandler
func (m *MockMongoHandler) FindWaitingMessages(now time.Time, opts *options.FindOptions, subscriptionId string) ([]DBMessage, error) {
	args := m.Called(now, opts, subscriptionId)
	return args.Get(0).([]DBMessage), args.Error(1)
}

// Mock methods for KafkaHandler
func (k *MockKafkaHandler) PickMessage(topic string, partition int32, offset int64) (KafkaMessage, error) {
	args := k.Called(topic, partition, offset)
	return args.Get(0).(KafkaMessage), args.Error(1)
}

func (k *MockKafkaHandler) RepublishMessage(message KafkaMessage) error {
	args := k.Called(message)
	return args.Error(0)
}

func TestHandleRepublishingEntry_Acquired(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Mock repubslishWaitingEventsFunc
	republishWaitingEventsFunc = func(subscriptionId string) {}

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	ctx := context.Background()

	republishingCacheEntry := RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)

	// Call the function under test
	HandleRepublishingEntry(testSubscriptionResource)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}

func TestHandleRepublishingEntry_NotAcquired(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Mock repubslishWaitingEventsFunc
	republishWaitingEventsFunc = func(subscriptionId string) {}

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)

	ctx := context.Background()

	republishingCacheEntry := RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)
	cache.RepublishingCache.Lock(ctx, testSubscriptionId)

	// Call the function under test
	HandleRepublishingEntry(testSubscriptionResource)

	// Assertions
	assertions.True(cache.RepublishingCache.IsLocked(ctx, testSubscriptionId))

	// Unlock the cache entry
	defer cache.RepublishingCache.Unlock(ctx, testSubscriptionId)
}

func TestRepublishWaitingEvents(t *testing.T) {
	// Initialize mocks
	mockMongo := new(MockMongoHandler)
	mockKafka := new(MockKafkaHandler)

	// Replace real handlers with mocks
	mongoHandler = mockMongo
	kafkaHandler = mockKafka

	// Set configurations for the test
	config.Current.Republishing.BatchSize = 10

	subscriptionId := "test-subscription"

	// Mock data
	dbMessages := []DBMessage{
		{Topic: "test-topic", Coordinates: &Coordinates{Partition: 1, Offset: 100}},
		{Topic: "test-topic", Coordinates: &Coordinates{Partition: 1, Offset: 101}},
	}

	kafkaMessage := KafkaMessage{Content: "test-content"}

	// Expectations for the batch
	mockMongo.On("FindWaitingMessages", mock.Anything, mock.Anything, subscriptionId).Return(dbMessages, nil).Once()
	mockKafka.On("PickMessage", "test-topic", int32(1), int64(100)).Return(kafkaMessage, nil).Once()
	//mockKafka.On("RepublishMessage", kafkaMessage).Return(nil).Once()
	mockKafka.On("PickMessage", "test-topic", int32(1), int64(101)).Return(kafkaMessage, nil).Once()
	mockKafka.On("RepublishMessage", kafkaMessage).Return(nil).Twice()

	// Call the function
	republishWaitingEvents(subscriptionId)

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
	republishingCacheEntry := RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
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
	republishingCacheEntry := RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
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
	republishingCacheEntry := RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
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
	republishingCacheEntry := RepublishingCache{SubscriptionId: testSubscriptionId, RepublishingUpTo: time.Now()}
	cache.RepublishingCache.Set(ctx, testSubscriptionId, republishingCacheEntry)

	// call the function under test
	ForceDelete(ctx, testSubscriptionId)

	// Assertions
	assertions.False(cache.RepublishingCache.ContainsKey(ctx, testSubscriptionId))
}
