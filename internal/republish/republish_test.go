package republish

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/mongo"
	"golaris/internal/test"
	"os"
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

// Mock structures for Mongo and Kafka handlers

type MockKafkaHandler struct {
	mock.Mock
}

// Mock methods for KafkaHandler
func (k *MockKafkaHandler) PickMessage(topic string, partition int32, offset int64) (sarama.ConsumerMessage, error) {
	args := k.Called(topic, partition, offset)
	return args.Get(0).(sarama.ConsumerMessage), args.Error(1)
}

func (k *MockKafkaHandler) RepublishMessage(message sarama.ConsumerMessage) error {
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
	mockMongo := new(test.MockMongoHandler)
	mockKafka := new(test.MockKafkaHandler)

	// Replace real handlers with mocks
	mongo.CurrentConnection = mockMongo
	kafka.CurrentHandler = mockKafka

	// Set configurations for the test
	config.Current.Republishing.BatchSize = 10

	// Mock data
	subscriptionId := "test-subscription"

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
	mockKafka.On("PickMessage", "test-topic", &partitionValue1, &offsetValue1).Return(&kafkaMessage, nil).Once()
	mockKafka.On("PickMessage", "test-topic", &partitionValue2, &offsetValue2).Return(&kafkaMessage, nil).Once()
	mockKafka.On("RepublishMessage", &kafkaMessage).Return(nil).Twice()

	// Call the function under test
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
