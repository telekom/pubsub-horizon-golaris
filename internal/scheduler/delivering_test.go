package scheduler

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/test"
	"testing"
)

func TestCheckDeliveringEvents(t *testing.T) {
	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockKafka := new(test.MockKafkaHandler)
	kafka.CurrentHandler = mockKafka

	config.Current.Republishing.DeliveringStatesOffsetMins = 30

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
			DeliveryType:   "callback",
			Coordinates: &message.Coordinates{
				Partition: &partitionValue2,
				Offset:    &offsetValue2,
			}},
	}

	mockMongo.On("FindDeliveringMessagesByDeliveryType", mock.Anything, mock.Anything).Return(dbMessages, nil, nil)

	expectedKafkaMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	}

	mockKafka.On("PickMessage", mock.AnythingOfType("message.StatusMessage")).Return(expectedKafkaMessage, nil)
	mockKafka.On("RepublishMessage", expectedKafkaMessage, "", "").Return(nil)

	checkDeliveringEvents()

	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindDeliveringMessagesByDeliveryType", mock.Anything, mock.Anything)

	mockKafka.AssertExpectations(t)
	mockKafka.AssertCalled(t, "PickMessage", mock.AnythingOfType("message.StatusMessage"))
	mockKafka.AssertCalled(t, "RepublishMessage", expectedKafkaMessage, "", "")
}
