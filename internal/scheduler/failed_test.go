package scheduler

import (
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

	// Testdaten
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

	// Mock-Antworten einrichten
	mockMongo.On("FindFailedMessagesWithCallbackUrlNotFoundException", mock.Anything, mock.Anything).Return(dbMessage, nil, nil)
	mockCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123").Return(subscription, nil)

	// Erwartete Kafka-Nachricht
	expectedKafkaMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 1,
		Offset:    100,
		Key:       []byte("test-key"),
		Value:     []byte(`{"uuid": "12345", "event": {"id": "67890"}}`),
	}

	// Setze die Erwartungen für den Kafka-Handler
	mockKafka.On("PickMessage", mock.AnythingOfType("message.StatusMessage")).Return(expectedKafkaMessage, nil)
	mockKafka.On("RepublishMessage", mock.Anything, "SERVER_SENT_EVENT", "").Return(nil)

	// Funktion aufrufen
	checkFailedEvents()

	// Überprüfe die Erwartungen
	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindFailedMessagesWithCallbackUrlNotFoundException", mock.Anything, mock.Anything)

	mockCache.AssertExpectations(t)
	mockCache.AssertCalled(t, "Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub123")

	mockKafka.AssertExpectations(t)
	mockKafka.AssertCalled(t, "PickMessage", mock.AnythingOfType("message.StatusMessage"))
	mockKafka.AssertCalled(t, "RepublishMessage", expectedKafkaMessage, "SERVER_SENT_EVENT", "")
}
