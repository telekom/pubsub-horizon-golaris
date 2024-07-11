package test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"golaris/internal/config"
)

type MockKafkaHandler struct {
	client *mongodrv.Client
	config *config.Mongo
	mock.Mock
}

func (m *MockKafkaHandler) PickMessage(topic string, partition *int32, offset *int64) (*sarama.ConsumerMessage, error) {
	args := m.Called(topic, partition, offset)
	return args.Get(0).(*sarama.ConsumerMessage), args.Error(1)
}

func (m *MockKafkaHandler) RepublishMessage(message *sarama.ConsumerMessage) error {
	args := m.Called(message)
	return args.Error(0)
}
