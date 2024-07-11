package test

import (
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/config"
	"time"
)

type MockMongoHandler struct {
	client *mongodrv.Client
	config *config.Mongo
	mock.Mock
}

// ToDo: Here we need to discuss which FAILED events we want to republish!

func (m *MockMongoHandler) FindFailedMessagesWithXYZException(status string, timestamp time.Time, pageable options.FindOptions) ([]message.StatusMessage, error) {
	args := m.Called(status, timestamp, pageable)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindDeliveringMessagesByDeliveryType(status string, timestamp time.Time, pageable options.FindOptions, deliveryType string) ([]message.StatusMessage, error) {
	args := m.Called(status, timestamp, pageable, deliveryType)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindWaitingMessages(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error) {
	args := m.Called(timestamp, pageable, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}
