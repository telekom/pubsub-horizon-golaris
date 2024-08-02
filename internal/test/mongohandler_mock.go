// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"pubsub-horizon-golaris/internal/config"
	"time"
)

type MockMongoHandler struct {
	client *mongodrv.Client
	config *config.Mongo
	mock.Mock
}

func (m *MockMongoHandler) FindWaitingMessages(timestamp time.Time, subscriptionId string) ([]message.StatusMessage, error) {
	args := m.Called(timestamp, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindDeliveringMessagesByDeliveryType(timestamp time.Time) ([]message.StatusMessage, error) {
	args := m.Called(timestamp)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindProcessedMessagesByDeliveryTypeSSE(timestamp time.Time, subscriptionId string) ([]message.StatusMessage, error) {
	args := m.Called(timestamp, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindFailedMessagesWithCallbackUrlNotFoundException(timestamp time.Time) ([]message.StatusMessage, error) {
	args := m.Called(timestamp)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}
