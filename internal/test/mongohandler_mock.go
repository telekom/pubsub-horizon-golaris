// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"pubsub-horizon-golaris/internal/config"
	"time"
)

type MockMongoHandler struct {
	client *mongodrv.Client
	config *config.Mongo
	mock.Mock
}

func (m *MockMongoHandler) FindWaitingMessages(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error) {
	args := m.Called(timestamp, pageable, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindDeliveringMessagesByDeliveryType(status string, timestamp time.Time, pageable options.FindOptions, deliveryType string) ([]message.StatusMessage, error) {
	args := m.Called(status, timestamp, pageable, deliveryType)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindWaitingAndDeliveringMessages(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error) {
	args := m.Called(timestamp, pageable, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindProcessedMessagesByDeliveryTypeSSE(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error) {
	args := m.Called(timestamp, pageable, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}

func (m *MockMongoHandler) FindFailedMessagesWithXYZException(status string, timestamp time.Time, pageable options.FindOptions) ([]message.StatusMessage, error) {
	args := m.Called(status, timestamp, pageable)
	return args.Get(0).([]message.StatusMessage), args.Error(1)
}
