// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/message"
)

type MockMongoHandler struct {
	mock.Mock
}

func (m *MockMongoHandler) FindDistinctSubscriptionsForWaitingEvents(beginTimestamp, endTimestamp time.Time) ([]string, error) {
	args := m.Called(beginTimestamp, endTimestamp)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockMongoHandler) FindWaitingMessages(
	timestamp time.Time,
	lastTimestamp any,
	subscriptionId string,
) ([]message.StatusMessage, any, error) {
	args := m.Called(timestamp, lastTimestamp, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Get(1), args.Error(2)
}

func (m *MockMongoHandler) FindDeliveringMessagesByDeliveryType(
	timestamp time.Time,
	lastTimestamp any,
) ([]message.StatusMessage, any, error) {
	args := m.Called(timestamp, lastTimestamp)
	return args.Get(0).([]message.StatusMessage), args.Get(1), args.Error(2)
}

func (m *MockMongoHandler) FindProcessedMessagesByDeliveryTypeSSE(
	timestamp time.Time,
	lastTimestamp any,
	subscriptionId string,
) ([]message.StatusMessage, any, error) {
	args := m.Called(timestamp, lastTimestamp, subscriptionId)
	return args.Get(0).([]message.StatusMessage), args.Get(1), args.Error(2)
}

func (m *MockMongoHandler) FindFailedMessagesWithCallbackUrlNotFoundException(
	timestamp time.Time,
	lastTimestamp any,
) ([]message.StatusMessage, any, error) {
	args := m.Called(timestamp, lastTimestamp)
	return args.Get(0).([]message.StatusMessage), args.Get(1), args.Error(2)
}
