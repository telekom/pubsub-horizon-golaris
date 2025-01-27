// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/stretchr/testify/mock"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/test"
	"testing"
)

func TestCheckWaitingEvents(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockWaitingHandler := new(test.MockWaitingHandler)
	WaitingHandlerService = mockWaitingHandler

	mockCircuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = mockCircuitBreakerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	// Prepare testdata
	var mockedDbSubscriptionIds = []string{"subscription-1", "subscription-2"}
	mockedCircuitBreakerSubscriptionsMap := map[string]struct{}{"subscription-1": struct{}{}, "subscription-2": struct{}{}}

	// Prepare mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)

	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)

	mockWaitingHandler.On("GetCircuitBreakerSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)
	mockWaitingHandler.On("GetRepublishingSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)

	// Call function to test
	waitingHandler := new(waitingHandler)
	waitingHandler.CheckWaitingEvents()

	// Assertions
	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything)

}
