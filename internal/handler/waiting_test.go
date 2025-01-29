// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/test"
	"testing"
)

func TestCheckWaitingEvents_NoActionNeededWhileNoWaitingEvents(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockWaitingHandler := new(test.MockWaitingHandler)
	WaitingHandlerService = mockWaitingHandler

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	// Prepare testdata
	var mockedDbSubscriptionIds []string                            // no subscriptions with waiting events in db
	mockedCircuitBreakerSubscriptionsMap := map[string]struct{}{}   // no circuit breaker entries
	mockedRepublishingSubscriptionsMap := make(map[string]struct{}) // no republishing entries

	// Prepare mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)
	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)
	mockWaitingHandler.On("GetCircuitBreakerSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)
	mockWaitingHandler.On("GetRepublishingSubscriptionsMap").Return(mockedRepublishingSubscriptionsMap, nil)
	mockRepublishingCache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Call function to test
	waitingHandler := new(waitingHandler)
	waitingHandler.CheckWaitingEvents()

	// Assertions
	t.Run("Assertions", func(t *testing.T) {
		mockHandlerCache.AssertExpectations(t) // handlerCache mocks called
		mockMongo.AssertExpectations(t)        // mongo mocks called

		// Ensure that no republishing entry is created for this scenario. No Cache needs to be read.
		mockWaitingHandler.AssertNotCalled(t, "GetCircuitBreakerSubscriptionsMap")
		mockWaitingHandler.AssertNotCalled(t, "GetRepublishingSubscriptionsMap")
		mockRepublishingCache.AssertNotCalled(t, "Set", mock.Anything, mock.Anything)
	})
}

func TestCheckWaitingEvents_NoActionNeededWhileExistingCbEntry(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockWaitingHandler := new(test.MockWaitingHandler)
	WaitingHandlerService = mockWaitingHandler

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	// Prepare testdata
	var mockedDbSubscriptionIds = []string{"subscription-1"}                                  // two subscriptions with waiting events in db
	mockedCircuitBreakerSubscriptionsMap := map[string]struct{}{"subscription-1": struct{}{}} // existing circuit breaker entries
	mockedRepublishingSubscriptionsMap := make(map[string]struct{})                           // no republishing entries

	// Prepare mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)
	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)
	mockWaitingHandler.On("GetCircuitBreakerSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)
	mockWaitingHandler.On("GetRepublishingSubscriptionsMap").Return(mockedRepublishingSubscriptionsMap, nil)
	mockRepublishingCache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Call function to test
	waitingHandler := new(waitingHandler)
	waitingHandler.CheckWaitingEvents()

	// Assertions
	t.Run("Assertions", func(t *testing.T) {
		mockHandlerCache.AssertExpectations(t)   // handlerCache mocks called
		mockMongo.AssertExpectations(t)          // mongo mocks called
		mockWaitingHandler.AssertExpectations(t) // waitingHandler mocks called

		// Ensure that no republishing entry is created for this scenario
		mockRepublishingCache.AssertNotCalled(t, "Set", mock.Anything, mock.Anything)
	})
}

func TestCheckWaitingEvents_NoActionNeededWhileExistingRepublishEntry(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockWaitingHandler := new(test.MockWaitingHandler)
	WaitingHandlerService = mockWaitingHandler

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	// Prepare testdata
	var mockedDbSubscriptionIds = []string{"subscription-1"}                                // two subscriptions with waiting events in db
	mockedCircuitBreakerSubscriptionsMap := make(map[string]struct{})                       // no circuit breaker entries
	mockedRepublishingSubscriptionsMap := map[string]struct{}{"subscription-1": struct{}{}} // existing republishing entries                                                       // no republishing entries

	// Prepare mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)
	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)
	mockWaitingHandler.On("GetCircuitBreakerSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)
	mockWaitingHandler.On("GetRepublishingSubscriptionsMap").Return(mockedRepublishingSubscriptionsMap, nil)
	mockRepublishingCache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Call function to test
	waitingHandler := new(waitingHandler)
	waitingHandler.CheckWaitingEvents()

	// Assertions
	t.Run("Assertions", func(t *testing.T) {
		mockHandlerCache.AssertExpectations(t)   // handlerCache mocks called
		mockMongo.AssertExpectations(t)          // mongo mocks called
		mockWaitingHandler.AssertExpectations(t) // waitingHandler mocks called

		// Ensure that no republishing entry is created for this scenario
		mockRepublishingCache.AssertNotCalled(t, "Set", mock.Anything, mock.Anything)
	})
}

func TestCheckWaitingEvents_ActionNeededWhileNoMatchingCacheEntries(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockWaitingHandler := new(test.MockWaitingHandler)
	WaitingHandlerService = mockWaitingHandler

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	// Prepare testdata
	var mockedDbSubscriptionIds = []string{"subscription-1", "subscription-2"}                             // two subscriptions with waiting events in db
	mockedCircuitBreakerSubscriptionsMap := map[string]struct{}{"not-matching-subscription-1": struct{}{}} // no circuit breaker entries
	mockedRepublishingSubscriptionsMap := map[string]struct{}{"not-matching-subscription-2": struct{}{}}   // no republishing entries                                                       // no republishing entries

	// Prepare mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)
	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)
	mockWaitingHandler.On("GetCircuitBreakerSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)
	mockWaitingHandler.On("GetRepublishingSubscriptionsMap").Return(mockedRepublishingSubscriptionsMap, nil)
	mockRepublishingCache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(len(mockedDbSubscriptionIds))

	// Call function to test
	waitingHandler := new(waitingHandler)
	waitingHandler.CheckWaitingEvents()

	// Assertions
	t.Run("Assertions", func(t *testing.T) {
		mockHandlerCache.AssertExpectations(t)   // handlerCache mocks called
		mockMongo.AssertExpectations(t)          // mongo mocks called
		mockWaitingHandler.AssertExpectations(t) // waitingHandler mocks called

		// Ensure that two republishing entry are created for this scenario
		mockRepublishingCache.AssertNumberOfCalls(t, "Set", len(mockedDbSubscriptionIds))
	})
}

func TestCheckWaitingEvents_ActionNeededWhileSubsetHasNoCacheEntries(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockWaitingHandler := new(test.MockWaitingHandler)
	WaitingHandlerService = mockWaitingHandler

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	// Prepare testdata
	var mockedDbSubscriptionIds = []string{"subscription-1", "subscription-2"}                // two subscriptions with waiting events in db
	mockedCircuitBreakerSubscriptionsMap := map[string]struct{}{"subscription-1": struct{}{}} // existing circuit breaker entry for one subscription         // no circuit breaker entries
	mockedRepublishingSubscriptionsMap := make(map[string]struct{})                           // no republishing entries                                                       // no republishing entries

	// Prepare mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)
	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)
	mockWaitingHandler.On("GetCircuitBreakerSubscriptionsMap").Return(mockedCircuitBreakerSubscriptionsMap, nil)
	mockWaitingHandler.On("GetRepublishingSubscriptionsMap").Return(mockedRepublishingSubscriptionsMap, nil)
	mockRepublishingCache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Call function to test
	waitingHandler := new(waitingHandler)
	waitingHandler.CheckWaitingEvents()

	// Assertions
	t.Run("Assertions", func(t *testing.T) {
		mockHandlerCache.AssertExpectations(t)   // handlerCache mocks called
		mockMongo.AssertExpectations(t)          // mongo mocks called
		mockWaitingHandler.AssertExpectations(t) // waitingHandler mocks called

		// Ensure that one republishing entry is created for this scenario
		mockRepublishingCache.AssertNumberOfCalls(t, "Set", 1)
	})
}

func TestGetCircuitBreakerSubscriptionsMap_ReturnsCorrectSubscriptions(t *testing.T) {

	// CircuitBreakerCache is needed for this test
	mockCircuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = mockCircuitBreakerCache

	// Create test data []message.CircuitBreakerMessage
	circuitBreakerEntries := []message.CircuitBreakerMessage{
		{
			SubscriptionId: "subscriptionId-1", Status: enum.CircuitBreakerStatus("OPEN"),
		},
		{
			SubscriptionId: "subscriptionId-2", Status: enum.CircuitBreakerStatus("Closed"),
		},
	}
	// Mock function call GetQuery
	mockCircuitBreakerCache.On("GetQuery", mock.Anything, mock.Anything).Return(circuitBreakerEntries, nil)

	// Call function to test
	waitingHandler := new(waitingHandler)
	subscriptions, err := waitingHandler.GetCircuitBreakerSubscriptionsMap()

	// Assertions
	assert.Equal(t, map[string]struct{}{"subscriptionId-1": {}, "subscriptionId-2": {}}, subscriptions)
	assert.Nil(t, err)
}
