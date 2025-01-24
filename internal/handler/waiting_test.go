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

var Mock = mock.Mock{}

//func getCircuitBreakerCacheMap() (map[string]struct{}, error) {
//	args := Mock.Called()
//	return args.Get(0).(map[string]struct{}), args.Error(1)
//}

func TestCheckWaitingEvents(t *testing.T) {

	mockMongo := new(test.MockMongoHandler)
	mongo.CurrentConnection = mockMongo

	mockCircuitBreakerCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = mockCircuitBreakerCache

	mockRepublishingCache := new(test.RepublishingMockMap)
	cache.RepublishingCache = mockRepublishingCache

	mockHandlerCache := new(test.MockHandlerCache)
	cache.HandlerCache = mockHandlerCache

	// Testdata
	var mockedDbSubscriptionIds = []string{"subscription-1", "subscription-2"}
	mockedCircuitBreakerSubscriptionsMap := map[string]struct{}{"subscription-1": struct{}{}, "subscription-2": struct{}{}}

	// Mocks
	mockHandlerCache.On("NewLockContext", mock.Anything).Return(context.Background())
	mockHandlerCache.On("TryLockWithTimeout", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockHandlerCache.On("Unlock", mock.Anything, mock.Anything).Return(nil)

	mockMongo.On("FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything).Return(mockedDbSubscriptionIds, nil)

	funcGetCircuitBreakerCacheMap = func() (map[string]struct{}, error) {
		return mockedCircuitBreakerSubscriptionsMap, nil
	}

	funcGetRepublishingCacheMap = func() (map[string]struct{}, error) {
		return nil, nil
	}

	// Call the function to test
	CheckWaitingEvents()

	// Assertions
	mockMongo.AssertExpectations(t)
	mockMongo.AssertCalled(t, "FindDistinctSubscriptionsForWaitingEvents", mock.Anything, mock.Anything)

}
