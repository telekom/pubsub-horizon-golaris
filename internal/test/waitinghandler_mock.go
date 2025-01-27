package test

import "github.com/stretchr/testify/mock"

type MockWaitingHandler struct {
	mock.Mock
}

func (f *MockWaitingHandler) CheckWaitingEvents() {
	return
}

func (f *MockWaitingHandler) GetCircuitBreakerSubscriptionsMap() (map[string]struct{}, error) {
	args := f.Called()
	return args.Get(0).(map[string]struct{}), args.Error(1)
}

func (f *MockWaitingHandler) GetRepublishingSubscriptionsMap() (map[string]struct{}, error) {
	args := f.Called()
	return args.Get(0).(map[string]struct{}), args.Error(1)
}
