// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/mock"
	"time"
)

type HealthCheckMockMap struct {
	mock.Mock
}

func (h *HealthCheckMockMap) Get(ctx context.Context, key interface{}) (interface{}, error) {
	args := h.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (h *HealthCheckMockMap) Delete(ctx context.Context, key interface{}) error {
	args := h.Called(ctx, key)
	return args.Error(0)
}

func (h *HealthCheckMockMap) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) NewLockContext(ctx context.Context) context.Context {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) Unlock(ctx context.Context, key interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) ForceUnlock(ctx context.Context, key interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) Set(ctx context.Context, key interface{}, value interface{}) error {
	args := h.Called(ctx, key, value)
	return args.Error(0)
}

func (h *HealthCheckMockMap) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) Clear(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) Lock(ctx context.Context, key interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (h *HealthCheckMockMap) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	//TODO implement me
	panic("implement me")
}
