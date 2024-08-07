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

type FailedMockHandler struct {
	mock.Mock
}

func (f *FailedMockHandler) Get(ctx context.Context, key interface{}) (interface{}, error) {
	args := f.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (f *FailedMockHandler) Set(ctx context.Context, key interface{}, value interface{}) error {
	args := f.Called(ctx, key, value)
	return args.Error(0)
}

func (f *FailedMockHandler) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	args := f.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (f *FailedMockHandler) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	args := f.Called(ctx)
	return args.Get(0).([]types.Entry), args.Error(1)
}

func (f *FailedMockHandler) NewLockContext(ctx context.Context) context.Context {
	args := f.Called(ctx)
	return args.Get(0).(context.Context)
}

func (f *FailedMockHandler) Delete(ctx context.Context, key interface{}) error {
	args := f.Called(ctx, key)
	return args.Error(0)
}

func (f *FailedMockHandler) Unlock(ctx context.Context, key interface{}) error {
	args := f.Called(ctx, key)
	return args.Error(0)
}

func (f *FailedMockHandler) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	args := f.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (f *FailedMockHandler) ForceUnlock(ctx context.Context, key interface{}) error {
	args := f.Called(ctx, key)
	return args.Error(0)
}

func (f *FailedMockHandler) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	args := f.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (f *FailedMockHandler) Clear(ctx context.Context) error {
	args := f.Called(ctx)
	return args.Error(0)
}

func (f *FailedMockHandler) Lock(ctx context.Context, key interface{}) error {
	args := f.Called(ctx, key)
	return args.Error(0)
}
