// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/mock"
	"time"
)

type MockHandlerCache struct {
	mock.Mock
}

func (c *MockHandlerCache) Get(ctx context.Context, key interface{}) (interface{}, error) {
	args := c.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (c *MockHandlerCache) Set(ctx context.Context, key interface{}, value interface{}) error {
	args := c.Called(ctx, key, value)
	return args.Error(0)
}

func (c *MockHandlerCache) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	args := c.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (c *MockHandlerCache) TryLockWithLease(ctx context.Context, key interface{}, duration time.Duration) (bool, error) {
	args := c.Called(ctx, key, duration)
	return args.Bool(0), args.Error(1)
}

func (c *MockHandlerCache) TryLockWithLeaseAndTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	args := c.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (c *MockHandlerCache) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	args := c.Called(ctx)
	return args.Get(0).([]types.Entry), args.Error(1)
}

func (c *MockHandlerCache) NewLockContext(ctx context.Context) context.Context {
	args := c.Called(ctx)
	return args.Get(0).(context.Context)
}

func (c *MockHandlerCache) Delete(ctx context.Context, key interface{}) error {
	args := c.Called(ctx, key)
	return args.Error(0)
}

func (c *MockHandlerCache) Unlock(ctx context.Context, key interface{}) error {
	args := c.Called(ctx, key)
	return args.Error(0)
}

func (c *MockHandlerCache) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	args := c.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (c *MockHandlerCache) ForceUnlock(ctx context.Context, key interface{}) error {
	args := c.Called(ctx, key)
	return args.Error(0)
}

func (c *MockHandlerCache) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	args := c.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (c *MockHandlerCache) Clear(ctx context.Context) error {
	args := c.Called(ctx)
	return args.Error(0)
}

func (c *MockHandlerCache) Lock(ctx context.Context, key interface{}) error {
	args := c.Called(ctx, key)
	return args.Error(0)
}
