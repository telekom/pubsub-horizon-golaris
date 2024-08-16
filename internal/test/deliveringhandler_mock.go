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

type DeliveringMockHandler struct {
	mock.Mock
}

func (d *DeliveringMockHandler) Get(ctx context.Context, key interface{}) (interface{}, error) {
	args := d.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (d *DeliveringMockHandler) Set(ctx context.Context, key interface{}, value interface{}) error {
	args := d.Called(ctx, key, value)
	return args.Error(0)
}

func (d *DeliveringMockHandler) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	args := d.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (d *DeliveringMockHandler) TryLockWithLeaseAndTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	args := d.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (d *DeliveringMockHandler) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	args := d.Called(ctx)
	return args.Get(0).([]types.Entry), args.Error(1)
}

func (d *DeliveringMockHandler) NewLockContext(ctx context.Context) context.Context {
	args := d.Called(ctx)
	return args.Get(0).(context.Context)
}

func (d *DeliveringMockHandler) Delete(ctx context.Context, key interface{}) error {
	args := d.Called(ctx, key)
	return args.Error(0)
}

func (f *DeliveringMockHandler) Unlock(ctx context.Context, key interface{}) error {
	args := f.Called(ctx, key)
	return args.Error(0)
}

func (d *DeliveringMockHandler) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	args := d.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (d *DeliveringMockHandler) ForceUnlock(ctx context.Context, key interface{}) error {
	args := d.Called(ctx, key)
	return args.Error(0)
}

func (d *DeliveringMockHandler) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	args := d.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (d *DeliveringMockHandler) Clear(ctx context.Context) error {
	args := d.Called(ctx)
	return args.Error(0)
}

func (d *DeliveringMockHandler) Lock(ctx context.Context, key interface{}) error {
	args := d.Called(ctx, key)
	return args.Error(0)
}
