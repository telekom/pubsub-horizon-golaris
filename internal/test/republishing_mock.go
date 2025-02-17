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

type RepublishingMockMap struct {
	mock.Mock
}

func (r *RepublishingMockMap) Get(ctx context.Context, key interface{}) (interface{}, error) {
	args := r.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (r *RepublishingMockMap) Set(ctx context.Context, key interface{}, value interface{}) error {
	args := r.Called(ctx, key, value)
	return args.Error(0)
}

func (r *RepublishingMockMap) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	args := r.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (r *RepublishingMockMap) TryLockWithLease(ctx context.Context, key interface{}, duration time.Duration) (bool, error) {
	args := r.Called(ctx, key, duration)
	return args.Bool(0), args.Error(1)
}

func (d *RepublishingMockMap) TryLockWithLeaseAndTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	args := d.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (r *RepublishingMockMap) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	args := r.Called(ctx)
	return args.Get(0).([]types.Entry), args.Error(1)
}

func (r *RepublishingMockMap) NewLockContext(ctx context.Context) context.Context {
	args := r.Called(ctx)
	return args.Get(0).(context.Context)
}

func (r *RepublishingMockMap) Delete(ctx context.Context, key interface{}) error {
	args := r.Called(ctx, key)
	return args.Error(0)
}

func (r *RepublishingMockMap) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	args := r.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (r *RepublishingMockMap) ForceUnlock(ctx context.Context, key interface{}) error {
	args := r.Called(ctx, key)
	return args.Error(0)
}

func (r *RepublishingMockMap) Unlock(ctx context.Context, key interface{}) error {
	args := r.Called(ctx, key)
	return args.Error(0)
}

func (r *RepublishingMockMap) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	args := r.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (r *RepublishingMockMap) Clear(ctx context.Context) error {
	args := r.Called(ctx)
	return args.Error(0)
}

func (r *RepublishingMockMap) Lock(ctx context.Context, key interface{}) error {
	args := r.Called(ctx, key)
	return args.Error(0)
}
