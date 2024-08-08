package test

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/mock"
	"time"
)

type WaitingMockHandler struct {
	mock.Mock
}

func (w *WaitingMockHandler) Get(ctx context.Context, key interface{}) (interface{}, error) {
	args := w.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (w *WaitingMockHandler) Set(ctx context.Context, key interface{}, value interface{}) error {
	args := w.Called(ctx, key, value)
	return args.Error(0)
}

func (w *WaitingMockHandler) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	args := w.Called(ctx, key, timeout)
	return args.Bool(0), args.Error(1)
}

func (w *WaitingMockHandler) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	args := w.Called(ctx)
	return args.Get(0).([]types.Entry), args.Error(1)
}

func (w *WaitingMockHandler) NewLockContext(ctx context.Context) context.Context {
	args := w.Called(ctx)
	return args.Get(0).(context.Context)
}

func (w *WaitingMockHandler) Delete(ctx context.Context, key interface{}) error {
	args := w.Called(ctx, key)
	return args.Error(0)
}

func (w *WaitingMockHandler) Unlock(ctx context.Context, key interface{}) error {
	args := w.Called(ctx, key)
	return args.Error(0)
}

func (w *WaitingMockHandler) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	args := w.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (w *WaitingMockHandler) ForceUnlock(ctx context.Context, key interface{}) error {
	args := w.Called(ctx, key)
	return args.Error(0)
}

func (w *WaitingMockHandler) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	args := w.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (w *WaitingMockHandler) Clear(ctx context.Context) error {
	args := w.Called(ctx)
	return args.Error(0)
}

func (w *WaitingMockHandler) Lock(ctx context.Context, key interface{}) error {
	args := w.Called(ctx, key)
	return args.Error(0)
}
