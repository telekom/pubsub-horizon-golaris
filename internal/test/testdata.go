// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/types"
	"time"
)

func NewTestCbMessage(testSubscriptionId string) message.CircuitBreakerMessage {
	testCircuitBreakerMessage := message.CircuitBreakerMessage{
		SubscriptionId: testSubscriptionId,
		Status:         enum.CircuitBreakerStatusOpen,
		LoopCounter:    0,
		LastOpened:     types.NewTimestamp(time.Now().UTC()),
		LastModified:   types.NewTimestamp(time.Now().UTC()),
	}
	return testCircuitBreakerMessage
}

func NewTestSubscriptionResource(testSubscriptionId string, testCallbackUrl string, testEnvironment string) *resource.SubscriptionResource {
	testSubscriptionResource := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:        testSubscriptionId,
				Callback:              testCallbackUrl,
				EnforceGetHealthCheck: false,
			},
			Environment: testEnvironment,
		},
	}
	return testSubscriptionResource
}
