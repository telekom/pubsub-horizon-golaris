// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck

import (
	"context"
	"time"
)

type HealthCheckCacheEntry struct {
	Environment       string    `json:"environment"`
	Method            string    `json:"method"`
	CallbackUrl       string    `json:"callbackUrl"`
	LastChecked       time.Time `json:"lastChecked"`
	LastCheckedStatus int       `json:"lastCheckedStatus"`
}

type PreparedHealthCheckData struct {
	Ctx              context.Context       `json:"ctx"`
	HealthCheckKey   string                `json:"healthCheckKey"`
	HealthCheckEntry HealthCheckCacheEntry `json:"healthCheckEntry"`
	IsAcquired       bool                  `json:"isAcquired"`
}
