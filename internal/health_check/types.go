// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package health_check

import (
	"context"
	"time"
)

type HealthCheck struct {
	Environment       string    `json:"environment"`
	Method            string    `json:"method"`
	CallbackUrl       string    `json:"callbackUrl"`
	RepublishingCount int       `json:"republishingCount"`
	LastChecked       time.Time `json:"lastChecked"`
	LastCheckedStatus int       `json:"lastCheckedStatus"`
}

type PreparedHealthCheckData struct {
	Ctx              context.Context `mapstructure:"ctx"`
	HealthCheckKey   string          `mapstructure:"healthCheckKey"`
	HealthCheckEntry HealthCheck     `mapstructure:"healthCheckEntry"`
	IsAcquired       bool            `mapstructure:"isAcquired"`
}
