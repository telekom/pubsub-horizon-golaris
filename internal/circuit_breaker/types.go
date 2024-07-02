// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuit_breaker

import (
	"context"
	"golaris/internal/health_check"
)

type PreparedHealthCheckData struct {
	ctx              context.Context          `mapstructure:"ctx"`
	healthCheckKey   string                   `mapstructure:"healthCheckKey"`
	healthCheckEntry health_check.HealthCheck `mapstructure:"healthCheckEntry"`
	isAcquired       bool                     `mapstructure:"isAcquired"`
}
