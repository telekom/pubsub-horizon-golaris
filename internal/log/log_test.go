// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"bytes"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetValidLogLevel(t *testing.T) {
	var buf bytes.Buffer
	log.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	SetLogLevel("debug")
	assert.Equal(t, zerolog.DebugLevel, log.Logger.GetLevel())
}

func TestSetInvalidLogLevel(t *testing.T) {
	var buf bytes.Buffer
	log.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	SetLogLevel("invalid")
	assert.Equal(t, zerolog.InfoLevel, log.Logger.GetLevel())
}
