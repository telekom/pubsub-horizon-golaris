// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"os"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/test"
	"testing"
)

func TestMain(m *testing.M) {
	test.SetupDocker(&test.Options{
		MongoDb:   false,
		Hazelcast: true,
	})
	config.Current = test.BuildTestConfig()
	cache.Initialize()

	code := m.Run()

	test.TeardownDocker()
	os.Exit(code)
}
