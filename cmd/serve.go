// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/cobra"
	"golaris/internal/api"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/log"
	"golaris/internal/mongo"
	"golaris/internal/scheduler"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the actual scheduler",
	Run:   startGolarisService,
}

func initialize() {
	config.Load()

	log.SetLogLevel(config.Current.LogLevel)

	cache.Initialize()
	mongo.Initialize()
	kafka.Initialize()

	// TODO Mock cb-messages for local development until comet is adapted
	//go test.CreateMockedCircuitBreakerMessages(0)
}

func startGolarisService(cmd *cobra.Command, args []string) {
	initialize()

	scheduler.StartScheduler()
	api.Listen(config.Current.Port)
}
