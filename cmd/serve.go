// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/cobra"
	"pubsub-horizon-golaris/internal/api"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/listener"
	"pubsub-horizon-golaris/internal/log"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/notify"
	"pubsub-horizon-golaris/internal/scheduler"
	"pubsub-horizon-golaris/internal/tracing"
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
	notify.Initialize()
	listener.Initialize()
	tracing.Initialize()
}

func startGolarisService(cmd *cobra.Command, args []string) {
	initialize()

	scheduler.StartScheduler()
	api.Listen(config.Current.Port)
}
