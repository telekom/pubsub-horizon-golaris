package cmd

import (
	"github.com/spf13/cobra"
	"golaris/internal/api"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/mock"
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

	cache.Initialize()
	mongo.Initialize()
	kafka.Initialize()

	// TODO Mock cb-messages until comet is adapted
	mock.CreateMockedCircuitBreakerMessages(1)
}

func startGolarisService(cmd *cobra.Command, args []string) {
	initialize()

	scheduler.StartScheduler()
	api.Listen(config.Current.Port)
}
