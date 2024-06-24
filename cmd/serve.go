package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golaris/internal/config"
	"golaris/internal/golaris"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the actual service",
	Run:   startGolarisService,
}

func startGolarisService(cmd *cobra.Command, args []string) {
	log.Info().Msg("Starting Golaris service")
	config.LoadConfiguration()

	golaris.InitializeService()
	listenApiPort()
}

func listenApiPort() {
	golaris.Listen(config.Current.Port)
}
