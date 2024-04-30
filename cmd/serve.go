package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golaris/config"
	"golaris/service"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the actual service",
	Run:   startGolarisService,
}

func startGolarisService(cmd *cobra.Command, args []string) {
	log.Info().Msg("Starting Golaris service")
	config.LoadConfiguration()

	service.InitializeService()
	listenApiPort()
}

func listenApiPort() {
	service.Listen(config.Current.Port)
}
