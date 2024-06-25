package cmd

import (
	"github.com/spf13/cobra"
	"golaris/internal/api"
	"golaris/internal/config"
	"golaris/internal/scheduler"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the actual scheduler",
	Run:   startGolarisService,
}

func startGolarisService(cmd *cobra.Command, args []string) {
	scheduler.StartScheduler()
	api.Listen(config.Current.Port)
}
