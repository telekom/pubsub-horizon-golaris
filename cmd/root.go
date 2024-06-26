package cmd

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(initCmd, serveCmd)
}

var rootCmd = &cobra.Command{
	Use:   "Golaris",
	Short: "Our scheduler for handling circuitBreakerMessages",
}
