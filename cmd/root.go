package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "plan-change-capturer",
		Short: "A generator for Cobra based Applications",
		Long: `Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newTransportCmd())
	rootCmd.AddCommand(newCaptureCmd())
	rootCmd.AddCommand(newQueryCmd())
}
