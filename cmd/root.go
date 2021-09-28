package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "plan-change-capturer",
		Short: "A tool used to capture plan changes among different versions of TiDB",
		Long: `PCC(plan-change-capturer) is a tool used to capture plan changes among different versions of TiDB.
Please see the tutorial for more information(https://docs.google.com/document/d/10gOlEylBfexiTs3Ysocpvgc8so8NRJIYb5l4a9WYtbY/edit?usp=sharing).`,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize()
	//rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newExportCmd())
	rootCmd.AddCommand(newImportCmd())
	rootCmd.AddCommand(newCaptureCmd())
	rootCmd.AddCommand(newCheckCmd())
}
