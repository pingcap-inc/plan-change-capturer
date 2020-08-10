package cmd

import (
	"github.com/spf13/cobra"
)

func newHelpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "help",
		Short: "TODO",
		Long:  `TODO`,
		Run: func(cmd *cobra.Command, args []string) {
		}}
	return cmd
}
