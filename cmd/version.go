package cmd

import (
	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "TODO",
		Long:  `TODO`,
		Run: func(cmd *cobra.Command, args []string) {
		}}
	return cmd
}
