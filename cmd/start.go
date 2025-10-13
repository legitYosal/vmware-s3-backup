package cmd

import (
	"github.com/spf13/cobra"
)

// startCmd represents the start command. Like 'list', it's a parent command.
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a backup or restore process.",
	Long:  `The start command is used to initiate various long-running operations like backup cycles or restores.`,
}

func init() {
	rootCmd.AddCommand(startCmd)
}
