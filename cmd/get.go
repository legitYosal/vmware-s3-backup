package cmd

import (
	"github.com/spf13/cobra"
)

// getCmd represents the get command. It doesn't do anything on its own,
// but rather serves as a parent for other 'get' subcommands.
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get various resources like VMs, backups, etc.",
	Long:  `The get command provides access to getting different types of resources managed by the backup tool.`,
}

func init() {
	rootCmd.AddCommand(getCmd)
}
