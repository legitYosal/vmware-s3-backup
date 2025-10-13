package cmd

import (
	"github.com/spf13/cobra"
)

// listCmd represents the list command. It doesn't do anything on its own,
// but rather serves as a parent for other 'list' subcommands.
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List various resources like VMs, backups, etc.",
	Long:  `The list command provides access to listing different types of resources managed by the backup tool.`,
	// Run: func(cmd *cobra.Command, args []string) {
	//  // By not providing a Run function, this command will show its help text
	//  // if called without a subcommand.
	// },
}

func init() {
	rootCmd.AddCommand(listCmd)
}
