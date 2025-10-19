package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var getBackupCmd = &cobra.Command{
	Use:   "backup [VM_KEY]",
	Short: "Get a backup with detailed information",
	Long:  `Get a backup with detailed information from the S3 bucket`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		vmKey := args[0]
		vm, err := cli.S3DB.GetVirtualObjectMachine(cmd.Context(), vmKey)
		if err != nil {
			return fmt.Errorf("failed to get backup: %w", err)
		}
		Tableizer(vm)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(getBackupCmd)
}
