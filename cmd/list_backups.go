package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var listBackupsCmd = &cobra.Command{
	Use:   "backups",
	Short: "Lists all available backups",
	Long:  `Lists all available backups from the S3 bucket`,
	RunE: func(cmd *cobra.Command, args []string) error {
		vms, err := cli.S3DB.ListVirtualObjectMachines(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to list backups: %w", err)
		}
		Tableizer(vms)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(listBackupsCmd)
}
