package cmd

import (
	"fmt"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/spf13/cobra"
)

var deleteBackup = &cobra.Command{
	Use:   "delete-backup <VM_KEY> <DISK_KEY>",
	Short: "Delete a backup",
	Long:  "Delete a backup",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		vmKey := args[0]
		diskKey := args[1]
		manifestKey := vms3.GetDiskManifestObjectKey(vms3.CreateDiskObjectKey(vmKey, diskKey))
		diskManifest, err := cli.S3DB.GetVirtualObjectDiskManifest(cmd.Context(), manifestKey)
		if err != nil {
			return fmt.Errorf("failed to get disk manifest: %w", err)
		}
		err = diskManifest.CleanUpS3(cmd.Context(), cli.S3DB)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(deleteBackup)
}
