package cmd

import (
	"fmt"
	"log/slog"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/spf13/cobra"
)

var validateBackups = &cobra.Command{
	Use:   "validate-backup <VM_KEY> <DISK_KEY>",
	Short: "Validate backup",
	Long:  "Validate backup",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		vmKey := args[0]
		diskKey := args[1]
		manifestKey := vms3.GetDiskManifestObjectKey(vms3.CreateDiskObjectKey(vmKey, diskKey))
		diskManifest, err := cli.S3DB.GetVirtualObjectDiskManifest(cmd.Context(), manifestKey)
		if err != nil {
			return fmt.Errorf("failed to get disk manifest: %w", err)
		}
		diskManifest.ValidateOnS3(cmd.Context(), cli.S3DB)
		if err != nil {
			return fmt.Errorf("failed to validate disk manifest: %w", err)
		}
		slog.Info("Disk manifest validated successfully")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(validateBackups)
}
