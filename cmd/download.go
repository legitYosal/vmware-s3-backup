package cmd

import (
	"fmt"
	"log/slog"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/spf13/cobra"
)

var downloadBackupCmd = &cobra.Command{
	Use:   "download-backup <VM_KEY> <DISK_KEY> <LOCAL_PATH>",
	Short: "Download a backup from S3",
	Long:  `The download-backup command downloads a backup from S3 to a local directory.`,
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		vmKey := args[0]
		diskKey := args[1]
		localPath := args[2]
		vmList, err := cli.S3DB.ListVirtualObjectMachines(cmd.Context(), vmKey)
		if err != nil {
			return err
		}
		if len(vmList) == 0 {
			return fmt.Errorf("VM %s not found", vmKey)
		}
		vm := vmList[0]
		var vmDisk *vms3.VirtualObjectDisk
		for _, disk := range vm.Disks {
			slog.Debug("checking disk", "disk", disk.DiskKey)
			if disk.DiskKey == diskKey {
				vmDisk = disk
				break
			}
		}
		if vmDisk == nil {
			return fmt.Errorf("Disk %s not found", diskKey)
		}
		err = vmDisk.RestoreDiskToLocalPath(cmd.Context(), cli.S3DB, localPath)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(downloadBackupCmd)
}
