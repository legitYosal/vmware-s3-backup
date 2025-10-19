package cmd

import (
	"github.com/spf13/cobra"
)

var downloadBackupCmd = &cobra.Command{
	Use:   "download-backup <VM_KEY> <DISK_KEY> <LOCAL_PATH>",
	Short: "Download a backup from S3",
	Long:  `The download-backup command downloads a backup from S3 to a local directory.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 3 {
			cmd.Help()
			return
		}
		vmKey := args[0]
		diskKey := args[1]
		localPath := args[2]
		vmList, err := cli.S3DB.ListVirtualObjectMachines(cmd.Context(), vmKey)
		if err != nil {
			cmd.Help()
			return
		}
		vm := vmList[0]
		for _, disk := range vm.Disks {
			if disk.DiskKey == diskKey {
				err = disk.RestoreDiskToLocalPath(cmd.Context(), cli.S3DB, localPath)
				if err != nil {
					cmd.Help()
					return
				}
				return
			}
		}
	},
}

func init() {
	downloadCmd.AddCommand(downloadBackupCmd)
}
