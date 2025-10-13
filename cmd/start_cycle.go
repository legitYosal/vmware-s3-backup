package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// startCycleCmd represents the 'start cycle' command
var startCycleCmd = &cobra.Command{
	Use:   "cycle [VM_NAME]",
	Short: "Starts a full backup cycle for a specific VM.",
	Long: `This command initiates a complete backup cycle for the specified virtual machine.
It will perform a full backup and store it in the configured S3 bucket.`,
	Args: cobra.ExactArgs(1), // Ensures exactly one argument (the VM name) is passed.
	RunE: func(cmd *cobra.Command, args []string) error {
		vmName := args[0]

		fmt.Printf("Starting backup cycle for VM: %s\n", vmName)
		fmt.Printf("Using S3 Bucket: %s in region %s\n", cfg.S3BucketName, cfg.S3Region)

		// Pass the global config and the specific vmName to your business logic.
		// err := backup.StartBackupCycle(cfg.VMWareHOST, cfg.VMWareUsername, cfg.VMWarePassword, vmName)
		// if err != nil {
		// 	return fmt.Errorf("backup cycle failed for vm %s: %w", vmName, err)
		// }

		// fmt.Println("Backup cycle completed successfully.")
		return nil
	},
}

func init() {
	// Add 'cycle' as a subcommand to the 'start' command.
	startCmd.AddCommand(startCycleCmd)

	// You can add flags specific to this command here if needed.
	// For example:
	// startCycleCmd.Flags().BoolP("incremental", "i", false, "Perform an incremental backup")
}
