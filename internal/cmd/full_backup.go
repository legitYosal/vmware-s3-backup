package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	// THIS IS THE KEY: The CLI imports your public library package!
	"github.com/legitYosal/vmware-s3-backup/pkg/backup"
)

var fullBackupCmd = &cobra.Command{
	Use:   "full [flags]",
	Short: "Perform a full backup of a VM.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 1. Get flag values
		vmName, _ := cmd.Flags().GetString("vm-name")
		vcenterURL, _ := cmd.Flags().GetString("vcenter-url")
		vcenterUser, _ := cmd.Flags().GetString("vcenter-user")
		vcenterPass, _ := cmd.Flags().GetString("vcenter-pass")
		s3Bucket, _ := cmd.Flags().GetString("s3-bucket")
		s3URL, _ := cmd.Flags().GetString("s3-url")
		s3SecretKey, _ := cmd.Flags().GetString("s3-secret-key")
		s3AccessKey, _ := cmd.Flags().GetString("s3-access-key")

		// 2. Create the config for your library using the flags
		cfg := backup.Config{
			VMWareURL:      vcenterURL,
			VMWareUsername: vcenterUser,
			VMWarePassword: vcenterPass,
			S3URL:          s3URL,
			S3SecretKey:    s3SecretKey,
			S3AccessKey:    s3AccessKey,
			S3BucketName:   s3Bucket,
		}

		// 3. Create a new client from your library
		client, err := backup.NewClient(cfg)
		if err != nil {
			return fmt.Errorf("failed to create backup client: %w", err)
		}

		// 4. Execute the library method
		fmt.Printf("CLI: Initiating full backup for VM '%s'...\n", vmName)
		return client.FullCopy(context.Background(), vmName)
	},
}

func init() {
	// Add command-specific flags
	fullBackupCmd.Flags().String("vm-name", "", "The name of the VM to back up")
	fullBackupCmd.MarkFlagRequired("vm-name")

	// Add the subcommand to the root command
	rootCmd.AddCommand(fullBackupCmd)
}
