package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "vmbackup-cli",
	Short: "A CLI tool to backup VMware VMs to S3.",
	Long:  `vmbackup-cli is a command-line interface to perform full and incremental backups of VMware virtual machines directly to S3 storage.`,
}

// Execute is the main entrypoint for the Cobra application.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// init is called by Go when the package is initialized.
func init() {
	// Add global persistent flags here, available to all subcommands.
	rootCmd.PersistentFlags().String("vcenter-url", "", "VCenter URL (e.g., https://vcenter.example.com)")
	rootCmd.PersistentFlags().String("vcenter-user", "", "VCenter username")
	rootCmd.PersistentFlags().String("vcenter-pass", "", "VCenter password")
	rootCmd.PersistentFlags().String("s3-bucket", "", "Target S3 bucket name")
	rootCmd.PersistentFlags().String("s3-url", "", "Target S3 bucket URL")
	rootCmd.PersistentFlags().String("s3-secret-key", "", "Target S3 bucket secret key")
	rootCmd.PersistentFlags().String("s3-access-key", "", "Target S3 bucket access key")
	rootCmd.PersistentFlags().String("s3-region", "", "Target S3 bucket region")

	// Mark required flags
	rootCmd.MarkPersistentFlagRequired("vcenter-url")
	rootCmd.MarkPersistentFlagRequired("vcenter-user")
	rootCmd.MarkPersistentFlagRequired("vcenter-pass")
	rootCmd.MarkPersistentFlagRequired("s3-bucket")
	rootCmd.MarkPersistentFlagRequired("s3-url")
	rootCmd.MarkPersistentFlagRequired("s3-secret-key")
	rootCmd.MarkPersistentFlagRequired("s3-access-key")
	rootCmd.MarkPersistentFlagRequired("s3-region")
}
