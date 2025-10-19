package cmd

import "github.com/spf13/cobra"

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download a VM from S3",
	Long:  `The download command downloads a VM from S3 to a local directory.`,
	// Run: func(cmd *cobra.Command, args []string) {
	// 	// By not providing a Run function, this command will show its help text
	// 	// if called without a subcommand.
	// },
}

func init() {
	rootCmd.AddCommand(downloadCmd)
}
