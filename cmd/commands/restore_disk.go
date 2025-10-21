package commands

import "github.com/spf13/cobra"

var restoreDiskCmd = &cobra.Command{
	Use:   "restore-disk --data-store-name <DATA_STORE_NAME> --local-path <LOCAL_PATH> --remote-path <REMOTE_PATH>",
	Short: "Restore a disk from S3",
	Long:  `The restore-disk command restores a disk from S3 to a local directory.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dataStoreName := cmd.Flag("data-store-name").Value.String()
		localPath := cmd.Flag("local-path").Value.String()
		remotePath := cmd.Flag("remote-path").Value.String()
		err := cli.RestoreDisk(cmd.Context(), localPath, remotePath, dataStoreName)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	restoreDiskCmd.Flags().String("data-store-name", "", "Data store name")
	restoreDiskCmd.MarkFlagRequired("data-store-name")
	restoreDiskCmd.Flags().String("local-path", "", "Local path")
	restoreDiskCmd.MarkFlagRequired("local-path")
	restoreDiskCmd.Flags().String("remote-path", "", "Remote path")
	restoreDiskCmd.MarkFlagRequired("remote-path")
	rootCmd.AddCommand(restoreDiskCmd)
}
