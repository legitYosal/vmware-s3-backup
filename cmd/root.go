package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/legitYosal/vmware-s3-backup/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// config holds all the global configuration values for the CLI.
// These can be set via command-line flags or environment variables.
// type config struct {
// 	VMwareURL      string
// 	VMwareUsername string
// 	VMwarePassword string
// 	S3URL          string
// 	S3SecretKey    string
// 	S3AccessKey    string
// 	S3BucketName   string
// 	S3Region       string
// }

var cfg config.Config

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "vmbackup-cli",
	Short: "A CLI tool to backup VMware VMs to an S3-compatible backend.",
	Long: `vmbackup-cli is a powerful and flexible tool for creating and managing
backups of VMware virtual machines and storing them in any S3-compatible storage.`,
	// This function is run before any subcommand's Run function.
	// We use it to unmarshal configuration from Viper into our struct.
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return viper.Unmarshal(&cfg)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// init is called when the package is imported. It's used to set up
// the application's flags and configuration handling.
func init() {
	// Use Viper for robust configuration management
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv() // Automatically read in environment variables that match

	// Define persistent flags, which are available to this command
	// and all its subcommands.
	rootCmd.PersistentFlags().String("vmware-url", "", "VMware vCenter/ESXi URL (env: VMWARE_URL)")
	rootCmd.PersistentFlags().String("vmware-username", "", "VMware vCenter/ESXi username (env: VMWARE_USERNAME)")
	rootCmd.PersistentFlags().String("vmware-password", "", "VMware vCenter/ESXi password (env: VMWARE_PASSWORD)")
	rootCmd.PersistentFlags().String("s3-url", "", "S3 endpoint URL (env: S3_URL)")
	rootCmd.PersistentFlags().String("s3-secret-key", "", "S3 secret key (env: S3_SECRET_KEY)")
	rootCmd.PersistentFlags().String("s3-access-key", "", "S3 access key (env: S3_ACCESS_KEY)")
	rootCmd.PersistentFlags().String("s3-bucket-name", "", "S3 bucket name (env: S3_BUCKET_NAME)")
	rootCmd.PersistentFlags().String("s3-region", "", "S3 region (env: S3_REGION)")

	// Bind each cobra flag to its corresponding viper key and environment variable.
	viper.BindPFlag("vmwareUrl", rootCmd.PersistentFlags().Lookup("vmware-url"))
	viper.BindPFlag("vmwareUsername", rootCmd.PersistentFlags().Lookup("vmware-username"))
	viper.BindPFlag("vmwarePassword", rootCmd.PersistentFlags().Lookup("vmware-password"))
	viper.BindPFlag("s3Url", rootCmd.PersistentFlags().Lookup("s3-url"))
	viper.BindPFlag("s3SecretKey", rootCmd.PersistentFlags().Lookup("s3-secret-key"))
	viper.BindPFlag("s3AccessKey", rootCmd.PersistentFlags().Lookup("s3-access-key"))
	viper.BindPFlag("s3BucketName", rootCmd.PersistentFlags().Lookup("s3-bucket-name"))
	viper.BindPFlag("s3Region", rootCmd.PersistentFlags().Lookup("s3-region"))
}
