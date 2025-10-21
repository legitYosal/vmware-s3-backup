package commands

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/legitYosal/vmware-s3-backup/pkg/backup"
	"github.com/legitYosal/vmware-s3-backup/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfg *config.Config
var cli *backup.VmwareS3BackupClient
var logger *slog.Logger

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "vmbackup-cli",
	Short: "A CLI tool to backup VMware VMs to an S3-compatible backend.",
	Long: `vmbackup-cli is a powerful and flexible tool for creating and managing
backups of VMware virtual machines and storing them in any S3-compatible storage.`,
	// This function is run before any subcommand's Run function.
	// We use it to unmarshal configuration from Viper into our struct.
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		cfg = &config.Config{}
		err := viper.Unmarshal(cfg)
		if err != nil {
			return fmt.Errorf("failed to unmarshal configuration: %w", err)
		}
		level := slog.LevelInfo
		if cfg.Debug {
			level = slog.LevelDebug
		}
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
		}))
		slog.SetDefault(logger)
		cli, err = backup.NewVmwareS3BackupClient(cfg)
		if err != nil {
			return fmt.Errorf("failed to create VMware S3 backup client: %w", err)
		}
		cli.Connect(cmd.Context())
		return nil
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
	viper.AutomaticEnv() // Automatically read in environment variables that match

	// Define persistent flags, which are available to this command
	// and all its subcommands.
	rootCmd.PersistentFlags().String("vmware-host", "", "VMware vCenter/ESXi URL (env: VMWARE_HOST)")
	rootCmd.PersistentFlags().String("vmware-username", "", "VMware vCenter/ESXi username (env: VMWARE_USERNAME)")
	rootCmd.PersistentFlags().String("vmware-password", "", "VMware vCenter/ESXi password (env: VMWARE_PASSWORD)")
	rootCmd.PersistentFlags().String("s3-url", "", "S3 endpoint URL (env: S3_URL)")
	rootCmd.PersistentFlags().String("s3-secret-key", "", "S3 secret key (env: S3_SECRET_KEY)")
	rootCmd.PersistentFlags().String("s3-access-key", "", "S3 access key (env: S3_ACCESS_KEY)")
	rootCmd.PersistentFlags().String("s3-bucket-name", "", "S3 bucket name (env: S3_BUCKET_NAME)")
	rootCmd.PersistentFlags().String("s3-region", "", "S3 region (env: S3_REGION)")
	rootCmd.PersistentFlags().Bool("debug", false, "Debug mode (env: DEBUG)")

	// Bind each cobra flag to its corresponding viper key and environment variable.
	viper.BindPFlag("VMWARE_HOST", rootCmd.PersistentFlags().Lookup("vmware-host"))
	viper.BindPFlag("VMWARE_USERNAME", rootCmd.PersistentFlags().Lookup("vmware-username"))
	viper.BindPFlag("VMWARE_PASSWORD", rootCmd.PersistentFlags().Lookup("vmware-password"))
	viper.BindPFlag("S3_URL", rootCmd.PersistentFlags().Lookup("s3-url"))
	viper.BindPFlag("S3_SECRET_KEY", rootCmd.PersistentFlags().Lookup("s3-secret-key"))
	viper.BindPFlag("S3_ACCESS_KEY", rootCmd.PersistentFlags().Lookup("s3-access-key"))
	viper.BindPFlag("S3_BUCKET_NAME", rootCmd.PersistentFlags().Lookup("s3-bucket-name"))
	viper.BindPFlag("S3_REGION", rootCmd.PersistentFlags().Lookup("s3-region"))
	viper.BindPFlag("DEBUG", rootCmd.PersistentFlags().Lookup("debug"))

	viper.BindEnv("VMWARE_HOST", "VMWARE_HOST")
	viper.BindEnv("VMWARE_USERNAME", "VMWARE_USERNAME")
	viper.BindEnv("VMWARE_PASSWORD", "VMWARE_PASSWORD")
	viper.BindEnv("S3_URL", "S3_URL")
	viper.BindEnv("S3_SECRET_KEY", "S3_SECRET_KEY")
	viper.BindEnv("S3_ACCESS_KEY", "S3_ACCESS_KEY")
	viper.BindEnv("S3_BUCKET_NAME", "S3_BUCKET_NAME")
	viper.BindEnv("S3_REGION", "S3_REGION")
	viper.BindEnv("DEBUG", "DEBUG")
}
