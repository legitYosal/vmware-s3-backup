package cmd

import "github.com/spf13/cobra"

var vmCmd = &cobra.Command{
	Use:   "vm",
	Short: "VM commands",
	Long:  "VM commands provides actions on VMs",
}

func init() {
	rootCmd.AddCommand(vmCmd)
}
