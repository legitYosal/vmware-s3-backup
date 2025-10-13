package cmd

import (
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/vmware/govmomi/find"
)

var vmEnableCbtCmd = &cobra.Command{
	Use:   "enable-cbt <vm-name>",
	Short: "Enable CBT on a VM",
	Long:  "Enable CBT on a VM",
	RunE: func(cmd *cobra.Command, args []string) error {
		vmPath := args[0]
		vm, err := cli.FindVMByPath(cmd.Context(), vmPath)
		if err != nil {
			if _, ok := err.(*find.NotFoundError); ok {
				slog.Error("VM does not exist, please check with `list vms` command")
				return err
			}
			slog.Error("Error finding VM", "error", err)
			return err
		}
		enabled, err := vm.CheckCBTEnabled()
		if err != nil {
			slog.Error("Error checking CBT enabled", "error", err)
			return err
		}
		if !enabled {
			err = vm.EnableCBT(cmd.Context(), true)
			if err != nil {
				slog.Error("Error enabling CBT", "error", err)
				return err
			}
			slog.Info("CBT enabled successfully")
		} else {
			slog.Info("CBT is already enabled")
		}
		return nil
	},
}

func init() {
	vmCmd.AddCommand(vmEnableCbtCmd)
}
