package cmd

import (
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/vmware/govmomi/find"
)

// startCycleCmd represents the 'start cycle' command
var startCycleCmd = &cobra.Command{
	Use:   "cycle [VM_PATH]",
	Short: "Starts a full backup cycle for a specific VM.",
	Long: `This command initiates a complete backup cycle for the specified virtual machine.
It will perform a full backup and store it in the configured S3 bucket.`,
	Args: cobra.ExactArgs(1), // Ensures exactly one argument (the VM name) is passed.
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
		enableCBT, err := cmd.Flags().GetBool("enable-cbt")
		if err != nil {
			slog.Error("Error getting enable-cbt flag", "error", err)
			return err
		}
		enabled, err := vm.CheckCBTEnabled()
		if err != nil {
			slog.Error("Error checking CBT enabled", "error", err)
			return err
		}
		if !enabled {
			if enableCBT {
				err = vm.EnableCBT(cmd.Context(), true)
				if err != nil {
					slog.Error("Error enabling CBT", "error", err)
					return err
				}
			} else {
				slog.Error("CBT is not enabled, please enable it in the vmware vcenter/esxi or pass --enable-cbt")
				return err
			}
		}
		snapshotRef, err := vm.FindDanglingSnapshot(cmd.Context())
		if err != nil {
			slog.Error("Error finding dangling snapshot", "error", err)
			return err
		}
		consolidate, err := cmd.Flags().GetBool("consolidate")
		if err != nil {
			slog.Error("Error getting consolidate flag", "error", err)
			return err
		}
		if snapshotRef != nil {
			if consolidate {
				err = vm.ConsolidateDanglingSnapshot(cmd.Context(), snapshotRef)
				if err != nil {
					slog.Error("Error consolidating snapshot", "error", err)
					return err
				}
			} else {
				slog.Error("Dangling snapshot found, please remove it")
				return err
			}
		}

		err = vm.StartCycle(cmd.Context(), vm.GetName())
		if err != nil {
			slog.Error("Error starting cycle", "error", err)
			return err
		}
		return nil
	},
}

func init() {
	// Add 'cycle' as a subcommand to the 'start' command.
	startCmd.AddCommand(startCycleCmd)

	// You can add flags specific to this command here if needed.
	// For example:
	// startCycleCmd.Flags().BoolP("incremental", "i", false, "Perform an incremental backup")
	startCycleCmd.Flags().BoolP("consolidate", "c", false, "Consolidate the snapshots")
	startCycleCmd.Flags().BoolP("enable-cbt", "e", false, "Enable CBT on the vm")
}
