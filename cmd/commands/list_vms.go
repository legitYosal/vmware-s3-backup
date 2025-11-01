package commands

import (
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
)

type smallVMData struct {
	ID   string
	Name string
	Path string
}

// listVmsCmd represents the 'list vms' command
var listVmsCmd = &cobra.Command{
	Use:   "list-vms",
	Short: "Lists all available virtual machines with detailed information",
	Long:  `Connects to the specified VMware vCenter or ESXi host and retrieves a detailed list of all virtual machines including status, memory, CPUs, and disks.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		detailed, err := cmd.Flags().GetBool("detailed")
		if err != nil {
			return fmt.Errorf("failed to get detailed flag value: %w", err)
		}
		if detailed {
			vms, err := cli.DetailedListVMs(cmd.Context())
			if err != nil {
				slog.Error("failed to list VMs", "error", err)
				return fmt.Errorf("failed to list VMs: %w", err)
			}
			Tableizer(vms)
		} else {
			var smallVms []smallVMData
			vms, err := cli.ListVMs(cmd.Context())
			for _, vm := range vms {
				smallVms = append(smallVms, smallVMData{
					ID:   vm.ID,
					Name: vm.Name,
					Path: vm.Path,
				})
			}
			if err != nil {
				slog.Error("failed to list VMs", "error", err)
				return fmt.Errorf("failed to list VMs: %w", err)
			}
			Tableizer(smallVms)
		}
		return nil
	},
}

func init() {
	// Add the 'vms' command as a subcommand of the 'list' command.
	rootCmd.Flags().BoolP("detailed", "d", false, "Include extensive configuration and disk details for each VM.")
	rootCmd.AddCommand(listVmsCmd)
}
