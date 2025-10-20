package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

const TempBackupSnapshotName = "vmware-backup-snapshot"

type DetailedSnapshot struct {
	Ref        *types.ManagedObjectReference
	Properties *mo.VirtualMachineSnapshot
}
type DetailedVirtualMachine struct {
	Ref            *object.VirtualMachine
	Properties     *mo.VirtualMachine
	Sockets        []*DiskTarget
	SnapshotRef    *DetailedSnapshot
	S3BackupClient *VmwareS3BackupClient
}

// RefreshProperties reloads the VM properties from vSphere
func (c *DetailedVirtualMachine) RefreshProperties(ctx context.Context) error {
	var vm mo.VirtualMachine
	err := c.Ref.Properties(ctx, c.Ref.Reference(), []string{
		"name",
		"config",
		"config.changeTrackingEnabled",
	}, &vm)
	if err != nil {
		return fmt.Errorf("failed to refresh VM properties: %w", err)
	}
	c.Properties.Config = vm.Config
	return nil
}

// CheckCBTEnabled checks if CBT is enabled by examining the extraConfig settings
func (c *DetailedVirtualMachine) CheckCBTEnabled() (bool, error) {
	// First check the Config.ChangeTrackingEnabled field
	if c.Properties.Config.ChangeTrackingEnabled != nil && *c.Properties.Config.ChangeTrackingEnabled {
		return true, nil
	}

	// Also check the extraConfig for ctkEnabled setting
	if c.Properties.Config.ExtraConfig != nil {
		for _, option := range c.Properties.Config.ExtraConfig {
			if optionValue, ok := option.(*types.OptionValue); ok {
				if optionValue.Key == "ctkEnabled" {
					// Check if value is "true" or "TRUE" (case insensitive)
					if val, ok := optionValue.Value.(string); ok {
						if val == "true" || val == "TRUE" || val == "True" {
							return true, nil
						}
					}
					// Also handle boolean values
					if val, ok := optionValue.Value.(bool); ok {
						return val, nil
					}
				}
			}
		}
	}

	return false, nil
}

func (c *DetailedVirtualMachine) FindAndConsolidateDanglingSnapshot(ctx context.Context) error {
	snapshotRef, err := c.FindDanglingSnapshot(ctx)
	if err != nil {
		return err
	}
	if snapshotRef != nil {
		consolidate := true
		_, err := c.Ref.RemoveSnapshot(ctx, snapshotRef.Value, false, &consolidate)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (c *DetailedVirtualMachine) ConsolidateDanglingSnapshot(ctx context.Context, snapshotRef *types.ManagedObjectReference) error {
	consolidate := true
	task, err := c.Ref.RemoveSnapshot(ctx, snapshotRef.Value, false, &consolidate)
	if err != nil {
		return err
	}
	// wait for the snapshot to be removed
	_, err = task.WaitForResult(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (c *DetailedVirtualMachine) FindDanglingSnapshot(ctx context.Context) (*types.ManagedObjectReference, error) {
	snapshotRef, err := c.Ref.FindSnapshot(ctx, TempBackupSnapshotName)
	if err != nil {
		if _, ok := err.(*find.NotFoundError); ok {
			return nil, nil
		}
		if err.Error() == "no snapshots for this VM" {
			return nil, nil
		}
		return nil, err
	}
	if snapshotRef != nil {
		return snapshotRef, nil
	}
	return nil, nil
}

func (c *DetailedVirtualMachine) CreateBackupSnapshot(ctx context.Context) error {
	slog.Debug("Creating backup snapshot", "vmName", c.Properties.Name)
	task, err := c.Ref.CreateSnapshot(ctx, TempBackupSnapshotName, "Ephemeral snapshot for migration", false, false)
	if err != nil {
		slog.Error("Failed to create backup snapshot", "error", err)
		return err
	}
	info, err := task.WaitForResult(ctx) // NOTE: you can pass a progress channel here
	if err != nil {
		slog.Error("Failed to wait for snapshot creation", "error", err)
		return err
	}
	ref, ok := info.Result.(types.ManagedObjectReference)
	if !ok {
		slog.Error("Failed to get snapshot reference", "result", info.Result)
		return fmt.Errorf("failed to create snapshot: %v", info.Result)
	}
	var properties mo.VirtualMachineSnapshot
	err = c.Ref.Properties(ctx, ref, []string{"config.hardware"}, &properties)
	if err != nil {
		slog.Error("Failed to get snapshot properties", "error", err)
		return err
	}
	c.SnapshotRef = &DetailedSnapshot{
		Ref:        &ref,
		Properties: &properties,
	}
	slog.Debug("Backup snapshot created successfully", "vmName", c.Properties.Name)
	return nil
}

func (c *DetailedVirtualMachine) StartNBDSockets(ctx context.Context, vmKey string) error {
	err := c.CreateBackupSnapshot(ctx)
	if err != nil {
		return err
	}
	slog.Debug("Starting NBD sockets", "vmName", c.Properties.Name)
	for _, device := range c.SnapshotRef.Properties.Config.Hardware.Device {
		switch disk := device.(type) {
		case *types.VirtualDisk:
			backing := disk.Backing.(types.BaseVirtualDeviceFileBackingInfo)
			info := backing.GetVirtualDeviceFileBackingInfo()
			password := c.S3BackupClient.Configuration.VMWarePassword

			slog.Debug("Starting NBD socket for disk", "diskName", disk.DeviceInfo.GetDescription().Label)

			socket, err := nbdkit.NewNBDKitSocketConfig(
				c.S3BackupClient.VDDKConfig.Endpoint.Host,
				c.S3BackupClient.VDDKConfig.Endpoint.User.Username(),
				password, c.S3BackupClient.VDDKConfig.Thumbprint,
				c.Ref.Reference().Value,
				c.SnapshotRef.Ref.Value,
				info.FileName,
				c.S3BackupClient.VDDKConfig.Compression,
			).Build()
			if err != nil {
				return err
			}

			if err := socket.Start(); err != nil {
				return err
			}
			slog.Debug("NBD socket started successfully", "diskName", disk.DeviceInfo.GetDescription().Label)

			diskTarget, err := NewDiskTarget(disk, socket, c, vmKey)
			if err != nil {
				return err
			}
			c.Sockets = append(c.Sockets, diskTarget)
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		slog.Warn("Received interrupt signal, cleaning up...")

		err := c.Stop(ctx)
		if err != nil {
			slog.Error("Failed to stop nbdkit servers", "error", err)
		}

		os.Exit(1)
	}()

	return nil
}

func (c *DetailedVirtualMachine) Stop(ctx context.Context) error {
	for _, socket := range c.Sockets {
		if err := socket.SocketRef.Stop(); err != nil {
			return err
		}
	}
	return nil
}

func (c *DetailedVirtualMachine) StartCycle(ctx context.Context, vmKey string) error {
	/*
		Now every validation is passed,
		also nbdkit sockets are started,
		now we are going to first of all:
		1. check if we need a fully copy or not
		2. if full copy we are going to from 0 to 100 copy all data to s3
		3. if incremental copy we are going to copy only the changed data to s3
	*/
	slog.Debug("Starting cycle", "vmName", c.Properties.Name)
	err := c.StartNBDSockets(ctx, vmKey)
	if err != nil {
		return err
	}
	defer func() {
		err := c.Stop(ctx)
		if err != nil {
			slog.Error("Failed to stop nbdkit servers", "error", err)
		}
	}()
	for index, socket := range c.Sockets {
		err := socket.StartSync(ctx)
		if err != nil {
			slog.Error("Failed to start sync for disk", "index", index, "error", err)
		} else {
			slog.Debug("Sync finished for disk", "index", index)
		}
	}
	return nil
}

// EnableCBT enables Changed Block Tracking (CBT) on the virtual machine and all its disks.
// According to VMware documentation, CBT must be enabled globally and per-disk.
// After enabling, either a VM reboot or a stun/unstun cycle is required to activate CBT.
func (c *DetailedVirtualMachine) EnableCBT(ctx context.Context, stun bool) error {
	// 1. First, check if CBT is already enabled.
	enabled, err := c.CheckCBTEnabled()
	if err != nil {
		return fmt.Errorf("failed to check current CBT status: %w", err)
	}
	if enabled {
		slog.Debug("CBT is already enabled for VM", "vmName", c.Properties.Name)
		return nil
	}

	slog.Debug("Enabling CBT for VM", "vmName", c.Properties.Name)

	// 2. Build the list of configuration changes.
	var extraConfigs []types.BaseOptionValue

	// Add the global CBT setting (ctkEnabled=true)
	extraConfigs = append(extraConfigs, &types.OptionValue{
		Key:   "ctkEnabled",
		Value: "true",
	})

	// 3. Create a map of controllers for easy lookup
	controllers := make(map[int32]types.BaseVirtualController)
	for _, device := range c.Properties.Config.Hardware.Device {
		switch ctl := device.(type) {
		case *types.VirtualSCSIController:
			controllers[ctl.Key] = ctl
		case *types.ParaVirtualSCSIController:
			controllers[ctl.Key] = ctl
		case *types.VirtualLsiLogicController:
			controllers[ctl.Key] = ctl
		case *types.VirtualLsiLogicSASController:
			controllers[ctl.Key] = ctl
		case *types.VirtualBusLogicController:
			controllers[ctl.Key] = ctl
		case *types.VirtualSATAController:
			controllers[ctl.Key] = ctl
		case *types.VirtualNVMEController:
			controllers[ctl.Key] = ctl
		case *types.VirtualIDEController:
			controllers[ctl.Key] = ctl
		}
	}

	// 4. Enable CBT per-disk (e.g., scsi0:0.ctkEnabled=true, scsi0:1.ctkEnabled=true)
	diskCount := 0
	for _, device := range c.Properties.Config.Hardware.Device {
		if disk, ok := device.(*types.VirtualDisk); ok {
			controller, found := controllers[disk.ControllerKey]
			if !found {
				diskLabel := "unknown"
				if disk.DeviceInfo != nil {
					diskLabel = disk.DeviceInfo.GetDescription().Label
				}
				slog.Warn("Controller not found for disk, skipping CBT enable for this disk", "disk", diskLabel)
				continue
			}

			if disk.UnitNumber == nil {
				diskLabel := "unknown"
				if disk.DeviceInfo != nil {
					diskLabel = disk.DeviceInfo.GetDescription().Label
				}
				slog.Warn("Disk unit number is nil, skipping CBT enable for this disk", "disk", diskLabel)
				continue
			}

			// Determine the controller prefix and format the CBT key
			var prefix string
			busNumber := controller.GetVirtualController().BusNumber
			unitNumber := *disk.UnitNumber

			switch controller.(type) {
			case *types.VirtualSCSIController, *types.ParaVirtualSCSIController,
				*types.VirtualLsiLogicController, *types.VirtualLsiLogicSASController,
				*types.VirtualBusLogicController:
				prefix = "scsi"
			case *types.VirtualSATAController:
				prefix = "sata"
			case *types.VirtualNVMEController:
				prefix = "nvme"
			case *types.VirtualIDEController:
				prefix = "ide"
			default:
				slog.Warn("Unsupported controller type for CBT", "controller", fmt.Sprintf("%T", controller))
				continue
			}

			// Build the per-disk CBT key (e.g., "scsi0:0.ctkEnabled")
			ctkKey := fmt.Sprintf("%s%d:%d.ctkEnabled", prefix, busNumber, unitNumber)

			diskLabel := fmt.Sprintf("disk-%d", diskCount)
			if disk.DeviceInfo != nil {
				diskLabel = disk.DeviceInfo.GetDescription().Label
			}

			slog.Debug("Enabling CBT for disk", "disk", diskLabel, "key", ctkKey)
			extraConfigs = append(extraConfigs, &types.OptionValue{
				Key:   ctkKey,
				Value: "true",
			})
			diskCount++
		}
	}

	if diskCount == 0 {
		slog.Warn("No disks found to enable CBT on", "vmName", c.Properties.Name)
	} else {
		slog.Debug("Prepared CBT configuration", "vmName", c.Properties.Name, "disks", diskCount)
	}

	// 5. Apply the new configuration to the VM via ReconfigVM_Task
	configSpec := types.VirtualMachineConfigSpec{
		ExtraConfig: extraConfigs,
	}

	task, err := c.Ref.Reconfigure(ctx, configSpec)
	if err != nil {
		return fmt.Errorf("failed to submit reconfigure task for enabling CBT: %w", err)
	}

	slog.Debug("Waiting for CBT reconfiguration task to complete...", "vmName", c.Properties.Name)
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("reconfigure task for enabling CBT failed: %w", err)
	}

	if stun {
		err = c.StunUnstun(ctx)
		if err != nil {
			return fmt.Errorf("error at stun-unstun cycle: %w", err)
		}
	}

	// Refresh VM properties to reflect the CBT changes
	if err := c.RefreshProperties(ctx); err != nil {
		slog.Warn("Failed to refresh VM properties after enabling CBT", "error", err)
		// Don't return error here, CBT was successfully enabled
	}

	slog.Debug("CBT enabled successfully on VM", "vmName", c.Properties.Name, "disks", diskCount)

	return nil
}

// StunUnstun performs a stun/unstun cycle by creating and immediately removing a snapshot.
// This activates CBT without requiring a VM reboot (zero downtime).
// This should be called after EnableCBT() to activate CBT immediately.
func (c *DetailedVirtualMachine) StunUnstun(ctx context.Context) error {
	slog.Debug("Performing stun/unstun cycle to activate CBT without reboot", "vmName", c.Properties.Name)

	stunSnapshotName := "temp-cbt-activation-stun"
	stunSnapshotDesc := "Temporary snapshot to activate CBT (will be automatically removed)"

	// 1. Create a temporary snapshot (stun)
	slog.Debug("Creating temporary snapshot for stun", "vmName", c.Properties.Name, "snapshotName", stunSnapshotName)
	createTask, err := c.Ref.CreateSnapshot(ctx, stunSnapshotName, stunSnapshotDesc, false, false)
	if err != nil {
		return fmt.Errorf("failed to create stun snapshot: %w", err)
	}

	slog.Debug("Waiting for snapshot creation to complete", "vmName", c.Properties.Name)
	if err := createTask.Wait(ctx); err != nil {
		return fmt.Errorf("stun snapshot creation failed: %w", err)
	}
	slog.Debug("Stun snapshot created successfully", "vmName", c.Properties.Name)

	// 2. Immediately remove the snapshot (unstun)
	slog.Debug("Removing temporary snapshot for unstun", "vmName", c.Properties.Name)
	consolidate := true
	removeTask, err := c.Ref.RemoveSnapshot(ctx, stunSnapshotName, false, &consolidate)
	if err != nil {
		return fmt.Errorf("failed to submit task for removing stun snapshot: %w", err)
	}

	slog.Debug("Waiting for snapshot removal to complete", "vmName", c.Properties.Name)
	if err := removeTask.Wait(ctx); err != nil {
		return fmt.Errorf("stun snapshot removal failed: %w", err)
	}

	slog.Debug("Stun/unstun cycle completed successfully - CBT is now active", "vmName", c.Properties.Name)
	return nil
}

func (c *DetailedVirtualMachine) GetName() string {
	return c.Properties.Name
}

func (c *DetailedVirtualMachine) GetID() string {
	return c.Properties.Config.Uuid
}
