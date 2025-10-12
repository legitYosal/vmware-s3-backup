package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

const MaxChunkSize = 64 * 1024 * 1024

const TempBackupSnapshotName = "vmware-backup-snapshot"

type DetailedSnapshot struct {
	Ref        *types.ManagedObjectReference
	Properties *mo.VirtualMachineSnapshot
}
type DetailedVirtualMachine struct {
	Ref            *object.VirtualMachine
	Properties     *mo.VirtualMachine
	Sockets        []*DiskSocket
	SnapshotRef    *DetailedSnapshot
	S3BackupClient *VmwareS3BackupClient
}
type DiskSocket struct {
	Disk      *types.VirtualDisk
	SocketRef *nbdkit.NbdkitSocket
}

func (c *DetailedVirtualMachine) CheckCBTEnabled() (bool, error) {
	return !(c.Properties.Config.ChangeTrackingEnabled == nil || !*c.Properties.Config.ChangeTrackingEnabled), nil
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

func (c *DetailedVirtualMachine) FindDanglingSnapshot(ctx context.Context) (*types.ManagedObjectReference, error) {
	snapshotRef, err := c.Ref.FindSnapshot(ctx, TempBackupSnapshotName)
	if err != nil {
		return nil, err
	}
	if snapshotRef != nil {
		return snapshotRef, nil
	}
	return nil, nil
}

func (c *DetailedVirtualMachine) CreateBackupSnapshot(ctx context.Context) error {
	task, err := c.Ref.CreateSnapshot(ctx, TempBackupSnapshotName, "Ephemeral snapshot for migration", false, false)
	if err != nil {
		return err
	}
	info, err := task.WaitForResult(ctx) // NOTE: you can pass a progress channel here
	if err != nil {
		return err
	}
	ref, ok := info.Result.(types.ManagedObjectReference)
	if !ok {
		return fmt.Errorf("failed to create snapshot: %v", info.Result)
	}
	var properties mo.VirtualMachineSnapshot
	err = c.Ref.Properties(ctx, ref, []string{"config.hardware"}, &properties)
	if err != nil {
		return err
	}
	c.SnapshotRef = &DetailedSnapshot{
		Ref:        &ref,
		Properties: &properties,
	}
	return nil
}

func (c *DetailedVirtualMachine) StartNBDSockets(ctx context.Context) error {
	err := c.CreateBackupSnapshot(ctx)
	if err != nil {
		return err
	}
	for _, device := range c.SnapshotRef.Properties.Config.Hardware.Device {
		switch disk := device.(type) {
		case *types.VirtualDisk:
			backing := disk.Backing.(types.BaseVirtualDeviceFileBackingInfo)
			info := backing.GetVirtualDeviceFileBackingInfo()

			password, _ := c.S3BackupClient.VDDKConfig.Endpoint.User.Password()
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

			c.Sockets = append(c.Sockets, &DiskSocket{
				SocketRef: socket,
				Disk:      disk,
			})
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
