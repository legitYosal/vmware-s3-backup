package vmware

import (
	"context"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

type CompleteVirtualMachine struct {
	M *object.VirtualMachine
	O *mo.VirtualMachine
}

func (c *CompleteVirtualMachine) CheckCBTEnabled() (bool, error) {
	return !(c.O.Config.ChangeTrackingEnabled == nil || !*c.O.Config.ChangeTrackingEnabled), nil
}

func (c *CompleteVirtualMachine) FindAndConsolidateDanglingSnapshot(ctx context.Context) error {
	snapshotRef, err := c.FindDanglingSnapshot(ctx)
	if err != nil {
		return err
	}
	if snapshotRef != nil {
		consolidate := true
		_, err := c.M.RemoveSnapshot(ctx, snapshotRef.Value, false, &consolidate)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (c *CompleteVirtualMachine) FindDanglingSnapshot(ctx context.Context) (*types.ManagedObjectReference, error) {
	snapshotRef, err := c.M.FindSnapshot(ctx, "vmware-backup-snapshot")
	if err != nil {
		return nil, err
	}
	if snapshotRef != nil {
		return snapshotRef, nil
	}
	return nil, nil
}
