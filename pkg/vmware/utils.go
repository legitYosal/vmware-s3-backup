package vmware

import (
	"fmt"
	"log/slog"

	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

func GetPersistentDiskIdentifier(disk *types.VirtualDisk) (string, error) {
	if disk.VDiskId != nil && disk.VDiskId.Id != "" {
		return "vmdiskid:" + disk.VDiskId.Id, nil
	}
	if disk.DiskObjectId != "" {
		return "diskobj:" + disk.DiskObjectId, nil
	}
	// if b := disk.Backing; b != nil {
	// 	switch v := b.(type) {
	// 	case *types.VirtualDiskFlatVer2BackingInfo:
	// 		if v.Uuid != "" {
	// 			return "uuid:" + strings.ToLower(v.Uuid), nil
	// 		}
	// 		if v.BackingObjectId != "" {
	// 			return "backingobj:" + v.BackingObjectId, nil
	// 		}
	// 	case *types.VirtualDiskSeSparseBackingInfo:
	// 		if v.Uuid != "" {
	// 			return "uuid:" + strings.ToLower(v.Uuid), nil
	// 		}
	// 		if v.BackingObjectId != "" {
	// 			return "backingobj:" + v.BackingObjectId, nil
	// 		}
	// 	case *types.VirtualDiskSparseVer2BackingInfo:
	// 		if v.Uuid != "" {
	// 			return "uuid:" + strings.ToLower(v.Uuid), nil
	// 		}
	// 		if v.BackingObjectId != "" {
	// 			return "backingobj:" + v.BackingObjectId, nil
	// 		}
	// 	case *types.VirtualDiskRawDiskVer2BackingInfo:
	// 		// Not common on ESXi; if present we may have Uuid
	// 		if v.Uuid != "" {
	// 			return "uuid:" + strings.ToLower(v.Uuid), nil
	// 		}
	// 	case *types.VirtualDiskRawDiskMappingVer1BackingInfo:
	// 		if v.Uuid != "" {
	// 			return "uuid:" + strings.ToLower(v.Uuid), nil
	// 		}
	// 		if v.LunUuid != "" {
	// 			return "uuid:" + strings.ToLower(v.LunUuid), nil
	// 		}
	// 		if v.BackingObjectId != "" {
	// 			return "backingobj:" + v.BackingObjectId, nil
	// 		}
	// 	}
	// }
	slog.Error("Failed to find persistent identifier (DiskObjectId)", "diskKey", disk.Key, "diskLabel", disk.DeviceInfo.GetDescription().Label)
	slog.Error("probably running on esxi version less than 6, should we support this?")
	return "", fmt.Errorf("could not find persistent identifier for disk with key %d", disk.Key)
}

func FindBootDiskKey(vm *mo.VirtualMachine) *int32 {
	if vm.Config != nil && vm.Config.BootOptions != nil {
		for _, dev := range vm.Config.BootOptions.BootOrder {
			if d, ok := dev.(*types.VirtualMachineBootOptionsBootableDiskDevice); ok {
				k := int32(d.DeviceKey)
				return &k
			}
		}
	}
	// Heuristic fallback: smallest (controller.busNumber, unitNumber) ignoring SCSI unit 7.
	var (
		bestKey  *int32
		bestBus  = int32(1 << 30)
		bestUnit = int32(1 << 30)
	)
	// Build map controllerKey -> busNumber
	busByCtl := map[int32]int32{}
	for _, dv := range vm.Config.Hardware.Device {
		switch c := dv.(type) {
		case *types.VirtualSATAController:
			busByCtl[c.Key] = c.BusNumber
		case *types.VirtualSCSIController:
			// SCSI has scsiCtlrUnitNumber + busNumber; use BusNumber for ordering
			busByCtl[c.Key] = c.BusNumber
		case *types.VirtualNVMEController:
			busByCtl[c.Key] = c.BusNumber
		}
	}
	for _, dv := range vm.Config.Hardware.Device {
		disk, ok := dv.(*types.VirtualDisk)
		if !ok || disk.UnitNumber == nil {
			continue
		}
		unit := *disk.UnitNumber
		// skip SCSI unit 7 if controller is SCSI
		if _, isScsi := vmDeviceIsSCSI(vm, disk.ControllerKey); isScsi && unit == 7 {
			continue
		}
		bus := busByCtl[disk.ControllerKey]
		if bus < bestBus || (bus == bestBus && unit < bestUnit) {
			bestBus = bus
			bestUnit = unit
			k := disk.Key
			bestKey = &k
		}
	}
	return bestKey
}

func vmDeviceIsSCSI(vm *mo.VirtualMachine, controllerKey int32) (int32, bool) {
	for _, dv := range vm.Config.Hardware.Device {
		if ctl, ok := dv.(*types.VirtualSCSIController); ok && ctl.Key == controllerKey {
			return ctl.BusNumber, true
		}
	}
	return 0, false
}

func CanonicalDiskKey(vm *mo.VirtualMachine, disk *types.VirtualDisk) (string, error) {
	if boot := FindBootDiskKey(vm); boot != nil && *boot == disk.Key {
		return "root", nil
	}
	id, err := GetPersistentDiskIdentifier(disk)
	if err != nil {
		return "", err
	}
	return id, nil
}
