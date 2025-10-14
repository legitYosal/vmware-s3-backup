package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"
	"libguestfs.org/libnbd"
)

const MaxChunkSize = 64 * 1024 * 1024 // 64 MB

type DiskTarget struct {
	Disk            *types.VirtualDisk
	SocketRef       *nbdkit.NbdkitSocket
	VM              *DetailedVirtualMachine
	VMKey           string
	CurrentChangeID *vmware.ChangeID
}

type DiskMetadata struct {
	Name      string `json:"name"`
	ID        string `json:"id"`
	ObjectKey string `json:"object_key"`
	ChangeID  string `json:"change_id"`
	DiskKey   string `json:"disk_key"`
	SizeBytes int64  `json:"size_bytes"`
}

func NewDiskTarget(disk *types.VirtualDisk, socketRef *nbdkit.NbdkitSocket, vm *DetailedVirtualMachine, vmKey string) (*DiskTarget, error) {
	changeID, err := vmware.GetChangeID(disk)
	if err != nil {
		return nil, err
	}
	return &DiskTarget{
		Disk:            disk,
		SocketRef:       socketRef,
		VM:              vm,
		VMKey:           vmKey,
		CurrentChangeID: changeID,
	}, nil
}

func (d *DiskMetadata) GetChangeID() (*vmware.ChangeID, error) {
	changeID, err := vmware.ParseChangeID(d.ChangeID)
	if err != nil {
		return nil, err
	}
	return changeID, nil
}

func (d *DiskTarget) GetDiskObjectKey() string {
	return vms3.CreateDiskObjectKey(d.VMKey, d.GetDiskKey()) // this will be like: vm-data-<vmkey>/disk-data-<int>
}

func (d *DiskTarget) GetDiskKey() string {
	return strconv.Itoa(int(d.Disk.Key))
}

func (d *DiskTarget) GetDiskSizeBytes() int64 {
	return d.Disk.CapacityInBytes
}

func (d *DiskTarget) GetCurrentDiskMetadata() *DiskMetadata {
	return &DiskMetadata{
		Name:      d.VM.GetName(),
		ID:        d.VM.GetID(),
		ObjectKey: d.GetDiskObjectKey(),
		DiskKey:   d.GetDiskKey(),
		SizeBytes: d.GetDiskSizeBytes(),
		ChangeID:  d.CurrentChangeID.Value,
	}
}

func (d *DiskTarget) GetOldDiskMetadataFromS3(ctx context.Context) (*DiskMetadata, error) {
	metadata, err := d.VM.S3BackupClient.S3DB.GetMetadataInObject(
		ctx,
		d.VM.S3BackupClient.Configuration.S3BucketName,
		d.GetDiskObjectKey(),
	)
	if err != nil {
		return nil, err
	}
	if metadata == "" {
		return nil, nil
	}
	var diskMetadata DiskMetadata
	err = json.Unmarshal([]byte(metadata), &diskMetadata)
	if err != nil {
		return nil, err
	}
	return &diskMetadata, nil
}

func (d *DiskTarget) NeedsFullCopy(ctx context.Context, diskMetaData *DiskMetadata) (bool, error) {
	if diskMetaData == nil {
		return true, nil
	}

	oldChangeID, err := diskMetaData.GetChangeID()
	if err != nil {
		return true, err
	}
	if oldChangeID == nil {
		return true, nil
	}

	snapshotChangeID := d.CurrentChangeID

	if snapshotChangeID == nil {
		return true, nil
	}

	if oldChangeID.UUID != snapshotChangeID.UUID {
		slog.Warn("Change ID mismatch", "oldChangeID", oldChangeID, "snapshotChangeID", snapshotChangeID)
		return true, nil
	}
	slog.Debug("Does not need full copy", "oldChangeID", oldChangeID)
	return false, nil
}

func (d *DiskTarget) StartSync(ctx context.Context) error {
	diskMetaData, err := d.GetOldDiskMetadataFromS3(ctx)
	if err != nil {
		return err
	}
	needFullCopy, err := d.NeedsFullCopy(ctx, diskMetaData)
	if err != nil {
		return err
	}
	slog.Debug("Need full copy", "needFullCopy", needFullCopy)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		slog.Warn("Received interrupt signal, cleaning up...")
		os.Exit(1)
	}()

	// Create multipart upload
	currentMetadata := d.GetCurrentDiskMetadata()
	metadataJSON, err := json.Marshal(currentMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal disk metadata: %w", err)
	}

	mpu, err := d.VM.S3BackupClient.S3DB.CreateMultipartUpload(
		ctx,
		d.VM.S3BackupClient.Configuration.S3BucketName,
		d.GetDiskObjectKey(),
		string(metadataJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	// Boot workers (4 workers by default)
	mpu.BootWorkers(ctx, 4)

	if true {
		err := d.FullCopy(ctx, mpu)
		if err != nil {
			return err
		}
	}
	// else {
	// 	oldChangeID, err := diskMetaData.GetChangeID()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = d.IncrementalCopy(ctx, mpu, oldChangeID)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// Wait for all workers to finish
	if err := mpu.Wait(); err != nil {
		return fmt.Errorf("worker pool error: %w", err)
	}

	// Complete the multipart upload
	if err := mpu.CompleteMultipartUpload(ctx); err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	slog.Info("Disk sync completed successfully", "diskKey", d.GetDiskKey())
	return nil
}

func (d *DiskTarget) FullCopy(ctx context.Context, mpu *vms3.MultiPartUpload) error {

	slog.Info("Starting full copy to S3", "diskKey", d.GetDiskKey(), "sizeBytes", d.GetDiskSizeBytes())

	handle, err := libnbd.Create()
	if err != nil {
		return err
	}
	defer handle.Close()

	err = handle.ConnectUri(d.SocketRef.LibNBDExportName())
	if err != nil {
		return err
	}

	var partNumber int32 = 1
	for offset := int64(0); offset < d.Disk.CapacityInBytes; {
		// Determine the size of the chunk to read
		chunkSize := int64(MaxChunkSize)
		// If the remaining bytes are less than a full chunk, read only what's left
		if (d.Disk.CapacityInBytes - offset) < chunkSize {
			chunkSize = d.Disk.CapacityInBytes - offset
		}

		buf := make([]byte, chunkSize)
		err = handle.Pread(buf, uint64(offset), nil)
		if err != nil {
			return fmt.Errorf("failed to read from nbd at offset %d: %w", offset, err)
		}

		// Send to worker pool for upload
		if err := mpu.SendPart(partNumber, buf); err != nil {
			return fmt.Errorf("failed to send part %d for upload: %w", partNumber, err)
		}

		slog.Debug("Sent part to workers", "partNumber", partNumber, "offset", offset, "chunkSize", chunkSize)
		partNumber++
		offset += chunkSize
	}

	slog.Info("Full copy to S3 completed", "diskKey", d.GetDiskKey(), "totalParts", partNumber-1)
	return nil
}

func (d *DiskTarget) IncrementalCopy(ctx context.Context, mpu *vms3.MultiPartUpload, oldChangeID *vmware.ChangeID) error {
	slog.Info("Starting incremental copy to S3", "diskKey", d.GetDiskKey(), "oldChangeID", oldChangeID.Value)

	handle, err := libnbd.Create()
	if err != nil {
		return err
	}
	defer handle.Close()

	err = handle.ConnectUri(d.SocketRef.LibNBDExportName())
	if err != nil {
		return err
	}

	var partNumber int32 = 1
	startOffset := int64(0)
	for {
		req := types.QueryChangedDiskAreas{
			This:        d.VM.Ref.Reference(),
			Snapshot:    d.VM.SnapshotRef.Ref,
			DeviceKey:   d.Disk.Key,
			StartOffset: startOffset,
			ChangeId:    oldChangeID.Value,
		}

		res, err := methods.QueryChangedDiskAreas(ctx, d.VM.Ref.Client(), &req)
		if err != nil {
			return err
		}

		diskChangeInfo := res.Returnval

		for _, area := range diskChangeInfo.ChangedArea {
			for offset := area.Start; offset < area.Start+area.Length; {
				chunkSize := area.Length - (offset - area.Start)
				if chunkSize > MaxChunkSize {
					chunkSize = MaxChunkSize
				}

				buf := make([]byte, chunkSize)
				err = handle.Pread(buf, uint64(offset), nil)
				if err != nil {
					return err
				}

				// Send to worker pool for upload
				if err := mpu.SendPart(partNumber, buf); err != nil {
					return fmt.Errorf("failed to send part %d for upload: %w", partNumber, err)
				}

				slog.Debug("Sent changed part to workers", "partNumber", partNumber, "offset", offset, "chunkSize", chunkSize)
				partNumber++
				offset += chunkSize
			}
		}

		startOffset = diskChangeInfo.StartOffset + diskChangeInfo.Length

		if startOffset >= d.Disk.CapacityInBytes {
			break
		}
	}

	slog.Info("Incremental copy to S3 completed", "diskKey", d.GetDiskKey(), "totalParts", partNumber-1)
	return nil
}
