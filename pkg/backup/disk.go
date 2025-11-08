package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"
	"libguestfs.org/libnbd"
)

type DiskTarget struct {
	Disk                 *types.VirtualDisk
	SocketRef            *nbdkit.NbdkitSocket
	VM                   *DetailedVirtualMachine
	VMKey                string
	CurrentChangeID      *vmware.ChangeID
	BackupDiskIdentifier string
	DeviceKey            int32
}

// these can be of two types of multipart copy or upload
type DiskSector struct {
	StartOffset int64
	EndOffset   int64
	Length      int64
}

func NewDiskTarget(disk *types.VirtualDisk, socketRef *nbdkit.NbdkitSocket, vm *DetailedVirtualMachine, vmKey string) (*DiskTarget, error) {
	changeID, err := vmware.GetChangeID(disk)
	if err != nil {
		return nil, err
	}
	if vm.Properties.Config == nil || vm.Properties.Config.Hardware.Device == nil {
		return nil, fmt.Errorf("VM config or hardware list is nil, cannot determine disk IDs")
	}
	identifier, err := vmware.CanonicalDiskKey(vm.Properties, disk)
	if err != nil {
		return nil, err
	}
	return &DiskTarget{
		Disk:                 disk,
		SocketRef:            socketRef,
		VM:                   vm,
		VMKey:                vmKey,
		CurrentChangeID:      changeID,
		BackupDiskIdentifier: identifier,
		DeviceKey:            disk.Key,
	}, nil
}

func (d *DiskTarget) GetDiskObjectKey() string {
	return vms3.CreateDiskObjectKey(d.VMKey, d.GetDiskKey()) // this will be like: vm-data-<vmkey>/disk-data-<int>
}

func (d *DiskTarget) GetDiskKey() string {
	return d.BackupDiskIdentifier
}

func (d *DiskTarget) GetDiskSizeBytes() int64 {
	return d.Disk.CapacityInBytes
}

func (d *DiskTarget) GetCurrentDiskManifest() *vms3.DiskManifest {
	return &vms3.DiskManifest{
		Name:      d.VM.GetName(),
		ID:        d.VM.GetID(),
		ObjectKey: d.GetDiskObjectKey(),
		DiskKey:   d.GetDiskKey(),
		SizeBytes: d.GetDiskSizeBytes(),
		ChangeID:  d.CurrentChangeID.Value,
	}
}

func (d *DiskTarget) GetOldDiskManifestFromS3(ctx context.Context) (*vms3.DiskManifest, error) {
	manifestObjectKey := vms3.GetDiskManifestObjectKey(d.GetDiskObjectKey())
	diskManifest, err := d.VM.S3BackupClient.S3DB.GetVirtualObjectDiskManifest(
		ctx,
		manifestObjectKey,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk manifest from S3: %w", err)
	}
	return diskManifest, nil
}

func (d *DiskTarget) NeedsFullCopy(ctx context.Context, diskManifest *vms3.DiskManifest) (bool, error) {
	if diskManifest == nil {
		return true, nil
	}

	validated, _ := diskManifest.ValidateOnS3(ctx, d.VM.S3BackupClient.S3DB)
	if !validated {
		slog.Warn("Disk manifest is not valid, a partial failed copy was performed, starting a new full copy")
		return true, nil
	}

	oldChangeID, err := diskManifest.GetChangeID()
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
	if diskManifest.SizeBytes != d.GetDiskSizeBytes() {
		slog.Warn("Size mismatch vm is resized or replaced", "oldSize", diskManifest.SizeBytes, "newSize", d.GetDiskSizeBytes())
		return true, nil
	}

	slog.Debug("Does not need full copy", "oldChangeID", oldChangeID)
	return false, nil
}

func (d *DiskTarget) StartSync(ctx context.Context) error {
	diskManifest, err := d.GetOldDiskManifestFromS3(ctx)
	if err != nil {
		return err
	}
	needFullCopy, err := d.NeedsFullCopy(ctx, diskManifest)
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

	su := vms3.NewSimpleUpload(
		d.VM.S3BackupClient.S3DB,
	)
	su.BootWorkers(ctx, 4)

	if needFullCopy {
		if diskManifest != nil {
			err := diskManifest.CleanUpS3(ctx, d.VM.S3BackupClient.S3DB)
			if err != nil {
				return fmt.Errorf("failed to clean up old disk manifest from s3: %w", err)
			}
		}
		err := d.FullCopy(ctx, su)
		if err != nil {
			return err
		}
	} else {
		err = d.IncrementalCopy(ctx, su, diskManifest)
	}

	// Wait for all workers to finish
	if err := su.Wait(); err != nil {
		return fmt.Errorf("worker pool error: %w", err)
	}

	slog.Info("Disk sync completed successfully", "diskKey", d.GetDiskKey())
	return nil
}

func isChunkAllZeros(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func calculateSHA256(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func (d *DiskTarget) FullCopy(ctx context.Context, su *vms3.SimpleUpload) error {

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

	diskObjectKey := d.GetDiskObjectKey()

	numberOfSparseParts := 0

	diskFullChunksMetadata := []*vms3.S3FullChunkMetadata{}

	var partNumber int32 = 1
	for offset := int64(0); offset < d.Disk.CapacityInBytes; {
		// Determine the size of the chunk to read
		chunkSize := int64(vms3.MaxChunkSize)
		// If the remaining bytes are less than a full chunk, read only what's left
		if (d.Disk.CapacityInBytes - offset) < chunkSize {
			chunkSize = d.Disk.CapacityInBytes - offset
		}

		buf := make([]byte, chunkSize)
		err = handle.Pread(buf, uint64(offset), nil)
		if err != nil {
			return fmt.Errorf("failed to read from nbd at offset %d: %w", offset, err)
		}

		isAllZeros := isChunkAllZeros(buf)

		if isAllZeros {
			diskFullChunksMetadata = append(diskFullChunksMetadata, &vms3.S3FullChunkMetadata{
				StartOffset: offset,
				Length:      chunkSize,
				PartNumber:  partNumber,
				Compression: vms3.S3CompressionSparse,
				Checksum:    "",
			})
			slog.Debug("Add Sparse chunk to manifest", "startOffset", offset, "length", chunkSize, "partNumber", partNumber)
			numberOfSparseParts++
		} else {
			buf, err = vms3.CompressBufferZstd(buf)
			if err != nil {
				return fmt.Errorf("failed to compress buffer: %w", err)
			}
			metadata := &vms3.S3FullChunkMetadata{
				StartOffset: offset,
				Length:      chunkSize,
				PartNumber:  partNumber,
				Compression: vms3.S3CompressionZstd,
				Checksum:    calculateSHA256(buf),
			}
			metadataJSON, err := metadata.ToJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal S3 full chunk metadata: %w", err)
			}
			if err := su.DispatchUpload(ctx, vms3.GetS3FullObjectKey(diskObjectKey, partNumber), buf, metadataJSON, metadata.Checksum); err != nil {
				return fmt.Errorf("failed to dispatch upload: %w", err)
			}
			diskFullChunksMetadata = append(diskFullChunksMetadata, metadata)
		}
		partNumber++
		offset += chunkSize
	}
	diskManifest := &vms3.DiskManifest{
		Name:                d.VM.GetName(),
		ID:                  d.VM.GetID(),
		ObjectKey:           diskObjectKey,
		ChangeID:            d.CurrentChangeID.Value,
		DiskKey:             d.GetDiskKey(),
		SizeBytes:           d.GetDiskSizeBytes(),
		NumberOfSparseParts: numberOfSparseParts,
		FullChunksMetadata:  diskFullChunksMetadata,
	}
	metadataJSON, err := json.Marshal(diskManifest)
	if err != nil {
		return fmt.Errorf("failed to marshal disk metadata: %w", err)
	}
	if err := su.DispatchUpload(ctx, vms3.GetDiskManifestObjectKey(diskObjectKey), metadataJSON, "", calculateSHA256(metadataJSON)); err != nil {
		return fmt.Errorf("failed to dispatch upload for manifest file: %w", err)
	}
	slog.Info("Full copy to S3 completed", "diskKey", d.GetDiskKey(), "totalParts", partNumber-1)
	return nil
}

func (d *DiskTarget) IncrementalCopy(ctx context.Context, su *vms3.SimpleUpload, oldManifest *vms3.DiskManifest) error {
	slog.Info("Starting incremental copy", "diskKey", d.GetDiskKey(), "oldManifest", oldManifest.ChangeID)
	startOffset := int64(0)
	var changedSectors []*DiskSector
	for {
		req := types.QueryChangedDiskAreas{
			This:        d.VM.Ref.Reference(),
			Snapshot:    d.VM.SnapshotRef.Ref,
			DeviceKey:   d.Disk.Key,
			StartOffset: startOffset,
			ChangeId:    oldManifest.ChangeID,
		}
		res, err := methods.QueryChangedDiskAreas(ctx, d.VM.Ref.Client(), &req)
		if err != nil {
			return err
		}
		diskChangeInfo := res.Returnval
		for _, area := range diskChangeInfo.ChangedArea {
			changedSectors = append(changedSectors, &DiskSector{
				StartOffset: area.Start,
				Length:      area.Length,
				EndOffset:   area.Start + area.Length,
			})
		}
		startOffset = diskChangeInfo.StartOffset + diskChangeInfo.Length

		if startOffset >= d.Disk.CapacityInBytes {
			break
		}
	}

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
		chunkSize := int64(vms3.MaxChunkSize)
		if (d.Disk.CapacityInBytes - offset) < chunkSize {
			chunkSize = d.Disk.CapacityInBytes - offset
		}
		// check if this chunk is in the changed sectors or not
		isChanged := false
		for _, sector := range changedSectors {
			if sector.StartOffset < offset+chunkSize && sector.EndOffset > offset {
				isChanged = true
				slog.Debug("chunk has changed area", "partNumber", partNumber)
				break
			}
		}
		if !isChanged {
			// do not change anything on the s3 and continue
			partNumber++
			offset += chunkSize
			continue
		}
		buf := make([]byte, chunkSize)
		err = handle.Pread(buf, uint64(offset), nil)
		if err != nil {
			return fmt.Errorf("failed to read from nbd at offset %d: %w", offset, err)
		}
		isAllZeros := isChunkAllZeros(buf)

		if partNumber > int32(len(oldManifest.FullChunksMetadata)) {
			// this is a new part not existing before
			if isAllZeros {
				oldManifest.FullChunksMetadata = append(
					oldManifest.FullChunksMetadata,
					&vms3.S3FullChunkMetadata{
						StartOffset: offset,
						Length:      chunkSize,
						PartNumber:  partNumber,
						Compression: vms3.S3CompressionSparse,
						Checksum:    "",
					},
				)
				slog.Debug("Added new sparse part to manifest", "partNumber", partNumber, "startOffset", offset, "length", chunkSize)
				oldManifest.NumberOfSparseParts++
			} else {
				buf, err = vms3.CompressBufferZstd(buf)
				if err != nil {
					return fmt.Errorf("failed to compress buffer: %w", err)
				}
				metadata := &vms3.S3FullChunkMetadata{
					StartOffset: offset,
					Length:      chunkSize,
					PartNumber:  partNumber,
					Compression: vms3.S3CompressionZstd,
					Checksum:    calculateSHA256(buf),
				}
				metadataJSON, err := metadata.ToJSON()
				if err != nil {
					return fmt.Errorf("failed to marshal S3 full chunk metadata: %w", err)
				}
				if err := su.DispatchUpload(ctx, vms3.GetS3FullObjectKey(oldManifest.ObjectKey, partNumber), buf, metadataJSON, metadata.Checksum); err != nil {
					return fmt.Errorf("failed to dispatch upload: %w", err)
				}
				oldManifest.FullChunksMetadata = append(oldManifest.FullChunksMetadata, metadata)
				slog.Debug("adding new compressed chunk to manifest", "partNumber", partNumber)
			}
		} else {
			previousMetadata := oldManifest.FullChunksMetadata[partNumber-1]
			if isAllZeros {
				if previousMetadata.Compression == vms3.S3CompressionSparse {
					slog.Debug("sparse chunk is already sparse, nothing to do, this may not occur usually", "partNumber", partNumber)
					// do nothing
				} else {
					previousMetadata.Compression = vms3.S3CompressionSparse
					previousMetadata.Checksum = ""
					previousMetadata.Length = chunkSize
					oldManifest.NumberOfSparseParts++
					// i should delete the previous part from the s3
					// NOTE later put this on the worker too
					err := d.VM.S3BackupClient.S3DB.DeleteObject(ctx, vms3.GetS3FullObjectKey(oldManifest.ObjectKey, partNumber))
					if err != nil {
						return fmt.Errorf("failed to delete object from s3: %w", err)
					}
					slog.Debug("part changed to sparse, updating the manifest", "partNumber", partNumber)
				}
			} else {
				if previousMetadata.Compression == vms3.S3CompressionSparse {
					oldManifest.NumberOfSparseParts--
					slog.Debug("sparse chunk changed to compressed, updating the manifest", "partNumber", partNumber)
				} else {
					slog.Debug("compressed chunk changed, re-uploading again", "partNumber", partNumber)
				}
				buf, err = vms3.CompressBufferZstd(buf)
				if err != nil {
					return fmt.Errorf("failed to compress buffer: %w", err)
				}
				previousMetadata.Compression = vms3.S3CompressionZstd
				previousMetadata.Checksum = calculateSHA256(buf)
				previousMetadata.Length = chunkSize
				metadataJSON, err := previousMetadata.ToJSON()
				if err != nil {
					return fmt.Errorf("failed to marshal S3 full chunk metadata: %w", err)
				}
				if err := su.DispatchUpload(ctx, vms3.GetS3FullObjectKey(oldManifest.ObjectKey, partNumber), buf, metadataJSON, previousMetadata.Checksum); err != nil {
					return fmt.Errorf("failed to dispatch upload: %w", err)
				}
			}
		}
		offset += chunkSize
		partNumber++
	}
	oldManifest.ChangeID = d.CurrentChangeID.Value
	oldManifest.SizeBytes = d.Disk.CapacityInBytes
	metadataJSON, err := json.Marshal(oldManifest)
	if err != nil {
		return fmt.Errorf("failed to marshal disk metadata: %w", err)
	}
	if err := su.DispatchUpload(ctx, vms3.GetDiskManifestObjectKey(oldManifest.ObjectKey), metadataJSON, "", calculateSHA256(metadataJSON)); err != nil {
		return fmt.Errorf("failed to dispatch upload for manifest file: %w", err)
	}
	slog.Info("Incremental copy to S3 completed", "diskKey", d.GetDiskKey(), "totalParts", partNumber-1)
	return nil
}
