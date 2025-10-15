package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"

	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"
	"libguestfs.org/libnbd"
)

const MaxChunkSize = 64 * 1024 * 1024       // 64 MB
const S3MinChunkSizeLimit = 5 * 1024 * 1024 // 5 MB

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

// these can be of two types of multipart copy or upload
type DiskSector struct {
	StartOffset int64
	Length      int64
	PartNumber  int32
	Type        vms3.PartUploadType
	Squashed    bool
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

	if needFullCopy {
		err := d.FullCopy(ctx, mpu)
		if err != nil {
			return err
		}
	} else {
		oldChangeID, err := diskMetaData.GetChangeID()
		if err != nil {
			return err
		}
		err = d.IncrementalCopyDetection(ctx, mpu, oldChangeID)
		if err != nil {
			return err
		}
	}

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
		if err := mpu.SendPart(partNumber, buf, "", vms3.PartUploadTypeUpload); err != nil {
			return fmt.Errorf("failed to send part %d for upload: %w", partNumber, err)
		}

		slog.Debug("Sent part to workers", "partNumber", partNumber, "offset", offset, "chunkSize", chunkSize)
		partNumber++
		offset += chunkSize
	}

	slog.Info("Full copy to S3 completed", "diskKey", d.GetDiskKey(), "totalParts", partNumber-1)
	return nil
}

func (d *DiskTarget) SquashSectors(ctx context.Context, combinedSectors []*DiskSector) ([]*DiskSector, error) {
	slog.Debug("Refining sectors", "combinedSectors", combinedSectors)
	/*
		Here we have some guarantees about the sectors:
		1. all of the file from 0 to end, is in these sectors, as upload sections or copy sections
		2. never a small changed section is next to another small section => 1-2 MB changed and then 2 - 3MB changed, this is only possible when 2 -3 is not changed
		3. we are never iterating backward, we always will iterate forward => this is needed to reduce the complexity
		4. We can append to a changed sector if it is even bigger than MaxChunkSize => this is the cost we are paying
			but it will be at max 64MB + 5MB = 69MB ??? -> is this guaranteed? i don't think so
		5. everything is sequential executed

		Problems to solve:
		1. we may have too much small chunks next to each other
	*/
	var refinedSectors []*DiskSector
	for index, sector := range combinedSectors {
		if sector.Squashed {
			// this sector is already squashed no need to work on it
			continue
		}
		if sector.Length >= S3MinChunkSizeLimit {
			sector.PartNumber = int32(len(refinedSectors) + 1)
			refinedSectors = append(refinedSectors, sector)
			// then we will move to the next sector
			continue
		}
		// after this all these sectors are smaller than 5MB
		if sector.Type == vms3.PartUploadTypeCopy {
			/*
				now here we have the case when a copy sector, is below 5MB
				and we will get an error in multipart upload, then how to fix
				this?
				we are sure that the next sector is changed, why? it will be reported with a different
				length if it was
				so we can just squash this sector which is not changed but is small, to the next sector,
				and forget about it
			*/
			if index+1 >= len(combinedSectors) {
				// this is the last sector, we can just add it and continue
				// we do not change the last sector as it is free to go
				sector.PartNumber = int32(len(refinedSectors) + 1)
				refinedSectors = append(refinedSectors, sector)
				break
			}
			sector.Squashed = true
			nextSector := combinedSectors[index+1]
			nextSector.Length = nextSector.Length + sector.Length
			nextSector.StartOffset = sector.StartOffset
			// we will not do anything
			// not appending to refinedSectors -> as this sector is squashed
			// we are not appending the nextSector -> as itself may be squashed to its next sector we do not know yet
		} else if sector.Type == vms3.PartUploadTypeUpload {
			/*
				In this case we can just increase the length to 5MB without thinking about the next sectors,
				as we are able to extend this part to 64MB but we are going with 5MB, as it will cause simpler logic

				so we will try to change the length to 5MB and then iterate on the next sectors
				and make them squashed if required
			*/
			if d.Disk.CapacityInBytes-sector.StartOffset < S3MinChunkSizeLimit {
				sector.Length = d.Disk.CapacityInBytes - sector.StartOffset
			} else {
				sector.Length = S3MinChunkSizeLimit
			}
			sector.PartNumber = int32(len(refinedSectors) + 1)
			// start offset is the same, we do not care about it
			refinedSectors = append(refinedSectors, sector)
			sectorNewEnd := sector.StartOffset + sector.Length
			// then we will iterate on the next sectors until they are well over 5MB next
			for i := index + 1; i < len(combinedSectors); i++ {
				nextSector := combinedSectors[i]
				nextSectorEnd := nextSector.StartOffset + nextSector.Length
				if nextSector.StartOffset < sectorNewEnd {
					if nextSectorEnd <= sectorNewEnd {
						// fully engulfed the whole next sector
						nextSector.Length = 0
						nextSector.Squashed = true
					} else {
						// partially engulfed here
						nextSector.Length = nextSectorEnd - sectorNewEnd
						nextSector.StartOffset = sectorNewEnd
					}
				} else {
					// it is finished
					break
				}
			}
		} else {
			return nil, fmt.Errorf("unknown sector type when executing refinement")
		}
	}
	return refinedSectors, nil
}

func (d *DiskTarget) CleanUploadSectors(ctx context.Context, changedSectors []*DiskSector) ([]*DiskSector, error) {
	/*
		Let us talk about it,
		if i have a chunk in the disk, which is bigger than 5MB, it is guaranteed it will not have any over read part ->
			0 - 65MB is a changed sector -> this will be a 65MB chunk as part 1
			0 - 129MB is 1. 0 - 64MB and 2. 64MB - 129MB
		there is no guarantee to have a small stand alone chunk well below 5MB -> for example 1KB or 1MB
		this chunk is not guaranteed to have small sectors before and after it -> we do not know for example 0 - 1 C, 1 - 2 N, 2 - 3 C, 3 - 4 N
		there is no guarantee to have a small stand alone not changed chunk in the middle of the disk sectors

		This function will ignore the 5MB limit and find the not changed sectors and return a slice of combined sectors
	*/
	slog.Debug("Cleaning upload sectors", "changedSectors", changedSectors)
	// first of make sure sectors are sorted by the start or part number
	sort.Slice(changedSectors, func(i, j int) bool {
		return changedSectors[i].PartNumber < changedSectors[j].PartNumber
	})

	var CombinedSectors []*DiskSector
	fullDiskSize := d.Disk.CapacityInBytes
	currentCursor := int64(0)
	sectorEnd := int64(0)

	for _, sector := range changedSectors {
		sectorStart := sector.StartOffset
		sectorEnd = sectorStart + sector.Length // -> start is 0, and length is 64 -> it will be from 0 to 63
		// sectorEnd is not safe and it is the next sector start point, it is not exactly used in this sector
		if sectorEnd >= fullDiskSize {
			// this is the last sector, this is free to go in any size
			sector.PartNumber = int32(len(CombinedSectors) + 1)
			CombinedSectors = append(CombinedSectors, sector)
			break
		}
		if sectorStart == currentCursor {
			// this means that a sector is immediately started after another sector
			// previous sector was from 0 - 64 MB and the new sector is from 64MB - 128MB
			// we are going to append this and continue to the next sector
			sector.PartNumber = int32(len(CombinedSectors) + 1)
			CombinedSectors = append(CombinedSectors, sector)
			currentCursor = sectorEnd
			continue
		} else if sectorStart > currentCursor {
			// we have found a new not changed area from currentCursor to the sectorStart - 1
			CombinedSectors = append(CombinedSectors, &DiskSector{
				StartOffset: currentCursor,
				Length:      sectorStart - currentCursor,
				PartNumber:  int32(len(CombinedSectors) + 1),
				Type:        vms3.PartUploadTypeCopy,
				Squashed:    false,
			})
			sector.PartNumber = int32(len(CombinedSectors) + 1)
			CombinedSectors = append(CombinedSectors, sector)
			currentCursor = sectorEnd
			continue
		} else if sectorStart < currentCursor {
			// this may not happen
			return nil, fmt.Errorf("sector start is less than current cursor")
		}
	}
	if sectorEnd < fullDiskSize {
		CombinedSectors = append(CombinedSectors, &DiskSector{
			StartOffset: sectorEnd,
			Length:      fullDiskSize - sectorEnd,
			PartNumber:  int32(len(CombinedSectors) + 1),
			Type:        vms3.PartUploadTypeCopy,
			Squashed:    false,
		})
	}
	finalizedSectors, err := d.SquashSectors(ctx, CombinedSectors)
	if err != nil {
		return nil, err
	}
	totalCopySize := int64(0)
	totalUploadSize := int64(0)
	for _, sector := range finalizedSectors {
		slog.Debug("Combined sector", "Number", sector.PartNumber, "StartOffset", sector.StartOffset, "Length", sector.Length, "Type", sector.Type, "EndOffset", sector.StartOffset+sector.Length)
		if sector.Type == vms3.PartUploadTypeCopy {
			totalCopySize += sector.Length
		} else if sector.Type == vms3.PartUploadTypeUpload {
			totalUploadSize += sector.Length
		}
	}

	slog.Debug("Combined sectors", "FileSize", fullDiskSize, "finalizedSectors", finalizedSectors, "totalCopySizeMB", totalCopySize/1024/1024, "totalUploadSizeMB", totalUploadSize/1024/1024)
	return finalizedSectors, nil
}

func (d *DiskTarget) IncrementalCopyDetection(ctx context.Context, mpu *vms3.MultiPartUpload, oldChangeID *vmware.ChangeID) error {
	slog.Info("Starting incremental copy detection", "diskKey", d.GetDiskKey(), "oldChangeID", oldChangeID.Value)
	startOffset := int64(0)
	partNumber := int32(0)
	var chunkSize int64
	var changedSectors []*DiskSector
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
				chunkSize = area.Length + area.Start - offset
				if chunkSize > MaxChunkSize {
					nextChunkSize := chunkSize - MaxChunkSize
					if nextChunkSize >= S3MinChunkSizeLimit {
						chunkSize = MaxChunkSize
					} else {
						// we do not change the chunk size -> it may be for example 68 MB
						// NOTE This will cause a problem: WE DO NOT GUARANTEE EVERY CHUNK IS AT MAX 64MB, they can be bigger
					}
				}
				changedSectors = append(changedSectors, &DiskSector{
					StartOffset: offset,
					Length:      chunkSize,
					PartNumber:  partNumber,
					Type:        vms3.PartUploadTypeUpload,
					Squashed:    false,
				})
				partNumber++
				offset += chunkSize

			}
		}
		startOffset = diskChangeInfo.StartOffset + diskChangeInfo.Length

		if startOffset == d.Disk.CapacityInBytes {
			break
		}
	}

	refinedSectors, err := d.CleanUploadSectors(ctx, changedSectors)
	if err != nil {
		return err
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
	for _, sector := range refinedSectors {
		if sector.Type == vms3.PartUploadTypeUpload {
			buf := make([]byte, sector.Length)
			err = handle.Pread(buf, uint64(sector.StartOffset), nil)
			if err != nil {
				return fmt.Errorf("failed to read from NBD at offset %d, chunkSize %d: %w", sector.StartOffset, sector.Length, err)
			}
			if err := mpu.SendPart(
				sector.PartNumber,
				buf,
				"",
				vms3.PartUploadTypeUpload,
			); err != nil {
				return fmt.Errorf("failed to send upload part %d to worker: %w", sector.PartNumber, err)
			}
		} else if sector.Type == vms3.PartUploadTypeCopy {
			if err := mpu.SendPart(
				sector.PartNumber,
				nil,
				fmt.Sprintf("bytes=%d-%d", sector.StartOffset, sector.StartOffset+sector.Length),
				vms3.PartUploadTypeCopy,
			); err != nil {
				return fmt.Errorf("failed to send copy part %d to worker: %w", sector.PartNumber, err)
			}
		} else {
			return fmt.Errorf("unknown sector type when executing incremental copy detection")
		}
	}
	return nil
}
