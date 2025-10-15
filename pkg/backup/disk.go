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
		err = d.IncrementalCopy(ctx, mpu, oldChangeID)
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

	var partNumber int32 = 1          // this is increased each offset
	startOffset := int64(0)           // this is the start offset
	startNotChangedOffset := int64(0) // this is the start offset of the not changed area

	var endNotChangedOffset int64 // this is the end offset of the not changed area
	var chunkSize int64

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

		diskChangeInfo := res.Returnval // this is obtained each iteration, for larger sections, it has a bucket of changed areas

		/*
			this will work like this:
			starts from 0 and requests a list of changed areas
			it will return a list like this:
			[{Start: 0, Length: 1024MB}, {Start: 1024MB, Length: 1024MB}]
			for first iteration it will be:
			    offset = 0
				while offset is less than 0 + 1024
				chunkSize = 1024 - (0 - 0) = 1024
				if chunkSize > MaxChunkSize which is 64MB set chunkSize to 64MB
				so chunkSize will be 64 MB
				except when it is in the end of the changed area, it will be less than 64MB,
				the point is that it must be bigger than 5MB for multipart upload to work
				now it will create a buffer and read from handle the offset
				so it will read anywhere between 1 to 64MB from the handle
				then it will send the part to worker for upload
				it will then ++ part number
				and also will make offset = offset + chunkSize
				   so it will in the next iteration read from the same area but the other data
				then it will continue to the next changed area maybe another sector 1 gig ahead we do not know
				then it iterates again by moving the startOffset, so we can assume the diskChangeInfo will not have all the changed sectors,
				it will iterate again and ask again where it is changed, so it will be like this:
				hi where is changed from byte 0 to the end of the file?
				hey it is changed from byte 1024MB about 300MB
				    ok i will work on it
					yep i am finished till the 1024MB + 300MB sector
				hi again where is changed from byte 1024MB + 300MB to the end of the file?

				so we do need to find out the exact sectors which are not changed,
				we have find out the changed areas, we do not care how much is not changed,
				1 byte or 1 gig, we are going to find them all,
		*/
		for _, area := range diskChangeInfo.ChangedArea { // this may have multiple smaller areas
			/*
				what happens if the previous changed area have applied over read? and the new area is a part of that 4MB+ sector?
				we have increased the offset well over the next area.start, and we are going to reset the offset to
				the next area.Start,
				so for example lets consider this:
				1. last iteration had start : 2048MB end 2049MB
				2. the loop will over read to 2053MB so the offset will be 2053 and will be garbage collected and removed
				3. at the same time the startNotChangedOffset will be set to 2053 and we will be back to this loop
				4. so we will get the next area, which is I think is guaranteed to be next in the sorted order
				5. in a normal event we must continue with new offset
				6. but in over read, we must check some things
				7. we must check if if new offset is bigger than startNotChangedOffset
				8. if it is bigger, then we are safe to say that next area is well ahead of over read data
					for example if we have over read from 2048 to 2053, the next area is in 2054
					and then we will set the offset to 2054, the new end will be 2054
					so the 2053 - 2054 will be considered as multipart copied
				9. if it is not bigger, we must check two things, does it contain the area fully or not?
				10. if the over read area contains the new area, we will continue to next area,
					for example we have over read from 2048 to 2053, and the next area is from 2050 to 2051
					so the area.Start + area.Length is still less than 2053, so we will continue to the next area

			*/
			/*
				lets see a scenario here:
				0 => 1024MB not changed
				1024MB => 2048MB is changed
				2048MB => 2049MB is not changed
				2049MB => 2050MB is changed
				2050MB => 3000MB is not changed and finishes

				we do not know how many of these are grouped to gether in the main loop
				lets assume they are in these two loops:
					for _, area := range diskChangeInfo.ChangedArea {
					for offset := area.Start; offset < area.Start+area.Length; {
				so we start by the 0 - 1024MB which is not changed,
				so the first area will be 1024 to 2048

				startNotChangedOffset = 0
				for the first iteration
					offset = 1024
					endNotChangedOffset = 1024
					startNotChangedOffset is less than endNotChangedOffset: true
					also chunksize will be 64
					it will send the part 1 to the worker
					then at the end it will set:
						part++
						offset = 1024 + 64
						startNotChangedOffset = 1024 + 64
				the second iteration:
					offset = 1024 + 64
					endNotChangedOffset = 1024 + 64
					startNotChangedOffset is less than endNotChangedOffset: false
					also chunksize will be 64
					send part 2
					part ++
					offset = 1024 + 64 + 64
					startNotChangedOffset = 1024 + 64 + 64
				so it will continue until it reaches the end of the changed area at 2048
				which by the way 1024 is divisible by 64, so it will be 16 chunks and no partial part
				so for the last internal iteration it will set:
					partNumber = XXX
					offset = 2048
					startNotChangedOffset = 2048

					and then it will move to the next changed area
					which is 2049MB to 2050MB
				so the first iteration will be like this:
					offset = 2049
					endNotChangedOffset = 2049
					startNotChangedOffset is less than endNotChangedOffset: true
					as it is only 1MB the chunksize will be also 1MB
					which is less than 5MB so we will check:
							2049 + 1 = 2050 is it bigger than 3000?
							no so we will set the chunk size to 5MB
					now we are going to read from 2049 to 2054
					we will send the new part
					part ++
					offset = 2049 + 5
					startNotChangedOffset = 2049 + 5

				so in the next iteration it will break because offset is not 2054 and more than 2049 + 1
				it will set start offset to for example 3000 then it will ask again if there is anything changed above 3000
				and will recieve nothing and then it will continue to finish
				the point is that we are not catching the last not changed area

			*/
			for offset := area.Start; offset < area.Start+area.Length; { // this will walk only one area
				/*
					what happens if the previous iteration have a over read?
					this loop will break and we will move to the outer loop
				*/
				if area.Start < startNotChangedOffset {
					// this usually does not happen because we always keep the startNotChangedOffset equal or less than the offset(the old offset)
					// so when a new offset is less than startNotChangedOffset we definitely have a over read
					if area.Start+area.Length <= startNotChangedOffset {
						// this is the case where the previous iteration have a over read and the new area is a part of that 4MB+ sector
						// Skip this area entirely as it's already been uploaded
						slog.Debug("Skipping area already covered by over-read", "areaStart", area.Start, "areaEnd", area.Start+area.Length, "startNotChangedOffset", startNotChangedOffset)
						break
					} else {
						// this is the case when we have a changed sector with a part of it in the over read and a part of it in the new area.
						// for example we have over read from 2048 to 2053, and the next area is from 2052 to 3000
						// so we will set the offset to startNotChangedOffset instead of area.Start, because we have copied the over read data already
						offset = startNotChangedOffset
					}
				}
				endNotChangedOffset = offset - 1
				if startNotChangedOffset < endNotChangedOffset {
					// we have a not changed area from startNotChangedOffset to endNotChangedOffset (inclusive)
					mpu.SendPart(partNumber, nil, fmt.Sprintf("bytes=%d-%d", startNotChangedOffset, endNotChangedOffset), vms3.PartUploadTypeCopy)
					partNumber++
				}
				chunkSize = area.Length + area.Start - offset
				if chunkSize > MaxChunkSize { // if it is bigger than for example 64MB
					chunkSize = MaxChunkSize
				} else if chunkSize < S3MinChunkSizeLimit { // if it is less than 5MB
					slog.Debug("Changed chunk size is less than 5MB", "chunkSize", chunkSize)
					// we must check if this is the last chunk we are going to send to s3, it is at the end of the file or not
					/*
						offset is our head of cursor pointer always,
						so we can find out if we move about the exact size of chunkSize
						ahead, what happens
					*/
					if offset+chunkSize+1 >= d.Disk.CapacityInBytes {
						// this is the last chunk of the whole file, so we are free send it as it is
						slog.Debug("This is the last chunk we are going to send to s3", "offset", offset, "chunkSize", chunkSize)
					} else {
						/*
							This is going to be a lot of head ache,
							so basically, we are going to read the next X MB of data with it,
							so for example if the remained chunk size is about 128KB,
							we are going to anyway read about 4MB and 800KB, and send it to s3,
							the problems may occur:
								1. first of all, we do not know what was in the next 4MB file, maybe there was other sectors
								2. we must handle if we have a offset which has overtaken the next part
								3. we must handle the un changed part for multi part copy

							We name this OVER READ Scenario
						*/
						slog.Warn("Changed chunk size is less than 5MB but not the last chunk, changing it to 5MB", "chunkSize", chunkSize)
						chunkSize = S3MinChunkSizeLimit
						// will this break the logic? or will it break the nbdkit handle? we are not sure about it
					}
				}
				// startOffset = offset
				// endOffset = offset + chunkSize

				buf := make([]byte, chunkSize)
				/*
					Here we are going to read chunkSize of data from the offset by using the nbdkit handle
					which enables us to read in any offset, in any size we want
					the point is that for less than 5MB scenario we will also read about 4MB of unchanged
					data, which will add to network overhead but is acceptable for the moment
				*/
				err = handle.Pread(buf, uint64(offset), nil)
				if err != nil {
					return fmt.Errorf("failed to read from NBD at offset %d, chunkSize %d: %w", offset, chunkSize, err)
				}

				// Send to worker pool for upload
				if err := mpu.SendPart(partNumber, buf, "", vms3.PartUploadTypeUpload); err != nil {
					return fmt.Errorf("failed to send part %d for upload: %w", partNumber, err)
				}
				/*
					in this section, we have uploaded the part from the offset to the offset + chunkSize
					so other than increasing the part number and the offset, we have to consider another thing
					when we upload this chunk, we know these variables were pointing to:
					    startNotChangedOffset => it was pointing to where the previous changed disk area was ended, for example:
							1. when it was offset 0, if area starts at 1024, it is still 0
							2. when for example the last area was from 2048MB to 3000MB, the startNotChangedOffset was 3000MB
						endNotChangedOffset => it is when the offset first starts, for example:
							1. when a area starts at X and ends at Y, always the end not changed is set to X
					so in the start of each iteration, we must know about the last startNotChangedOffset and also we know
					when it is started at the offset therefor endNotChangedOffset, also maybe there is a scenario these
					two are the same, which we handle, then we will send multi part copy
					we do not care about the outer loops, as all the real things are happening in this loop
				*/
				slog.Debug("Sent changed part to workers", "partNumber", partNumber, "offset", offset, "chunkSize", chunkSize)
				partNumber++
				offset += chunkSize
				startNotChangedOffset = offset
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
