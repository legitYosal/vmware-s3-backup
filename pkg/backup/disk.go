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
		err = d.IncrementalCopyDetection(ctx, oldChangeID)
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
	for _, sector := range finalizedSectors {
		slog.Debug("Combined sector", "Number", sector.PartNumber, "StartOffset", sector.StartOffset, "Length", sector.Length, "Type", sector.Type)
	}
	slog.Debug("Combined sectors", "FileSize", fullDiskSize, "finalizedSectors", finalizedSectors)
	return finalizedSectors, nil
}

func (d *DiskTarget) IncrementalCopyDetection(ctx context.Context, oldChangeID *vmware.ChangeID) error {
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

	changedSectors, err := d.CleanUploadSectors(ctx, changedSectors)
	if err != nil {
		return err
	}

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
				/*
					new problem => under copy strategy
					we have a over ready strategy as we said, when we have a chunk of data which is less than 5MB, then we are going to read 5MB anyway
					now the problem is that, for the copy chunk this rule, remains =>> NEW Challenge => we will name this UNDER COPY
					so for example if we have this layout =>
						0 - 1024 not changed
						1024 - 2048 changed
						2048 - 2049 no changed
						2049 - 3000 changed
					then algorithm will try to copy the 0 - 1024 as part 1
					and then uploads 1024 - 2048 as part 2
					then it copies 2048 - 2049 as part 3 -> causing error
					then it uploads 2049 - 3000 as part 4

					so what are we going to do about the under copy part?
					we are going to add it to the next part, so we are again doing an under copy,
					what happens next? we get drowned in a nested scenario for example:
					0 - 1024 not changed
					1024_64K - 1024_128K changed
					1024_256K - 1025 not changed
					1026 - 1026_512K changed
					1026_512K - 1027 not changed
					1027 - 1028 changed
					= > so at this point doing this will cause heavy loss and pain for us, so we are going to
					separate concerns and do this in another thread or function
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
