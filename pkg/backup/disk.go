package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
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

type DiskManifest struct {
	Name                string                 `json:"name"`
	ID                  string                 `json:"id"`
	ObjectKey           string                 `json:"object_key"`
	ChangeID            string                 `json:"change_id"`
	DiskKey             string                 `json:"disk_key"`
	SizeBytes           int64                  `json:"size_bytes"`
	NumberOfSparseParts int                    `json:"number_of_sparse_parts"`
	FullChunksMetadata  []*S3FullChunkMetadata `json:"full_chunks_metadata"`
}

// these can be of two types of multipart copy or upload
type DiskSector struct {
	StartOffset int64
	Length      int64
	PartNumber  int32
	Type        vms3.PartUploadType
	Squashed    bool
}

type S3Compression string

const (
	S3CompressionNone   S3Compression = "none"
	S3CompressionZstd   S3Compression = "zstd"
	S3CompressionSparse S3Compression = "sparse"
)

type S3FullChunkMetadata struct {
	StartOffset int64
	Length      int64
	PartNumber  int32
	Compression S3Compression
}

func NewS3FullChunkMetadata(startOffset int64, length int64, partNumber int32, compression S3Compression) *S3FullChunkMetadata {
	return &S3FullChunkMetadata{
		StartOffset: startOffset,
		Length:      length,
		PartNumber:  partNumber,
		Compression: compression,
	}
}

func (s *S3FullChunkMetadata) ToJSON() (string, error) {
	jsonData, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
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

func (d *DiskManifest) GetChangeID() (*vmware.ChangeID, error) {
	changeID, err := vmware.ParseChangeID(d.ChangeID)
	if err != nil {
		return nil, err
	}
	return changeID, nil
}

func (d *DiskTarget) GetDiskObjectKey() string {
	return vms3.CreateDiskObjectKey(d.VMKey, d.GetDiskKey()) // this will be like: vm-data-<vmkey>/disk-data-<int>
}

func (d *DiskTarget) GetS3FullObjectKey(prefixKey string, partNumber int32) string {
	// vm-uuid/disk-key/full/part-number
	return fmt.Sprintf("%s/full/%06d", prefixKey, partNumber)
}
func (d *DiskTarget) GetS3IncrementalObjectKey(prefixKey string, datetime time.Time) string {
	// vm-uuid/disk-key/incremental/YYYY-MM-DD-HH-MM-SS
	return fmt.Sprintf("%s/incremental/%s", prefixKey, datetime.Format("2006-01-02-15-04-05"))
}
func (d *DiskTarget) GetDiskManifestObjectKey(prefixKey string) string {
	return fmt.Sprintf("%s/manifest.json", prefixKey)
}

func (d *DiskTarget) GetDiskKey() string {
	return strconv.Itoa(int(d.Disk.Key))
}

func (d *DiskTarget) GetDiskSizeBytes() int64 {
	return d.Disk.CapacityInBytes
}

func (d *DiskTarget) GetCurrentDiskManifest() *DiskManifest {
	return &DiskManifest{
		Name:      d.VM.GetName(),
		ID:        d.VM.GetID(),
		ObjectKey: d.GetDiskObjectKey(),
		DiskKey:   d.GetDiskKey(),
		SizeBytes: d.GetDiskSizeBytes(),
		ChangeID:  d.CurrentChangeID.Value,
	}
}

func (d *DiskTarget) GetOldDiskManifestFromS3(ctx context.Context) (*DiskManifest, error) {
	manifestObjectKey := d.GetDiskManifestObjectKey(d.GetDiskObjectKey())
	data, err := d.VM.S3BackupClient.S3DB.GetObject(
		ctx,
		d.VM.S3BackupClient.Configuration.S3BucketName,
		manifestObjectKey,
	)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var diskManifest DiskManifest
	err = json.Unmarshal(data, &diskManifest)
	if err != nil {
		return nil, err
	}
	return &diskManifest, nil
}

func compressBufferZstd(data []byte) ([]byte, error) {
	var compressedBuf bytes.Buffer
	writer, err := zstd.NewWriter(&compressedBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}
	_, err = writer.Write(data)
	if err != nil {
		writer.Close() // Ensure the writer is closed on error
		return nil, fmt.Errorf("failed to write data to zstd writer: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zstd writer: %w", err)
	}
	return compressedBuf.Bytes(), nil
}

func decompressBufferZstd(compressedData []byte, originalSize int) ([]byte, error) {
	dstBuf := make([]byte, originalSize)
	compressedReader := bytes.NewReader(compressedData)
	reader, err := zstd.NewReader(compressedReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer reader.Close()
	n, err := io.ReadFull(reader, dstBuf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		// io.ErrUnexpectedEOF can happen if the originalSize was wrong (too small)
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}
	if n != originalSize {
		// This is a critical error, meaning the decompression failed or size was wrong.
		return nil, fmt.Errorf("decompressed size mismatch: expected %d bytes, got %d", originalSize, n)
	}
	return dstBuf, nil
}

func (d *DiskTarget) NeedsFullCopy(ctx context.Context, diskManifest *DiskManifest) (bool, error) {
	if diskManifest == nil {
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

	// Create multipart upload
	currentMetadata := d.GetCurrentDiskManifest()
	metadataJSON, err := json.Marshal(currentMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal disk metadata: %w", err)
	}

	su := vms3.NewSimpleUpload(
		d.VM.S3BackupClient.Configuration.S3BucketName,
		d.VM.S3BackupClient.S3DB,
	)
	su.BootWorkers(ctx, 4)

	// Boot workers (4 workers by default)
	su.DispatchUpload(ctx, d.GetDiskObjectKey(), metadataJSON, string(metadataJSON))

	if needFullCopy {
		err := d.FullCopy(ctx, su)
		if err != nil {
			return err
		}
	} else {
		// oldChangeID, err := diskManifest.GetChangeID()
		// if err != nil {
		// 	return err
		// }
		// err = d.IncrementalCopyDetection(ctx, su, oldChangeID)
		// if err != nil {
		// 	return err
		// }
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

	diskFullChunksMetadata := []*S3FullChunkMetadata{}

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

		isAllZeros := isChunkAllZeros(buf)

		if isAllZeros {
			// metadata, err := NewS3FullChunkMetadata(offset, chunkSize, partNumber, S3CompressionSparse).ToJSON()
			// if err != nil {
			// 	return fmt.Errorf("failed to marshal S3 full chunk metadata: %w", err)
			// }
			// if err := su.DispatchUpload(ctx, d.GetS3FullObjectKey(diskObjectKey, partNumber), []byte{}, metadata); err != nil {
			// 	return fmt.Errorf("failed to dispatch sparse upload: %w", err)
			// }
			diskFullChunksMetadata = append(diskFullChunksMetadata, &S3FullChunkMetadata{
				StartOffset: offset,
				Length:      chunkSize,
				PartNumber:  partNumber,
				Compression: S3CompressionSparse,
			})
			numberOfSparseParts++
		} else {
			buf, err = compressBufferZstd(buf)
			if err != nil {
				return fmt.Errorf("failed to compress buffer: %w", err)
			}
			metadata, err := NewS3FullChunkMetadata(offset, chunkSize, partNumber, S3CompressionZstd).ToJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal S3 full chunk metadata: %w", err)
			}
			if err := su.DispatchUpload(ctx, d.GetS3FullObjectKey(diskObjectKey, partNumber), buf, metadata); err != nil {
				return fmt.Errorf("failed to dispatch upload: %w", err)
			}
			diskFullChunksMetadata = append(diskFullChunksMetadata, &S3FullChunkMetadata{
				StartOffset: offset,
				Length:      chunkSize,
				PartNumber:  partNumber,
				Compression: S3CompressionZstd,
			})
		}

		slog.Debug("Sent part to workers", "partNumber", partNumber, "offset", offset, "chunkSize", chunkSize)
		partNumber++
		offset += chunkSize
	}
	diskManifest := &DiskManifest{
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
	if err := su.DispatchUpload(ctx, d.GetDiskManifestObjectKey(diskObjectKey), metadataJSON, ""); err != nil {
		return fmt.Errorf("failed to dispatch upload for manifest file: %w", err)
	}
	slog.Info("Full copy to S3 completed", "diskKey", d.GetDiskKey(), "totalParts", partNumber-1)
	return nil
}

// func (d *DiskTarget) SquashSectors(ctx context.Context, combinedSectors []*DiskSector) ([]*DiskSector, error) {
// 	slog.Debug("Refining sectors", "combinedSectors", combinedSectors)
// 	/*
// 		Here we have some guarantees about the sectors:
// 		1. all of the file from 0 to end, is in these sectors, as upload sections or copy sections
// 		2. never a small changed section is next to another small section => 1-2 MB changed and then 2 - 3MB changed, this is only possible when 2 -3 is not changed
// 		3. we are never iterating backward, we always will iterate forward => this is needed to reduce the complexity
// 		4. We can append to a changed sector if it is even bigger than MaxChunkSize => this is the cost we are paying
// 			but it will be at max 64MB + 5MB = 69MB ??? -> is this guaranteed? i don't think so
// 		5. everything is sequential executed

// 		Problems to solve:
// 		1. we may have too much small chunks next to each other
// 	*/
// 	var refinedSectors []*DiskSector
// 	for index, sector := range combinedSectors {
// 		if sector.Squashed {
// 			// this sector is already squashed no need to work on it
// 			continue
// 		}
// 		if sector.Length >= S3MinChunkSizeLimit {
// 			sector.PartNumber = int32(len(refinedSectors) + 1)
// 			refinedSectors = append(refinedSectors, sector)
// 			// then we will move to the next sector
// 			continue
// 		}
// 		// after this all these sectors are smaller than 5MB
// 		if sector.Type == vms3.PartUploadTypeCopy {
// 			/*
// 				now here we have the case when a copy sector, is below 5MB
// 				and we will get an error in multipart upload, then how to fix
// 				this?
// 				we are sure that the next sector is changed, why? it will be reported with a different
// 				length if it was
// 				so we can just squash this sector which is not changed but is small, to the next sector,
// 				and forget about it
// 			*/
// 			if index+1 >= len(combinedSectors) {
// 				// this is the last sector, we can just add it and continue
// 				// we do not change the last sector as it is free to go
// 				sector.PartNumber = int32(len(refinedSectors) + 1)
// 				refinedSectors = append(refinedSectors, sector)
// 				break
// 			}
// 			sector.Squashed = true
// 			nextSector := combinedSectors[index+1]
// 			nextSector.Length = nextSector.Length + sector.Length
// 			nextSector.StartOffset = sector.StartOffset
// 			// we will not do anything
// 			// not appending to refinedSectors -> as this sector is squashed
// 			// we are not appending the nextSector -> as itself may be squashed to its next sector we do not know yet
// 		} else if sector.Type == vms3.PartUploadTypeUpload {
// 			/*
// 				In this case we can just increase the length to 5MB without thinking about the next sectors,
// 				as we are able to extend this part to 64MB but we are going with 5MB, as it will cause simpler logic

// 				so we will try to change the length to 5MB and then iterate on the next sectors
// 				and make them squashed if required
// 			*/
// 			if d.Disk.CapacityInBytes-sector.StartOffset < S3MinChunkSizeLimit {
// 				sector.Length = d.Disk.CapacityInBytes - sector.StartOffset
// 			} else {
// 				sector.Length = S3MinChunkSizeLimit
// 			}
// 			sector.PartNumber = int32(len(refinedSectors) + 1)
// 			// start offset is the same, we do not care about it
// 			refinedSectors = append(refinedSectors, sector)
// 			sectorNewEnd := sector.StartOffset + sector.Length
// 			// then we will iterate on the next sectors until they are well over 5MB next
// 			for i := index + 1; i < len(combinedSectors); i++ {
// 				nextSector := combinedSectors[i]
// 				nextSectorEnd := nextSector.StartOffset + nextSector.Length
// 				if nextSector.StartOffset < sectorNewEnd {
// 					if nextSectorEnd <= sectorNewEnd {
// 						// fully engulfed the whole next sector
// 						nextSector.Length = 0
// 						nextSector.Squashed = true
// 					} else {
// 						// partially engulfed here
// 						nextSector.Length = nextSectorEnd - sectorNewEnd
// 						nextSector.StartOffset = sectorNewEnd
// 					}
// 				} else {
// 					// it is finished
// 					break
// 				}
// 			}
// 		} else {
// 			return nil, fmt.Errorf("unknown sector type when executing refinement")
// 		}
// 	}
// 	return refinedSectors, nil
// }

// func (d *DiskTarget) CleanUploadSectors(ctx context.Context, changedSectors []*DiskSector) ([]*DiskSector, error) {
// 	/*
// 		Let us talk about it,
// 		if i have a chunk in the disk, which is bigger than 5MB, it is guaranteed it will not have any over read part ->
// 			0 - 65MB is a changed sector -> this will be a 65MB chunk as part 1
// 			0 - 129MB is 1. 0 - 64MB and 2. 64MB - 129MB
// 		there is no guarantee to have a small stand alone chunk well below 5MB -> for example 1KB or 1MB
// 		this chunk is not guaranteed to have small sectors before and after it -> we do not know for example 0 - 1 C, 1 - 2 N, 2 - 3 C, 3 - 4 N
// 		there is no guarantee to have a small stand alone not changed chunk in the middle of the disk sectors

// 		This function will ignore the 5MB limit and find the not changed sectors and return a slice of combined sectors
// 	*/
// 	slog.Debug("Cleaning upload sectors", "changedSectors", changedSectors)
// 	// first of make sure sectors are sorted by the start or part number
// 	sort.Slice(changedSectors, func(i, j int) bool {
// 		return changedSectors[i].PartNumber < changedSectors[j].PartNumber
// 	})

// 	var CombinedSectors []*DiskSector
// 	fullDiskSize := d.Disk.CapacityInBytes
// 	currentCursor := int64(0)
// 	sectorEnd := int64(0)

// 	for _, sector := range changedSectors {
// 		sectorStart := sector.StartOffset
// 		sectorEnd = sectorStart + sector.Length // -> start is 0, and length is 64 -> it will be from 0 to 63
// 		// sectorEnd is not safe and it is the next sector start point, it is not exactly used in this sector
// 		if sectorEnd >= fullDiskSize {
// 			// this is the last sector, this is free to go in any size
// 			sector.PartNumber = int32(len(CombinedSectors) + 1)
// 			CombinedSectors = append(CombinedSectors, sector)
// 			break
// 		}
// 		if sectorStart == currentCursor {
// 			// this means that a sector is immediately started after another sector
// 			// previous sector was from 0 - 64 MB and the new sector is from 64MB - 128MB
// 			// we are going to append this and continue to the next sector
// 			sector.PartNumber = int32(len(CombinedSectors) + 1)
// 			CombinedSectors = append(CombinedSectors, sector)
// 			currentCursor = sectorEnd
// 			continue
// 		} else if sectorStart > currentCursor {
// 			// we have found a new not changed area from currentCursor to the sectorStart - 1
// 			CombinedSectors = append(CombinedSectors, &DiskSector{
// 				StartOffset: currentCursor,
// 				Length:      sectorStart - currentCursor,
// 				PartNumber:  int32(len(CombinedSectors) + 1),
// 				Type:        vms3.PartUploadTypeCopy,
// 				Squashed:    false,
// 			})
// 			sector.PartNumber = int32(len(CombinedSectors) + 1)
// 			CombinedSectors = append(CombinedSectors, sector)
// 			currentCursor = sectorEnd
// 			continue
// 		} else if sectorStart < currentCursor {
// 			// this may not happen
// 			return nil, fmt.Errorf("sector start is less than current cursor")
// 		}
// 	}
// 	if sectorEnd < fullDiskSize {
// 		CombinedSectors = append(CombinedSectors, &DiskSector{
// 			StartOffset: sectorEnd,
// 			Length:      fullDiskSize - sectorEnd,
// 			PartNumber:  int32(len(CombinedSectors) + 1),
// 			Type:        vms3.PartUploadTypeCopy,
// 			Squashed:    false,
// 		})
// 	}
// 	finalizedSectors, err := d.SquashSectors(ctx, CombinedSectors)
// 	if err != nil {
// 		return nil, err
// 	}
// 	totalCopySize := int64(0)
// 	totalUploadSize := int64(0)
// 	for _, sector := range finalizedSectors {
// 		slog.Debug("Combined sector", "Number", sector.PartNumber, "StartOffset", sector.StartOffset, "Length", sector.Length, "Type", sector.Type, "EndOffset", sector.StartOffset+sector.Length)
// 		if sector.Type == vms3.PartUploadTypeCopy {
// 			totalCopySize += sector.Length
// 		} else if sector.Type == vms3.PartUploadTypeUpload {
// 			totalUploadSize += sector.Length
// 		}
// 	}

// 	slog.Debug("Combined sectors", "FileSize", fullDiskSize, "finalizedSectors", finalizedSectors, "totalCopySizeMB", totalCopySize/1024/1024, "totalUploadSizeMB", totalUploadSize/1024/1024)
// 	return finalizedSectors, nil
// }

// func (d *DiskTarget) IncrementalCopyDetection(ctx context.Context, su *vms3.SimpleUpload, oldChangeID *vmware.ChangeID) error {
// 	slog.Info("Starting incremental copy detection", "diskKey", d.GetDiskKey(), "oldChangeID", oldChangeID.Value)
// 	startOffset := int64(0)
// 	partNumber := int32(0)
// 	var chunkSize int64
// 	var changedSectors []*DiskSector
// 	for {
// 		req := types.QueryChangedDiskAreas{
// 			This:        d.VM.Ref.Reference(),
// 			Snapshot:    d.VM.SnapshotRef.Ref,
// 			DeviceKey:   d.Disk.Key,
// 			StartOffset: startOffset,
// 			ChangeId:    oldChangeID.Value,
// 		}
// 		res, err := methods.QueryChangedDiskAreas(ctx, d.VM.Ref.Client(), &req)
// 		if err != nil {
// 			return err
// 		}
// 		diskChangeInfo := res.Returnval
// 		for _, area := range diskChangeInfo.ChangedArea {
// 			for offset := area.Start; offset < area.Start+area.Length; {
// 				chunkSize = area.Length + area.Start - offset
// 				if chunkSize > MaxChunkSize {
// 					nextChunkSize := chunkSize - MaxChunkSize
// 					if nextChunkSize >= S3MinChunkSizeLimit {
// 						chunkSize = MaxChunkSize
// 					} else {
// 						// we do not change the chunk size -> it may be for example 68 MB
// 						// NOTE This will cause a problem: WE DO NOT GUARANTEE EVERY CHUNK IS AT MAX 64MB, they can be bigger
// 					}
// 				}
// 				changedSectors = append(changedSectors, &DiskSector{
// 					StartOffset: offset,
// 					Length:      chunkSize,
// 					PartNumber:  partNumber,
// 					Type:        vms3.PartUploadTypeUpload,
// 					Squashed:    false,
// 				})
// 				partNumber++
// 				offset += chunkSize

// 			}
// 		}
// 		startOffset = diskChangeInfo.StartOffset + diskChangeInfo.Length

// 		if startOffset == d.Disk.CapacityInBytes {
// 			break
// 		}
// 	}

// 	refinedSectors, err := d.CleanUploadSectors(ctx, changedSectors)
// 	if err != nil {
// 		return err
// 	}
// 	handle, err := libnbd.Create()
// 	if err != nil {
// 		return err
// 	}
// 	defer handle.Close()

// 	err = handle.ConnectUri(d.SocketRef.LibNBDExportName())
// 	if err != nil {
// 		return err
// 	}
// 	for _, sector := range refinedSectors {
// 		if sector.Type == vms3.PartUploadTypeUpload {
// 			buf := make([]byte, sector.Length)
// 			err = handle.Pread(buf, uint64(sector.StartOffset), nil)
// 			if err != nil {
// 				return fmt.Errorf("failed to read from NBD at offset %d, chunkSize %d: %w", sector.StartOffset, sector.Length, err)
// 			}
// 			if err := mpu.SendPart(
// 				sector.PartNumber,
// 				buf,
// 				"",
// 				vms3.PartUploadTypeUpload,
// 			); err != nil {
// 				return fmt.Errorf("failed to send upload part %d to worker: %w", sector.PartNumber, err)
// 			}
// 		} else if sector.Type == vms3.PartUploadTypeCopy {
// 			if err := mpu.SendPart(
// 				sector.PartNumber,
// 				nil,
// 				fmt.Sprintf("bytes=%d-%d", sector.StartOffset, sector.StartOffset+sector.Length-1),
// 				vms3.PartUploadTypeCopy,
// 			); err != nil {
// 				return fmt.Errorf("failed to send copy part %d to worker: %w", sector.PartNumber, err)
// 			}
// 		} else {
// 			return fmt.Errorf("unknown sector type when executing incremental copy detection")
// 		}
// 	}
// 	return nil
// }
