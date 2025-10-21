package main

import (
	"fmt"
	"log/slog"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
)

func (c *VmwareS3BackupConnection) GetSize() (uint64, error) {
	return size, nil
}

func (c *VmwareS3BackupConnection) CanMultiConn() (bool, error) {
	return true, nil
}

func IsSparse(partNumber int32) bool {
	return diskManifest.FullChunksMetadata[partNumber].Compression == vms3.S3CompressionSparse
}

func IsData(partNumber int32) bool {
	return diskManifest.FullChunksMetadata[partNumber].Compression == vms3.S3CompressionZstd
}

func (c *VmwareS3BackupConnection) PRead(buf []byte, offset uint64, flags uint32) error {
	partNumber := int32(offset/vms3.MaxChunkSize) + 1
	partStart := uint64(partNumber) * vms3.MaxChunkSize
	partEnd := partStart + vms3.MaxChunkSize
	readSize := uint64(len(buf))
	if readSize > vms3.MaxChunkSize {
		slog.Error("read size is greater than max chunk size", "readSize", readSize, "maxChunkSize", vms3.MaxChunkSize)
		return fmt.Errorf("read size is greater than max chunk size")
	}
	readEnd := offset + readSize
	if readEnd > uint64(partEnd) {
		err := safeDownload.LoadPart(partNumber)
		if err != nil {
			return fmt.Errorf("failed to load part: %w", err)
		}
		err = safeDownload.LoadPart(partNumber + 1)
		if err != nil {
			return fmt.Errorf("failed to load part: %w", err)
		}
		section1Start := offset - partStart
		section1Length := partEnd - offset
		section2Start := uint64(0)
		section2Length := readSize - section1Length
		data1, err := lruCache.ReadFrom(partNumber, section1Start, section1Length)
		if err != nil {
			return fmt.Errorf("failed to read from cache: %w", err)
		}
		data2, err := lruCache.ReadFrom(partNumber+1, section2Start, section2Length)
		if err != nil {
			return fmt.Errorf("failed to read from cache: %w", err)
		}
		copy(buf, data1)
		copy(buf[section1Length:], data2)
		lruCache.UnlockPart(partNumber)
		lruCache.UnlockPart(partNumber + 1)

	} else {
		safeDownload.LoadPart(partNumber)
		data, err := lruCache.ReadFrom(partNumber, offset-partStart, readSize)
		if err != nil {
			return fmt.Errorf("failed to read from cache: %w", err)
		}
		copy(buf, data)
		lruCache.UnlockPart(partNumber)
	}

	return nil
}

func (c *VmwareS3BackupConnection) CanWrite() (bool, error) {
	return false, nil
}

func (c *VmwareS3BackupConnection) CanFlush() (bool, error) {
	return false, nil
}

func (c *VmwareS3BackupConnection) Close() {
	for partNumber := range lruCache.queue {
		lruCache.UnlockPart(int32(partNumber))
	}
}
