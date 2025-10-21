package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"libguestfs.org/nbdkit"
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
	partNumber := int32(offset / vms3.MaxChunkSize)
	partStart := uint64(partNumber) * vms3.MaxChunkSize
	partEnd := partStart + vms3.MaxChunkSize
	readSize := uint64(len(buf))
	if readSize > vms3.MaxChunkSize {
		nbdkit.Error(fmt.Sprintf("read size is greater than max chunk size: %d > %d", readSize, vms3.MaxChunkSize))
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
			lruCache.MakePartFree(partNumber)
			return fmt.Errorf("failed to read from cache: %w", err)
		}
		data2, err := lruCache.ReadFrom(partNumber+1, section2Start, section2Length)
		if err != nil {
			lruCache.MakePartFree(partNumber)
			lruCache.MakePartFree(partNumber + 1)
			return fmt.Errorf("failed to read from cache: %w", err)
		}
		copy(buf, data1)
		copy(buf[section1Length:], data2)
		lruCache.MakePartFree(partNumber)
		lruCache.MakePartFree(partNumber + 1)

	} else {
		safeDownload.LoadPart(partNumber)
		data, err := lruCache.ReadFrom(partNumber, offset-partStart, readSize)
		if err != nil {
			lruCache.MakePartFree(partNumber)
			return fmt.Errorf("failed to read from cache: %w", err)
		}
		copy(buf, data)
		lruCache.MakePartFree(partNumber)
	}
	pid := os.Getpid()
	var stackBuf [64]byte
	n := runtime.Stack(stackBuf[:], false)
	s := string(stackBuf[:n])
	var gID uint64
	fmt.Sscanf(s, "goroutine %d ", &gID)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	nbdkit.Debug(fmt.Sprintf("***STATS: PID: %d, GID: %d, TMem: %d, HMem: %d, GRoutine count: %d, LRU count: %d, TotalDownloadMB: %d", pid, gID, memStats.TotalAlloc/1024/1024, memStats.HeapAlloc/1024/1024, runtime.NumGoroutine(), lruCache.GetNumberOfParts(), totalDownload/1024/1024))
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
		lruCache.MakePartFree(int32(partNumber))
	}
}
