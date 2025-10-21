package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
)

type SafeDownload struct {
	mutex sync.Mutex
	locks map[int32]*sync.Mutex
}

func NewSafeDownload() *SafeDownload {
	return &SafeDownload{
		locks: make(map[int32]*sync.Mutex),
	}
}

func (s *SafeDownload) LoadPartFromS3(partNumber int32) error {
	partKey := vms3.GetS3FullObjectKey(diskManifest.ObjectKey, partNumber+1)
	partData, err := s3DB.GetObject(context.Background(), partKey)
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	decompressedData, err := vms3.DecompressBufferZstd(partData, diskManifest.FullChunksMetadata[partNumber].Length)
	if err != nil {
		return fmt.Errorf("failed to decompress data: %w", err)
	}
	lruCache.AddPart(partNumber, decompressedData)
	return nil
}

func (s *SafeDownload) LoadPart(partNumber int32) error {
	s.mutex.Lock()
	partLock, ok := s.locks[partNumber]
	if !ok {
		partLock = &sync.Mutex{}
		s.locks[partNumber] = partLock
	}
	s.mutex.Unlock()
	s.locks[partNumber].Lock()

	defer func() {
		lock, ok := s.locks[partNumber]
		if ok {
			lock.Unlock()
			s.mutex.Lock()
			delete(s.locks, partNumber)
			s.mutex.Unlock()
		}
	}()

	if diskManifest.FullChunksMetadata[partNumber].Compression == vms3.S3CompressionZstd {
		if lruCache.HasPart(partNumber) {
			lruCache.LockPart(partNumber)

			slog.Debug("Cache HIT, Part is already in lru cache, skipping load from s3")
		} else {
			err := s.LoadPartFromS3(partNumber)
			if err != nil {
				return fmt.Errorf("failed to load part from s3: %w", err)
			}
			slog.Debug("Part loaded into lru cache", "partNumber", partNumber, "length", diskManifest.FullChunksMetadata[partNumber].Length)
		}
	} else {
		slog.Debug("Part is sparse, skipping load from s3")
	}
	return nil
}
