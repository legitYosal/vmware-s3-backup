package vms3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
)

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

func (d *DiskManifest) GetChangeID() (*vmware.ChangeID, error) {
	changeID, err := vmware.ParseChangeID(d.ChangeID)
	if err != nil {
		return nil, err
	}
	return changeID, nil
}

func (d *DiskManifest) CleanUpS3(ctx context.Context, s3DB *S3DB) error {
	err := s3DB.DeleteRecursively(ctx, d.ObjectKey)
	if err != nil {
		return fmt.Errorf("failed to delete objects from s3: %w", err)
	}
	return nil
}

func (d *DiskManifest) MatchChecksum(ctx context.Context, chunk *S3FullChunkMetadata, s3DB *S3DB) error {
	const maxRetries = 3
	key := GetS3FullObjectKey(d.ObjectKey, chunk.PartNumber)
	for attempt := 0; attempt < maxRetries; attempt++ {
		chk, err := s3DB.GetObjectChecksum(ctx, key)
		if err != nil {
			slog.Error("Error fetching head request for object", "objectKey", key)
			select {
			case <-ctx.Done():
				return ctx.Err() // Context cancelled
			case <-time.After(time.Duration(attempt+1) * time.Second):
			}
			continue
		}
		if chk != chunk.Checksum {
			return fmt.Errorf("checksum mismatch for chunk %d: expected %s, got %s", chunk.PartNumber, chunk.Checksum, chk)
		}
		return nil
	}
	return fmt.Errorf("failed to get object checksum after %d attempts", maxRetries)
}

func (d *DiskManifest) ValidateOnS3(ctx context.Context, s3DB *S3DB) (bool, error) {
	/*
		it will iterate on the manifest FullChunksMetadata and for each chunk
		which is not sparse, it will send a head request and get the checksum in the
		checksum-sha256 header and compare it to the Checksum in the manifest for that chunk
	*/
	slog.Debug("Validating the backup started")
	semaphore := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var validationFailed atomic.Bool

	for _, chunk := range d.FullChunksMetadata {
		if chunk.Compression == S3CompressionSparse || chunk.Checksum == "" {
			continue
		}
		if validationFailed.Load() {
			break
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case semaphore <- struct{}{}:
			// Acquired slot, proceed.
		}
		wg.Add(1)
		go func(chunk *S3FullChunkMetadata) {
			defer wg.Done()

			// Release slot
			defer func() { <-semaphore }()

			if err := d.MatchChecksum(ctx, chunk, s3DB); err != nil {
				validationFailed.Store(true)
				slog.Error("Checksum mismatch for chunk", "chunk", chunk, "error", err)
			}
			slog.Debug("Chunk validated successfully", "partNumber", chunk.PartNumber)
		}(chunk)
	}
	wg.Wait()
	if validationFailed.Load() {
		return false, nil
	}
	return true, nil
}

const S3FullObjectPartsKeyPrefix = "full"

func GetS3FullObjectKey(prefixKey string, partNumber int32) string {
	// vm-uuid/disk-key/full/part-number
	return fmt.Sprintf("%s/%s/%06d", prefixKey, S3FullObjectPartsKeyPrefix, partNumber)
}
func GetDiskManifestObjectKey(prefixKey string) string {
	return fmt.Sprintf("%s/manifest.json", prefixKey)
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
	Checksum    string
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

type VirtualObjectDisk struct {
	ObjectKey   string
	DiskKey     string
	Manifest    *DiskManifest
	PartKeys    []string
	ManifestKey string
}

type VirtualObjectMachine struct {
	ObjectKey   string
	VMKey       string
	Disks       []*VirtualObjectDisk
	RootDiskKey string
}

func CompressBufferZstd(data []byte) ([]byte, error) {
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

func DecompressBufferZstd(compressedData []byte, originalSize int64) ([]byte, error) {
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
	if int64(n) != originalSize {
		// This is a critical error, meaning the decompression failed or size was wrong.
		return nil, fmt.Errorf("decompressed size mismatch: expected %d bytes, got %d", originalSize, n)
	}
	return dstBuf, nil
}

func (v *VirtualObjectDisk) RestoreDiskToLocalPath(ctx context.Context, s3DB *S3DB, localPath string) error {
	slog.Info("Restoring disk to local path", "objectKey", v.ObjectKey, "localPath", localPath)
	downloader := NewSimpleDownloadS3(s3DB)
	downloader.BootWorkers(MaxConcurrentDownloads)

	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open destination file: %w", err)
	}
	defer file.Close()

	semaphore := make(chan struct{}, MaxConcurrentDownloads)

	go func() {
		defer downloader.WaitFinish()
		for _, partMetadata := range v.Manifest.FullChunksMetadata {
			if partMetadata.Compression == S3CompressionSparse {
				continue
			}
			fullPartObjectKey := GetS3FullObjectKey(v.ObjectKey, partMetadata.PartNumber)
			job := DownloadJob{
				ObjectKey:    fullPartObjectKey,
				OriginalSize: partMetadata.Length,
				PartIndex:    partMetadata.PartNumber,
				CheckSum:     partMetadata.Checksum,
			}
			semaphore <- struct{}{}
			downloader.jobsCh <- job // this is dispatched to worker to download and will be received in the results channel
		}
	}()

	finishedDownloadMapping := make(map[int32]*FinishedPart)

	expectingPart := int32(1)
	totalReceived := 0
	totalParts := len(v.Manifest.FullChunksMetadata)
	totalS3Parts := totalParts - v.Manifest.NumberOfSparseParts
	for expectingPart <= int32(totalParts) {
		partMetadata := v.Manifest.FullChunksMetadata[expectingPart-1]
		if partMetadata.Compression == S3CompressionSparse {
			// if err := file.Truncate(partMetadata.StartOffset + partMetadata.Length); err != nil {
			// 	return fmt.Errorf("failed to truncate file: %w", err)
			// }
			expectingPart++
			continue
		}
		if partMetadata.PartNumber != expectingPart {
			slog.Error("part number mismatch, this must be sorted", "expected", expectingPart, "actual", partMetadata.PartNumber)
			return fmt.Errorf("part number mismatch: expected %d, got %d", expectingPart, partMetadata.PartNumber)
		}
		select {
		case err := <-downloader.errCh:
			return fmt.Errorf("error during download: %w", err)
		default:
		}

		if part, ok := finishedDownloadMapping[partMetadata.PartNumber]; ok {
			_, err := file.WriteAt(part.Data, partMetadata.StartOffset)
			if err != nil {
				return fmt.Errorf("failed to write data to file: %w", err)
			}
			delete(finishedDownloadMapping, partMetadata.PartNumber)
			expectingPart++
			<-semaphore
			continue
		}

		result, ok := <-downloader.resultsCh
		if !ok {
			if totalReceived < totalS3Parts {
				slog.Error("results channel closed unexpectedly before all parts were received", "totalReceived", totalReceived, "totalS3Parts", totalS3Parts)
				return fmt.Errorf("results channel closed unexpectedly at part %d of %d", totalReceived, totalS3Parts)
			} else if totalReceived == totalS3Parts {
				// NOTE THIS MAY INTRODUCE A BUG
				break
			} else {
				slog.Error("received more parts than expected", "totalReceived", totalReceived, "totalS3Parts", totalS3Parts)
				return fmt.Errorf("received more parts than expected: %d", totalReceived)
			}
		}
		totalReceived++
		finishedDownloadMapping[result.PartIndex] = &result
	}

	return nil
}
