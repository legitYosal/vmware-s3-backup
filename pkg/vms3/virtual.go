package vms3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

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

func DecompressBufferZstd(compressedData []byte, originalSize int) ([]byte, error) {
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
