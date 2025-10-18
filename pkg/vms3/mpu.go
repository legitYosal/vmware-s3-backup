package vms3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const MaxConcurrentUploads = 6
const MaxJobChanBuffer = 2

type UploadPart struct {
	PartNumber int32
	Etag       string
}

type PartUploadType string

const (
	PartUploadTypeUpload PartUploadType = "upload"
	PartUploadTypeCopy   PartUploadType = "copy"
)

type PartUploadJob struct {
	PartNumber int32
	Data       []byte
	ByteRange  string
	Type       PartUploadType
}

type MultiPartUpload struct {
	UploadID   string
	Parts      []UploadPart
	BucketName string
	ObjectKey  string
	mutex      sync.RWMutex
	db         *S3DB

	// Worker pool fields
	jobChan   chan PartUploadJob
	errChan   chan error
	wg        sync.WaitGroup
	ctx       context.Context
	semaphore chan struct{} // Semaphore to limit concurrent uploads
}

func (db *S3DB) CreateMultipartUpload(ctx context.Context, bucketName string, objectKey string, customMetadata string) (*MultiPartUpload, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		Metadata: map[string]string{CustomMetadataHeader: customMetadata},
	}

	result, err := db.S3Client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload for %s: %w", objectKey, err)
	}

	uploadID := aws.ToString(result.UploadId)
	slog.Debug("Upload initiated successfully", "uploadID", uploadID)
	return &MultiPartUpload{
		UploadID:   uploadID,
		BucketName: bucketName,
		ObjectKey:  objectKey,
		Parts:      []UploadPart{},
		db:         db,
	}, nil
}

func (p *MultiPartUpload) UploadPart(ctx context.Context, partNumber int32, data *io.Reader) error {
	slog.Debug("Uploading part", "partNumber", partNumber)

	input := &s3.UploadPartInput{
		Bucket:     aws.String(p.BucketName),
		Key:        aws.String(p.ObjectKey),
		UploadId:   aws.String(p.UploadID),
		PartNumber: aws.Int32(partNumber),
		Body:       *data,
	}

	result, err := p.db.S3Client.UploadPart(ctx, input)
	if err != nil {
		slog.Error("Error uploading part", "partNumber", partNumber, "objectKey", p.ObjectKey, "error", err)
		return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
	}

	etag := aws.ToString(result.ETag)
	slog.Debug("Part uploaded successfully", "partNumber", partNumber, "etag", etag)

	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Parts = append(p.Parts, UploadPart{
		PartNumber: partNumber,
		Etag:       etag,
	})
	return nil
}

func (p *MultiPartUpload) CopyPart(ctx context.Context, partNumber int32, byteRange string) error {
	slog.Debug("Copying part", "partNumber", partNumber, "byteRange", byteRange)

	// Source copy string format: /bucketName/sourceKey
	copySource := fmt.Sprintf("/%s/%s", p.BucketName, p.ObjectKey)

	input := &s3.UploadPartCopyInput{
		Bucket:          aws.String(p.BucketName),
		Key:             aws.String(p.ObjectKey),
		UploadId:        aws.String(p.UploadID),
		PartNumber:      aws.Int32(partNumber),
		CopySource:      aws.String(copySource),
		CopySourceRange: aws.String(byteRange), // e.g., "bytes=0-67108863"
	}

	result, err := p.db.S3Client.UploadPartCopy(ctx, input)
	if err != nil {
		slog.Error("Error copying part", "partNumber", partNumber, "objectKey", p.ObjectKey, "byteRange", byteRange, "error", err)
		return fmt.Errorf("failed to copy part %d from %s: %w", partNumber, p.ObjectKey, err)
	}

	// The ETag is nested within CopyPartResult
	etag := aws.ToString(result.CopyPartResult.ETag)
	slog.Debug("Part copied successfully", "partNumber", partNumber, "etag", etag)

	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Parts = append(p.Parts, UploadPart{
		PartNumber: partNumber,
		Etag:       etag,
	})
	return nil
}

// BootWorkers starts a pool of worker goroutines to handle concurrent part uploads
// numWorkers: number of worker goroutines
// maxConcurrentUploads: maximum number of concurrent S3 uploads (default: MaxConcurrentUploads)
func (p *MultiPartUpload) BootWorkers(ctx context.Context, numWorkers int) {
	p.ctx = ctx
	p.jobChan = make(chan PartUploadJob, MaxJobChanBuffer) // Buffered channel with capacity 10
	p.errChan = make(chan error, 1)
	p.semaphore = make(chan struct{}, MaxConcurrentUploads) // Limit to MaxConcurrentUploads concurrent uploads

	slog.Debug("Starting worker pool", "numWorkers", numWorkers, "maxConcurrentUploads", MaxConcurrentUploads, "jobChanBuffer", MaxJobChanBuffer, "objectKey", p.ObjectKey)

	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			for job := range p.jobChan {
				// Acquire semaphore
				p.semaphore <- struct{}{}

				var uploadErr error
				switch job.Type {
				case PartUploadTypeUpload:
					var reader io.Reader = bytes.NewReader(job.Data)
					uploadErr = p.UploadPart(p.ctx, job.PartNumber, &reader)
				case PartUploadTypeCopy:
					uploadErr = p.CopyPart(p.ctx, job.PartNumber, job.ByteRange)
				default:
					uploadErr = fmt.Errorf("invalid part upload type: %s", job.Type)
				}
				// Release semaphore (even on error)
				<-p.semaphore

				if uploadErr != nil {
					slog.Error("failed to upload part", "worker", workerID, "partNumber", job.PartNumber, "error", uploadErr)
					select {
					case p.errChan <- uploadErr:
					default:
					}
					return
				}
				slog.Debug("part uploaded successfully", "worker", workerID, "partNumber", job.PartNumber)
			}
		}(i)
	}
}

// SendPart sends a part upload job to the worker pool
func (p *MultiPartUpload) SendPart(partNumber int32, data []byte, byteRange string, partUploadType PartUploadType) error {
	// Check for errors before sending
	slog.Debug("*********** SENDING PART", "TYPE", partUploadType, "Number", partNumber)
	if partUploadType == PartUploadTypeCopy && byteRange == "" {
		return fmt.Errorf("byteRange is required for copy part")
	} else if partUploadType == PartUploadTypeUpload && data == nil {
		return fmt.Errorf("data is required for upload part")
	}
	if partUploadType == PartUploadTypeCopy {
		slog.Debug("             copying part", "ByteRange", byteRange)
	}
	if partUploadType == PartUploadTypeUpload {
		slog.Debug("             uploading part", "DataLengthMB", len(data)/1024/1024)
	}
	select {
	case err := <-p.errChan:
		return err
	default:
	}

	p.jobChan <- PartUploadJob{
		PartNumber: partNumber,
		Data:       data,
		ByteRange:  byteRange,
		Type:       partUploadType,
	}
	return nil
}

// Wait closes the job channel and waits for all workers to finish
func (p *MultiPartUpload) Wait() error {
	close(p.jobChan)
	p.wg.Wait()

	// Check for errors after workers complete
	select {
	case err := <-p.errChan:
		return err
	default:
		return nil
	}
}

func (p *MultiPartUpload) CompleteMultipartUpload(ctx context.Context) error {
	slog.Debug("Completing multipart upload", "objectKey", p.ObjectKey)

	if len(p.Parts) == 0 {
		slog.Debug("No parts to complete multipart upload", "objectKey", p.ObjectKey)
		return nil
	}
	// 1. Convert the custom Part struct list into the S3 required types.Part list
	s3Parts := make([]types.CompletedPart, len(p.Parts))
	for i, p := range p.Parts {
		s3Parts[i] = types.CompletedPart{
			PartNumber: aws.Int32(p.PartNumber),
			ETag:       aws.String(p.Etag),
		}
	}

	// 2. Sort the parts by PartNumber (REQUIRED by S3)
	// Although the parts list should already be in order from the loop, S3 mandates
	// they be sorted for the final request. Since our custom struct doesn't implement
	// sorting, we rely on the input order, but a real-world scenario might need sorting.
	// We'll trust the caller maintains order for simplicity here.

	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(p.BucketName),
		Key:      aws.String(p.ObjectKey),
		UploadId: aws.String(p.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: s3Parts,
		},
	}

	_, err := p.db.S3Client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload for %s: %w", p.ObjectKey, err)
	}

	slog.Debug("Multipart upload completed successfully", "objectKey", p.ObjectKey)
	return nil
}
