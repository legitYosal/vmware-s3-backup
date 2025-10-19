package vms3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

const MaxConcurrentDownloads = 4

const JobChannelBuffer = 4

// max memory usage is probably around 4 * 2 * 64MB at max

type DownloadJob struct {
	ObjectKey    string
	PartIndex    int32
	OriginalSize int64
	CheckSum     string
}

type FinishedPart struct {
	PartIndex int32
	Data      []byte
}

type SimpleDownloadS3 struct {
	S3DB      *S3DB
	jobsCh    chan DownloadJob
	resultsCh chan FinishedPart
	errCh     chan error
	wg        sync.WaitGroup
}

func NewSimpleDownloadS3(s3DB *S3DB) *SimpleDownloadS3 {
	return &SimpleDownloadS3{
		S3DB:      s3DB,
		jobsCh:    make(chan DownloadJob, JobChannelBuffer),
		resultsCh: make(chan FinishedPart, MaxConcurrentDownloads),
		errCh:     make(chan error, 1),
	}
}

func (s *SimpleDownloadS3) BootWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		s.wg.Add(1)
		go s.RunWorker(i)
	}
}

func calculateSHA256(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *SimpleDownloadS3) RunWorker(id int) {
	defer s.wg.Done()

	for {
		select {
		case job, ok := <-s.jobsCh:
			if !ok {
				// jobsCh was closed, worker exits gracefully
				return
			}
			ctx := context.Background()
			partData, err := s.S3DB.GetObject(ctx, job.ObjectKey)
			if err != nil {
				s.errCh <- fmt.Errorf("worker %d failed on object %s: %w", id, job.ObjectKey, err)
				return
			}
			if job.CheckSum != "" {
				checksum := calculateSHA256(partData)
				if checksum != job.CheckSum {
					s.errCh <- fmt.Errorf("worker %d failed on object %s: checksum mismatch: expected %s, got %s", id, job.ObjectKey, job.CheckSum, checksum)
					return
				}
			}
			decompressedData, err := DecompressBufferZstd(partData, job.OriginalSize)
			if err != nil {
				s.errCh <- fmt.Errorf("worker %d failed on object %s: %w", id, job.ObjectKey, err)
				return
			}

			s.resultsCh <- FinishedPart{
				PartIndex: job.PartIndex,
				Data:      decompressedData,
			}

		case <-s.errCh:
			// An error occurred elsewhere, worker exits gracefully
			return
		}
	}
}

func (s *SimpleDownloadS3) WaitFinish() {
	close(s.jobsCh)
	s.wg.Wait()
	close(s.resultsCh)
}
