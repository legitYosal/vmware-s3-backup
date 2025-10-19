package vms3

import (
	"context"
	"log/slog"
	"sync"
)

type SimpleUploadJob struct {
	ObjectKey string
	Data      []byte
	Metadata  string
}

type SimpleUpload struct {
	DB *S3DB

	jobChan   chan SimpleUploadJob
	errChan   chan error
	wg        sync.WaitGroup
	semaphore chan struct{}
}

func NewSimpleUpload(db *S3DB) *SimpleUpload {
	return &SimpleUpload{
		DB: db,
	}
}

func (s *SimpleUpload) BootWorkers(ctx context.Context, numWorkers int) {
	s.jobChan = make(chan SimpleUploadJob, MaxJobChanBuffer)
	s.errChan = make(chan error, 1)
	s.semaphore = make(chan struct{}, MaxConcurrentUploads)

	slog.Debug("Starting simple upload workers", "numWorkers", numWorkers, "maxConcurrentUploads", MaxConcurrentUploads, "jobChanBuffer", MaxJobChanBuffer)
	for i := 0; i < numWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			for job := range s.jobChan {
				slog.Debug("Uploading object", "workerID", workerID, "objectKey", job.ObjectKey)
				s.semaphore <- struct{}{}
				err := s.DB.UploadFile(ctx, job.ObjectKey, job.Data, job.Metadata)
				<-s.semaphore
				if err != nil {
					s.errChan <- err
					return
				}
			}

		}(i)
	}
}

func (s *SimpleUpload) DispatchUpload(ctx context.Context, objectKey string, data []byte, customMetadata string) error {
	select {
	case err := <-s.errChan:
		return err
	default:
		// No error, continue
	}
	s.jobChan <- SimpleUploadJob{
		ObjectKey: objectKey,
		Data:      data,
		Metadata:  customMetadata,
	}
	return nil
}

func (s *SimpleUpload) Wait() error {
	close(s.jobChan)
	s.wg.Wait()

	// Check for errors after workers complete
	select {
	case err := <-s.errChan:
		return err
	default:
		return nil
	}
}
