package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/legitYosal/vmware-s3-backup/pkg/backup"
	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Test the backup tool",
	Long:  "Test the backup tool",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		// Prepare disk metadata
		diskMetadata := backup.DiskMetadata{
			Name:      "test",
			ID:        "test-id",
			SizeBytes: 1024,
			ObjectKey: "test",
			ChangeID:  "test",
			DiskKey:   "test",
		}
		marshaled, err := json.Marshal(diskMetadata)
		if err != nil {
			return fmt.Errorf("failed to marshal disk metadata: %w", err)
		}

		// Create multipart upload
		mpu, err := cli.S3DB.CreateMultipartUpload(ctx, cli.Configuration.S3BucketName, "test-object", string(marshaled))
		if err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}

		// Boot workers (4 workers, max 16 concurrent uploads via semaphore)
		mpu.BootWorkers(ctx, 4)

		// Read file and send parts to worker pool
		chunkSize := 1024 * 1024 * 10 // 10MB
		filePath := "test_file.bin"
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		var partNumber int32 = 1
		for {
			buf := make([]byte, chunkSize)
			n, readErr := file.Read(buf)
			if n > 0 {
				chunkData := make([]byte, n)
				copy(chunkData, buf[:n])

				// Send part to worker pool
				if err := mpu.SendPart(partNumber, chunkData); err != nil {
					return fmt.Errorf("failed to send part %d: %w", partNumber, err)
				}
				partNumber++
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				return fmt.Errorf("failed to read chunk: %w", readErr)
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

		slog.Info("test completed successfully", "totalParts", partNumber-1)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(testCmd)
}
