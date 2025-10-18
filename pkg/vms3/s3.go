package vms3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	S3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3DB struct {
	S3Client *s3.Client
}

func NewS3DB(s3Client *s3.Client) *S3DB {
	return &S3DB{
		S3Client: s3Client,
	}
}

const ObjectKeyPrefix = "vm-data"
const DiskObjectKeyPrefix = "disk-data"
const MetadataObjectKeyPrefix = "metadata-data"
const CustomMetadataHeader = "custom-metadata"

func CreateS3Client(ctx context.Context, s3URL string, s3AccessKey string, s3SecretKey string, s3Region string) (*S3DB, error) {
	cfg, err := S3Config.LoadDefaultConfig(ctx,
		S3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s3AccessKey, s3SecretKey, "")),
		S3Config.WithRegion(s3Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &s3URL
		o.UsePathStyle = true
	})
	return NewS3DB(s3Client), nil
}

func CreateVMPrefixKey(vmkey string) string {
	return fmt.Sprintf("%s-%s", ObjectKeyPrefix, vmkey)
}
func CreateMetadataObjectKey(vmKey string, diskKey string) string {
	return fmt.Sprintf("%s/%s-%s", CreateVMPrefixKey(vmKey), MetadataObjectKeyPrefix, diskKey)
}
func CreateDiskObjectKey(vmKey string, diskKey string) string {
	return fmt.Sprintf("%s/%s-%s", CreateVMPrefixKey(vmKey), DiskObjectKeyPrefix, diskKey)
}

func (s *S3DB) GetObject(ctx context.Context, bucketName string, objectKey string) ([]byte, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}
	getObjectOutput, err := s.S3Client.GetObject(ctx, getObjectInput)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			slog.Debug("No such key found", "bucketName", bucketName, "objectKey", objectKey)
			return nil, nil
		}
		var respErr interface{ HTTPStatusCode() int } // Interface to access the status code
		if errors.As(err, &respErr) {
			if respErr.HTTPStatusCode() == http.StatusNotFound { // http.StatusNotFound is 404
				slog.Debug("S3 object not found via HTTP 404 check", "bucketName", bucketName, "objectKey", objectKey)
				return nil, nil
			}
		}
		slog.Error("Error getting metadata from S3 object", "bucketName", bucketName, "objectKey", objectKey, "error", err)
		return nil, fmt.Errorf("failed to get metadata for object %s in bucket %s: %w", objectKey, bucketName, err)
	}
	body, err := io.ReadAll(getObjectOutput.Body)
	if err != nil {
		slog.Error("Error reading object from S3 bucket", "bucketName", bucketName, "objectKey", objectKey, "error", err)
		return nil, fmt.Errorf("failed to read object %s from bucket %s: %w", objectKey, bucketName, err)
	}
	return body, nil
}

func (s *S3DB) PutJsonData(ctx context.Context, bucketName string, objectKey string, jsonData []byte) error {
	bodyReader := bytes.NewReader(jsonData)
	input := &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bodyReader,
		ContentLength:        aws.Int64(int64(len(jsonData))),
		ContentType:          aws.String("application/json"),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}
	_, err := s.S3Client.PutObject(ctx, input)
	if err != nil {
		slog.Error("Error uploading object to S3 bucket", "bucketName", bucketName, "objectKey", objectKey, "error", err)
		return fmt.Errorf("failed to put object %s into bucket %s: %w", objectKey, bucketName, err)
	}
	slog.Debug("Successfully uploaded JSON object to s3 bucket", "bucketName", bucketName, "objectKey", objectKey)
	return nil
}

func (s *S3DB) GetMetadataInObject(ctx context.Context, bucketName string, objectKey string) (string, error) {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	headObjectOutput, err := s.S3Client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			slog.Debug("No such key found", "bucketName", bucketName, "objectKey", objectKey)
			return "", nil
		}
		var respErr interface{ HTTPStatusCode() int } // Interface to access the status code
		if errors.As(err, &respErr) {
			if respErr.HTTPStatusCode() == http.StatusNotFound { // http.StatusNotFound is 404
				slog.Debug("S3 object not found via HTTP 404 check", "bucketName", bucketName, "objectKey", objectKey)
				return "", nil
			}
		}
		slog.Error("Error getting metadata from S3 object", "bucketName", bucketName, "objectKey", objectKey, "error", err)
		return "", fmt.Errorf("failed to get metadata for object %s in bucket %s: %w", objectKey, bucketName, err)
	}

	// S3 returns User-Defined Metadata with the 'x-amz-meta-' prefix removed
	// and keys converted to lowercase.
	return headObjectOutput.Metadata[CustomMetadataHeader], nil
}

func (s *S3DB) UploadFile(ctx context.Context, bucketName string, objectKey string, data []byte, customMetadata string) error {
	bodyReader := bytes.NewReader(data)
	input := &s3.PutObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		Body:     bodyReader,
		Metadata: map[string]string{CustomMetadataHeader: customMetadata},
	}
	_, err := s.S3Client.PutObject(ctx, input)
	return err
}

func (s *S3DB) ListObjects(ctx context.Context, bucketName string, prefix string) ([]string, error) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}
	p := s3.NewListObjectsV2Paginator(s.S3Client, params)
	var keys []string
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}
