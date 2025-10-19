package vms3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	S3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3DB struct {
	S3Client   *s3.Client
	BucketName string
}

func NewS3DB(s3Client *s3.Client, bucketName string) *S3DB {
	return &S3DB{
		S3Client:   s3Client,
		BucketName: bucketName,
	}
}

const ObjectKeyPrefix = "vm-data"
const DiskObjectKeyPrefix = "disk-data"
const MetadataObjectKeyPrefix = "metadata-data"
const CustomMetadataHeader = "custom-metadata"

func CreateS3Client(ctx context.Context, s3URL string, s3AccessKey string, s3SecretKey string, s3Region string, bucketName string) (*S3DB, error) {
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
	return NewS3DB(s3Client, bucketName), nil
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

func (s *S3DB) GetObject(ctx context.Context, objectKey string) ([]byte, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(objectKey),
	}
	getObjectOutput, err := s.S3Client.GetObject(ctx, getObjectInput)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			slog.Debug("No such key found", "bucketName", s.BucketName, "objectKey", objectKey)
			return nil, nil
		}
		var respErr interface{ HTTPStatusCode() int } // Interface to access the status code
		if errors.As(err, &respErr) {
			if respErr.HTTPStatusCode() == http.StatusNotFound { // http.StatusNotFound is 404
				slog.Debug("S3 object not found via HTTP 404 check", "bucketName", s.BucketName, "objectKey", objectKey)
				return nil, nil
			}
		}
		slog.Error("Error getting metadata from S3 object", "bucketName", s.BucketName, "objectKey", objectKey, "error", err)
		return nil, fmt.Errorf("failed to get metadata for object %s in bucket %s: %w", objectKey, s.BucketName, err)
	}
	body, err := io.ReadAll(getObjectOutput.Body)
	if err != nil {
		slog.Error("Error reading object from S3 bucket", "bucketName", s.BucketName, "objectKey", objectKey, "error", err)
		return nil, fmt.Errorf("failed to read object %s from bucket %s: %w", objectKey, s.BucketName, err)
	}
	return body, nil
}

func (s *S3DB) PutJsonData(ctx context.Context, objectKey string, jsonData []byte) error {
	bodyReader := bytes.NewReader(jsonData)
	input := &s3.PutObjectInput{
		Bucket:               aws.String(s.BucketName),
		Key:                  aws.String(objectKey),
		Body:                 bodyReader,
		ContentLength:        aws.Int64(int64(len(jsonData))),
		ContentType:          aws.String("application/json"),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}
	_, err := s.S3Client.PutObject(ctx, input)
	if err != nil {
		slog.Error("Error uploading object to S3 bucket", "bucketName", s.BucketName, "objectKey", objectKey, "error", err)
		return fmt.Errorf("failed to put object %s into bucket %s: %w", objectKey, s.BucketName, err)
	}
	slog.Debug("Successfully uploaded JSON object to s3 bucket", "bucketName", s.BucketName, "objectKey", objectKey)
	return nil
}

func (s *S3DB) GetMetadataInObject(ctx context.Context, objectKey string) (string, error) {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(objectKey),
	}

	headObjectOutput, err := s.S3Client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			slog.Debug("No such key found", "bucketName", s.BucketName, "objectKey", objectKey)
			return "", nil
		}
		var respErr interface{ HTTPStatusCode() int } // Interface to access the status code
		if errors.As(err, &respErr) {
			if respErr.HTTPStatusCode() == http.StatusNotFound { // http.StatusNotFound is 404
				slog.Debug("S3 object not found via HTTP 404 check", "bucketName", s.BucketName, "objectKey", objectKey)
				return "", nil
			}
		}
		slog.Error("Error getting metadata from S3 object", "bucketName", s.BucketName, "objectKey", objectKey, "error", err)
		return "", fmt.Errorf("failed to get metadata for object %s in bucket %s: %w", objectKey, s.BucketName, err)
	}

	// S3 returns User-Defined Metadata with the 'x-amz-meta-' prefix removed
	// and keys converted to lowercase.
	return headObjectOutput.Metadata[CustomMetadataHeader], nil
}

func (s *S3DB) UploadFile(ctx context.Context, objectKey string, data []byte, customMetadata string) error {
	bodyReader := bytes.NewReader(data)
	input := &s3.PutObjectInput{
		Bucket:   aws.String(s.BucketName),
		Key:      aws.String(objectKey),
		Body:     bodyReader,
		Metadata: map[string]string{CustomMetadataHeader: customMetadata},
	}
	_, err := s.S3Client.PutObject(ctx, input)
	return err
}

func (s *S3DB) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.BucketName),
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

func (db *S3DB) CreateMultipartUpload(ctx context.Context, objectKey string, customMetadata string) (*MultiPartUpload, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(db.BucketName),
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
		BucketName: db.BucketName,
		ObjectKey:  objectKey,
		Parts:      []UploadPart{},
		db:         db,
	}, nil
}

func (db *S3DB) ListVirtualObjectMachines(ctx context.Context) ([]*VirtualObjectMachine, error) {
	objects, err := db.ListObjects(ctx, ObjectKeyPrefix+"-")
	if err != nil {
		return nil, err
	}
	var vmList []*VirtualObjectMachine
	for _, object := range objects {
		slog.Debug("Processing virtual machine object", "object", object)
		// vmKey := strings.TrimPrefix(object, ObjectKeyPrefix+"-")
		// disks, err := db.ListVirtualObjectDisks(ctx, object)
		// if err != nil {
		// 	return nil, err
		// }
		// if len(disks) == 0 {
		// 	slog.Debug("No disks found for VM", "vmObjectKey", object)
		// 	continue
		// }
		// vm := &VirtualObjectMachine{
		// 	ObjectKey:   object,
		// 	Disks:       disks,
		// 	VMKey:       vmKey,
		// 	RootDiskKey: disks[0].ObjectKey,
		// }
		// vmList = append(vmList, vm)
	}
	return vmList, nil
}

func (db *S3DB) GetVirtualObjectMachine(ctx context.Context, vmKey string) (*VirtualObjectMachine, error) {
	vmObjectKey := CreateVMPrefixKey(vmKey)
	disks, err := db.ListVirtualObjectDisks(ctx, vmObjectKey)
	if err != nil {
		return nil, err
	}
	if len(disks) == 0 {
		return nil, fmt.Errorf("no disks found for VM %s", vmKey)
	}
	return &VirtualObjectMachine{
		ObjectKey:   vmObjectKey,
		Disks:       disks,
		VMKey:       vmKey,
		RootDiskKey: disks[0].ObjectKey,
	}, nil
}

func (db *S3DB) ListVirtualObjectDisks(ctx context.Context, vmObjectKey string) ([]*VirtualObjectDisk, error) {
	objects, err := db.ListObjects(ctx, vmObjectKey+"/"+DiskObjectKeyPrefix+"-")
	if err != nil {
		return nil, err
	}
	var disks []*VirtualObjectDisk
	for _, object := range objects {
		slog.Debug("Processing virtual object disk", "object", object)
		diskKey := strings.TrimPrefix(object, vmObjectKey+"/"+DiskObjectKeyPrefix+"-")
		manifestKey := GetDiskManifestObjectKey(object)
		manifest, err := db.GetVirtualObjectDiskManifest(ctx, manifestKey)
		if err != nil {
			slog.Error("Error getting virtual object disk manifest", "error", err)
			return nil, err
		}
		partKeys, err := db.ListObjects(ctx, object+"/"+S3FullObjectPartsKeyPrefix+"/")
		if err != nil {
			slog.Error("Error listing virtual object disk parts", "error", err)
			return nil, err
		}
		if len(partKeys) != (len(manifest.FullChunksMetadata) - manifest.NumberOfSparseParts) {
			return nil, fmt.Errorf("number of full parts in the manifest does not match the number of parts in the s3, conflicting data for vm %s and disk %s", vmObjectKey, object)
		}
		disk := &VirtualObjectDisk{
			ObjectKey:   object,
			ManifestKey: manifestKey,
			DiskKey:     diskKey,
			Manifest:    manifest,
			PartKeys:    partKeys,
		}
		disks = append(disks, disk)
	}
	return disks, nil
}

func (db *S3DB) GetVirtualObjectDiskManifest(ctx context.Context, manifestObjectKey string) (*DiskManifest, error) {
	data, err := db.GetObject(ctx, manifestObjectKey)
	if err != nil {
		return nil, err
	}
	var diskManifest DiskManifest
	err = json.Unmarshal(data, &diskManifest)
	if err != nil {
		return nil, err
	}
	return &diskManifest, nil
}
