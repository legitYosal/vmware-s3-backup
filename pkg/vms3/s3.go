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
const ChecksumHeader = "checksum-sha256"

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
	m, ok := headObjectOutput.Metadata[CustomMetadataHeader]
	if !ok {
		return "", fmt.Errorf("custom metadata header not found in the object %s in bucket %s", objectKey, s.BucketName)
	}
	return m, nil
}

func (s *S3DB) GetObjectChecksum(ctx context.Context, objectKey string) (string, error) {
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
	m, ok := headObjectOutput.Metadata[ChecksumHeader]
	if !ok {
		return "", fmt.Errorf("checksum header not found in the object %s in bucket %s", objectKey, s.BucketName)
	}
	if len(m) == 0 {
		return "", fmt.Errorf("checksum header is empty in the object %s in bucket %s", objectKey, s.BucketName)
	}
	return m, nil
}

func (s *S3DB) UploadFile(ctx context.Context, objectKey string, data []byte, customMetadata string, hash string) error {
	bodyReader := bytes.NewReader(data)
	input := &s3.PutObjectInput{
		Bucket:   aws.String(s.BucketName),
		Key:      aws.String(objectKey),
		Body:     bodyReader,
		Metadata: map[string]string{CustomMetadataHeader: customMetadata, ChecksumHeader: hash},
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

func (db *S3DB) ListVirtualObjectMachines(ctx context.Context, vmKeyFilter string) ([]*VirtualObjectMachine, error) {
	// put vmKey = "" for listing all vm machines
	objects, err := db.ListObjects(ctx, ObjectKeyPrefix+"-"+vmKeyFilter)
	if err != nil {
		return nil, err
	}
	var vmList []*VirtualObjectMachine
	vmKeyMapping := make(map[string]*VirtualObjectMachine)
	diskKeyMapping := make(map[string]*VirtualObjectDisk)
	for _, object := range objects {
		if len(strings.Split(object, "/")) < 3 {
			slog.Error("object not in the correct format in the vm backup ", "object", object)
			return nil, fmt.Errorf("object not in the correct format in the vm backup %s", object)
		}
		vmObjectKey := strings.Split(object, "/")[0]
		vmKey := strings.TrimPrefix(vmObjectKey, ObjectKeyPrefix+"-")
		if _, ok := vmKeyMapping[vmKey]; !ok {
			vm := &VirtualObjectMachine{
				ObjectKey:   vmObjectKey,
				VMKey:       vmKey,
				Disks:       []*VirtualObjectDisk{},
				RootDiskKey: "",
			}
			vmKeyMapping[vmKey] = vm
			vmList = append(vmList, vm)
		}
		diskObjectKey := strings.Split(object, "/")[1]
		diskKey := vmKey + "-" + strings.TrimPrefix(diskObjectKey, DiskObjectKeyPrefix+"-")
		if _, ok := diskKeyMapping[diskKey]; !ok {
			manifestKey := GetDiskManifestObjectKey(vmObjectKey + "/" + diskObjectKey)
			manifest, err := db.GetVirtualObjectDiskManifest(ctx, manifestKey)
			if err != nil {
				slog.Error("Error getting virtual object disk manifest", "error", err)
				return nil, err
			}
			disk := &VirtualObjectDisk{
				ObjectKey:   vmObjectKey + "/" + diskObjectKey,
				DiskKey:     diskKey,
				Manifest:    manifest,
				PartKeys:    []string{},
				ManifestKey: manifestKey,
			}
			vm := vmKeyMapping[vmKey]
			vm.Disks = append(vm.Disks, disk)
			diskKeyMapping[diskKey] = disk
		}
		thirdPartOfKey := strings.Split(object, "/")[2]
		if thirdPartOfKey == S3FullObjectPartsKeyPrefix {
			disk := diskKeyMapping[diskKey]
			disk.PartKeys = append(disk.PartKeys, object)
		}
	}
	for _, vm := range vmList {
		if len(vm.Disks) == 0 {
			slog.Error("no disks found for vm %s", "vmKey", vm.VMKey)
			return nil, fmt.Errorf("no disks found for vm %s", vm.VMKey)
		}
		vm.RootDiskKey = vm.Disks[0].ObjectKey // NOTE THIS IS NOT SAFE AND I DO NOT KNOW HOW TO FIND IT REALLY RIGHT NOW
		for _, disk := range vm.Disks {
			if len(disk.PartKeys) != (len(disk.Manifest.FullChunksMetadata) - disk.Manifest.NumberOfSparseParts) {
				slog.Error("number of full parts in the manifest does not match the number of parts in the s3, conflicting data for vm %s and disk %s", vm.VMKey, disk.DiskKey)
				return nil, fmt.Errorf("number of full parts in the manifest does not match the number of parts in the s3, conflicting data for vm %s and disk %s", vm.VMKey, disk.DiskKey)
			}
		}
	}
	return vmList, nil
}

func (db *S3DB) GetVirtualObjectDiskManifest(ctx context.Context, manifestObjectKey string) (*DiskManifest, error) {
	data, err := db.GetObject(ctx, manifestObjectKey)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	var diskManifest DiskManifest
	err = json.Unmarshal(data, &diskManifest)
	if err != nil {
		return nil, err
	}
	return &diskManifest, nil
}

func (db *S3DB) DeleteObject(ctx context.Context, objectKey string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(db.BucketName),
		Key:    aws.String(objectKey),
	}
	_, err := db.S3Client.DeleteObject(ctx, input)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			slog.Debug("No such key found", "bucketName", db.BucketName, "objectKey", objectKey)
			return nil
		}
		var respErr interface{ HTTPStatusCode() int } // Interface to access the status code
		if errors.As(err, &respErr) {
			if respErr.HTTPStatusCode() == http.StatusNotFound { // http.StatusNotFound is 404
				slog.Debug("S3 object not found via HTTP 404 check", "bucketName", db.BucketName, "objectKey", objectKey)
				return nil
			}
		}
		slog.Error("Error deleting object from S3 bucket", "bucketName", db.BucketName, "objectKey", objectKey, "error", err)
		return fmt.Errorf("failed to delete object %s from bucket %s: %w", objectKey, db.BucketName, err)
	}
	return nil
}

func (db *S3DB) DeleteRecursively(ctx context.Context, objectKeyPrefix string) error {
	paginator := s3.NewListObjectsV2Paginator(db.S3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(db.BucketName),
		Prefix: aws.String(objectKeyPrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get page of objects: %w", err)
		}
		if len(page.Contents) == 0 {
			slog.Debug("No more objects found for deletion.", "objectKeyPrefix", objectKeyPrefix)
			break // No objects found in this page
		}
		objectsToDelete := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: object.Key,
			})
		}
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(db.BucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(false), // Set to false to see detailed errors/deletions
			},
		}
		output, err := db.S3Client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
		if len(output.Errors) > 0 {
			for _, deleteError := range output.Errors {
				slog.Error("Error deleting object %q: Code: %s, Message: %s\n",
					"objectKey", aws.ToString(deleteError.Key),
					"code", aws.ToString(deleteError.Code),
					"message", aws.ToString(deleteError.Message))
			}
			return fmt.Errorf("some objects failed to delete in the batch")
		}
	}
	return nil
}
