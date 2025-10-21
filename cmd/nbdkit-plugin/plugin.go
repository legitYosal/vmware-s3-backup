package main

import (
	"context"
	"fmt"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
	"libguestfs.org/nbdkit"
)

const MaxConcurrentS3Downloads = 4

func (p *VmwareS3BackupPlugin) Open(readonly bool) (nbdkit.ConnectionInterface, error) {
	return &VmwareS3BackupConnection{}, nil
}

func (c *VmwareS3BackupPlugin) DumpPlugin() {
	fmt.Println("vmware-s3-backup plugin")
	fmt.Println("description: VMware S3 Backup Plugin")
	fmt.Println("source: https://github.com/legitYosal/vmware-s3-backup")
	fmt.Println("config: --s3-url <s3-url>")
	fmt.Println("config: --s3-secret-key <s3-secret-key>")
	fmt.Println("config: --s3-access-key <s3-access-key>")
	fmt.Println("config: --s3-region <s3-region>")
	fmt.Println("config: --s3-bucket-name <s3-bucket-name>")
	fmt.Println("config: --vm-key <vm-key>")
	fmt.Println("config: --disk-key <disk-key>")
}

func (c *VmwareS3BackupPlugin) Config(key string, value string) error {
	switch key {
	case "s3-url":
		s3Url = value
	case "s3-secret-key":
		s3SecretKey = value
	case "s3-access-key":
		s3AccessKey = value
	case "s3-region":
		s3Region = value
	case "s3-bucket-name":
		s3BucketName = value
	case "vm-key":
		vmKey = value
	case "disk-key":
		diskKey = value
	default:
		return fmt.Errorf("unknown config key: %s", key)
	}
	return nil
}

func (c *VmwareS3BackupPlugin) ConfigComplete() error {
	if s3Url == "" {
		return fmt.Errorf("s3-url is required")
	}
	if s3SecretKey == "" {
		return fmt.Errorf("s3-secret-key is required")
	}
	if s3AccessKey == "" {
		return fmt.Errorf("s3-access-key is required")
	}
	if s3Region == "" {
		return fmt.Errorf("s3-region is required")
	}
	if s3BucketName == "" {
		return fmt.Errorf("s3-bucket-name is required")
	}
	if vmKey == "" {
		return fmt.Errorf("vm-key is required")
	}
	if diskKey == "" {
		return fmt.Errorf("disk-key is required")
	}
	var err error
	s3DB, err = vms3.CreateS3Client(context.Background(), s3Url, s3AccessKey, s3SecretKey, s3Region, s3BucketName)
	if err != nil {
		return fmt.Errorf("failed to create s3 client: %w", err)
	}
	nbdkit.Debug(fmt.Sprintf("S3 client created successfully"))
	diskManifestKey := vms3.GetDiskManifestObjectKey(vms3.CreateDiskObjectKey(vmKey, diskKey))
	diskManifest, err = s3DB.GetVirtualObjectDiskManifest(context.Background(), diskManifestKey)
	if err != nil {
		return fmt.Errorf("failed to get disk manifest: %w", err)
	}
	nbdkit.Debug(fmt.Sprintf("Disk manifest retrieved successfully"))
	size = uint64(diskManifest.SizeBytes)
	lruCache = NewLruCache()
	safeDownload = NewSafeDownload()
	safeDownload.LoadPart(1)
	// NOTE wer are not configuring slog here therefor loosing all of pkg logs, later write a custom handler for slog
	nbdkit.Debug(fmt.Sprintf("Config complete"))
	return nil
}
