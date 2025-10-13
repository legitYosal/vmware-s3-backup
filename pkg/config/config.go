package config

import (
	"context"
	"fmt"
	"os"
)

type Config struct {
	VMWareURL      string
	VMWareUsername string
	VMWarePassword string
	S3URL          string
	S3SecretKey    string
	S3AccessKey    string
	S3BucketName   string
	S3Region       string
}

func NewClientConfig(vmwareURL, vmwareUsername, vmwarePassword, s3URL, s3SecretKey, s3AccessKey, s3BucketName, s3Region string) Config {
	return Config{
		VMWareURL:      vmwareURL,
		VMWareUsername: vmwareUsername,
		VMWarePassword: vmwarePassword,
		S3URL:          s3URL,
		S3SecretKey:    s3SecretKey,
		S3AccessKey:    s3AccessKey,
		S3BucketName:   s3BucketName,
		S3Region:       s3Region,
	}
}

func ValidateConfig(cfg Config) error {
	if cfg.VMWareURL == "" {
		return fmt.Errorf("vcenter URL must be provided")
	}
	if cfg.S3BucketName == "" {
		return fmt.Errorf("s3 bucket name must be provided")
	}
	if cfg.S3URL == "" {
		return fmt.Errorf("s3 url must be provided")
	}
	if cfg.S3SecretKey == "" {
		return fmt.Errorf("s3 secret key must be provided")
	}
	if cfg.S3AccessKey == "" {
		return fmt.Errorf("s3 access key must be provided")
	}
	if cfg.VMWareUsername == "" {
		return fmt.Errorf("vcenter username must be provided")
	}
	if cfg.VMWarePassword == "" {
		return fmt.Errorf("vcenter password must be provided")
	}
	if cfg.S3Region == "" {
		return fmt.Errorf("s3 region must be provided")
	}
	return nil
}

func LoadClientConfigFromENV(ctx context.Context) (Config, error) {
	cfg := NewClientConfig(
		os.Getenv("VMWARE_URL"),
		os.Getenv("VMWARE_USERNAME"),
		os.Getenv("VMWARE_PASSWORD"),
		os.Getenv("S3_URL"),
		os.Getenv("S3_SECRET_KEY"),
		os.Getenv("S3_ACCESS_KEY"),
		os.Getenv("S3_BUCKET_NAME"),
		os.Getenv("S3_REGION"),
	)
	if err := ValidateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
