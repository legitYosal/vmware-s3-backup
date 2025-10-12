package backup

import (
	"context"
	"fmt"
	"net/url"
	"time"

	S3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/session/keepalive"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
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

type VmwareS3BackupClient struct {
	Configuration Config
	VmwareFinder  *find.Finder
	S3Client      *s3.Client
	VDDKConfig    *vmware.VddkConfig
}

type VMData struct {
	Name string
	ID   string
	Path string
	// Status string
	// Memory int
	// CPU    int
	// Disk   int
}

func NewClient(cfg Config) (*VmwareS3BackupClient, error) {
	if cfg.VMWareURL == "" {
		return nil, fmt.Errorf("vcenter URL must be provided")
	}
	if cfg.S3BucketName == "" {
		return nil, fmt.Errorf("s3 bucket name must be provided")
	}
	if cfg.S3URL == "" {
		return nil, fmt.Errorf("s3 url must be provided")
	}
	if cfg.S3SecretKey == "" {
		return nil, fmt.Errorf("s3 secret key must be provided")
	}
	if cfg.S3AccessKey == "" {
		return nil, fmt.Errorf("s3 access key must be provided")
	}
	if cfg.VMWareUsername == "" {
		return nil, fmt.Errorf("vcenter username must be provided")
	}
	if cfg.VMWarePassword == "" {
		return nil, fmt.Errorf("vcenter password must be provided")
	}
	if cfg.S3Region == "" {
		return nil, fmt.Errorf("s3 region must be provided")
	}
	c := &VmwareS3BackupClient{
		Configuration: cfg,
	}

	return c, nil
}

func (c *VmwareS3BackupClient) ConnectToVMware(ctx context.Context) error {
	endpointURL := &url.URL{
		Scheme: "https",
		Host:   c.Configuration.VMWareURL,
		User:   url.UserPassword(c.Configuration.VMWareUsername, c.Configuration.VMWarePassword),
		Path:   "sdk",
	}
	// thumbprint, err := vmware.GetEndpointThumbprint(endpointURL)
	// if err != nil {
	// 	return err
	// }
	soapClient := soap.NewClient(endpointURL, true)
	vimClient, err := vim25.NewClient(ctx, soapClient)
	if err != nil {
		return fmt.Errorf("failed to create VMware client: %w", err)
	}

	vimClient.RoundTripper = keepalive.NewHandlerSOAP(
		vimClient.RoundTripper,
		15*time.Second,
		nil,
	)
	mgr := session.NewManager(vimClient)
	err = mgr.Login(ctx, endpointURL.User)
	if err != nil {
		return fmt.Errorf("failed to login to VMware: %w", err)
	}

	finder := find.NewFinder(vimClient)
	c.VmwareFinder = finder
	return nil
}

func (c *VmwareS3BackupClient) ConnectToS3(ctx context.Context) error {
	cfg, err := S3Config.LoadDefaultConfig(ctx,
		S3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.Configuration.S3AccessKey, c.Configuration.S3SecretKey, "")),
		S3Config.WithRegion(c.Configuration.S3Region),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &c.Configuration.S3URL
		o.UsePathStyle = true
	})
	c.S3Client = s3Client
	return nil
}

func (c *VmwareS3BackupClient) InitNbdkit(ctx context.Context) error {
	endpointURL := &url.URL{
		Scheme: "https",
		Host:   c.Configuration.VMWareURL,
		User:   url.UserPassword(c.Configuration.VMWareUsername, c.Configuration.VMWarePassword),
		Path:   "sdk",
	}
	thumbprint, err := vmware.GetEndpointThumbprint(endpointURL)
	if err != nil {
		return err
	}
	c.VDDKConfig = &vmware.VddkConfig{
		Debug:       true,
		Endpoint:    endpointURL,
		Thumbprint:  thumbprint,
		Compression: nbdkit.NoCompression, // NOTE migratekit supports more
	}
	return nil
}

func (c *VmwareS3BackupClient) Connect(ctx context.Context) error {
	c.ConnectToVMware(ctx)
	c.ConnectToS3(ctx)
	c.InitNbdkit(ctx)
	return nil
}

// FullCopy performs a full backup of a VM to the configured S3 bucket.
func (c *VmwareS3BackupClient) FullCopy(ctx context.Context, vmName string) error {
	fmt.Printf("--> (Library) Starting FULL backup for VM '%s' to bucket '%s'\n", vmName, c.Configuration.S3BucketName)
	//
	// THIS IS WHERE YOUR FULL COPY LOGIC FROM OUR PREVIOUS DISCUSSION GOES
	//
	fmt.Printf("--> (Library) Full backup for VM '%s' completed.\n", vmName)
	return nil
}

// IncrementalCopy performs an incremental backup.
func (c *VmwareS3BackupClient) IncrementalCopy(ctx context.Context, vmName string) error {
	fmt.Printf("--> (Library) Starting INCREMENTAL backup for VM '%s'\n", vmName)
	//
	// THIS IS WHERE YOUR INCREMENTAL LOGIC GOES
	//
	return nil
}

func (c *VmwareS3BackupClient) FindVMByPath(ctx context.Context, vmPath string) (*DetailedVirtualMachine, error) {
	vm, err := c.VmwareFinder.VirtualMachine(ctx, vmPath)
	if err != nil {
		return nil, err
	}
	var o mo.VirtualMachine
	err = vm.Properties(ctx, vm.Reference(), []string{"config"}, &o)
	if err != nil {
		return nil, err
	}
	return &DetailedVirtualMachine{
		Ref:        vm,
		Properties: &o,
	}, nil
}

func (c *VmwareS3BackupClient) ListVMs(ctx context.Context) ([]VMData, error) {
	var vmData []VMData
	vmList, err := c.VmwareFinder.VirtualMachineList(ctx, "*")
	if err != nil {
		return nil, fmt.Errorf("failed to list VMs: %w", err)
	}
	for _, vm := range vmList {
		vmData = append(vmData, VMData{
			Name: vm.Name(),
			ID:   vm.Reference().Value,
			Path: vm.InventoryPath,
		})
	}
	return vmData, nil
}
