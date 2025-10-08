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
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware_nbdkit"
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

type Client struct {
	config        Config
	vmwareFinder  *find.Finder
	s3Client      *s3.Client
	vddkConfig    *vmware_nbdkit.VddkConfig
	nbdkitServers *vmware_nbdkit.NbdkitServers
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

func NewClient(cfg Config) (*Client, error) {
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
	c := &Client{
		config: cfg,
	}

	return c, nil
}

func (c *Client) ConnectToVMware(ctx context.Context) error {
	endpointURL := &url.URL{
		Scheme: "https",
		Host:   c.config.VMWareURL,
		User:   url.UserPassword(c.config.VMWareUsername, c.config.VMWarePassword),
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
	c.vmwareFinder = finder
	return nil
}

func (c *Client) ConnectToS3(ctx context.Context) error {
	cfg, err := S3Config.LoadDefaultConfig(ctx,
		S3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.config.S3AccessKey, c.config.S3SecretKey, "")),
		S3Config.WithRegion(c.config.S3Region),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &c.config.S3URL
		o.UsePathStyle = true
	})
	c.s3Client = s3Client
	return nil
}

func (c *Client) InitNbdkit(ctx context.Context) error {
	endpointURL := &url.URL{
		Scheme: "https",
		Host:   c.config.VMWareURL,
		User:   url.UserPassword(c.config.VMWareUsername, c.config.VMWarePassword),
		Path:   "sdk",
	}
	thumbprint, err := vmware.GetEndpointThumbprint(endpointURL)
	if err != nil {
		return err
	}
	c.vddkConfig = &vmware_nbdkit.VddkConfig{
		Debug:       true,
		Endpoint:    endpointURL,
		Thumbprint:  thumbprint,
		Compression: nbdkit.NoCompression, // NOTE migratekit supports more
	}
	c.nbdkitServers = vmware_nbdkit.NewNbdkitServers(c.vddkConfig)
	return nil
}

func (c *Client) Connect(ctx context.Context) error {
	c.ConnectToVMware(ctx)
	c.ConnectToS3(ctx)
	c.InitNbdkit(ctx)
	return nil
}

// FullCopy performs a full backup of a VM to the configured S3 bucket.
func (c *Client) FullCopy(ctx context.Context, vmName string) error {
	fmt.Printf("--> (Library) Starting FULL backup for VM '%s' to bucket '%s'\n", vmName, c.config.S3BucketName)
	//
	// THIS IS WHERE YOUR FULL COPY LOGIC FROM OUR PREVIOUS DISCUSSION GOES
	//
	fmt.Printf("--> (Library) Full backup for VM '%s' completed.\n", vmName)
	return nil
}

// IncrementalCopy performs an incremental backup.
func (c *Client) IncrementalCopy(ctx context.Context, vmName string) error {
	fmt.Printf("--> (Library) Starting INCREMENTAL backup for VM '%s'\n", vmName)
	//
	// THIS IS WHERE YOUR INCREMENTAL LOGIC GOES
	//
	return nil
}

func (c *Client) FindVMByPath(ctx context.Context, vmPath string) (*vmware.CompleteVirtualMachine, error) {
	vm, err := c.vmwareFinder.VirtualMachine(ctx, vmPath)
	if err != nil {
		return nil, err
	}
	var o mo.VirtualMachine
	err = vm.Properties(ctx, vm.Reference(), []string{"config"}, &o)
	if err != nil {
		return nil, err
	}
	return &vmware.CompleteVirtualMachine{
		M: vm,
		O: &o,
	}, nil
}

func (c *Client) ListVMs(ctx context.Context) ([]VMData, error) {
	var vmData []VMData
	vmList, err := c.vmwareFinder.VirtualMachineList(ctx, "*")
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
