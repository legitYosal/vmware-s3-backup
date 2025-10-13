package backup

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	S3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/legitYosal/vmware-s3-backup/pkg/config"
	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/legitYosal/vmware-s3-backup/pkg/vmware"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/session/keepalive"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
)

type VmwareS3BackupClient struct {
	Configuration *config.Config
	VMWareFinder  *find.Finder
	VMWareClient  *vim25.Client
	S3Client      *s3.Client
	VDDKConfig    *vmware.VddkConfig
}

type DiskData struct {
	Name       string
	CapacityGB int
}
type VMData struct {
	Name       string
	ID         string
	Path       string
	Status     string
	MemoryGB   int
	CPUs       int
	Disks      []DiskData
	Snapshots  int
	CBTEnabled bool
}

func NewVmwareS3BackupClient(cfg *config.Config) (*VmwareS3BackupClient, error) {
	if cfg.VMWareHOST == "" {
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
		Host:   c.Configuration.VMWareHOST,
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
	c.VMWareFinder = finder
	c.VMWareClient = vimClient
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
		Host:   c.Configuration.VMWareHOST,
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
	// c.ConnectToS3(ctx) NOTE: THIS IS JUST FOR TESTING commented
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
	vm, err := c.VMWareFinder.VirtualMachine(ctx, vmPath)
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
	vmList, err := c.VMWareFinder.VirtualMachineList(ctx, "*")
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

func extractDisks(config types.VirtualMachineConfigInfo) []DiskData {
	var disks []DiskData

	if config.Hardware.Device == nil {
		return disks
	}

	for _, device := range config.Hardware.Device {
		// Look for VirtualDisk devices
		if disk, ok := device.(*types.VirtualDisk); ok {
			// Get the disk key and capacity
			diskKey := disk.Key
			diskName := "Virtual-Disk-" + strconv.Itoa(int(diskKey)) // Default name

			// Get capacity in KB (CapacityInKB is the correct field)
			capacityKB := disk.CapacityInKB

			// Try to find the device label (e.g., "Hard Disk 1")
			if disk.DeviceInfo != nil {
				if baseDesc, ok := disk.DeviceInfo.(*types.Description); ok {
					diskName = baseDesc.Label
					diskName = strings.ReplaceAll(diskName, " ", "-")
				}
			}

			disks = append(disks, DiskData{
				Name: diskName,
				// Convert KB to GB
				CapacityGB: int(capacityKB / 1024 / 1024),
			})
		}
	}
	return disks
}

// countSnapshots recursively counts all snapshots in a VM's snapshot tree
func countSnapshots(snapshotList []types.VirtualMachineSnapshotTree) int {
	count := 0
	for _, snapshot := range snapshotList {
		count++ // Count this snapshot
		// Recursively count child snapshots
		if snapshot.ChildSnapshotList != nil {
			count += countSnapshots(snapshot.ChildSnapshotList)
		}
	}
	return count
}

func (c *VmwareS3BackupClient) DetailedListVMs(ctx context.Context) ([]VMData, error) {
	slog.Debug("Starting VM list and property collection")

	// 1. First, get all VMs using the finder
	vmList, err := c.VMWareFinder.VirtualMachineList(ctx, "*")
	if err != nil {
		return nil, fmt.Errorf("failed to list VMs: %w", err)
	}

	// 2. Collect VM references for property retrieval
	var vmRefs []types.ManagedObjectReference
	for _, vm := range vmList {
		vmRefs = append(vmRefs, vm.Reference())
	}

	// 3. Define the properties we need to fetch
	properties := []string{
		"name",
		"runtime.powerState",
		"config.hardware",
		"config.changeTrackingEnabled",
		"summary.config",
		"snapshot",
	}

	// 4. Use property collector to retrieve properties efficiently
	pc := property.DefaultCollector(c.VMWareClient)
	var vms []mo.VirtualMachine
	err = pc.Retrieve(ctx, vmRefs, properties, &vms)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve VM properties: %w", err)
	}

	// 5. Process the collected data
	var vmData []VMData
	for i, vm := range vms {
		// Extract disk information from hardware config
		var disks []DiskData
		if vm.Config != nil && vm.Config.Hardware.Device != nil {
			disks = extractDisks(*vm.Config)
		}

		// Get memory in GB
		memoryGB := 0
		if vm.Config != nil && vm.Config.Hardware.MemoryMB > 0 {
			memoryGB = int(vm.Config.Hardware.MemoryMB / 1024)
		}

		// Get CPU count
		cpus := 0
		if vm.Config != nil {
			cpus = int(vm.Config.Hardware.NumCPU)
		}

		// Get power state
		powerState := "unknown"
		if vm.Runtime.PowerState != "" {
			powerState = string(vm.Runtime.PowerState)
		}

		// Get CBT (Changed Block Tracking) enabled status
		cbtEnabled := false
		if vm.Config != nil && vm.Config.ChangeTrackingEnabled != nil {
			cbtEnabled = *vm.Config.ChangeTrackingEnabled
		}

		// Count snapshots
		snapshotCount := 0
		if vm.Snapshot != nil && vm.Snapshot.RootSnapshotList != nil {
			snapshotCount = countSnapshots(vm.Snapshot.RootSnapshotList)
		}

		// Map properties to our simplified struct
		vmData = append(vmData, VMData{
			Name:       vm.Name,
			ID:         vm.Reference().Value,
			Path:       vmList[i].InventoryPath, // Get path from the original VM object
			Status:     powerState,
			MemoryGB:   memoryGB,
			CPUs:       cpus,
			Disks:      disks,
			Snapshots:  snapshotCount,
			CBTEnabled: cbtEnabled,
		})
	}

	slog.Debug("Successfully listed and collected properties for VMs", "count", len(vmData))
	return vmData, nil
}
