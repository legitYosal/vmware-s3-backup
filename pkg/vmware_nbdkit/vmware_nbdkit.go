package vmware_nbdkit

import (
	"net/url"

	"github.com/legitYosal/vmware-s3-backup/pkg/nbdkit"
	"github.com/vmware/govmomi/vim25/types"
)

const MaxChunkSize = 64 * 1024 * 1024

type VddkConfig struct {
	Debug       bool
	Endpoint    *url.URL
	Thumbprint  string
	Compression nbdkit.CompressionMethod
}

type NbdkitServer struct {
	Servers *NbdkitServers
	Disk    *types.VirtualDisk
	Nbdkit  *nbdkit.NbdkitServer
}

type NbdkitServers struct {
	VddkConfig *VddkConfig
	// VirtualMachine *object.VirtualMachine
	SnapshotRef types.ManagedObjectReference
	Servers     []*NbdkitServer
}

func NewNbdkitServers(vddk *VddkConfig) *NbdkitServers {
	return &NbdkitServers{
		VddkConfig: vddk,
		// VirtualMachine: vm,
		Servers: []*NbdkitServer{},
	}
}
