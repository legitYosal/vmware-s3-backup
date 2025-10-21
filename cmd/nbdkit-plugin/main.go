package main

import (
	"C"
	"unsafe"

	"libguestfs.org/nbdkit"
)

var pluginName = "vmware-s3-backup"

type VmwareS3BackupPlugin struct {
	nbdkit.Plugin
}

type VmwareS3BackupConnection struct {
	nbdkit.Connection
}

var size uint64 = 1024 * 1024

func (p *VmwareS3BackupPlugin) Open(readonly bool) (nbdkit.ConnectionInterface, error) {
	return &VmwareS3BackupConnection{}, nil
}

func (c *VmwareS3BackupConnection) GetSize() (uint64, error) {
	return size, nil
}

func (c *VmwareS3BackupConnection) PRead(buf []byte, offset uint64,
	flags uint32) error {
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	return nil
}

func plugin_init() unsafe.Pointer {
	// If your plugin needs to do any initialization, you can
	// either put it here or implement a Load() method.
	// ...

	// Then you must call the following function.
	return nbdkit.PluginInitialize(pluginName, &VmwareS3BackupPlugin{})
}

func main() {}
