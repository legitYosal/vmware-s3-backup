package main

import (
	"C"
	"unsafe"

	"libguestfs.org/nbdkit"
)
import (
	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
)

var pluginName = "vmware-s3-backup"

type VmwareS3BackupPlugin struct {
	nbdkit.Plugin
}

type VmwareS3BackupConnection struct {
	nbdkit.Connection
}

var size uint64
var s3Url string
var s3SecretKey string
var s3AccessKey string
var s3Region string
var s3BucketName string
var vmKey string
var diskKey string

var s3DB *vms3.S3DB
var diskManifest *vms3.DiskManifest
var numberOfParts int32
var lruCache *LruCache
var safeDownload *SafeDownload
var zeroBuffer []byte
var totalDownload int64

//export plugin_init
func plugin_init() unsafe.Pointer {
	// If your plugin needs to do any initialization, you can
	// either put it here or implement a Load() method.
	// ...

	// Then you must call the following function.
	return nbdkit.PluginInitialize(pluginName, &VmwareS3BackupPlugin{})
}

func main() {}
