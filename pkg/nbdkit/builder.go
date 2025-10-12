package nbdkit

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
)

type CompressionMethod string

const TempDirectoryPrefix = "vmware-backup-s3-temop"

const (
	NoCompression     CompressionMethod = "none"
	ZlibCompression   CompressionMethod = "zlib"
	FastLzCompression CompressionMethod = "fastlz"
	SkipzCompression  CompressionMethod = "skipz"
)

type NBDKitSocketConfig struct {
	server      string
	username    string
	password    string
	thumbprint  string
	vm          string
	snapshot    string
	filename    string
	compression CompressionMethod
}

func NewNBDKitSocketConfig(server, username, password, thumbprint, vm, snapshot, filename string, compression CompressionMethod) *NBDKitSocketConfig {
	return &NBDKitSocketConfig{
		server:      server,
		username:    username,
		password:    password,
		thumbprint:  thumbprint,
		vm:          vm,
		snapshot:    snapshot,
		filename:    filename,
		compression: compression,
	}
}

func (s *NBDKitSocketConfig) Build() (*NbdkitSocket, error) {
	tmp, err := os.MkdirTemp("", "vmware-s3-backup-")
	if err != nil {
		slog.Error("failed to create temp directory", "error", err)
		return nil, err
	}
	socket := fmt.Sprintf("%s/nbdkit.sock", tmp)
	pidFile := fmt.Sprintf("%s/nbdkit.pid", tmp)
	os.Setenv("LD_LIBRARY_PATH", "/usr/lib64/vmware-vix-disklib/lib64")
	cmd := exec.Command(
		"nbdkit",
		"--exit-with-parent",
		"--readonly",
		"--foreground",
		fmt.Sprintf("--unix=%s", socket),
		fmt.Sprintf("--pidfile=%s", pidFile),
		"vddk",
		fmt.Sprintf("server=%s", s.server),
		fmt.Sprintf("user=%s", s.username),
		fmt.Sprintf("password=%s", s.password),
		fmt.Sprintf("thumbprint=%s", s.thumbprint),
		fmt.Sprintf("compression=%s", s.compression),
		fmt.Sprintf("vm=moref=%s", s.vm),
		fmt.Sprintf("snapshot=%s", s.snapshot),
		"transports=file:nbdssl:nbd",
		s.filename,
	)
	return &NbdkitSocket{
		cmd:     cmd,
		socket:  socket,
		pidFile: pidFile,
	}, nil
}
