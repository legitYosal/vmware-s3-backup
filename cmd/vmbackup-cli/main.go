package main

import (
	// Import the internal package where cobra commands are defined
	"github.com/legitYosal/vmware-s3-backup/internal/cmd"
)

func main() {
	cmd.Execute()
}
