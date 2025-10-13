package main

import (
	// Import the internal package where cobra commands are defined
	"github.com/legitYosal/vmware-s3-backup/cmd"
)

func main() {
	cmd.Execute()
}
