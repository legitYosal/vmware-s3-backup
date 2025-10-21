BIN_DIR := bin

CLI_SOURCE := ./cmd/cli
CLI_TARGET := $(BIN_DIR)/vmware-s3-backup

PLUGIN_SOURCE := ./cmd/nbdkit-plugin
PLUGIN_TARGET := $(BIN_DIR)/vmware-s3-backup.so

CGO_FLAGS := -I/usr/include/nbdkit -lnbdkit_plugin

.PHONY: all prepare build clean

all: prepare build

prepare:
	go mod tidy
	go mod vendor

## build: Compiles both the CLI and the plugin.
build: $(CLI_TARGET) $(PLUGIN_TARGET)

# Rule for the CLI executable
$(CLI_TARGET):
	mkdir -p $(BIN_DIR)
	go build -o $@ $(CLI_SOURCE)

# Rule for the NBDkit shared library plugin (requires CGO)
$(PLUGIN_TARGET):
	mkdir -p $(BIN_DIR)
	# CGO_CFLAGS and CGO_LDFLAGS are set via the environment for this single command
	CGO_CFLAGS="$(CGO_FLAGS)" CGO_LDFLAGS="$(LDFLAGS)" \
	go build \
		-buildmode=plugin \
		-tags "netgo osusergo" \
		-o $@ $(PLUGIN_SOURCE)

clean:
	rm -rf $(BIN_DIR)
