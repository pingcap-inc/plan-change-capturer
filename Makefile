BUILD_BIN_PATH := $(shell pwd)/bin

default: build

build: pcc-ctl

# Tools
pcc-ctl: export GO111MODULE=on
pcc-ctl: export GOPROXY=https://proxy.golang.org
pcc-ctl:
	CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/plan-change-capturer main.go

clean-build:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)

clean: clean-build

.PHONY: all ci vendor tidy clean-test clean-build clean