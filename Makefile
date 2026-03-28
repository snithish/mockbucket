SHELL := /bin/bash

GO := go
BINARY := mockbucketd
BIN_DIR := ./bin
CONFIG ?= mockbucket.yaml

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.DEFAULT_GOAL := help

.PHONY: help build run test fmt fmt-check lint tidy compat docker clean

help:
	@printf "Available targets:\n"
	@printf "  build     Build the mockbucket daemon binary in $(BIN_DIR)\n"
	@printf "  run       Run the configured daemon (builds first)\n"
	@printf "  test      Run the Go test suite\n"
	@printf "  fmt       Run gofmt over tracked Go files\n"
	@printf "  fmt-check Check formatting without modifying files\n"
	@printf "  lint      Run go vet\n"
	@printf "  tidy      Run go mod tidy\n"
	@printf "  compat    Execute the compatibility tests (uv run --project scripts/compat mockbucket-compat test)\n"
	@printf "  docker    Build the Docker image\n"
	@printf "  clean     Remove $(BIN_DIR)\n"

build:
	@mkdir -p $(BIN_DIR)
	@$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) ./cmd/mockbucketd

run: build
	@$(BIN_DIR)/$(BINARY) --config $(CONFIG)

test:
	@$(GO) test ./...

fmt:
	@gofmt -w $$(git ls-files '*.go')

fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Files not formatted:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

lint:
	@$(GO) vet ./...

tidy:
	@$(GO) mod tidy

compat: build
	@uv run --project scripts/compat mockbucket-compat test

docker:
	@docker build -t mockbucketd .

clean:
	@rm -rf $(BIN_DIR)
