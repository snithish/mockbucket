SHELL := /bin/bash

GO := go
BINARY := mockbucketd
BIN_DIR := ./bin
CONFIG ?= mockbucket.yaml

.DEFAULT_GOAL := help

.PHONY: help build run test fmt tidy compat clean

help:
	@printf "Available targets:\n"
	@printf "  build   Build the mockbucket daemon binary in $(BIN_DIR)\n"
	@printf "  run     Run the configured daemon (builds first)\n"
	@printf "  test    Run the Go test suite\n"
	@printf "  fmt     Run gofmt over tracked Go files\n"
	@printf "  tidy    Run go mod tidy\n"
	@printf "  compat  Execute the compatibility tests (uv run python scripts/compat/run_all.py)\n"
	@printf "  clean   Remove $(BIN_DIR)\n"

build: $(BIN_DIR)/$(BINARY)

$(BIN_DIR)/$(BINARY): $(shell find cmd -maxdepth 2 -name '*.go') go.mod go.sum
	@mkdir -p $(BIN_DIR)
	@$(GO) build -o $(BIN_DIR)/$(BINARY) ./cmd/mockbucketd

run: build
	@$(BIN_DIR)/$(BINARY) --config $(CONFIG)

test:
	@$(GO) test ./...

fmt:
	@gofmt -w $$(git ls-files '*.go')

tidy:
	@$(GO) mod tidy

compat:
	@uv run python scripts/compat/run_all.py

clean:
	@rm -rf $(BIN_DIR)
