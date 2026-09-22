# Quorum build tasks.
#
# make is not installed by default on Windows, where this project is primarily
# developed, so make.ps1 mirrors every target here. The two are kept in step by
# test/tooling/targets_test.go, which fails if either grows a target the other
# does not have. A build script that only works on the maintainer's machine is
# a portfolio liability, not a convenience.
#
# Everything here needs only the Go toolchain. Protobuf codegen uses buf, a
# pure-Go compiler installed by `make tools`; there is no native protoc
# dependency.

GO       ?= go
BIN      ?= bin
CONFIG   ?= cluster.yaml
GOPATHBIN := $(shell $(GO) env GOPATH)/bin
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -X main.version=$(VERSION)

BUF_VERSION            ?= latest
GOLANGCI_LINT_VERSION  ?= latest
PROTOC_GEN_GO_VERSION  ?= latest
PROTOC_GEN_GRPC_VERSION?= latest

.DEFAULT_GOAL := help

.PHONY: help tools proto proto-check build test race cover vet lint fmt fmt-check ci run up down clean

help: ## Show available targets
	@printf 'Quorum %s\n\n' '$(VERSION)'
	@grep -hE '^[a-z][a-z0-9-]*:.*## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*## "}{printf "  %-12s %s\n", $$1, $$2}'

tools: ## Install pinned dev tools (buf, protoc plugins, golangci-lint) into GOPATH/bin
	$(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GRPC_VERSION)
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

proto: ## Regenerate Go code from the .proto files
	PATH="$(GOPATHBIN):$$PATH" buf generate

proto-check: proto ## Fail if checked-in generated code is stale
	@git diff --exit-code -- gen \
		|| (echo "gen/ is out of date: run 'make proto' and commit the result" && exit 1)

build: ## Build every binary into bin/
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN)/ ./cmd/...

test: ## Run the test suite
	$(GO) test ./...

# The race detector needs cgo and a 64-bit C compiler. That is the default on
# Linux and macOS. On Windows it often is not -- an old 32-bit MinGW earlier on
# PATH fails with "sorry, unimplemented: 64-bit mode not compiled in" -- so
# make.ps1 locates a usable compiler itself rather than skipping the check.
race: ## Run the test suite under the race detector
	$(GO) test -race ./...

cover: ## Run tests and report coverage per package
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out

vet: ## Run go vet
	$(GO) vet ./...

lint: vet ## Run go vet and golangci-lint
	PATH="$(GOPATHBIN):$$PATH" golangci-lint run

fmt: ## Format all Go source
	$(GO) fmt ./...

fmt-check: ## Fail if any Go source is unformatted
	@unformatted=$$(gofmt -l . | grep -v '^gen/' || true); \
	if [ -n "$$unformatted" ]; then \
		echo "unformatted files:"; echo "$$unformatted"; exit 1; \
	fi

ci: fmt-check lint build test race ## Everything CI would run
	@echo "ci: ok"

run: build ## Show the cluster plan and one node's configuration
	./$(BIN)/quorumctl plan -config $(CONFIG)
	@echo
	./$(BIN)/quorum-node -id 1 -config $(CONFIG)

up: build ## Start every node in the config as a separate process
	./$(BIN)/quorumctl up -config $(CONFIG)

down: build ## Stop every node started by up
	./$(BIN)/quorumctl down

clean: ## Remove build output, coverage data and runtime cluster state
	rm -rf $(BIN) coverage.out data
