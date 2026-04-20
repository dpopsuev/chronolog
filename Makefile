.PHONY: build version test fmt vet lint lint-new preflight install-hooks

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

version:
	@echo $(VERSION)

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION)" -o chronolog ./cmd/chronolog

test:
	go test ./... -count=1

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

lint-new:
	golangci-lint run --new-from-rev=HEAD ./...

preflight: fmt vet lint test

install-hooks:
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make lint-new' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "pre-commit hook installed (runs make lint-new)"
