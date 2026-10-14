SHELL := /bin/sh

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: ci go-ci web-ci fmt vet build build-server test evals discover-draft web-install web-lint web-build web-test

ci: go-ci web-ci

go-ci: fmt vet build test evals

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...
	go vet -tags integration ./...
	go vet -tags claude ./internal/agent/...

build:
	go build ./...

# The server binary with its build version (internal/version, GET /healthz).
build-server:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X piperoom/internal/version.Version=$(VERSION)" -o bin/piperoom ./cmd/server

test:
	go test ./...

# Validates evals/scenarios against the live tool definitions; no model calls,
# no API key, no network, so it is safe in go-ci.
evals:
	go run ./evals/cmd/evalrun -validate

# Reads the cluster from the config env vars; refuses to overwrite an existing
# draft (run cmd/discover with -force for that). Not part of ci: needs a cluster.
discover-draft:
	go run ./cmd/discover -out owners.draft.yaml

web-ci: web-install web-lint web-build web-test

web-install:
	cd web && npm ci --ignore-scripts && node node_modules/esbuild/install.js

web-lint:
	cd web && npm run lint

web-build:
	cd web && npm run build

web-test:
	cd web && npx vitest run
