SHELL := /bin/sh

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# The gofmt of the toolchain go.mod selects: a gofmt from an older Go install on
# PATH formats differently and fails on clean files.
GOFMT = "$$(go env GOROOT)/bin/gofmt"
# web/node_modules holds Go files of npm packages (flatted); they are not ours
# to format, vet or test, but `./...` includes them once web-install has run.
# Listed per tag set: a package whose files all carry a tag is listed only with it.
go_pkgs = $$(go list $(1) ./... | grep -v '/web/node_modules/')
GO_PKGS = $(call go_pkgs,)
TAGGED_ONLY := evalgen,e2e

.PHONY: ci go-ci web-ci fmt vet build build-server test evals discover-draft web-install web-lint web-build web-test

ci: go-ci web-ci

go-ci: fmt vet build test evals

# gofmt prints a parse error to stderr and nothing to stdout, so the exit status
# of the pipeline is checked on its own: `test -z "$(...)"` alone would pass.
fmt:
	@out="$$(find . -name '*.go' -not -path './web/node_modules/*' -not -path './.git/*' -print0 | xargs -0 $(GOFMT) -l)" || { echo "make fmt: gofmt failed (a file does not parse?)" >&2; exit 1; }; \
	if [ -n "$$out" ]; then echo "$$out"; echo "make fmt: files above are not gofmt-clean" >&2; exit 1; fi

# Same tag sets as CI (.github/workflows/ci.yml, Go job).
vet:
	go vet $(GO_PKGS)
	go vet -tags integration $(call go_pkgs,-tags integration)
	go vet -tags claude ./internal/agent/... ./evals/...
	go vet -tags $(TAGGED_ONLY) $(call go_pkgs,-tags $(TAGGED_ONLY))

build:
	go build $(GO_PKGS)

# The server binary with its build version (internal/version, GET /healthz).
build-server:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X piperoom/internal/version.Version=$(VERSION)" -o bin/piperoom ./cmd/server

test:
	go test $(GO_PKGS)

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
