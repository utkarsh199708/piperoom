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

.PHONY: ci go-ci web-ci fmt vet build build-server test evals evals-gen-check discover-draft quickstart web-install web-lint web-build web-test

ci: go-ci web-ci

go-ci: fmt vet build test evals evals-gen-check

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

# Fails and lists the stale files when the generated eval files differ from
# evals/gen output. Fix: go run -tags evalgen ./evals/gen
evals-gen-check:
	go run -tags evalgen ./evals/gen -check

# Reads the cluster from the config env vars; refuses to overwrite an existing
# draft (run cmd/discover with -force for that). Not part of ci: needs a cluster.
discover-draft:
	go run ./cmd/discover -out owners.draft.yaml

# Read-only quickstart (tasks/P1-quickstart-readonly.md): discover the pipelines
# behind CONNECT_URL (and the other config env vars) into $(QUICKSTART_OWNERS),
# every layer owned by you, then start the server with --quickstart: in-memory
# rooms, dev login on 127.0.0.1 only, write mode off. It prints the URL to open.
#   make quickstart                       # reuses an existing owners file
#   make quickstart FORCE=1               # rediscovers and overwrites it
#   make quickstart DISCOVER_FLAGS=-source-db
# The UI: PIPEROOM_WEB_DIR when set; else web/dist, built here first when npm is
# on PATH and web/dist has no index.html; else the URL serves the API only.
QUICKSTART_OWNERS ?= owners.yaml
DISCOVER_FLAGS ?=
FORCE ?=

quickstart:
	@if [ -e "$(QUICKSTART_OWNERS)" ] && [ "$(FORCE)" != 1 ]; then \
		echo "make quickstart: using the existing $(QUICKSTART_OWNERS) (FORCE=1 rediscovers and overwrites it)"; \
	else \
		go run ./cmd/discover -write-owners "$(QUICKSTART_OWNERS)" $(if $(filter 1,$(FORCE)),-force) $(DISCOVER_FLAGS) || exit 1; \
	fi
	@if [ -z "$$PIPEROOM_WEB_DIR" ] && [ ! -f web/dist/index.html ]; then \
		if command -v npm >/dev/null 2>&1; then \
			$(MAKE) --no-print-directory web-install web-build || exit 1; \
		else \
			echo "make quickstart: npm not found, so the URL serves the API only; install Node 24, or use deploy/docker-compose.quickstart.yml" >&2; \
		fi; \
	fi
	@if [ -z "$$PIPEROOM_WEB_DIR" ] && [ -f web/dist/index.html ]; then export PIPEROOM_WEB_DIR=web/dist; fi; \
	exec go run ./cmd/server --quickstart --owners "$(QUICKSTART_OWNERS)"

web-ci: web-install web-lint web-build web-test

web-install:
	cd web && npm ci --ignore-scripts && node node_modules/esbuild/install.js

web-lint:
	cd web && npm run lint

web-build:
	cd web && npm run build

web-test:
	cd web && npx vitest run
