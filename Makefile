SHELL := /bin/sh

.PHONY: ci go-ci web-ci fmt vet build test evals discover-draft web-install web-gomod web-lint web-build web-test

ci: go-ci web-ci

go-ci: fmt vet build test evals

# node_modules can hold Go files of npm packages (flatted): not ours to format.
fmt:
	test -z "$$(find . -name '*.go' -not -path '*/node_modules/*' -not -path './.git/*' | xargs gofmt -l)"

vet:
	go vet ./...
	go vet -tags integration ./...
	go vet -tags claude ./internal/agent/... ./evals/...
	go vet -tags evalgen,e2e ./...

build:
	go build ./...

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

web-ci: web-install web-gomod web-lint web-build web-test

# web/go.mod keeps the Go files npm installs (flatted) out of `go ./...`.
web-gomod:
	test -z "$$(go list ./... | grep node_modules)"

web-install:
	cd web && npm ci --ignore-scripts && node node_modules/esbuild/install.js

web-lint:
	cd web && npm run lint

web-build:
	cd web && npm run build

web-test:
	cd web && npx vitest run
