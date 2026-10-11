SHELL := /bin/sh

.PHONY: ci go-ci web-ci fmt vet build test web-install web-lint web-build web-test

ci: go-ci web-ci

go-ci: fmt vet build test

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

build:
	go build ./...

test:
	go test ./...

web-ci: web-install web-lint web-build web-test

web-install:
	cd web && npm ci

web-lint:
	cd web && npm run lint

web-build:
	cd web && npm run build

web-test:
	cd web && npx vitest run
