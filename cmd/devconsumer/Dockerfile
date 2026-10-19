# syntax=docker/dockerfile:1
# Build from the repo root: docker build -f cmd/devconsumer/Dockerfile .
FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
# No "go mod download" (it fetches every module in go.sum, ~133 MB of them never
# linked); go build downloads only what it compiles. Cache mounts need BuildKit.
COPY internal ./internal
COPY cmd/devconsumer ./cmd/devconsumer
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /devconsumer ./cmd/devconsumer

FROM alpine:3.22
COPY --from=build /devconsumer /devconsumer
ENTRYPOINT ["/devconsumer"]
