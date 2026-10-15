# Build from the repo root: docker build -f cmd/devconsumer/Dockerfile .
FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd/devconsumer ./cmd/devconsumer
RUN CGO_ENABLED=0 go build -o /devconsumer ./cmd/devconsumer

FROM alpine:3.22
COPY --from=build /devconsumer /devconsumer
ENTRYPOINT ["/devconsumer"]
