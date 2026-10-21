# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
# Piperoom pilot image: the Go server plus the built web UI, in one container.
#
#   docker build -t piperoom:pilot .
#   docker compose -f deploy/docker-compose.pilot.yml up     # see deploy/README.md
#
# Build from the repository root. Base images are pinned by tag. FOLLOW-UP:
# pin them by digest (image@sha256:...) once a release process owns the bumps.

# ---- stage 1: web UI -> /app/dist -------------------------------------------
FROM node:24-alpine AS web
WORKDIR /app
# Dependencies first, so the layer is reused until the lockfile changes.
COPY web/package.json web/package-lock.json ./
# Same as CI: no install scripts except esbuild's; see README.md "Web tooling".
RUN npm ci --ignore-scripts \
 && node node_modules/esbuild/install.js
COPY web/ ./
# VITE_API_BASE is deliberately NOT set: the built UI then calls the API and the
# WebSocket on the origin that served it (see web/src/api/client.ts).
RUN npm run build

# ---- stage 2: server binary -> /out/piperoom --------------------------------
FROM golang:1.27.3-alpine AS server
WORKDIR /src
COPY go.mod go.sum ./
# No separate "go mod download": it fetches every module in go.sum, including
# ones the server never links (the Databricks driver's kernel-bindings modules,
# ~133 MB zipped). go build below downloads only what it compiles; the cache
# mounts keep those modules and the build cache between builds (BuildKit).
COPY cmd/server ./cmd/server
COPY internal ./internal
COPY migrations ./migrations
# CGO off: a static binary that runs on distroless/static. -trimpath keeps the
# build machine's paths out of the binary and makes the build reproducible.
# VERSION names the build (GET /healthz, start-up log). Pass it with
# --build-arg VERSION=$(git describe --tags --always --dirty). The linker
# ignores -X for a symbol that does not exist, so this is safe on a tree
# without internal/version.
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X piperoom/internal/version.Version=${VERSION}" -o /out/piperoom ./cmd/server

# ---- stage 3: runtime --------------------------------------------------------
# distroless/static has CA certificates (needed for api.anthropic.com and TLS to
# the data stack) and tzdata, runs as uid 65532 by default, and has no shell.
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=server /out/piperoom /piperoom
COPY --from=web /app/dist /web

# The server serves this directory at "/" (internal/config: PIPEROOM_WEB_DIR).
# The two YAML files are mounted at /config by the compose file; see
# deploy/README.md. Everything else (database URLs, keys) comes from the
# environment.
ENV PIPEROOM_WEB_DIR=/web \
    PIPEROOM_HTTP_ADDR=0.0.0.0:8090 \
    PIPEROOM_AUTH_MODE=oidc \
    OWNERS_FILE=/config/owners.yaml \
    USERS_FILE=/config/users.yaml

EXPOSE 8090

# distroless has no curl or wget, so the server binary is its own probe: it
# calls GET /healthz on PIPEROOM_HTTP_ADDR (loopback when that is 0.0.0.0).
# /healthz is liveness only; GET /readyz also checks the room database.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/piperoom", "--healthcheck"]

# The image carries NO dev auth: no PIPEROOM_DEV_ALLOW_NONLOOPBACK, no dev
# mode, no users file. Dev login has no password (anyone who can reach the port
# can pick any user, including approving fixes), so it must be asked for
# explicitly at run time: deploy/docker-compose.dev-auth.yml, or
# -e PIPEROOM_AUTH_MODE=dev -e PIPEROOM_DEV_ALLOW_NONLOOPBACK=1.
# From R9 dev mode off loopback (which a container always is) also needs the
# acknowledgement -e PIPEROOM_DEV_ACK=dev-login-has-no-password, which the image
# never sets: the operator states it in their own deployment.
#
# The image starts in oidc mode and fails fast, with a message naming the
# missing variable, unless PIPEROOM_PUBLIC_URL (https), PIPEROOM_OIDC_ISSUER,
# PIPEROOM_OIDC_CLIENT_ID and PIPEROOM_OIDC_CLIENT_SECRET are set (see
# deploy/README.md, "Single sign-on (OIDC)"). A proxy pilot that cannot use OIDC
# yet builds from the git tag "pre-auth".
#
# Command overrides such as "--demo" are not an auth switch: --demo forces dev
# mode and, inside a container, still needs PIPEROOM_DEV_ALLOW_NONLOOPBACK=1.
USER nonroot:nonroot
ENTRYPOINT ["/piperoom"]
