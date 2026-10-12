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
RUN npm ci
COPY web/ ./
# VITE_API_BASE is deliberately NOT set: the built UI then calls the API and the
# WebSocket on the origin that served it (see web/src/api/client.ts).
RUN npm run build

# ---- stage 2: server binary -> /out/piperoom --------------------------------
FROM golang:1.27-alpine AS server
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/server ./cmd/server
COPY internal ./internal
COPY migrations ./migrations
# CGO off: a static binary that runs on distroless/static. -trimpath keeps the
# build machine's paths out of the binary and makes the build reproducible.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/piperoom ./cmd/server

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
    OWNERS_FILE=/config/owners.yaml \
    USERS_FILE=/config/users.yaml

EXPOSE 8090

# distroless has no curl or wget, so the server binary is its own probe: it
# calls GET /healthz on PIPEROOM_HTTP_ADDR (loopback when that is 0.0.0.0).
# /healthz is liveness only; GET /readyz also checks the room database.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/piperoom", "--healthcheck"]

# Inside a container the server must listen on 0.0.0.0, or Docker's port
# mapping cannot reach it. The server refuses any non-loopback address unless it
# is started with --allow-remote, because its only login today is the X-User
# header stub: anyone who can reach the port can act as any user. So do not
# publish this port to the internet. Run it behind the customer's VPN or an
# authenticating reverse proxy (see deploy/README.md).
#
# --allow-remote is in ENTRYPOINT, not CMD, so a command override such as
# "--demo" still starts a server that Docker's port mapping can reach.
USER nonroot:nonroot
ENTRYPOINT ["/piperoom", "--allow-remote"]
