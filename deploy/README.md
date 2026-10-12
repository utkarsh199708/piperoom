# Run Piperoom with Docker (pilot)

One image has the server and the web UI. One compose file starts it with its own Postgres.
You need Docker with Compose v2, and network access from the Docker host to your source
database, Kafka, Kafka Connect, Schema Registry, the warehouse, and `api.anthropic.com`.

## Read this first: network exposure

Piperoom has **no real login yet**. A user is whoever the `X-User` header says. Anyone who can
reach the port can act as any user, **including approving fixes**. So:

- Run it behind the customer's **VPN** or an **authenticating reverse proxy**. Never publish
  it to the internet.
- A proxy that does the authenticating must control **both** places where the server reads
  the identity: the `X-User` header (set it itself, strip any client value) **and** the
  `?user=` query parameter on the WebSocket route `/api/rooms/{id}/ws`, because browsers
  cannot send headers on a WebSocket. Force `user` to the authenticated user there, or
  reject requests whose `user` differs. Otherwise a logged-in person can watch a room
  as someone else.
- Inside the container the server must listen on `0.0.0.0`, so the image starts it with
  `--allow-remote` (the server refuses non-loopback addresses without it).
- The compose file publishes the port on `127.0.0.1` only. To let other machines reach it, set
  `PIPEROOM_BIND` in `deploy/.env` to the host's VPN address (better) or `0.0.0.0`.

More limits: [docs/pilot/README.md](../docs/pilot/README.md#read-this-first-pilot-limits).

## The 10-minute path

Run these from the repository root.

1. **Create the read-only accounts** that Piperoom uses on your systems. Follow
   [docs/pilot/permissions.md](../docs/pilot/permissions.md) and grant exactly what it lists:
   a read-only role on the source Postgres and on the warehouse, Kafka describe/read,
   Connect and Registry `GET` (write access only if you want fixes to run; read-only is a
   valid, safe pilot mode). Use a staging or replica pipeline first.

2. **Copy the settings file and fill it in.**

   ```sh
   cp deploy/pilot.env.example deploy/.env
   ```

   In `deploy/.env` set at least:

   | Setting | What |
   |---|---|
   | `ANTHROPIC_API_KEY` | the agent's API key (`ANTHROPIC_WORKSPACE_ID` too, for a user key) |
   | `PIPEROOM_POSTGRES_PASSWORD` | the bundled Postgres password. Generate it with `openssl rand -hex 32`. It goes into a URL unescaped, so it must not contain `/ @ : # ? % $` |
   | `SOURCE_DB_URL`, `WAREHOUSE_DB_URL` | the read-only connection URLs |
   | `KAFKA_BROKERS`, `CONNECT_URL`, `SCHEMA_REGISTRY_URL` | your hosts. `localhost` inside the container is the container itself, so use real host names |

   Add Kafka/Registry/Connect logins and TLS in sections 5 to 7 of the file if your clusters
   need them. Put certificate files in `deploy/certs/` and refer to them as `/certs/<file>`.
   `deploy/.env` holds secrets and is git-ignored.

3. **Say who owns what.** Copy the two examples and edit them (users, layers, your tables):

   ```sh
   cp deploy/config/owners.example.yaml deploy/config/owners.yaml
   cp deploy/config/users.example.yaml  deploy/config/users.yaml
   ```

   The guide is [docs/pilot/setup.md](../docs/pilot/setup.md#2-describe-your-pipeline-ownersyaml-and-usersyaml).
   The server refuses to start if the two files disagree; the error names the user and layer.

4. **Start it.**

   ```sh
   docker compose -f deploy/docker-compose.pilot.yml up
   ```

   The first run builds the image (a few minutes) and starts Postgres, which stores Piperoom's
   own room log in the `piperoom-pgdata` volume. Database migrations run by themselves.
   Add `-d` to run in the background.

5. **Open it:** `http://<host>:8090` (`http://localhost:8090` on the same machine; with the
   default `PIPEROOM_BIND` that is the only place it works). Pick a user from `users.yaml`.

## Check that it works

```sh
docker compose -f deploy/docker-compose.pilot.yml ps              # piperoom should be "healthy"
curl http://localhost:8090/healthz                                # {"status":"ok"}
curl http://localhost:8090/readyz                                 # also checks the room database
curl -H 'X-User: alice' http://localhost:8090/api/me              # your user, from users.yaml
docker compose -f deploy/docker-compose.pilot.yml logs -f piperoom
```

Then follow [docs/pilot/setup.md](../docs/pilot/setup.md#5-verify-with-a-dry-run-no-real-alert-needed)
(dry run, read-only check, approval gate) and point your alerts at
`POST /api/alerts/webhook` ([step 6](../docs/pilot/setup.md#6-wire-up-alerts)).

## Try the UI with no stack (demo mode)

No database, keys or data stack needed. Scripted agent, in-memory, nothing is saved:

```sh
docker compose -f deploy/docker-compose.pilot.yml run --rm --no-deps --service-ports piperoom --demo
```

`--no-deps` keeps the bundled Postgres from starting; the image already adds `--allow-remote`.
Demo mode still needs:

- `deploy/.env` with `PIPEROOM_POSTGRES_PASSWORD` set to any value (compose refuses to read
  the file without it, even though demo mode never connects to the database);
- `deploy/config/owners.yaml` and `deploy/config/users.yaml` (step 3). They must agree. If
  `users.yaml` is missing, demo mode uses built-in demo users, which only fit the dev
  `owners.yaml`, not yours.

It needs no API key and no data stack. Do not use demo mode for a real incident.

## What is in the image

| | |
|---|---|
| Build | `Dockerfile` at the repo root: Node 24 builds `web/dist`, Go 1.27 builds `cmd/server` (`CGO_ENABLED=0`, `-trimpath`), then both go into `gcr.io/distroless/static-debian13:nonroot` |
| Size | about 40 MB |
| Runs as | uid 65532 (`nonroot`), read-only root filesystem in compose, all capabilities dropped |
| Listens on | `0.0.0.0:8090` (`PIPEROOM_HTTP_ADDR`); the UI is served from `/web` (`PIPEROOM_WEB_DIR`) at `/`, the API at `/api` |
| Health | `HEALTHCHECK` runs `/piperoom --healthcheck`, which calls `GET /healthz` (distroless has no curl). `GET /readyz` also checks the room database, and suits a load balancer or Kubernetes readiness probe |
| Browser rules | The UI calls the API and WebSocket on its own origin, so no CORS setup is needed (CORS and the `localhost:5173` dev origins are off when the server serves the UI). The server sends a CSP, `X-Content-Type-Options`, `Referrer-Policy` and `frame-ancestors 'none'` |

Build the image alone: `docker build -t piperoom:pilot .`

Behind a reverse proxy that changes the `Host` header, set `PIPEROOM_PUBLIC_URL` (for example
`https://piperoom.corp.example`) so WebSocket connections from that address are accepted.
It must be a plain `http(s)` URL with a host name or IPv4 address (no wildcard, user name or
query); anything else stops the server at start-up with an error that names the variable.
The proxy must pass WebSocket upgrades on `/api/rooms/<id>/ws`.

## Day-2 operations

```sh
docker compose -f deploy/docker-compose.pilot.yml up -d --build   # after a git pull
docker compose -f deploy/docker-compose.pilot.yml down            # stop; data stays in the volume
docker compose -f deploy/docker-compose.pilot.yml down -v         # stop AND delete the room database
```

On a restart the server rebuilds every room from its log and starts a new agent for each open
room. A fix that was approved but not finished is marked failed, not re-run
([setup.md](../docs/pilot/setup.md#what-happens-on-a-restart)).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `set PIPEROOM_POSTGRES_PASSWORD in deploy/.env` | step 2 not done, or `deploy/.env` is missing |
| `env file .../deploy/.env not found` | run `cp deploy/pilot.env.example deploy/.env` |
| Exits with `users/owners config` | `users.yaml` and `owners.yaml` disagree about a layer owner |
| Exits with `load users file` or `load owners file` | `deploy/config/users.yaml` or `owners.yaml` is missing (step 3) |
| Exits with `PIPEROOM_PUBLIC_URL ... invalid host` (or similar) | the URL has a wildcard, a user name, a query or a bad port (see above) |
| Exits with `PIPEROOM_WEB_DIR ... has no index.html` | the image was built without the UI; rebuild with `up --build` |
| Exits with `open the data stack` | a URL or login in `deploy/.env` is wrong or unreachable from the container |
| Page does not load from another machine | `PIPEROOM_BIND` is still `127.0.0.1` |
| UI loads but live updates fail behind a proxy | the proxy does not pass WebSocket upgrades, or `PIPEROOM_PUBLIC_URL` is not set |
| No agent answer | `ANTHROPIC_API_KEY` is missing, or egress to `api.anthropic.com` is blocked |

## Not covered here

- Images are pinned by tag, not digest (`node:24-alpine`, `golang:1.27-alpine`,
  `distroless/static-debian13:nonroot`, `postgres:17`). Pin digests for a regulated site.
- The UI loads two fonts from Google Fonts. On a network with no internet the page still works
  with fallback fonts; the CSP allows only those two hosts for this.
- TLS: put a reverse proxy in front for HTTPS. The server speaks plain HTTP.
