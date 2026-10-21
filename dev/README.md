# The dev stack

Postgres → Debezium (Kafka Connect) → Kafka → `devconsumer` → warehouse, plus the Schema Registry, all in
`dev/docker-compose.yml`. Addresses and test credentials are in `AGENTS.md` §3.

## Image versions

The stack's images can be swapped by setting environment variables (compose reads them at `docker compose`
time). With no variables set, the defaults below are used, so the stack is unchanged from before the
variables existed.

| Variable | Default | Used for |
|---|---|---|
| `POSTGRES_IMAGE` | `postgres:17` | the `postgres` service image |
| `KAFKA_IMAGE` | `apache/kafka:4.1.0` | the `kafka` service image |
| `SCHEMA_REGISTRY_IMAGE` | `confluentinc/cp-schema-registry:8.0.0` | the `schema-registry` service image |
| `DEBEZIUM_VERSION` | `3.2.0.Final` | the Debezium Connect base image the `connect` service builds from (`dev/connect/Dockerfile`) |

Example (PowerShell):

```
$env:POSTGRES_IMAGE = 'postgres:16'
$env:KAFKA_IMAGE = 'apache/kafka:4.0.0'
docker compose -f dev/docker-compose.yml up -d
```

POSIX shells: `POSTGRES_IMAGE=postgres:16 KAFKA_IMAGE=apache/kafka:4.0.0 docker compose -f dev/docker-compose.yml up -d`.

## Try this first: the read-only quickstart

A handful of commands from a clean clone to a room the real agent investigates. Nothing it proposes can run: the
quickstart forces write mode off, keeps rooms in memory and serves this machine only (loopback, dev login).

```
go run ./cmd/devctl lock --owner me          # the dev stack is shared: take the lock
go run ./cmd/devctl up --owner me            # 1-3 min the first time

mkdir -p /tmp/piperoom-qs                    # PowerShell: mkdir $env:TEMP\piperoom-qs
go run ./cmd/discover -write-owners /tmp/piperoom-qs/owners.yaml -owner alice

export ANTHROPIC_API_KEY=...                 # from your shell, never from a file; optional, see below
go run ./cmd/server --quickstart --owners /tmp/piperoom-qs/owners.yaml
```

`discover` reads Connect, Kafka and the Registry, finds the `pg-orders-connector` pipeline (id `pg-orders`) and
writes an owners file in which `alice` owns every layer (the top-level `layers:` block). It checks the file with
the server's own loader first; a file that would not load is written as `owners.yaml.draft` with the reason on
top. The server prints one line, the URL to open (`http://127.0.0.1:8090/` unless `PIPEROOM_HTTP_ADDR` says
otherwise), and writes a one-user users file for `alice` that is removed when it stops.

Send the alert in a second terminal (dev webhook, `X-User` is the `-owner` you gave discover):

```
curl -s -X POST http://127.0.0.1:8090/api/alerts/webhook \
  -H 'Content-Type: application/json' -H 'X-User: alice' \
  -d '{"title":"pg-orders-connector task 0 FAILED","severity":"sev2","pipeline_id":"pg-orders","dedup_key":"try-1"}'
```

The answer is the new room (`201`). Watch it with
`curl -s -H 'X-User: alice' http://127.0.0.1:8090/api/rooms/<id>/events`, or build the UI once
(`cd web && npm run build`) and start the server with `PIPEROOM_WEB_DIR=web/dist` to see it in the browser.

- **With `ANTHROPIC_API_KEY`** the agent calls the read-only tools (connector status, schema registry, slot,
  lag, warehouse) and posts a hypothesis. On a healthy pipeline there is little to find: break it first with
  `go run ./cmd/devctl break schema-change --owner me` (see `scenarios.md`), and it proposes a fix. Approving
  every layer records the approvals; the proposal then shows `blocked: "write_mode_off"` and no step runs.
- **Without a key** the room still opens; the agent reports `agent.status` `error` ("model request failed")
  and proposes nothing.

When you are done: Ctrl-C the server, `go run ./cmd/devctl fix schema-change --owner me` if you broke it,
`go run ./cmd/devctl unlock --owner me`.

The same path is tested end to end by `go test -tags integration -run TestQuickstart ./internal/testenv/`
(`internal/testenv/quickstart.go`). Without a key it checks the no-key behaviour above. With a key it also runs
`devctl break schema-change` and `devctl fix schema-change`, so hold the stack lock as `PIPEROOM_STACK_OWNER`
and start from a fresh stack (`devctl reset`): `break` needs `orders.amount` still `integer`.

## The dev-only break/fix API (`devctl serve`)

For the web "Break something" menu (A6) there is a tiny local HTTP API:

```
go run ./cmd/devctl serve -addr 127.0.0.1:7071 --owner me
```

It serves `GET /scenarios`, `POST /break/{name}` and `POST /fix/{name}` over JSON, one run at a
time (a second while busy gets `409`). Like every mutating command it requires the shared-stack
lock (`--owner <tag>` or `PIPEROOM_STACK_OWNER`), re-checked before each run; a caller who no
longer holds the lock gets `423`. It has **no authentication**, so it refuses to bind anything
but a loopback address (`127.0.0.1`, `::1`) and is never built into the image (the Dockerfile builds
only `cmd/server`). On top of the loopback bind it defends against CSRF and DNS rebinding: it
rejects any request whose `Host` is not a loopback host on the listen port, answers CORS only for
the web menu's origin (`-allow-origin`, default `http://localhost:5173`), and requires every POST
to carry a non-simple marker (`Content-Type: application/json` or `X-Piperoom-Dev: 1`) so a
cross-site request always needs a preflight that fails. Stop it with Ctrl-C.

## Everything else here

| File | What it is for |
|---|---|
| `scenarios.md` | the failure scenarios `devctl break` / `fix` create, as observed on this stack; the stack lock |
| `shadow.md`, `owners.shadow.yaml` | shadow mode: the agent investigates silently and you grade it |
| `owners.yaml`, `users.yaml` | the four-person dev directory (arjun, priya, kabir, meera) the full server uses by default |
| `demo.env`, `demo-room-db.yml` | the live demo profile and its separate room database (`docs/demo/schema-change.md`) |
| `docker-compose.yml`, `connect/`, `connectors/`, `postgres-init/` | the stack itself |
