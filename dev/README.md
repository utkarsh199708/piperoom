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

## Staging profile: a second Connect and Schema Registry (X8)

The `staging` compose profile adds the stack that X8 tries a fix on before production: `connect-staging` and
`schema-registry-staging`, on the **same Kafka**. A plain `devctl up` does not start them; nothing in the default
stack depends on them.

```
go run ./cmd/devctl up --staging --owner me        # the default stack, then staging, connector RUNNING on both
go run ./cmd/devctl down --staging --owner me      # only staging, plus what it left in Postgres and Kafka
go run ./cmd/devctl reset --staging --owner me     # wipes everything, staging included, and starts both again
                                                   # (without --staging, reset tears staging down and leaves it down)
```

(`docker compose -f dev/docker-compose.yml --profile staging up -d --build --wait` starts the two services
alone; `devctl up --staging` also registers the connector.) `devctl down` stops staging too. Reset never touches a
room database: it only runs `docker compose down -v` on this project and counts warehouse rows; the demo's room
database is another project (`demo-room-db.yml`).

| What | Production | Staging |
|---|---|---|
| Connect REST (host) | `http://localhost:8083` | `http://localhost:18083` (service `connect-staging`; inside the network `http://connect-staging:8083`) |
| Schema Registry (host) | `http://localhost:8081` | `http://localhost:18081` (service `schema-registry-staging`; inside the network `http://schema-registry-staging:8081`) |
| Connect `group.id` | `piperoom-connect` | `piperoom-connect-staging` |
| Connect config, offset, status topics | `_connect_configs`, `_connect_offsets`, `_connect_status` | `_connect_configs_staging`, `_connect_offsets_staging`, `_connect_status_staging` |
| Registry store topic and group | `_schemas`, `schema-registry` | `_schemas_staging`, `schema-registry-staging` |
| Connector | `pg-orders-connector` | `pg-orders-connector` (same name and config, except the three rows below) |
| Debezium `topic.prefix` (data topic) | `pg` (`pg.public.orders`) | `pgstg` (`pgstg.public.orders`) |
| Replication slot / publication | `debezium_orders` / `dbz_publication` | `debezium_orders_staging` / `dbz_publication_staging` |

The staging ports bind to `127.0.0.1` (like the Connect and registry APIs, they take no login). The two stacks
share Kafka and Postgres, and nothing else: with the same `group.id` the two workers would be one Connect cluster,
and under the same prefix the staging connector would write into the topics `devconsumer` reads, so the table
above is what keeps staging from touching production. `down --staging` deletes the staging connector, the two
containers, the staging slot and publication (an inactive slot would keep retaining WAL on the source) and the
staging topics; run twice, the second run changes nothing.

`devctl` reads `STAGING_CONNECT_URL` and `STAGING_SCHEMA_REGISTRY_URL` if you moved the ports; it refuses a value
equal to the production `CONNECT_URL` or `SCHEMA_REGISTRY_URL` (the owners file has the same rule).

**Owners file.** `owners.staging.yaml` is `owners.yaml` plus a `staging: {connections: {connect_url, schema_registry_url}}`
block on the `orders` pipeline (`docs/api.md`, "X8 staging"); start the server with
`OWNERS_FILE=dev/owners.staging.yaml` and write mode on. It needs the owners loader of X8a. The Go integration tests
of X8b use `testenv.RequireStaging(t)`, which skips when the profile is not up;
`go test -tags integration -run TestStagingProfile ./internal/testenv/` checks the profile itself against a running stack.

## Worker metrics and the `logs` profile (Q3d)

**Metrics, always on.** The `connect` image carries the Prometheus JMX exporter 1.0.1 (checksum-verified in
`dev/connect/Dockerfile`), loaded through `KAFKA_OPTS` with the rules in `dev/connect/jmx-exporter.yml`. It serves
`http://localhost:9404/metrics` (host loopback; no login): Debezium's `debezium_metrics_*` (snapshot, streaming,
`RowsScanned` per table) and Kafka Connect's `kafka_connect_*` (task errors, skipped records, DLQ produce failures,
task and connector startup failures, rebalance state, connector and task status). Only those families are exported:
the rules have no catch-all. A JVM you start with `docker exec` in the `connect` container inherits `KAFKA_OPTS`
and cannot bind 9404 again; give it `-e KAFKA_OPTS=`. After pulling this change, rebuild the image
(`devctl up` builds; or `docker compose -f dev/docker-compose.yml build connect`).

**Logs, profile `logs`.** Loki 3.5.7 and Promtail 3.5.7. Promtail finds the `connect` container (project
`piperoom-dev`, service `connect`; not `connect-staging`) through the read-only Docker socket, joins stack traces to
the line that logged them, labels each entry `service`, `container` and `level`, and pushes to Loki on
`http://localhost:3100` (host loopback; no auth). Storage lives in the container and goes with `docker compose
down`.

```
docker compose -f dev/docker-compose.yml --profile logs up -d --wait   # the default stack plus loki and promtail
docker compose -f dev/docker-compose.yml --profile logs down           # stops them too (a plain `down` leaves them)
```

Promtail turns healthy once it tails the Connect container, so `--wait` returning means logs flow. Server settings
for this stack:

| Variable | Value | Tools |
|---|---|---|
| `DEBEZIUM_METRICS_URL` | `http://localhost:9404/metrics` | `debezium_snapshot_progress`, `debezium_metrics`, `connect_worker_metrics` (Q3b) |
| `CONNECT_METRICS_PREFIX` | unset (default `kafka_connect_`) | `connect_worker_metrics` |
| `WORKER_LOGS_LOKI_URL` | `http://localhost:3100` | `connect_worker_logs` (Q3c) |
| `WORKER_LOGS_SELECTOR` | `{service="connect"}` | `connect_worker_logs` |

Quick checks: `curl -s localhost:9404/metrics | grep -c '^kafka_connect_'` (non-zero once the connector runs) and
`curl -sG localhost:3100/loki/api/v1/query_range --data-urlencode 'query={service="connect"}' --data-urlencode limit=5`.
**Not run yet:** written without Docker (2026-10-23); the compose file passes `docker compose config` and the rules,
the Promtail regexes and the compose structure are unit-tested (`go test ./cmd/devctl -run 'JMX|LogsProfile|ConnectJMX'`).
The rules were also run once in the real `jmx_prometheus_javaagent` 1.0.1 (same checksum) against stand-in Connect and
Debezium MBeans: names as `connect_worker_metrics` reads them, except that exporter 1.x drops a trailing `_total`
(`..._task_startup_failure`), which the tool accepts. Nobody has seen the profile come up healthy or the two tools
return data on the stack. The first live run should record
the time to healthy here (the row's target is under 2 minutes).

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
| `owners.staging.yaml` | `owners.yaml` plus the `staging:` block of the staging profile (above) |
| `owners.yaml`, `users.yaml` | the four-person dev directory (arjun, priya, kabir, meera) the full server uses by default |
| `demo.env`, `demo-room-db.yml` | the live demo profile and its separate room database (`docs/demo/schema-change.md`) |
| `docker-compose.yml`, `connect/`, `connectors/`, `postgres-init/` | the stack itself |
