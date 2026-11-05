# Quickstart (read-only, about 5 minutes)

You get a live room on your own pipelines. The agent investigates and proposes; nothing it proposes runs.
The full reference is [setup.md](setup.md). You do not need to read it first.

**You need:** Go 1.27, Node.js for the UI, a reachable Kafka Connect, and `ANTHROPIC_API_KEY`.
Set `CONNECT_URL`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `SOURCE_DB_URL` and `WAREHOUSE_DB_URL` to yours.
Unset, each one points at the local dev stack (`internal/config/config.go`: `Load`). The table at the top of
[setup.md](setup.md#which-variables-matter-first) says what each one does.

## 1. Write `owners.yaml` from the cluster

    go run ./cmd/discover -write-owners owners.yaml

- It reads Connect, Kafka and the Registry and never writes to them. Add `-source-db` to also read
  replication slots from `SOURCE_DB_URL` (`cmd/discover/main.go`).
- Every layer of every pipeline is owned by one user: your OS user name, or `-owner <id>`. `-users users.yaml`
  also writes a matching users file (`cmd/discover/main.go`, `cmd/discover/owner.go`: `defaultOwner`).
- It refuses to overwrite a file unless you pass `-force`. A file that fails `owners.Load` is written as
  `owners.yaml.draft` with the error on top, and the exit code is 1 (`cmd/discover/write.go`: `installOwners`).
- It prints one row per pipeline. Note the `PIPELINE` id. `sink_tables` is left empty: fill it in, with
  `sink_ts_column`, to turn on warehouse checks (`cmd/discover/summary.go`: `printTable`).

## 2. Start the server

    (cd web && npm ci && npm run build)
    PIPEROOM_WEB_DIR=web/dist go run ./cmd/server --quickstart

- It prints the URL to open, once (`cmd/server/main.go`: `listenAndServe`). Without `PIPEROOM_WEB_DIR` that URL
  serves the API only.
- Rooms live in memory and are gone when the server stops. Login is dev login, on loopback only: a
  `PIPEROOM_HTTP_ADDR` another host can reach is refused (`cmd/server/quickstart.go`: `checkQuickstartAddr`).
- `--owners <path>` picks the owners file (default `./owners.yaml`). If it names one user and `USERS_FILE` is
  unset, the server writes a one-user users file to a temp folder and removes it on stop (`quickstartUsers`).
- Write mode is off. `PIPEROOM_WRITE_MODE=on` stops the start (`quickstartConfig`).

`make quickstart` (runs both steps) is coming with P1c; until it lands, run the two commands above.

## 3. Check three things in the UI

1. **Send a test alert** with your user id and the `PIPELINE` id from step 1. A room opens and the agent starts
   ([setup.md §6](setup.md#6-wire-up-alerts)):

       curl -X POST http://127.0.0.1:8090/api/alerts/webhook -H 'Content-Type: application/json' \
         -H 'X-User: <your user id>' -d '{"title":"test","pipeline_id":"<PIPELINE>"}'

2. **The banner** under the room header reads "Fixes are suggestions until write mode is on"
   (`web/src/components/WriteModeBanner.tsx`).
3. **Approve a proposal.** The button reads "Approve (will not run)". Once every layer approved, the card says
   "Approved · waiting for write mode" and no `action.executed` is written (`web/src/lib/writeMode.ts`;
   `internal/room/writemode.go`). `GET /healthz` returns `"write_mode": "off"` (`internal/httpapi/health.go`).

## 4. The one switch to write mode

Write mode is one switch for the whole process: `PIPEROOM_WRITE_MODE=on` (`cmd/server/writemode.go`). Per-layer
write mode is not built. It needs the full server (no `--quickstart`) with its room database in `PIPEROOM_DB_URL`,
and write grants from [permissions.md](permissions.md). Why to start read-only: [decision.md §4](decision.md#4-recommendation-read-only-by-default-shadow-as-a-toggle-then-write-mode).
