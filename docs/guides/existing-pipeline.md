# Connect Piperoom to an existing pipeline (read-only)

Already have a pipeline? Use the read-only quickstart: Piperoom investigates and proposes fixes, but
nothing executes. The step-by-step walk-through, with the three things to check in the UI, is
[`docs/pilot/quickstart.md`](../pilot/quickstart.md); this page is the summary and the owners-file reference.

## Requirements

You need Go 1.27 (plus Node 24 for the UI) or Docker, and your Kafka Connect, Kafka,
Schema Registry, source Postgres and warehouse, with the read-only roles of
[`docs/pilot/permissions.md`](../pilot/permissions.md). `discover` reads Connect, Kafka and the Registry. The server pings the
source Postgres and the warehouse at start and will not start if either is down; it connects to
Connect, Kafka and the Registry only when a tool first uses them. Point the standard variables at
them (`CONNECT_URL`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`);
unset, they default to the local dev stack. Set `ANTHROPIC_API_KEY` for the agent.

## Start

```sh
make quickstart                                          # Go: discover, build the UI, start
docker compose -f deploy/docker-compose.quickstart.yml up --build   # or Docker (host network)
```

`cmd/discover` writes `owners.yaml` with every layer owned by you (an existing file is kept;
`FORCE=1` rediscovers), then `server --quickstart` starts with in-memory rooms and dev login on
127.0.0.1 only, and prints the URL to open. The room says fixes are suggestions: approvals are
recorded, nothing runs. Send a test alert with the `curl` in [`docs/pilot/quickstart.md`](../pilot/quickstart.md).

Write mode is a separate, full-server setup: `PIPEROOM_WRITE_MODE=on` plus the Connect write
role in [`docs/pilot/permissions.md`](../pilot/permissions.md) (`--quickstart` refuses it); see
[`docs/pilot/setup.md`](../pilot/setup.md).

A batch pipeline (Airflow → dbt → warehouse) uses the same quickstart: see
[batch pipelines](batch-pipelines.md).

## Draft the owners file

`OWNERS_FILE` (default `dev/owners.yaml`) tells the server which pipelines exist and who
owns each layer; `USERS_FILE` (default `dev/users.yaml`) lists the user ids it may name.
For a new cluster, draft the owners file instead of writing it by hand:

```sh
go run ./cmd/discover -out owners.draft.yaml              # or: make discover-draft
go run ./cmd/discover -out owners.draft.yaml -source-db   # also check replication slots in SOURCE_DB_URL
go run ./cmd/discover -out -                              # print the draft to stdout
go run ./cmd/discover -out owners.draft.yaml -force       # overwrite an existing draft
```

`discover` only reads: Kafka Connect (`CONNECT_URL`), Kafka (`KAFKA_BROKERS`), the Schema
Registry (`SCHEMA_REGISTRY_URL`) and, with `-source-db`, the source Postgres
(`SOURCE_DB_URL`), with the credential and TLS settings from `internal/config/config.go`
(`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_TLS_*`, `CONNECT_*`,
`SCHEMA_REGISTRY_*`). It never writes to the cluster. `-timeout` (default `2m`) bounds the
whole run. Without `-force` it refuses to overwrite an existing `-out` file.

The draft has one pipeline per Debezium Postgres source connector, in the
`dev/owners.yaml` shape: `id`, `topic_prefix`, `source_tables`, `connectors` and
`consumer_groups` are filled from the cluster; notes (e.g. "source_tables guessed from
existing topics", "slot … inactive") sit in comments above each pipeline, and skipped
connectors and warnings are listed at the end. A human then fills in every `TODO`:

- `title` — a human-friendly name;
- `sink_tables` (warehouse `schema.table`s) and `sink_ts_column`;
- `source_tables` when the draft left them empty;
- each entry under `layers` — a user id from `USERS_FILE`.

Review it, then copy it into the file `OWNERS_FILE` points at by hand. `discover` never
writes `OWNERS_FILE` itself. Re-running it is covered in [`docs/runbook.md`](../runbook.md).

A MySQL source is `source_kind: mysql` on the pipeline in `owners.yaml`; see
[`docs/pilot/mysql-users.md`](../pilot/mysql-users.md).
