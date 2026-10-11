# A1 — Dev stack, failure scenarios, sample consumer

Read `AGENTS.md` first. You own: `dev/**`, `cmd/devctl/**`, `cmd/devconsumer/**`, `internal/testenv/**`.

## Goal

Everyone else tests against your stack. Make it **one command to start**, **one command to
break**, **one command to fix**, and make every failure realistic.

Already there (you now own it): `dev/docker-compose.yml`, `dev/connect/Dockerfile`
(Debezium Connect 3.2 + Confluent Avro converter), `dev/postgres-init/01-orders.sql`.
It works: connector `pg-orders-connector` streams `public.orders` to topic `pg.public.orders`
with Avro schemas in Schema Registry (BACKWARD compatibility).

## 1. Databases (`dev/postgres-init/`)

Init scripts run once, on a fresh container. Add:

- Databases `warehouse` and `piperoom` (room service state, used by A5).
- In `orders`: schema `piperoom_audit`, table `piperoom_audit.ddl_log` and an **event trigger**
  on `ddl_command_end` that writes one row per DDL command. Columns must map 1:1 to
  `clients.DDLChange`:
  `at timestamptz default now(), command_tag text, object_identity text, query text, username text`.
  Use `pg_event_trigger_ddl_commands()` and `current_query()`. Ignore DDL on `piperoom_audit` itself.
- In `warehouse`: table `analytics.orders(id bigint primary key, customer_id bigint, amount numeric(12,2),
  status text, updated_at timestamptz, loaded_at timestamptz default now())`.

## 2. Config files in `dev/`

Write these exactly (A3 and A5 parse them):

`dev/owners.yaml`
```yaml
pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg   # Debezium topic.prefix; actions scope subjects to <prefix>.<table>-key|-value
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya   # source team owns its schemas (api.md v0.1)
  kafka: arjun
  consumer: kabir
  warehouse: meera
```
`dev/users.yaml` — exactly the example in `docs/api.md` → Auth.

## 3. `cmd/devconsumer` — the sample consumer service

A small Go service, consumer group `order-sync`, that reads `pg.public.orders` and upserts into
`warehouse.analytics.orders`.

- Decode the Confluent wire format: byte 0 = magic `0`, bytes 1–4 = schema ID (big-endian),
  rest = Avro body. Fetch the writer schema from Schema Registry by ID (`GET /schemas/ids/{id}`),
  cache it, decode with `github.com/hamba/avro/v2`.
- Handle the Debezium envelope (`before`, `after`, `op` = c/u/d/r). Delete → delete the row.
- `amount` must work **both** as Avro `int` and as Avro `bytes` with logical type `decimal`
  (that is what Debezium emits after the schema change).
- Commit offsets only after the warehouse write succeeds (at-least-once; upsert makes it idempotent).
- Flags: `--fail-on-id <n>` (crash when it sees that order id, to make a poison pill),
  `--delay <duration>` per record (to make lag).
- Runs as a container in the compose file (service `devconsumer`, built from this repo) **and**
  with `go run ./cmd/devconsumer` on the host.

## 4. `cmd/devctl` — one CLI for the stack (works on Windows; no bash scripts)

```
devctl up                 # compose up, wait healthy, register connector, start devconsumer
devctl down               # compose down
devctl reset              # down -v, up, re-seed — back to a clean known state
devctl load --rate 5      # insert/update orders N per second until Ctrl+C
devctl break <scenario>   # cause a failure
devctl fix <scenario>     # the correct manual fix (used to check the agent's proposal)
devctl status             # one screen: connector + task states, group lag, slot retained WAL, warehouse freshness
```
Use `os/exec` for `docker compose`. Talk to Connect / Schema Registry with plain `net/http`.
Keep `status` simple; it is for humans, not the agent.

## 5. Failure scenarios

Each scenario: `break` makes it happen in < 60 s; document in `dev/scenarios.md` the **exact**
symptoms you observed (copy real error text), the root cause, and the fix.

| Scenario | Break | Expected symptoms | Fix |
|---|---|---|---|
| `schema-change` (**the demo**) | `ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)`, then insert rows | Debezium task 0 FAILED with a Schema Registry incompatibility error; topic gets no new records; group lag 0; warehouse stops updating; slot retained WAL grows | set subject `pg.public.orders-value` compatibility to `NONE` (or register the new schema), restart task 0, consumer handles decimal |
| `connector-down` | pause or delete the connector while `load` runs | connector PAUSED/missing; slot inactive; retained WAL grows | resume / re-create |
| `consumer-stuck` | restart devconsumer with `--fail-on-id` of a new row | group lag grows; group Empty or members flapping; warehouse stale; Connect healthy | restart without the flag (or skip that offset) |
| `long-transaction` | open a transaction that updates a row and stays open for 10 min | slot `restart_lsn` stops moving; retained WAL grows; no data loss | terminate that backend pid |

Verify each scenario for real. If the symptoms differ from the table, **write what really
happens** — the agent is tuned on your notes.

## 6. `internal/testenv` — helpers for integration tests (build tag `integration`)

```go
func Require(t *testing.T) config.Config   // skips the test if the stack is not reachable
func Name(prefix string) string            // "it_<prefix>_<random6>"
func WaitFor(t *testing.T, timeout time.Duration, cond func() (bool, error))
```

## Done when

- `go run ./cmd/devctl reset` from a clean machine gives a working pipeline (rows reach the warehouse).
- All four scenarios break and fix reliably, three times in a row each.
- `dev/scenarios.md` has real observed error text for each.
- Unit tests for the wire-format and Debezium-envelope decoding (fixtures with real bytes captured from the stack).

## Watch out

- Postgres init scripts only run on an empty data directory → `reset` must remove volumes.
- Debezium's default `decimal.handling.mode=precise` → Avro `bytes` + `decimal` logical type. Do not change it.
- Windows paths and line endings: compose mounts must work from PowerShell.
