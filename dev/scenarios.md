# Failure scenarios (observed on the dev stack, 2026-10-11)

Start clean with `go run ./cmd/devctl reset`. Use `devctl status` to watch, and `devctl load --rate 20`
in a second terminal to create traffic (needed to grow WAL or lag).

Everything below was **observed**, not guessed. If you see something different, update this file.

## Shared stack lock

The dev stack is shared by several agents, so every mutating command (`reset`, `break`, `fix`, `down`,
`up`) refuses to run unless you hold the lock file (`piperoom-bus/stack.lock`):

```
go run ./cmd/devctl lock --owner <your-tag>    # take the lock
go run ./cmd/devctl lock status                # who holds it
go run ./cmd/devctl reset --owner <your-tag>   # now allowed
go run ./cmd/devctl unlock --owner <your-tag>  # release when done
```

`--owner` may be replaced by the `PIPEROOM_STACK_OWNER` env var. If someone else holds the lock,
the command fails and names the holder — wait for them, or (only when the lock is older than 15 min,
e.g. a crashed run) take it with `go run ./cmd/devctl lock --steal` (it prints who it stole from).

Normal state:
```
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING
slot        debezium_orders active=true retained=26 kB
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   analytics.orders: 100 rows, newest updated_at …, stale <60 s
```

---

## 1. `schema-change` — the demo scenario

**Break:** `ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)`, then insert 2 rows.

**Root cause:** Debezium builds a new Avro value schema (`amount` becomes `bytes` + `decimal(12,2)`
instead of `int`). The subject `pg.public.orders-value` has compatibility `BACKWARD`; the new schema
is not backward compatible, Schema Registry rejects it (HTTP 409), and the connector task fails.

**Symptoms:**
```
connector   pg-orders-connector: RUNNING, tasks 0=FAILED
slot        debezium_orders active=false retained=213 kB      ← slot goes INACTIVE; grows with load
consumer    order-sync: state Stable, members 1, total lag 0  ← consumer is healthy: problem is upstream
warehouse   analytics.orders: stale; newest row is from before the ALTER
```
Task 0 trace (shortened — the real one embeds the full new and old schema JSON, ~3 KB each):
```
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
Caused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders :
Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema{…new schema…}
Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is
  incompatible with an earlier schema for subject "pg.public.orders-value", details: [{errorType:'MISSING_UNION_BRANCH',
  description:'The new schema is missing a type inside a union field at path '/fields/0/type/1' in the old schema',
  additionalInfo:'reader union lacking writer type: RECORD'}, {errorType:'MISSING_UNION_BRANCH', … '/fields/1/type/1' …},
  {oldSchemaVersion: 1}, {oldSchema: '{…}'}, {validateFields: 'false', compatibility: 'BACKWARD'}]; error code: 409
```
Note: the registry reports `MISSING_UNION_BRANCH` on `before`/`after`, **not** "amount changed type".
The field-level cause only shows by diffing schema versions (`amount: int → bytes decimal(12,2)`).

DDL audit row (`piperoom_audit.ddl_log`):
```
ALTER TABLE | public.orders | ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)
```

**Fix (`devctl fix schema-change`):** set subject compatibility to `NONE`, restart task 0, wait for the
new schema version to register (version 2 of the subject), set compatibility back to `BACKWARD`.
The consumer reads both versions (int and decimal); missed rows reach the warehouse. No data loss.

---

## 2. `connector-down`

**Break:** `PUT /connectors/pg-orders-connector/pause`.

**Symptoms:**
```
connector   pg-orders-connector: PAUSED, tasks 0=PAUSED
slot        debezium_orders active=true retained=330 kB     ← slot stays ACTIVE while paused (unlike FAILED)
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   stale
```
Retained WAL grows with load. **Fix:** `PUT …/resume`. Missed changes flow; no data loss.

---

## 3. `consumer-stuck` (poison pill)

**Break:** devconsumer restarts with `FAIL_ON_ID=<next order id>`, then that order is inserted.

**Symptoms:**
```
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING      ← Connect is healthy
consumer    order-sync: state Empty, members 0, total lag N    ← flaps Empty / PreparingRebalance while it crash-loops
warehouse   stale; newest row is the one before the poison pill
```
Container: `restarts=8 state=restarting` after ~40 s. Consumer log, repeated on every restart:
```
level=ERROR msg="devconsumer stopped" err="devconsumer: cannot process order 1648 at pg.public.orders/2@799 (poison pill)"
```
Lag grows only on the partition that holds the bad record; with load, total lag rises steadily.
**Fix:** restart devconsumer without the flag (real-world options: fix the consumer, or skip that offset).

---

## 4. `long-transaction` — the silent one

**Break:** a session (`application_name = piperoom-long-tx`) runs `BEGIN; UPDATE orders …;` and stays
open for 10 minutes.

**Symptoms:**
```
connector   RUNNING, tasks 0=RUNNING
slot        debezium_orders active=true, restart_lsn stuck (0/2362348 for the whole test), retained 625 kB → 1491 kB
consumer    Stable, lag 0
warehouse   fresh (stale 1 s) — new data still flows!
```
Nothing looks broken except `restart_lsn` not moving and retained WAL rising. `pg_stat_activity` shows the
open transaction (`xact_start` minutes ago, state `idle in transaction` or running `pg_sleep`).
**Fix:** `pg_terminate_backend(pid)`. Retained WAL dropped to 96 kB right away.
