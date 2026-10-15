# Failure scenarios (observed on the dev stack, 2026-10-11/12; G17–G19 and G3 on 2026-10-15)

Start clean with `go run ./cmd/devctl reset`. Use `devctl status` to watch, and `devctl load --rate 20`
in a second terminal to create traffic (needed to grow WAL or lag).

Everything below was **observed**, not guessed. If you see something different, update this file.
Versions: Postgres **17.11** (`postgres:17.11`, pinned in `dev/docker-compose.yml`), Debezium 3.2 Postgres
connector, Kafka 4.1 broker. Known problems of the stack itself are in [Known issues](#known-issues) at the end.

## What `reset` keeps: the room database (QA G19)

The Piperoom room service keeps rooms, events, approvals and sessions in its own Postgres service,
`roomdb` (`localhost:5433`, db `piperoom`, user `piperoom` / `piperoom-dev`; the `PIPEROOM_DB_URL` default).
It is not part of the pipeline: no scenario touches it, and its data lives on the named volume
`piperoom-dev_roomdb`.

| Command | Pipeline data (orders, WAL, slot, topics, offsets, schemas, warehouse) | Rooms (`roomdb`) |
|---|---|---|
| `devctl down` | kept | kept |
| `devctl reset` (and `devctl demo`) | **deleted**: the pipeline containers are removed with their volumes | kept |
| `devctl reset --rooms --yes` | deleted | **deleted** (`docker compose down -v`); `--rooms` without `--yes` is refused |
| `docker compose -f dev/docker-compose.yml down -v` by hand | deleted | **deleted** |

Before 2026-10-15 the room database was a second database (`piperoom`) inside the pipeline's Postgres on
`:5432`, so every `devctl reset` deleted all rooms. **If you still have rooms there**, copy them before your
next reset. `devctl up` with the new compose file starts `roomdb` and keeps the old Postgres volume, then:

```
go run ./cmd/devctl up --owner <tag>
docker exec -e PGPASSWORD=piperoom-dev piperoom-dev-roomdb-1 sh -c "pg_dump -h postgres -U piperoom -d piperoom --clean --if-exists --no-owner | psql -q -v ON_ERROR_STOP=1 -U piperoom -d piperoom >/dev/null"
docker exec piperoom-dev-postgres-1 dropdb -U piperoom piperoom   # optional: remove the old copy
```

The dump runs inside the `roomdb` container (it reaches the old database as `postgres:5432` on the compose
network), so the same line works in bash and PowerShell and nothing binary crosses a host pipe. `--clean`
lets it overwrite a `roomdb` that a server already migrated. Point a server you started by hand at the new
place: unset `PIPEROOM_DB_URL` (the default is `:5433`) or set
`PIPEROOM_DB_URL=postgres://piperoom:piperoom-dev@localhost:5433/piperoom?sslmode=disable`.

## Connect worker log on the host (QA G3)

Kafka Connect reports a task `RUNNING` while Debezium retries an error it treats as retriable
(`connector-auth`, and the decode error of [G17](#g17-schema-change-fix-loops-on-compressed-lz4-data-is-corrupt)),
so the real error is only in the worker log. The Connect container runs its normal start command
(`/docker-entrypoint.sh start`) under `dev/connect/worker-log.sh`, which copies everything the worker prints to
**`dev/.logs/connect/connect.log`** on the host (gitignored). That is the default `CONNECT_WORKER_LOG_FILE`, so
the server's `connect_worker_log` tool reads it with no setup.

- Lines are copied unchanged: the Debezium image's log4j layout
  (`2026-10-15 00:22:16,938 ERROR  Postgres|pg|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]`)
  with its timestamps, followed by the stack trace lines. `docker logs piperoom-dev-connect-1` shows the same.
- Size cap: on every container start the previous run's log moves to `connect.log.1`; while it runs, the file
  rotates to `connect.log.1` each time it passes 10 MiB (`WORKER_LOG_MAX_BYTES`). At most about 20 MiB in all.
- `devctl up` creates `dev/.logs/connect` writable for everyone, because the image's non-root user must write it
  on a Linux host. If you start the stack with plain `docker compose up` on Linux and the directory is not
  writable, Connect still starts; the script prints `worker-log: cannot write ...` and the log is only in
  `docker logs`.
- `devctl fix schema-change` reads this file to say why a task that shows `RUNNING` does not recover.

## Heartbeat: the slot advances on a quiet table (QA G18)

QA saw `devctl health` report `postgres FAIL retained 132.7 MB` on an idle stack: Debezium only confirms WAL
when it gets a change of a table it captures, `public.orders` was quiet for days, and the shared Postgres kept
writing (the room database lived there then). The dev connector now has a heartbeat:

- `dev/postgres-init/04-cdc.sql` creates `debezium.heartbeat` (one row) and puts it in `dbz_publication` next to
  `public.orders`.
- The connector config sets `heartbeat.interval.ms=10000` and
  `heartbeat.action.query=UPDATE debezium.heartbeat SET beat_at = now() WHERE id = 1`. Every 10 s the update
  reaches the slot as a change of a published table, Debezium processes it (it is not in `table.include.list`,
  so no topic is written for it, only the usual `__debezium-heartbeat.pg` records) and confirms its LSN at the
  next offset flush (Connect's default, 60 s).

Observed 2026-10-15 right after `devctl reset`, with **no** orders traffic (100 rows, newest `updated_at` from the
seed) while another database on the same server wrote about 1.4 MB of WAL every 15 s:

```
00:42:38 restart_lsn=0/1DAC650 confirmed=0/1DAC688 current=0/1F34A98 retained=1569 kB
00:43:08 restart_lsn=0/1DB2FF8 confirmed=0/1F383D8 current=0/21DA140 retained=4252 kB
00:44:09 restart_lsn=0/21E7200 confirmed=0/24890A8 current=0/272AE70 retained=5391 kB
00:45:10 restart_lsn=0/25D9F98 confirmed=0/29D6D50 current=0/2C78AE0 retained=6779 kB
```

`confirmed_flush_lsn` moves at every offset flush (60 s) and `restart_lsn` follows one step behind, so retained
WAL saw-tooths at about one minute of the server's writes instead of growing for ever. `debezium.heartbeat.beat_at`
shows the last beat. `long-transaction` still shows its symptom with the heartbeat on (observed 2026-10-15,
50-row inserts every 25 s): retained WAL grew 44 kB → 172 kB in two minutes while the transaction was open,
`restart_lsn` moved only 26 kB in that time, and after `devctl fix long-transaction` retained fell to 10 kB.

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

## Health check: `devctl health`

One read-only command that says whether the pipeline is healthy. It takes no lock, changes nothing,
and checks five layers concurrently. Each layer has its own 15 s timeout; a layer that errors or
times out is reported as FAIL and the other layers still run.

```
go run ./cmd/devctl health
```

Exit codes: **0** when every layer is OK; **1** when any layer FAILs (the report names the layer and its
owner from `dev/owners.yaml`); **2** when the check could not run at all (not inside the repo,
`dev/owners.yaml` unreadable or missing the `orders` pipeline, bad arguments) or when Ctrl+C cuts the
check short (a cancel is not an unhealthy pipeline). `devctl demo` prints the health report before the
break. After `--fix` it re-runs the check every 5 s for about 90 s, because the backlog drains after the
fix returns, and then prints the settled report. The window is checked between runs, so the last run can
finish a few seconds past 90 s.

Each layer:

| Layer (owner) | Check | FAIL when |
|---|---|---|
| `postgres` (priya) | slot `debezium_orders` | missing, `active=false`, or retained WAL > **100 MB** |
| `kafka-connect` (arjun) | connector `pg-orders-connector` and its tasks (Connect REST) | connector not `RUNNING`, no tasks, or any task not `RUNNING` |
| `kafka` (arjun) | topic `pg.public.orders` | the oldest source change not yet in the topic has waited **> 60 s** |
| `consumer` (kabir) | group `order-sync` (from `dev/owners.yaml`) | state not `Stable`, no members, or total lag **> 100** |
| `warehouse` (meera) | `analytics.orders` | the oldest source change not yet loaded has waited **> 60 s** |

Notes on the thresholds:
- The topic and warehouse layers measure the **age of the oldest change that has not arrived yet**
  (`updated_at` of the source rows newer than the newest topic record / warehouse row), not the age of
  the newest row. So an idle stack is OK, and a burst that never gets through still FAILs after 60 s.
  The age is computed on the source database clock; the topic line also prints the age of its newest
  record (wall clock, for humans only).
- Precision: the warehouse comparison uses exact microseconds (a row is pending only if it is strictly
  newer than the newest loaded row). Kafka record timestamps are milliseconds, so for the topic a row in
  the same millisecond as the newest record counts as delivered.
- Kafka's newest record is read at each partition's last offset. A topic whose last offset is a
  transaction marker never answers and shows as `no answer within 15s`.
- The retained-WAL limit is 100 MB; the observed demo values (26 kB healthy, about 1–2 MB broken) are far
  below it, so a slot that is active but not moving (`long-transaction`) is **not** flagged. Watch
  `restart_lsn` with `devctl status` for that case.

Output format (from the unit-test fakes; not yet checked against the live stack):

```
OK   postgres      slot debezium_orders: active=true, retained 26 kB (limit 100.0 MB)
OK   kafka-connect pg-orders-connector: RUNNING, tasks 0=RUNNING
OK   kafka         topic pg.public.orders: newest record 4 s old, oldest unsent change 0 s (limit 60 s)
FAIL consumer      order-sync: state Empty, members 0, total lag 1471 (limit 100)
OK   warehouse     analytics.orders: newest updated_at 08:04:12 UTC, oldest unloaded change 0 s (limit 60 s)
pipeline UNHEALTHY: consumer (kabir)
```

Expected health per scenario, derived from the symptoms above (**not yet verified with `devctl health`**):

| Scenario | Layers that should FAIL | Layers that stay OK |
|---|---|---|
| `schema-change` | kafka-connect (task 0 FAILED), postgres (slot inactive); kafka and warehouse once 60 s pass | consumer |
| `connector-down` (PAUSED) | kafka-connect; kafka and warehouse after 60 s | postgres (slot still active), consumer |
| `consumer-stuck` | consumer; warehouse after 60 s | kafka-connect, postgres, kafka |
| `consumer-lag` | consumer (no members, lag); warehouse after 60 s | kafka-connect, postgres, kafka |
| `slot-inactive` | kafka-connect (STOPPED); postgres; kafka and warehouse after 60 s | consumer |
| `connector-auth` | postgres (slot inactive); kafka and warehouse after 60 s | kafka-connect (task stays RUNNING, see §7), consumer |
| `long-transaction` | nothing until retained WAL passes 100 MB | all |

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
It is idempotent: when the migrated schema is already registered and task 0 is RUNNING (the agent fixed it
first, also with another `decimal.handling.mode`, e.g. `double`), it skips the wait and the restart, and it
still ends at `BACKWARD`. A connector left on `publication.autocreate.mode=filtered` is first switched to
`disabled` (see [G17](#g17-schema-change-fix-loops-on-compressed-lz4-data-is-corrupt)). If the task does not
recover, the fix says why (task state, newest error in the worker log) instead of a bare timeout.
The consumer reads both versions (int and decimal); missed rows reach the warehouse. No data loss.

Observed 2026-10-15 with the new connector config, each run after `devctl reset`: a 10 s `load --rate 20`
burst right before the break (the recipe that looped on G17 before), `load` running through break and fix, and
a connector put back on `filtered` first (the fix switched it to `disabled`). Every run: task 0 `FAILED` on the
409, fix done in 2–3 s, both post-ALTER rows in the warehouse, no decode error in the worker log, health 5/5.

**Agent fix with `decimal.handling.mode=double`** (QA G5): the new schema (amount `double`) is BACKWARD
compatible, so it registers and the connector streams; `devctl fix` then only restores `BACKWARD`. But
**devconsumer crash-loops** on it (`devconsumer: amount: want int or decimal, got float64`): that fix moves the
outage to the consumer layer, which is why it needs the consumer owner's approval too.

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
Retained WAL grows with load. **Fix:** the agent proposes `connect_resume_connector`, which runs only
after the kafka-connect owner approves. (Manual equivalent: `PUT …/resume`.) Missed changes flow; no data loss.

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

---

## 5. `slot-inactive` — stopped connector, WAL piling up

**Break:** `PUT /connectors/pg-orders-connector/stop` (Connect REST `stop`). `pause` is *not* enough:
a paused connector keeps its replication connection, so the slot stays active (see §2).

**Symptoms** (observed 2026-10-12 with `load --rate 20` running; before the break the status showed
`slot debezium_orders active=true retained=965 kB`). `devctl break slot-inactive` logs:
```
level=INFO msg="broken: connector STOPPED, slot debezium_orders inactive — run `devctl load` to grow retained WAL further" retained_before_bytes=1089824 retained_after_bytes=1129008
```
(the two byte counts are its own proof: one 100-row INSERT while stopped; the base is larger than the
965 kB above because `load` kept writing in between). After ~40 s of load:
```
connector   pg-orders-connector: STOPPED, tasks                 ← stopped connector has no tasks
slot        debezium_orders active=false retained=1332 kB      ← slot INACTIVE; grows with every commit
consumer    order-sync: state Stable, members 1, total lag 0   ← Kafka side healthy: problem is upstream
warehouse   analytics.orders: 648 rows, newest updated_at …, stale 59 s
```
`pg_stat_replication` is empty: nothing confirms WAL, `restart_lsn` stops moving, and retained
(`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`) grows with the load.

**Fix:** the agent proposes `connect_resume_connector`, which runs only after the kafka-connect owner
approves (manual equivalent: `devctl fix slot-inactive`, i.e. `PUT …/resume`). Debezium keeps the slot and its offsets
(no re-snapshot) and replays the retained WAL: slot back `active=true`, retained fell 1332 kB → 804 kB
right after resume and order-sync lag drained to 0; the warehouse caught up (648 → 1725 rows, stale 1 s).
No data loss.

---

## 6. `consumer-lag` — stopped consumer, lag grows

**Break:** `docker compose stop devconsumer` (the `order-sync` group leaves; `docker ps` shows
`piperoom-dev-devconsumer-1 Exited (0)`), while `load --rate 20` keeps writing. Before the break:
`consumer order-sync: state Stable, members 1, total lag 0` and a fresh warehouse.

**Symptoms** (observed 2026-10-12). `devctl break consumer-lag` logs
`total_lag=204` from its own 50-row proof INSERT; after ~45 s of load:
```
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING   ← upstream healthy (unlike §2/§5)
slot        debezium_orders active=true retained=417 kB
consumer    order-sync: state Empty, members 0, total lag 1471  ← grows with load
warehouse   analytics.orders: 3495 rows, newest updated_at …, stale 78 s  ← nothing consumes
```

**Fix (`devctl fix consumer-lag`):** `docker compose start devconsumer`. It rejoins the group,
lag 1471 → 0, warehouse catches up (3495 → 5295 rows, stale 0 s).
Do **not** use `docker compose up -d devconsumer` here: `up` recreates any container that does not
match the compose file (we hit this once — it recreated Postgres and killed the running `devctl load`).
`compose start` only starts the stopped container.

---

## 7. `connector-auth` — permission denied on the publication (matches C7 `publication-permission-denied`)

A real Postgres permission break, aligned with the C7 eval scenario
`evals/scenarios/publication-permission-denied.yaml` (same symptom wording).

**Break (`devctl break connector-auth`):** a DBA moves `public.orders` ownership to a dedicated
role (`orders_owner`), and the connector is pointed at the least-privilege role `debezium`
(non-superuser) with a Debezium-managed publication (`publication.autocreate.mode=filtered`; the dev
config itself uses `disabled`, see [G17](#g17-schema-change-fix-loops-on-compressed-lz4-data-is-corrupt)).
Concretely, `break` runs:

```
CREATE ROLE debezium LOGIN REPLICATION PASSWORD 'piperoom-dev'   (if absent)
CREATE ROLE orders_owner LOGIN                                   (if absent)
GRANT CONNECT ON DATABASE orders TO debezium
GRANT USAGE ON SCHEMA public TO debezium
GRANT SELECT ON public.orders TO debezium
ALTER PUBLICATION dbz_publication OWNER TO debezium
ALTER TABLE public.orders OWNER TO orders_owner
```
then re-registers the connector with `database.user=debezium` and `publication.autocreate.mode=filtered`
and restarts task 0.

**Root cause:** with `publication.autocreate.mode=filtered`, Debezium 3.2 runs
`ALTER PUBLICATION dbz_publication SET TABLE "public"."orders"` on every task start, which requires
owning the table. `debezium` owns the publication but not the table, so the task fails with
`must be owner of table orders`. The superuser `piperoom` would bypass that check, which is why the
connector must be switched to the non-superuser `debezium` for the break to bite.

**Real error text** (observed in `docker logs piperoom-dev-connect-1` and, from 2026-10-15, in the host file
`dev/.logs/connect/connect.log`; repeated on every retry):
```
org.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication dbz_publication for "public"."orders"
Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders
```
This is exactly the trace C7's `connect_status` recording shows.

**Symptoms** (observed 2026-10-12). Note Debezium treats the failure as *retriable*, so Kafka
Connect still reports `tasks 0=RUNNING` — but the task crash-loops and streaming never starts:
```
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING   ← crash-looping; the error is in the connect log
slot        debezium_orders active=false retained=6906 kB   ← slot INACTIVE (never starts streaming)
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   analytics.orders: stale                          ← no new records flow
```

**Fix (`devctl fix connector-auth`):** give ownership back and restart the task:
`ALTER TABLE public.orders OWNER TO piperoom`, `ALTER PUBLICATION dbz_publication OWNER TO piperoom`,
run `dev/postgres-init/04-cdc.sql` again (a filtered-mode start that succeeded would have dropped the heartbeat
table from the publication), re-register the dev connector config (`database.user=piperoom`,
`publication.autocreate.mode=disabled`), restart task 0. The slot goes back
`active=true` and the next insert reaches the warehouse (verified: 103 → 104 rows, stale 9 s).

Observed 2026-10-15 with the worker log on the host: right after the break, Connect showed `tasks 0=RUNNING`
while `dev/.logs/connect/connect.log` had, per retry,
`[...] ERROR Producer failure (io.debezium.pipeline.ErrorHandler)` /
`ConnectException: Unable to update filtered publication dbz_publication for "public"."orders"` /
`Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders`. After the fix the connector
was back on `piperoom` / `disabled`, the publication held `public.orders, debezium.heartbeat`, and a new
order reached the warehouse.

---

## Known issues

### G17: schema-change fix loops on "compressed lz4 data is corrupt"

**What QA saw** (2026-10-15, Postgres 17.11, Debezium 3.2): after `devctl break schema-change` the task showed
`RUNNING` but looped on `ERROR: compressed lz4 data is corrupt` at a logical-decoding LSN; neither the agent's
fix (a config change, which restarts the task) nor `devctl fix schema-change` (timed out after 60 s) recovered
it; a direct `pg_logical_slot_peek_binary_changes` on the slot worked.

**Reproduced here** (same versions) with the old connector config: `devctl reset`, `devctl load --rate 20` for
10 s, `devctl break schema-change` (task `FAILED` on the 409 as designed), `devctl fix schema-change`: the
restarted task stayed `RUNNING`, Postgres logged at every retry (every ~10 s)

```
ERROR:  compressed lz4 data is corrupt
STATEMENT:  START_REPLICATION SLOT "debezium_orders" LOGICAL 0/21FAF80 ("proto_version" '1', "publication_names" 'dbz_publication', "messages" 'true')
```

and the fix timed out after 60 s. The same break and fix with no traffic just before the break worked, which
is why it looked flaky.

**Cause.** Not compression, not a corrupt WAL, not specific to 17.11:

1. With `publication.autocreate.mode=filtered`, Debezium runs `ALTER PUBLICATION dbz_publication SET TABLE
   "public"."orders"` **on its replication connection** (`replication=database`) every time the task starts,
   just before `START_REPLICATION`. That statement opens `public.orders`, so the walsender process caches the
   table's **current** row type (amount `numeric`, after the break).
2. Logical decoding then starts at the slot's `restart_lsn` and sends every transaction that commits after the
   connector's last confirmed position. The `INSERT`s and `UPDATE`s of the last offset flush (Connect commits
   offsets, and Debezium confirms the LSN, only every 60 s) were written **before** the `ALTER`, with amount as
   a 4-byte `integer`. The walsender decodes them with the cached `numeric` row type.
3. Read as a varlena, the int's low byte is the length header. When `amount % 4 == 2` the header says
   "4-byte header, compressed inline", and the next 4 bytes (the start of the `status` text, for example
   `\x0f p l a` for `placed`) supply the compression method bits. Lowercase ASCII gives method `01` = lz4,
   so Postgres tries to decompress and fails with exactly `compressed lz4 data is corrupt`. Other amounts give
   garbage numerics instead of an error. Debezium treats the `PSQLException` as retriable, restarts, runs the
   `ALTER PUBLICATION` again, and loops.
4. The SQL functions (`pg_logical_slot_peek_*`) do not have the problem: a session that first read the table
   and then peeked the same slot still decoded every row correctly (they evidently reset the session's caches
   first). That is why QA's direct peek worked.

The slot's `restart_lsn` age does not matter by itself. What matters is whether changes written before the
`ALTER` come after the last confirmed position. On a slot that stood still (G18, no heartbeat) that is every
change since the last confirmation, so the break is certain to bite. On a fresh slot with no recent traffic,
there are none.

Evidence, with a throwaway walsender client (`pgconn`, `replication=database`) on a scratch table (20
`integer` rows with amounts 1001–1020, then `ALTER COLUMN amount TYPE numeric(12,2)`, then one more row):

| Before `START_REPLICATION` the session ran | Result on 16.15, 17.11 and 18.6 |
|---|---|
| nothing | all 21 rows decoded correctly |
| `SELECT pg_publication` only | correct |
| `SELECT count(*) FROM the table` | row 1 decoded with a garbage numeric, row 2 (amount 1002) `ERROR: compressed lz4 data is corrupt (XX001)` |
| `SELECT * FROM pg_publication_tables` (it opens each table) | same error |
| `ALTER PUBLICATION ... SET TABLE` (what Debezium does) | same error |

`default_toast_compression` is `pglz`, `wal_compression` is `off`, and no column of `orders` has a compression
method set, so no lz4 data exists in the database at all. On the broken stack, switching the connector to
`publication.autocreate.mode=disabled` (one config PUT, which restarts the task) decoded past the `ALTER`
right away: schema version 2 registered and the warehouse caught up.

**Fix in the dev stack.**

- The publication is created by `dev/postgres-init/04-cdc.sql`, and the dev connector runs with
  `publication.autocreate.mode=disabled`, so Debezium sends no SQL that touches `orders` on the replication
  connection. Restarts are safe after any DDL, including the agent's own fixes.
- `connector-auth` still needs the filtered mode for its error, so that break switches the connector to
  `filtered` (with the `debezium` role) and its fix registers the dev config again.
- `devctl fix schema-change`:
  - It moves a connector still on `filtered` (a stack registered before this change) to `disabled`, keeping its
    other keys, before it restarts the task.
  - It counts any non-`int` amount as "migrated schema registered" (an agent fix with
    `decimal.handling.mode=double` used to make it wait 60 s for a decimal version that never came).
  - It waits up to 90 s for `RUNNING` and 3 min for the new schema, logs what it sees every 30 s, and stops at
    once when the worker log shows the decode error. On failure it names the task state and the newest
    worker-log error.
- `devctl break schema-change` warns when the connector is not on `disabled`.

**For real pipelines** (not only the dev stack): a Debezium Postgres connector in `filtered` or `all_tables`
mode can hit this after any column type change, whenever it restarts with changes written before the DDL not
yet confirmed. Restarting cannot get it out. Switching to `disabled`, with a publication managed by the DBA,
does.

### G18 and G19

Fixed in the stack (heartbeat, separate `roomdb`), see the sections at the top.
