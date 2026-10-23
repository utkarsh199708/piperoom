# Failure scenarios (observed on the dev stack, 2026-10-11/12)

Start clean with `go run ./cmd/devctl reset`. Use `devctl status` to watch, and `devctl load --rate 20`
in a second terminal to create traffic (needed to grow WAL or lag).

Everything below was **observed**, not guessed. If you see something different, update this file.

Shadow mode (the agent investigates a break silently, you grade it): `dev/shadow.md`.

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
| `missed-table` (§8) | nothing: the connector streams, only the new table's rows are missing | all |
| `dead-slot` (§9) | kafka-connect (STOPPED); postgres (slot inactive); kafka and warehouse after 60 s | consumer |
| `consumer-bad-deploy` (§10) | consumer (stopped, no members) | kafka-connect, postgres, kafka; warehouse until a new change waits > 60 s |
| `dlq-backlog` (§11) | nothing: lag stays 0, the rejected orders are committed | all |
| `no-replica-identity` (§20) | nothing: inserts stream, only updates and deletes fail on the source | all |
| `missing-grant` (§21) | kafka-connect (task 0 FAILED), postgres (slot inactive); kafka and warehouse after 60 s | consumer |
| `blocked-slot` (§22) | kafka and warehouse after 60 s; kafka-connect if Connect reports task 0 not RUNNING while start() waits; postgres if the waiting slot is not listed yet | consumer |
| `idle-wal-growth` (§23) | nothing until retained WAL passes 100 MB (the break stops past 32 MiB) | all |
| `stale-source-offsets` (§24) | kafka-connect (task 0 FAILED), postgres (`debezium_orders` inactive); kafka and warehouse after 60 s | consumer |

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
It is idempotent: when the new schema is already registered and task 0 is RUNNING (the agent fixed it
first), it skips the wait and the restart, and it still ends at `BACKWARD`.
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
(non-superuser). Concretely, `break` runs:

```
CREATE ROLE debezium LOGIN REPLICATION PASSWORD 'piperoom-dev'   (if absent)
CREATE ROLE orders_owner LOGIN                                   (if absent)
GRANT CONNECT ON DATABASE orders TO debezium
GRANT USAGE ON SCHEMA public TO debezium
GRANT SELECT ON public.orders TO debezium
ALTER PUBLICATION dbz_publication OWNER TO debezium
ALTER TABLE public.orders OWNER TO orders_owner
```
then re-registers the connector with `database.user=debezium` and restarts task 0.

**Root cause:** with `publication.autocreate.mode=filtered`, Debezium 3.2 runs
`ALTER PUBLICATION dbz_publication SET TABLE "public"."orders"` on every task start, which requires
owning the table. `debezium` owns the publication but not the table, so the task fails with
`must be owner of table orders`. The superuser `piperoom` would bypass that check, which is why the
connector must be switched to the non-superuser `debezium` for the break to bite.

**Real error text** (observed in `docker logs piperoom-dev-connect-1`, repeated on every retry):
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
re-register the connector with `database.user=piperoom`, restart task 0. The slot goes back
`active=true` and the next insert reaches the warehouse (verified: 103 → 104 rows, stale 9 s).

---

## P3 scenarios: one incident per write action

Each break below is the incident one P3 action ends (`tasks/P3-fixes-that-end-incidents.md`). Written on
2026-10-18 **without a running stack** (no Docker in that session): the SQL, REST and Kafka steps are
unit-tested against fakes and kfake (`cmd/devctl/scenarios_p3_test.go`), and the Kafka helpers have
`integration`-tagged tests, but **nothing in §8–§11 has been observed yet**. Whoever runs them first on the
dev stack: replace the "Expected" blocks with what you saw, as in §1–§7.

Every object a break creates is named `devctl_*` / `devctl.*`; every fix removes it and is safe to run twice.

| Scenario | How the room opens | Action that ends it (P3) | `devctl fix` does |
|---|---|---|---|
| `missed-table` | alert: `devctl alert missed-table` (no watcher signal sees a table that is missing from Kafka) | `pg_add_publication_table`, then `connect_snapshot_table` (`via: signal_table`) for the rows written before | adds the table to the publication if missing, sends one incremental-snapshot signal if the topic is short, waits until every row is in Kafka; then back to baseline: connector config from `dev/connectors`, publication on `orders` only, `devctl_*` tables, topics and subjects removed |
| `dead-slot` | watcher: `slot_inactive`, `connector_down` (STOPPED, sev3) | `pg_drop_replication_slot` | stops the connector, drops the inactive slot (refuses one a running connector may still use), recreates it at the current WAL position, deletes the connector's stored offsets because they predate the new slot, sets `snapshot.mode=when_needed`, resumes, and waits for a marker row in the warehouse and order-sync lag 0 (re-snapshot: rows are re-emitted) |
| `consumer-bad-deploy` | alert: `devctl alert consumer-bad-deploy` (lag is 0; only a reconciliation sees it) | `kafka_reset_consumer_offsets` (group inactive, to the deploy time) | stops devconsumer, resets order-sync to the start of `pg.public.orders` (the load is an upsert by id, so a full replay is safe), starts the good build, waits for lag 0 |
| `dlq-backlog` | watcher: `dlq_growth` (`devctl.order-sync.dlq` is declared in `dev/owners.p3.yaml`) | `dlq_replay`, after the cause is fixed | deploys the good build, replays every DLQ record into `pg.public.orders` in order without the `__connect.errors.*` headers, deletes the DLQ topic, waits until every `gift` order is in the warehouse |

`devctl alert <scenario> [--server URL] [--as user]` posts the scenario's alert body (`cmd/devctl/alert.go`) to
`POST /api/alerts/webhook` with `X-User` (dev auth mode only; default server `PIPEROOM_HTTP_ADDR`, user
`arjun`). It takes no lock and changes nothing on the stack. Its `dedup_key` is `devctl-<scenario>`, so a
repeat joins the open room. `devctl demo --rehearse` waits for a watcher room, so of these four it only fits
`dead-slot` and `dlq-backlog`.

**These four scenarios need the server started with `OWNERS_FILE=dev/owners.p3.yaml`** (`devctl break|fix`
logs a reminder). That file is `dev/owners.yaml` plus what the scenarios and actions check:
`public.devctl_refunds` in `source_tables` (the actions refuse a table outside it), the DLQ in
`watch.dlq_topics` (`written_by: order-sync`), and `dlq_growth: {for: 30s}`. On the baseline file the
watcher does not see the DLQ and `pg_add_publication_table` / `connect_snapshot_table` refuse the table. The
additions stay out of `dev/owners.yaml` because `evals/gen` derives eval fixtures from it;
`TestOwnersP3MatchesBaseline` fails when the two files drift beyond them. devconsumer gets `DLQ_TOPIC`, `SKIP_OPS` and `REJECT_STATUS` from
`dev/docker-compose.yml`; the last two are empty (the good build) unless a break sets them.

## 8. `missed-table` — a new table that never reaches Kafka

**Break (`devctl break missed-table`):**
1. `CREATE TABLE public.devctl_refunds (id bigserial PRIMARY KEY, …)` and the Debezium signal table
   `public.devctl_signal (id varchar(42) PRIMARY KEY, type varchar(32), data varchar(2048))`. Both land in
   `piperoom_audit.ddl_log`.
2. Re-register the connector with `publication.autocreate.mode=disabled` (the DBA owns the publication),
   `table.include.list=public.orders,public.devctl_refunds,public.devctl_signal` (the app team added the
   table to the connector) and `signal.data.collection=public.devctl_signal`; restart task 0.
3. `ALTER PUBLICATION dbz_publication SET TABLE public.orders, public.devctl_signal`: the new table is not
   published. Insert 25 refunds.
4. Proof: an order written after the refunds reaches `analytics.orders` (the connector streams), and topic
   `pg.public.devctl_refunds` has no record.

**Root cause:** pgoutput only decodes tables in the publication. With autocreate disabled Debezium never adds
the table, so the include list says "capture it" and nothing fails: the task stays RUNNING, the slot active.

**Expected symptoms (not yet observed):**
```
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING
slot        debezium_orders active=true
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   fresh
pg_publication_tables (dbz_publication): orders, devctl_signal      ← devctl_refunds missing
topic pg.public.devctl_refunds: does not exist
```

**Fix:** the agent proposes `pg_add_publication_table` (postgres owner approves); new refunds flow from then
on. The 25 rows written before need `connect_snapshot_table` with `via: signal_table` (the connector's
`signal.data.collection`), which writes the same `execute-snapshot` row `devctl fix` writes.

## 8a. `missed-table-kafka-signal` — the missed-table break with the Kafka signal channel on

The same incident as §8, but the dev connector takes its snapshot signal over Kafka (`#620`'s signal
format), for the first live check of `connect_snapshot_table` with `via: signal_topic`.

**Break (`devctl break missed-table-kafka-signal`):** identical to `missed-table` (§8), except the connector
config also turns Kafka signalling on and the break first creates the one-partition signal topic
`devctl.signal`:

```
publication.autocreate.mode = disabled
table.include.list          = public.orders,public.devctl_refunds,public.devctl_signal
signal.data.collection      = public.devctl_signal
signal.enabled.channels     = source,kafka
signal.kafka.topic          = devctl.signal
signal.kafka.bootstrap.servers = kafka:9092
```

`connect_config` shows `signal.enabled.channels: source,kafka`; `pg_publication_tables` shows
`devctl_refunds` missing from `dbz_publication`. The 25 refunds never reach Kafka (topic
`pg.public.devctl_refunds` has no records), and nothing fails — the connector, task 0, the slot and
order-sync are all healthy.

**Fix (`devctl fix missed-table-kafka-signal`):** what `fix missed-table` does (add the table to the
publication if missing, send one incremental snapshot if the topic is short, then restore the connector
config from `dev/connectors` — Kafka signalling off again — the publication on `orders` only, and drop the
`devctl_*` tables, topics and subjects), plus deleting the `devctl.signal` topic. Idempotent.



## 9. `dead-slot` — a retired connector's slot keeps all WAL

**Break (`devctl break dead-slot`):** the orders connector is retired: Connect REST `stop` (the config stays
for the record). The slot `debezium_orders` loses its WAL sender, and a 100-row insert proves retained WAL
grows (`retained_before_bytes` / `retained_after_bytes` in the log, as in §5).

**Why this is not §5:** the mechanics are the same; the decision is not. In §5 the connector comes back, so
`connect_resume_connector` is right. Here nobody will read the slot again, so the fix that protects the
database is dropping it. The P3 action only drops the slot named in a pipeline connector's `slot.name` (P3b),
which is why the dead slot is the connector's own slot and not a second, unrelated one: an extra slot would
be refused by the action and is not watched by the watcher either. That the connector is retired is human
context the room gets from people; the C7 eval scenario (`dead-slot-wal.yaml`, P3g-1) has to state it in
its alert or a steer, or "resume" is the right answer there too.

**Expected symptoms (not yet observed):** as §5: `connector STOPPED`, `slot debezium_orders active=false`,
retained WAL growing with `devctl load`; the watcher fires `connector_down` (sev3, STOPPED) and
`slot_inactive` after 30 s.

**Fix:** `pg_drop_replication_slot` (postgres owner approves) frees the WAL at once (the slot is gone).
`devctl fix dead-slot` does the same, then puts the dev pipeline back. Order matters: **stop the connector**
(a stopped connector holds no WAL sender, and Connect only deletes offsets on a stopped connector) → drop the
slot → **recreate it** (`pg_create_logical_replication_slot`, `pgoutput`) at the current WAL position →
**delete the connector's stored offsets** (`DELETE /connectors/pg-orders-connector/offsets`) → set
`snapshot.mode=when_needed` → resume → wait for task 0 RUNNING, an active slot, **a new row in the warehouse**
and order-sync lag 0. The saved offset points into the dropped slot, so restoring it would fail the task with
"trying to read change stream starting at … is no longer available" (KAN-411, observed on the live stack in the
phase-3a eval). Clearing it makes Debezium **re-snapshot**: every row is re-emitted, not only the rows changed
while no slot existed (in production that step is `connect_snapshot_table`; the P3 action
`pg_drop_replication_slot` has the same consequence, see `prompt.go`). If the agent already dropped the slot, the
fix skips the drop; on a healthy stack it is a no-op. Idempotent: a second run ends healthy.

**Live status (KAN-411, 2026-10-22):** the first live run dropped the slot and then failed at
`waitConnector(RUNNING)` exactly as described above; the sequence above is the fix. It is unit-tested against
the fake stack (`cmd/devctl/scenarios_p3_test.go`), but **the reworked fix has not been re-run on a live stack
yet** — the stack lock was held by `qa` during this change. The next live run should replace this paragraph
with what was observed.

## 10. `consumer-bad-deploy` — offsets committed past orders never loaded

**Break (`devctl break consumer-bad-deploy`):** devconsumer is redeployed with `SKIP_OPS=c` (a build that
drops create events but commits their offsets; updates still load, so the warehouse looks fresh). 30 orders
are inserted; the break checks that order-sync's lag went back to 0 and that none of the 30 ids is in
`analytics.orders`. Then on-call stops devconsumer (`docker compose stop`), leaving the group Empty: the reset
action refuses a group with members.

**Expected symptoms (not yet observed):**
```
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING
consumer    order-sync: state Empty, members 0, total lag 0   ← grows only with `devctl load`
warehouse   analytics.orders: the 30 new ids missing
devconsumer log (before the stop): "devconsumer started … skip_ops=c"
```

**Fix:** `kafka_reset_consumer_offsets` to a time before the deploy (consumer owner approves), then the good
build starts and replays: lag jumps, then drains to 0, and the 30 orders arrive. `devctl fix
consumer-bad-deploy` resets to the start of the topic instead of a timestamp (it does not know the deploy
time; the upsert makes a full replay safe) and starts devconsumer with `SKIP_OPS` empty.

## 11. `dlq-backlog` — a strict release routes valid orders to the DLQ

**Break (`devctl break dlq-backlog`):** creates `devctl.order-sync.dlq` (1 partition), redeploys devconsumer
with `REJECT_STATUS=gift` (a release that validates `status` and does not know the new value `gift`), and
inserts 20 `gift` orders. Each is produced to the DLQ with its key, value and headers, plus the
`__connect.errors.*` context headers Kafka Connect uses (topic, partition, offset, exception class
`devconsumer.ValidationError`, message `unknown status "gift"`), so `dlq_peek` reads it like a sink
connector's DLQ. The break waits until the DLQ holds the 20 records.

**Expected symptoms (not yet observed):**
```
consumer    order-sync: state Stable, members 1, total lag 0   ← rejected records are committed
warehouse   fresh, but no 'gift' order
topic devctl.order-sync.dlq: 20 records
devconsumer log: level=WARN msg="record rejected to the DLQ" … reason="unknown status \"gift\""
watcher: dlq_growth fires after 30 s and clears once no new DLQ record arrived for its window (1 h)
```

**Fix:** deploy a build that accepts `gift` (the cause), then `dlq_replay` (kafka owner approves): the DLQ's
end offset does not change, `pg.public.orders` grows by 20, the orders load. `devctl fix dlq-backlog` does
both, then deletes the DLQ topic so a second run replays nothing. If the agent already replayed, the fix's
replay is a duplicate the upsert absorbs.

## 12. `retention-loss` — the topic's retention deleted records before the consumer read them

**Break (`devctl break retention-loss`):**
1. `docker compose stop devconsumer` — `order-sync` leaves the group, so its committed offsets stay put.
2. Set `retention.ms=1000` and `segment.ms=1000` on `pg.public.orders` (a misconfigured retention window,
   seconds instead of days), then insert 50 orders. Debezium streams them, the active segment rolls and the
   broker deletes the old segment holding the group's committed offset. The dev broker's
   `log.retention.check.interval.ms` and `log.segment.delete.delay.ms` are 1 s (`dev/docker-compose.yml`), so
   the deletion happens within seconds instead of the 5-minute + 1-minute production defaults.
3. The break waits until the log start offset has moved past `order-sync`'s committed offset on a partition,
   then logs the affected partitions.

**Root cause:** the topic's `retention.ms` was set too low, so Kafka deleted records before `order-sync`
consumed them. The group's committed offset now points **below** the log start: there are no records at that
offset any more, and the consumer cannot resume where it left off.

**Expected symptoms (not yet observed — no Docker in the session that wrote this):**
```
consumer    order-sync: state Empty, members 0          ← stopped by the break
kafka       pg.public.orders: log start offset moved past the committed offset
watcher     offset_below_log_start fires at once (sev1); devctl break logs partitions_below
```
The real Kafka symptom the agent would see: `kafka_consumer_lag` reports the group's committed offset below
the log start (`committed < start`, negative lag), and `kafka_topic_config` shows `retention.ms=1000` and
`segment.ms=1000`.

**Fix (`devctl fix retention-loss`):** restore `retention.ms` and `segment.ms` to the broker defaults
(delete the overrides) and `docker compose start devconsumer`. The consumer resets to the earliest offset
still on the topic (`auto.offset.reset=earliest`) and catches up; no new rows are lost from then on. The
rows deleted during the break are gone — in production the real fix is a backfill (a Debezium
`execute-snapshot` incremental snapshot), which is exactly what the agent should propose; `devctl fix` only
puts the stack back so the demo can run again. Idempotent: deleting already-deleted overrides and starting
an already-running consumer are both safe.

## 13. Batch scenarios — `dag-failed`, `dbt-model-broken`, `late-batch`

These break the `daily-revenue` batch pipeline (dev/batch.md): Airflow DAG `revenue_daily` runs dbt, which
builds `analytics.stg_orders` → `analytics.fct_revenue` in the warehouse. A batch pipeline has no watcher
signals (owners.batchWatch), so each break raises its alert through `devctl alert <scenario>`; the pipeline
is in `dev/owners.yaml`. They need the batch profile up and `AIRFLOW_URL` set (`dev/batch.md`).

| Scenario | Break | Symptom (tool receipt) | Fix |
|---|---|---|---|
| `dag-failed` | point `check_source` at a missing table (Airflow Variable `revenue_source_table` = `orders_missing`), trigger `revenue_daily` | the run is `failed` at `check_source` (`dag_runs`, then `task_instances` + `task_log_tail`); layer orchestrator | restore the variable to `orders`, trigger a run, wait for `success` |
| `dbt-model-broken` | write a broken model `fct_revenue_v2.sql` into `dev/batch/dbt/analytics/models/`, trigger | `check_source` passes but `dbt_run` fails; `dbt_run_results` shows `fct_revenue_v2` error and `task_log_tail` shows dbt's error; layer transform, model named | remove the broken model, trigger a run, wait for `success` |
| `late-batch` | `airflow dags pause revenue_daily` | `dag_schedule_gap` verdict `paused`; `analytics.fct_revenue` goes stale; layer orchestrator | `airflow dags unpause revenue_daily`, trigger a run, wait for `success` |

**Expected symptoms (not yet observed — no Docker in the session that wrote this):**
```
dag-failed:       dag_runs revenue_daily: 1 failed; task_instances: check_source failed (UndefinedTable), dbt_run upstream_failed
dbt-model-broken: dag_runs revenue_daily: 1 failed; dbt_run_results: fct_revenue_v2 error | message: column "revenue_amt" does not exist
late-batch:       dag_schedule_gap revenue_daily: verdict paused; the DAG is PAUSED, so no runs are scheduled
```

**Notes:**
- `dag-failed` does not touch the CDC sink: it repoints `check_source` at a missing table through the
  Airflow Variable `revenue_source_table` (default `orders`), so `analytics.orders` keeps loading while the
  DAG fails and devconsumer never notices the break.
- Each fix triggers a run and waits for `success`, so the break's failed state is overwritten and the fix is
  idempotent (restoring the variable and removing the broken model are both safe on an already-restored stack).
- The alerts are reconciliation/freshness checks, the kind a team's own monitoring would send; none of them
  can come from the watcher (a batch pipeline has no Kafka/Connect/slot to watch).

## Silent corruption: `sink-filtered-rows`, `sink-dropped-column`, `consumer-duplicates`, `toast-placeholder`

Four breaks where **nothing fails**: the connector is RUNNING, the slot is active, order-sync is Stable with
lag 0, and **`devctl health` stays green** after the break (the warehouse layer measures "pending" rows by
`updated_at` against the sink's newest row, so each break leaves its rows older than that). The only way in is
comparing source and sink, or reading the topic: the watcher's `count_drift`, `row_drift` and `dup_keys`
signals (X1). They run on the baseline `dev/owners.yaml` (no owners file switch). The drift checks fire every
`watch.reconcile_interval` (15 min by default; set `reconcile_interval: 1m` in a local owners file to see a
signal sooner). All four are re-runnable: break twice and fix once ends healthy, and a second fix is a no-op or
a harmless replay. The rows a break writes stay in `public.orders` (they are ordinary orders); `devctl reset`
removes them.

Not yet observed on a live stack (no Docker in the session that wrote them): the *Expected* blocks below
come from the code and from Postgres/Debezium behaviour, and the integration tests
(`go test -tags integration ./cmd/devctl -run 'TestIntegration(SinkFiltered|SinkDropped|ConsumerDup|Toast)'`)
are the first live check. If you see something different, update this file.

### 14. `sink-filtered-rows` — a filter in the consumer drops rows

**Break:** devconsumer restarts with `SKIP_WHERE=status=backfill` (`DEVCONSUMER_SKIP_WHERE` in compose): a
release whose filter drops every order in that status without loading it, and commits the offset. 40 `backfill`
orders are inserted, then one normal marker order; the break waits for the marker in the warehouse and for lag 0.

**Expected:**
```
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   fresh; SELECT count(*) FROM orders  -  SELECT count(*) FROM analytics.orders  >= 40
devctl health: pipeline healthy
devconsumer log: "devconsumer started … skip_where=status=backfill"
watcher: count_drift (sev2) on public.orders / analytics.orders, source - sink = missing rows (after 2 checks)
         row_drift (sev2): the sampled backfill keys are missing on the sink
```
On an idle stack the windows end at the sink's newest row (the marker), since the source has nothing newer;
under load they are the last closed hour shifted by the sink's lag.
**Fix:** devconsumer restarts without the filter, order-sync is reset to the start of `pg.public.orders` and
replays (the load is an upsert by id), then the fix waits until no `backfill` order is missing.

### 15. `sink-dropped-column` — the upsert stops writing `updated_at`

**Break:** devconsumer restarts with `DROP_COLUMNS=updated_at`: a release that rewrote its upsert and lost the
column (left out of both the INSERT and the update list). 20 orders are inserted whose source `updated_at` is
exactly the sink's current newest (late-arriving rows), so health cannot see them as pending; they load with
`updated_at` NULL.

**Expected:**
```
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   SELECT count(*) FROM analytics.orders WHERE updated_at IS NULL  >= 20 (source has the timestamps)
devctl health: pipeline healthy
watcher: row_drift (sev2): mismatched = sampled rows the sink holds with updated_at NULL
         count_drift (sev2, idle stack, after 2 checks): rows with updated_at NULL are outside the sink's window
```
Every drift read keys on `updated_at`, so the NULL rows look like lag: on an idle stack the windows end at the sink's
newest row, under load the landed-row probe samples the last hour by the wall clock and counts the rows the sink
holds with the column NULL. A sink table that loses the column itself is a row_drift "column missing on the sink".
**Fix:** devconsumer restarts with the column written and order-sync replays from the start of the topic; the
upsert fills every NULL; the fix waits until none is left.

### 16. `consumer-duplicates` — the same change events twice on the topic

**Break:** needs order-sync running and at lag 0. Writes 10 orders, waits for them in the warehouse, then
publishes the newest event of each of 10 keys a second time on `pg.public.orders` (same key, same payload, so
the same `source.lsn`), as an upstream redelivery would (an unclean Connect restart that replays from the last
flushed offset, a relay re-publishing a window). devconsumer reprocesses them without harm (upsert) and lag
returns to 0. A consumer cannot create duplicates by reading, so the break writes them; taking only the newest
event per key makes them duplicates, not LSN regressions (`order_skew`).

**Expected:**
```
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   correct (the upsert absorbs the copies)
devctl health: pipeline healthy
kafka_dup_keys pg.public.orders: >= 10 duplicate events, 0 order regressions
watcher: dup_keys (sev3)
```
**Fix:** Kafka cannot delete single records, so the fix waits for lag 0 and moves each partition's log start up
to order-sync's committed offset **minus the last record** (kept so the topic is never empty: health reads the
newest record). The duplicates sit before the end of the log and are consumed, so the sink loses nothing; the
topic's older history is gone, as with `kafka-delete-records`. A topic with no duplicate event is left alone
(second run: "skipped"). The count (the check the break and the fix use) is the records in the last 5000 per
partition that repeat an earlier record's key and value byte for byte. Because the cut drops history, run this
scenario on its own: the fixes of `sink-filtered-rows` and `sink-dropped-column` replay order-sync from the start
of the topic and only see what is left; `devctl reset` rebuilds the full history.

### 17. `toast-placeholder` — Debezium emits the unavailable-value placeholder

**Break:** needs order-sync running and at lag 0. `ALTER TABLE orders REPLICA IDENTITY DEFAULT` (the dev default
is FULL), then 3 orders with a 12.8 kB `status` (random hex, stored out of line by TOAST) are inserted and, once
in the warehouse, updated in a non-TOAST column (`amount`, `updated_at`). The update's WAL record has no value
for the unchanged TOAST column, so Debezium writes `__debezium_unavailable_value` as `status`, and devconsumer
loads that string.

**Expected:**
```
consumer    order-sync: state Stable, members 1, total lag 0
warehouse   SELECT count(*) FROM analytics.orders WHERE status = '__debezium_unavailable_value'  >= 3
source      the same rows have the real status
devctl health: pipeline healthy
pg_toast_risk (X1d): orders.status TOASTed, replica identity DEFAULT, placeholder seen N times in the sink
watcher: row_drift (sev2): mismatched = 3, detail "unavailable-value placeholder in status: 3 rows"; no count_drift
```
The timestamps agree, so row_drift comes from its placeholder count: the source's toastable columns
(`ToastColumnReader`) counted for Debezium's default placeholder in the sink (`PlaceholderCounter`).
**Fix:** `ALTER TABLE orders REPLICA IDENTITY FULL`, then rewrite `status` of every source row the sink holds as a
placeholder (`status = status || ''` builds a new value, so the change event carries it in full) and wait until
no placeholder is left. With no placeholder row only the `ALTER` runs. The change events already on the topic
keep the placeholder (history is not rewritten).

## Setup breaks: `devctl setup-break` / `setup-fix`

The six prerequisites a `piperoom-setup` run checks before it can build a
pipeline, each broken on the dev stack and restored again. They are separate
from the incident scenarios above: nothing in the CDC pipeline is the subject —
the *environment* is, and each break makes exactly one named setup check fail.

```
go run ./cmd/devctl setup-break --owner <your-tag> <name>
go run ./cmd/devctl setup-fix   --owner <your-tag> <name>
```

Both take the shared stack lock. `setup-break` first proves the check passes,
applies the break, then proves it fails; `setup-fix` applies the recorded fix
and proves the check passes. A stack that already fails (or already passes)
the check is left alone, so both commands are idempotent. They never delete
rows; `devctl reset` rebuilds the stack if a fix is interrupted.

| Scenario | Named check | What the break does | What a setup run reports |
|---|---|---|---|
| `flat-plugin-dir` | `plugin-layout` | moves every jar of `/kafka/connect/debezium-connector-postgres` into `/kafka/connect` itself | `jar_list`: "N jar(s) directly in /kafka/connect; each connector needs its own directory" |
| `missing-converter-jars` | `plugin-converter-jars` | moves `debezium-core-*.jar` and `debezium-api-*.jar` out of the connector directory | `jar_list`: connector missing `debezium-core` / `debezium-api` (`missing_converters`) |
| `wal-level-replica` | `postgres-wal-level` | stops the connector, drops its slot, restarts Postgres with `wal_level=replica` (a Compose override; `-c` on the command line beats `ALTER SYSTEM`) | `source_settings`: `wal_level=replica`; a logical slot cannot be created |
| `missing-grant` | `source-role-grants` | creates `devctl_setup_grant` (LOGIN, no REPLICATION, no SELECT) and points the connector at it | `source_role`: role is missing REPLICATION and SELECT on `public.orders` |
| `db-unreachable-from-connect` | `connect-source-reachable` | appends `192.0.2.1 postgres` to the Connect container's `/etc/hosts` (TEST-NET-1, a black hole) and restarts the task | the worker cannot open a TCP connection to `postgres:5432` |
| `plugin-after-scan` | `connect-plugin-scan` | takes the plugin directory out of the plugin path, restarts the worker (it scans without it), then puts the directory back without restarting | `/connector-plugins` lacks `PostgresConnector` while the jar is on disk: the scan ran before the install |

**Fixes:** move the jars back; restart Postgres on its own command line
(`fix wal-level-replica` also recreates the dropped slot, clears the
connector's offsets and sets `snapshot.mode=when_needed`, so Debezium
re-snapshots — the same sequence as `fix dead-slot`); re-register the baseline
connector user and drop the grant role; restart the Connect container (Docker
regenerates `/etc/hosts`, and the new JVM has an empty DNS cache — a mere task
restart keeps the cached bad address); restart the worker so the scan sees the
plugin.

**Caveats:**
- `wal-level-replica` is the invasive one on a *running* stack: Postgres
  refuses to start with an existing logical slot at `wal_level=replica`, so the
  break drops the dev connector's slot, and refuses when any other logical slot
  exists (the staging connector's, a q5 scenario's). The fix re-snapshots the source (no data loss, but the
  rows are re-emitted). A *fresh* setup has no slot, which is the state the
  break reproduces without that cost.
- A Connect restart reloads every plugin the Debezium image ships; on a busy
  machine the scan takes from seconds to several minutes. `connectStartTimeout`
  (6 min) in `cmd/devctl/setupbreaks.go` is the wait.
- The setup agent's model turns are not replayed here. The integration test
  (`go test -tags integration ./cmd/devctl -run TestIntegrationSetupBreaks`)
  applies each break that leaves the shared servers and connector alone
  (`flat-plugin-dir`, `missing-converter-jars`) and its recorded fix on the dev
  stack and asserts the named check fails, then passes: the recorded fix *is*
  the scenario's fix op sequence. The breaks that restart Postgres or Connect,
  or take `pg-orders-connector` down, run only against the fake stack there,
  since other integration packages use the same stack in parallel; their
  real-stack run is the U1-16 job. The recordings themselves are U1-19/U1-20 (`evals/setup`).
- A setup stack of its own (not the dev stack) is `setupstack.Setup(t)` in
  `internal/testenv/setupstack/setup.go`: it renders kafka + connect + source-postgres in
  a temp directory, installs the plugin, and tears the project down with the
  compose ops. `internal/testenv/setupstack/setup_integration_test.go` also proves the
  compose validator refuses a `privileged: true` file before `compose_up`.

### Oracle setup breaks (profile `oracle`, U9c)

The three prerequisites the U9a Oracle client reads, broken on the `oracle` profile (dev/oracle/README.md). Same
commands, lock and idempotence as above; the profile must be running (`docker compose -f dev/docker-compose.yml
--profile oracle up -d --wait oracle`), or break and fix stop with that command. devctl runs fixed SQL*Plus scripts
as SYSDBA through `docker exec` (`cmd/devctl/setupbreaks_oracle.go`), and each check evaluates the U9a client's own
types with the grant list its writer grants.

| Scenario | Named check | What the break does | What the check reports |
|---|---|---|---|
| `no-archivelog` | `oracle-archive-log-mode` | `SHUTDOWN IMMEDIATE`, `STARTUP MOUNT`, `ALTER DATABASE NOARCHIVELOG`, open the CDB and PDBs (about 7 s) | `LOG_MODE=NOARCHIVELOG; LogMiner needs ARCHIVELOG` (U9a `DatabaseInfo.LogMode`) |
| `no-supplemental-logging` | `oracle-supplemental-logging` | drops the `ALL COLUMNS` log group of `PIPEROOM.ORDERS`, then the database-level minimal supplemental logging | missing `SUPPLEMENTAL_LOG_DATA_MIN=NO` and the `ALL COLUMNS` group on `PIPEROOM.ORDERS` (U9a `SupplementalLogging.Minimal`, `TableSupplementalLogging`) |
| `missing-logminer-grant` | `oracle-logminer-grants` | `REVOKE LOGMINING FROM C##DBZUSER CONTAINER=ALL` | `C##DBZUSER is missing LOGMINING` (U9a `LogMinerGrants.SysPrivs`) |

**Fixes:** the same restart with `ALTER DATABASE ARCHIVELOG` (the recovery area set at first start stays in the
spfile); add the minimal supplemental logging and the table's `ALL COLUMNS` group back; grant every system
privilege, role and table `SELECT` the check reads (not only `LOGMINING`). `no-archivelog` ends every open session.
Unit tests: `go test ./cmd/devctl -run Oracle`. Integration (profile up; reads the checks through the U9a client
over go-ora): `go test -tags integration ./cmd/devctl -run TestIntegrationOracleSetupBreaks`.

## Setup breaks: MongoDB (`mongo` profile)

Two more `setup-break` / `setup-fix` pairs, on the opt-in `mongo` profile (dev/mongo/README.md) instead of the
Postgres pipeline. Start the profile first (`docker compose -f dev/docker-compose.yml --profile mongo up -d --wait
mongo`); the commands, the lock and the idempotence are the same as above. Each check is the
`internal/clients/mongodb` read the MongoDB setup source makes, connected as the capture user `piperoom_cdc`.

| Scenario | Named check | What the break does | What the client reports |
|---|---|---|---|
| `standalone-mongod` | `mongo-change-streams` | recreates the `mongo` service without `--replSet rs0` (a Compose override; same start script, keyFile and data volume) | `ChangeStreamSupport`: not supported, "standalone mongod has no change streams"; `Settings`: `server_type=standalone` |
| `no-oplog-role` | `mongo-oplog-read` | `revokeRolesFromUser('piperoom_cdc', [{role: 'read', db: 'local'}])` as the root user | `RoleInfo("admin.piperoom_cdc")`: `replication=false`, the user cannot read `local.oplog.rs` (`readAnyDatabase` never covers `local`) |

**Fixes:** recreate the service from the base file (`up -d --no-deps --wait mongo`): the member's replica set
config is still on the volume, so it elects itself primary again; grant `read` on `local` back. Measured on Linux:
the standalone break takes about 4 s and its fix about 8 s; the role pair takes under 1 s each.
`go test -tags integration ./cmd/devctl -run TestIntegrationMongoSetupBreaks` runs both pairs on the profile, and
skips when it is not up.

## Recovery: `rows-lost-after-retention`, `consumer-skipped-range`

Two breaks where orders are missing from `analytics.orders` while order-sync runs at lag 0, each ended by one X2
recovery action (Preview → approval → Execute → Verify counts). No watcher signal sees them on an idle stack, so
each has a reconciliation alert (`devctl alert <scenario>`). **They need the server on
`OWNERS_FILE=dev/owners.recovery.yaml`**: `dev/owners.yaml` plus the pipeline's `reprocess_topic`
(`pg.public.orders.reprocess`), the one topic `kafka_replay_range` may write to (`TestOwnersRecoveryMatchesBaseline`
keeps the two files in step). Both breaks register the connector with the signal table `public.devctl_signal`
(in `table.include.list` and `signal.data.collection`) so `pg_resnapshot_rows` can signal it, and create the
reprocess topic; both fixes put the connector, the publication and the topics back as `devctl reset` leaves them.
The orders a break writes are marked by `customer_id` (9001-9050, 9101-9140), so a fix finds them without saved
state. Eval scenarios: `evals/scenarios/rows-lost-after-retention.yaml`, `consumer-skipped-range.yaml` (X2e).

Not yet observed on a live stack (no Docker in the session that wrote them): the *Expected* blocks come from the
code and from Kafka/Debezium behaviour. Unit tests: `go test ./cmd/devctl -run 'RowsLost|SkippedRange|Recovery'`.

### 18. `rows-lost-after-retention` — retention deleted change events before order-sync read them

**Break:** stops devconsumer (order-sync's offsets stay committed), sets `retention.ms=1000` and `segment.ms=1000`
on `pg.public.orders`, and writes 50 orders in five batches of 10, 1.5 s apart, each once the previous one is on
the topic: each batch's first record rolls a new segment. It waits until the log start of every partition
reached the last batch (every older segment deleted: the topic's history and the first 40 orders), then
**removes the overrides** (nothing more is deleted) and starts devconsumer. Its committed offsets are below the
log start, so it resumes at the earliest retained offset (`auto.offset.reset=earliest`) without an error and
loads the last batch; a marker order proves it streams. The break logs the id span and how many are missing.

**Expected:**
```
consumer    order-sync: state Stable, members 1, total lag 0
kafka       pg.public.orders: log start = the last batch's offsets; ~11 records retained
warehouse   the first 40 of the 50 orders missing (fewer if the last batch missed a partition)
reconcile   "sink is missing 40 rows ... (connector side)": deleted records look unwritten to offsets by time
```
**Fix (what the agent proposes):** `pg_resnapshot_rows` on `public.orders` with `key_range` over the missing ids:
the connector re-emits exactly those rows from the source (`kafka_replay_range` refuses: the range is below the
log start). `devctl fix rows-lost-after-retention` removes any leftover override, inserts the same signal row
(`execute-snapshot`, INCREMENTAL, `additional-conditions` on `id`) when an order is missing, waits until none
is, and restores the baseline.

### 19. `consumer-skipped-range` — order-sync's offsets moved past events still on the topic

**Break:** stops devconsumer, writes 40 orders, waits until they are on `pg.public.orders`, commits order-sync's
offsets at the topic's end (`kafka-consumer-groups --reset-offsets --to-latest`, the deploy job's leftover step),
starts devconsumer and checks that none of the 40 reached the warehouse (a marker order does).

**Expected:**
```
consumer    order-sync: state Stable, members 1, total lag 0
kafka       the 40 change events retained (kafka_find_by_key / trace_record show their offsets)
warehouse   the 40 orders missing
```
**Fix (what the agent proposes):** `kafka_replay_range` of the skipped offsets per partition into
`pg.public.orders.reprocess`. On the dev stack devconsumer reads one topic; to watch the copy load, run a second
consumer on it: `docker compose -f dev/docker-compose.yml run --rm -e TOPIC=pg.public.orders.reprocess -e
GROUP=order-sync-reprocess devconsumer`. `devctl fix consumer-skipped-range` does not copy: it replays order-sync
from the start of the topic (the load is an upsert by id), waits until no skipped order is missing, and restores
the baseline (the reprocess topic is deleted).

## Source settings: `no-replica-identity`, `missing-grant`, `blocked-slot`, `idle-wal-growth` (Q2f)

Four database-side causes from the Q wave's Stack Overflow analysis (`docs/product/so-gaps-2026-10.md`), one per
Q2 admin action. Each break shows in a Q2a tool (#859: `source_settings`, `source_table_replication`,
`source_role_grants`, `source_blocking_sessions`) and opens a room through the watcher or `devctl alert <scenario>`.
Written on 2026-10-23 **without a running stack** (no Docker in that session): the SQL, REST and detached-session
steps are unit-tested against a fake stack (`go test ./cmd/devctl -run 'NoReplica|MissingGrant|BlockedSlot|IdleWAL'`),
but **nothing in §20-§23 has been observed yet**. Whoever runs them first: replace the "Expected" blocks with what
you saw, as in §1-§7. Every object a break creates is named `devctl_*` / `devctl-*`; every fix removes it and is
safe to run twice. Only `idle-wal-growth` needs another owners file: **`OWNERS_FILE=dev/owners.q2.yaml`**
(`dev/owners.yaml` plus `slot_wal: {fire: 32MiB, clear: 16MiB, for: 30s}`;
`TestOwnersQ2MatchesBaseline` keeps the two in step).

| Scenario | How the room opens | Q2a tool that shows it | Action that ends it |
|---|---|---|---|
| `no-replica-identity` | alert (`source: app`): updates on `public.orders` fail | `source_table_replication`: `replica_identity: nothing` | `pg_set_replica_identity` (mode `full`) |
| `missing-grant` | watcher: `connector_down` (task 0 FAILED), `slot_inactive` | `source_role_grants`: role `devctl_cdc` (the connector's `database.user`), `select: false` on `public.orders` | `source_grant_select` |
| `blocked-slot` | alert (`source: freshness`): no new order in `analytics.orders` | `source_blocking_sessions`: the blocker's pid, `blocks_slot: true` (identity hidden in a pipeline room: it locks no pipeline table) | `source_end_session` |
| `idle-wal-growth` | watcher: `slot_wal` (on `dev/owners.q2.yaml`) | `source_settings`: `max_slot_wal_keep_size = -1` (no cap) next to an active slot whose retained WAL grows | `source_create_heartbeat_table` (+ its follow-up config step) |

### 20. `no-replica-identity` — updates fail on the source

**Break (`devctl break no-replica-identity`):** `ALTER TABLE public.orders REPLICA IDENTITY NOTHING` (a migration
copied from a table that is not replicated). `dbz_publication` publishes updates and deletes, so Postgres refuses
every `UPDATE` and `DELETE` on the table; the break proves it with a no-op update (it must fail with "does not have
a replica identity") and proves inserts still stream with a marker order in the warehouse. Refuses when the table
already has `NOTHING`.

**Expected (not yet observed):**
```
app         UPDATE orders ... → ERROR: cannot update table "orders" because it does not have a replica identity and publishes updates
connector   pg-orders-connector: RUNNING, tasks 0=RUNNING; slot active; lag 0
source_table_replication   public.orders  has_primary_key=true  replica_identity=nothing
```

**Fix:** `pg_set_replica_identity` with mode `full` (the dev baseline, `dev/postgres-init/01-orders.sql`; `default`
works too, the table has a primary key). `devctl fix` runs `ALTER TABLE public.orders REPLICA IDENTITY FULL` and
proves an update succeeds; on a fixed stack it skips the `ALTER`.

### 21. `missing-grant` — the connector role cannot read the table

**Break (`devctl break missing-grant`):** the connector moves to a least-privilege role: `CREATE ROLE devctl_cdc
LOGIN REPLICATION`, `CONNECT` on `orders`, `USAGE` on `public` — and no `SELECT` on `public.orders` (the grant
request is still open). The connector is re-registered with `database.user=devctl_cdc`,
`publication.autocreate.mode=disabled` (the DBA owns the publication; with `filtered` the role would also have to
own the table, which is §7) and `snapshot.mode=always`, so every start reads the table. Task 0 restarts and the
break waits for `FAILED`. Refuses when the role already exists.

**Expected (not yet observed):**
```
connector   pg-orders-connector: RUNNING, tasks 0=FAILED  trace: ... permission denied for table orders
slot        debezium_orders active=false
source_role_grants   role devctl_cdc (connector pg-orders-connector config database.user): exists, login, replication, public.orders select=false
watcher     connector_down (sev2), slot_inactive after 30 s
```

**Fix:** `source_grant_select` (SELECT on the pipeline's tables for the connector's `database.user`, nothing else).
`devctl fix` grants it, restarts task 0 and waits for `RUNNING` (the snapshot now succeeds: the grant was the
cause), then puts the baseline back: the connector config from `dev/connectors` (`piperoom`), task 0 `RUNNING`,
an active slot, `DROP OWNED BY devctl_cdc; DROP ROLE devctl_cdc`, and a marker order in the warehouse.

### 22. `blocked-slot` — slot creation waits for an open transaction

**Break (`devctl break blocked-slot`):** an analyst's session opens a transaction and keeps it
(`docker exec -d`, as the role `devctl_analyst` (`LOGIN`, created if missing), `application_name=devctl-blocker`:
`BEGIN; SELECT pg_current_xact_id(); SELECT pg_sleep(1800)`, so it holds an xid for 30 minutes). The role is not the
connector's `database.user` (`piperoom`): `source_end_session` never ends a session of that user. Then the connector is set up again as after a host move: stopped (Connect REST
`stop`), `debezium_orders` dropped, its stored offsets deleted, resumed. Debezium's `CREATE_REPLICATION_SLOT` has
to wait for every transaction that held an xid when it started, so the task never gets past `start()`. The break
waits until a walsender waits on a `transactionid` lock (`pg_stat_activity`). Refuses when the slot is missing or
inactive, or a `devctl-blocker` session is already open.

**Expected (not yet observed):**
```
pg_stat_activity   walsender  wait_event_type=Lock wait_event=transactionid
source_blocking_sessions   pipeline room: pid=… state=active age=… blocks_slot=yes (outside the pipeline's tables; identity hidden)
                           unscoped: pid=… user=devctl_analyst app=devctl-blocker state=active … blocks_slot=yes query="SELECT pg_sleep(1800)"
connector   RUNNING; task 0 RUNNING or UNASSIGNED (start() has not returned) — not observed which
warehouse   no new order after the break
```
The watcher may fire `connector_down` if Connect reports task 0 as not `RUNNING`; the room is opened by
`devctl alert blocked-slot` either way.

**Fix:** `source_end_session` on the blocker (the pid is in `source_blocking_sessions` at Validate and Execute).
`devctl fix` terminates every `devctl-blocker` session and drops the role `devctl_analyst`; the slot creation finishes, Debezium snapshots (no
offsets: every row is re-emitted, the load is an upsert) and streams. It resumes a connector a half-run break left
`STOPPED`, then waits for task 0 `RUNNING`, an active slot, a marker order in the warehouse and order-sync lag 0.

### 23. `idle-wal-growth` — an active slot that confirms nothing

**Needs `OWNERS_FILE=dev/owners.q2.yaml`.** Do not run `devctl load` during this break: any order change lets
Debezium confirm and release the WAL.

**Break (`devctl break idle-wal-growth`):** `public.orders` is idle while another database on the same server
(`devctl_batch`, a nightly batch) writes about 50 MB per batch into `devctl_wal_filler`. Logical decoding is per
database, so Debezium receives nothing, commits no offset and never confirms the WAL: the slot stays **active** and
its retained WAL grows. The break inserts up to four batches until `debezium_orders` retains at least 32 MiB
(`slot_wal.fire`) and checks the slot is still active.

**Expected (not yet observed):**
```
slot        debezium_orders active=true, retained ≥ 32 MiB and growing with every batch
connector   RUNNING; lag 0; warehouse fresh
source_settings   wal_level=logical, max_replication_slots=10, max_wal_senders=10, max_slot_wal_keep_size=-1 (no cap)
watcher     slot_wal after 30 s (fire 32 MiB on dev/owners.q2.yaml)
```

**Fix:** `source_create_heartbeat_table`, then `heartbeat.interval.ms` / `heartbeat.action.query` through the
config action. The dev publication is filtered, so the table must also join `dbz_publication`:
`pg_add_publication_table` accepts only the pipeline's `source_tables`, and the heartbeat action refuses a table
listed there, so in dev that step is `devctl fix`'s own `ALTER PUBLICATION` (in production, the table is added to
`owners.yaml` after it is created, and `pg_add_publication_table` runs then). `devctl fix` creates
`public.devctl_heartbeat (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())`, re-registers the
connector with the table in `table.include.list`, `heartbeat.interval.ms=5000` and `heartbeat.action.query` (an
upsert of row 1), adds the table to `dbz_publication` if Debezium did not, and waits (up to 3 minutes: Connect
commits offsets every 60 s) until retained WAL is below 16 MiB (`slot_wal.clear`). Then the baseline: the
connector config from `dev/connectors`, `devctl_heartbeat` and `devctl_batch` dropped, the topics
`pg.public.devctl_heartbeat` and `__debezium-heartbeat.pg` and the table's subjects deleted. A slot already
below `clear` skips the heartbeat step.

## 24. `stale-source-offsets` — a repointed connector keeps its old offsets (Q5e)

Written on 2026-10-23 **without a running stack**, unit-tested against the same fake
(`go test ./cmd/devctl -run StaleSourceOffsets`); not observed yet.

**Break (`devctl break stale-source-offsets`):** a migration target `devctl_orders_v2` is created with the orders
table and ten restored rows, and `pg-orders-connector` is repointed at it (`database.dbname` and `slot.name`
`devctl_orders_v2`; a logical slot belongs to one database). The name and `topic.prefix` stay, and Kafka Connect
keys a source connector's offsets by connector name and source partition (`{"server": "pg"}`), so the connector
starts with the **old database's** stored LSN: Debezium skips the snapshot of the new database (its offsets say it
is done) and asks the new slot for a position from before the slot existed. The break reads the stored offsets
before (refuses when there are none) and after, and waits for task 0 `FAILED`. If this Debezium version streams
from the new slot instead of failing, the break says so and stops (the ten rows would then never reach Kafka).

**Expected (not yet observed; the failure is the same mechanism as KAN-411 in §9):**
```
connect_source_offsets   pg-orders-connector  partition {"server":"pg"}  offset {"lsn": <the orders database's LSN>, ...}
connector   RUNNING, tasks 0=FAILED  (offset before the new slot's start)
watcher     connector_down (sev2)
```

**Fix:** `connect_stop_connector`, then `connect_reset_source_offsets` to empty, in one proposal; on resume
Debezium snapshots the new database. `devctl fix` stops the connector and deletes its offsets the same way, but
then puts the dev baseline back instead of resuming on the new database: the connector config from
`dev/connectors` (`orders`, `debezium_orders`), `devctl_orders_v2` and its slot dropped, resume with no offsets
(Debezium snapshots `orders`; the load is an upsert), and waits for an active slot, a marker order in the warehouse
and order-sync lag 0. On a healthy stack without `devctl_orders_v2` it does nothing.

## MySQL source: `binlog-purged`, `history-topic-deleted`, `missing-replication-grant`, `server-id-clash` (Q8a)

Four causes of a MySQL pipeline from the Q wave's Stack Overflow analysis (`docs/design/mysql-source.md` §10.1), on
the opt-in `mysql` compose profile: MySQL 8.4 LTS (GTIDs on, ROW binlog, full row images, `server-id=223344`) read by
the Debezium MySQL connector `orders-mysql` on the shared `connect` worker (`database.server.id=5401`,
`topic.prefix=shop`, history topic `schema-history.shop`). Start it next to the default stack:

```
go run ./cmd/devctl up --owner <tag>
docker compose -f dev/docker-compose.yml --profile mysql up -d --wait mysql
```

Every break and fix first checks the profile answers (else it stops with that command) and registers `orders-mysql`
from `dev/connectors/orders-mysql.json` when it is missing, waiting for its snapshot in `shop.shop.orders`. They take
the stack lock like every other break. Rooms need the server on **`OWNERS_FILE=dev/owners.mysql.yaml`** (the
baseline plus pipeline `orders-mysql`, `source_kind: mysql`, layer owner `mysql`), which loads only once Q8d merges.
Users and passwords (dev only): `dev/mysql/README.md`.

Written on 2026-10-23 **without a running stack** (no Docker daemon in that session): the SQL and REST steps are
unit-tested against a fake profile (`go test ./cmd/devctl -run 'MySQL|BinlogPurged|HistoryTopic|MissingReplication|ServerIDClash'`),
but **nothing in §25-§28 has been observed yet**, including the Debezium error texts below (from the Debezium MySQL
connector's messages, unconfirmed on 3.2). Whoever runs them first: replace the "Expected" blocks with what you saw,
and record the profile's start time (the row asks for under 3 minutes). Scenarios label by cause, not by error text.

| Scenario | How the room opens | Tool that shows it | What ends it |
|---|---|---|---|
| `binlog-purged` | watcher: `connector_down` (task 0 FAILED) | `mysql_binlog_status` (Q8c): "the binlog the connector needs is purged" | `connect_stop_connector` + `connect_reset_source_offsets` (to empty): a re-snapshot |
| `history-topic-deleted` | watcher: `connector_down` | `kafka_schema_history` (Q8e): the topic is missing | `connect_update_connector_config` `snapshot.mode=recovery` |
| `missing-replication-grant` | watcher: `connector_down` | `source_role_grants` on MySQL: `REPLICATION SLAVE` false | a DBA's `GRANT` (drafted, no action: design §8.4) |
| `server-id-clash` | alert (`devctl alert server-id-clash`, `source: freshness`): both connectors stay RUNNING | `mysql_binlog_status`: `database.server.id` equals another connector's | `connect_delete_connector` on the shadow, or a new `database.server.id` for it |

### 25. `binlog-purged` — the binlog the connector needs is gone

**Break (`devctl break binlog-purged`):** `orders-mysql` is stopped (Connect REST `stop`: a maintenance window), three
orders are written, `FLUSH BINARY LOGS` runs twice and `PURGE BINARY LOGS TO` the newest file drops every older one,
so the stored file/position and the GTIDs after it are no longer on the server. Resumed, Debezium (default
`snapshot.mode` `initial`) cannot continue and fails. Refuses unless task 0 is `RUNNING`. The log line carries the
stored position and `@@gtid_purged`.

**Expected (not yet observed):**
```
connector   orders-mysql: RUNNING, tasks 0=FAILED  trace: ... The connector is trying to read binlog starting at ..., but this is no longer available on the server ...
            (with GTIDs it may instead say that GTIDs it needs have been purged)
mysql_binlog_status   stored gtids miss gtid_purged: the binlog the connector needs is purged
watcher     connector_down (sev2)
```

**Fix:** re-snapshot. `devctl fix` stops the connector, deletes its stored offsets and `schema-history.shop` (it
describes the old position; the snapshot writes it again), resumes, and waits for task 0 `RUNNING`, the history topic,
and a marker order in `shop.shop.orders`. On a `RUNNING` connector it does nothing.

### 26. `history-topic-deleted` — the schema history topic is missing

**Break (`devctl break history-topic-deleted`):** a topic clean-up deletes `schema-history.shop` while the connector
is stopped. The stored offset says the snapshot is done, so on resume Debezium needs the history to know the table
schemas at that position, finds no topic and fails. Refuses when the topic is already missing or task 0 is not
`RUNNING`.

**Expected (not yet observed):**
```
connector   orders-mysql: RUNNING, tasks 0=FAILED  trace: ... The db history topic is missing. You may attempt to recover it by reconfiguring the connector to recovery
kafka_schema_history   orders-mysql: schema-history.shop does not exist
watcher     connector_down (sev2)
```

**Fix:** `snapshot.mode=recovery` (Debezium 3.x; `schema_only_recovery` before 3.0): the connector rebuilds the
history from the current table schemas and keeps its binlog position. Safe here because no DDL ran since the stored
position; in an incident `kafka_schema_history` cannot prove that, and the action's preview says so. `devctl fix`
stops the connector (and deletes a partial history topic if one exists), sets the mode, resumes, waits for task 0
`RUNNING` and the topic, then puts the `dev/connectors` config back (no `snapshot.mode`) and waits for a marker order.
On a `RUNNING` connector with its history topic it does nothing.

### 27. `missing-replication-grant` — the connector user cannot read the binlog

**Break (`devctl break missing-replication-grant`):** a privilege clean-up runs `REVOKE REPLICATION SLAVE ON *.* FROM
'debezium'@'%'`. A global privilege change reaches a session only when it reconnects, so the break restarts task 0;
the new binlog connection is refused. Refuses unless the grant is there and task 0 is `RUNNING`.

**Expected (not yet observed):**
```
connector   orders-mysql: RUNNING, tasks 0=FAILED  trace: ... Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s) for this operation
source_role_grants   'debezium'@'%': replication_slave=false
watcher     connector_down (sev2)
```

**Fix:** `GRANT REPLICATION SLAVE ON *.* TO 'debezium'@'%'` (Piperoom drafts the GRANT and adds no action for it,
design §8.4), then a task restart. `devctl fix` grants it when missing, resumes or restarts the task, and waits for a
marker order. With the grant and a `RUNNING` task it does nothing.

### 28. `server-id-clash` — two connectors with one `database.server.id`

**Break (`devctl break server-id-clash`):** a second connector `orders-mysql-shadow` (someone's copy to test a
change) is registered with the **same** `database.server.id` (5401) and its own topics (`topic.prefix=shop_shadow`,
history `schema-history.shop-shadow`, `snapshot.mode=no_data`), so only the id clashes. The source keeps one binlog
dump thread per server id and kills the older one whenever the other connects, so the two connectors take turns:
both report `RUNNING` while their streams drop and reconnect. The watcher sees no failed task; open the room with
`devctl alert server-id-clash`. The break waits for the shadow's task `RUNNING`, then up to 60 s for the source's
"zombie dump thread" warning in `performance_schema.error_log` (inserting a marker order every poll); without it the
break still succeeds and logs a warning. Refuses when the shadow exists or `orders-mysql` is not `RUNNING`.

**Expected (not yet observed):**
```
connector   orders-mysql and orders-mysql-shadow: RUNNING, tasks 0=RUNNING (both)
error_log   [Warning] ... found a zombie dump thread with the same UUID/server_id. Source is killing the zombie dump thread(...)
connect log binlog client disconnects and reconnects; shop.shop.orders changes arrive late and in bursts
mysql_binlog_status   database.server.id 5401 of orders-mysql equals orders-mysql-shadow's
```

**Fix:** `connect_delete_connector` on the shadow (or `connect_update_connector_config` with a new
`database.server.id`). `devctl fix` stops the shadow, deletes its offsets, the connector and its topics
(`shop_shadow`, `shop_shadow.shop.*`, `schema-history.shop-shadow`; its Avro subjects stay), then resumes or restarts
`orders-mysql` when needed and waits for a marker order. Without the shadow it only checks `orders-mysql`.
