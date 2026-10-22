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
