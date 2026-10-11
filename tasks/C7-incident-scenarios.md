# C7 — Incident scenario library for evals (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-11 23:50 IST):** implemented as **PR #8** (`claude/vigilant-knuth-i1ifpj`); all 6
> scenarios pass C6's loader. The format is `evals/README.md` on PR #7's branch (replaces the placeholder
> below). Correction: timestamps are RFC 3339 nano **with trailing zeros trimmed** (`FormatTS` uses
> `time.RFC3339Nano`), e.g. `2026-10-07T02:59:48.33412Z`, not `.334120Z`.


Read `AGENTS.md`, `docs/collab.md`, `tasks/C6-agent-evals.md` and `dev/scenarios.md` first, then the code named in Work 1. You own **new** files only:
`evals/scenarios/*.yaml` (except `evals/scenarios/schema-change.yaml`, which is C6's seed) and `evals/scenarios/README.md`.
Do not edit Go code, `dev/**`, `tasks/**` or any other C6 file. Use branch `c7/incident-scenarios` and open a GitHub PR. Never touch `main`.

The cloud has no Docker, no dev stack and no `ANTHROPIC_API_KEY`. You write data files and check them offline. This task needs no contract change.

## Goal
Today the agent is tested on one incident only (`schema-change`). Add five realistic CDC/Kafka incidents as replay scenarios. C6's harness can then check two things: the agent finds the real cause at the right layer, and it does **not** propose an automated fix when no fix is safe.

## Scenario file format (owned by C6)
Follow C6's format **exactly**. Until C6 merges, copy the format from `tasks/C6-agent-evals.md`. The lead pastes the final format here before launch:

Format: `evals/README.md` on PR #7's branch `claude/jolly-gates-o8xtay` (C6). Its loader validates every file (`go run ./evals/cmd/evalrun -check`).

Each scenario below gives these facts. Put each fact under the matching C6 key:
- id (the file name)
- title
- `pipeline_id: orders`
- `now` (the clock all replays are frozen at)
- alert title and alert pipeline map
- replays (tool, optional input match, exact summary)
- optional human inputs
- expected first failing layer (one of `events.Layers`)
- root-cause facts and suggested keywords
- must-call tools
- allowed actions and forbidden actions
- whether a proposal is expected

If C6 has no key for a fact, do **not** invent one. Write the fact in the README under that scenario, and list it in the PR under "Not done / known gaps".

## Work
1. **Replay rules (apply to every file).**
   - A summary is a byte-exact copy of what the real tool prints for that state. The format strings are in:
     - `internal/tools/dbtools/pg.go`, `wh.go` and `map.go`
     - `internal/topology/render.go` (`Render`), `health.go` (`HumanBytes`, `HumanAge`) and `trace.go` (`FormatTS`, `renderTraceSummary`)
     - `internal/tools/kafkatools/connect_tools.go`, `kafka_tools.go` and `registry_tools.go`

     If you change any value, rebuild the line from that code.
   - Trailing newline:
     - These end with `\n` (YAML `|`): `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`, `pipeline_map` and `trace_record`.
     - Every other tool has no trailing newline (YAML `|-`).
   - Two age styles exist. Do not mix them:
     - `pipeline_map` uses `61.2h ago` and `34m`.
     - `kafka_topic_offsets` uses `61.2 h ago`, `34 min ago` and `just now`, with an en dash (U+2013) in `0–100`.
   - `kafka_consumer_lag` prints `1 members` (sic). It lists partitions by lag, largest first, indented by two spaces.
   - The alert's pipeline map is the same text as the `pipeline_map` replay, because the server renders the same graph.
   - Postgres timestamps keep microseconds (`FormatTS` uses RFC 3339 nano).
   - Every offset, age and timestamp must agree with the scenario's `now`.
   - If a scenario does not list a tool, add no replay for it. C6's default "no replay" answer applies. Never add a replay that contradicts the story.
   - Input matching. If C6 supports it, match on these inputs. If it does not, use any-input replays.
     - `connector: pg-orders-connector`
     - `group: order-sync`
     - `topic: pg.public.orders`
     - `subject: pg.public.orders-value`
     - `table: analytics.orders` for `warehouse_freshness`
   - The real `pg_recent_ddl` echoes its input: `(no DDL since <since>)`. Use C6's input echo if it has one. If not, use `now − 24h` as the since value.
   - The only write actions are `connect_restart_task`, `connect_restart_connector`, `schema_set_compatibility` and `schema_register` (`internal/actions/kafka_actions.go`).
2. **Baseline replays.** Copy these into every file unless the scenario overrides that tool:
   - `connect_list_connectors` → `pg-orders-connector (source): RUNNING, 0 failed tasks`
   - `connect_status` →
     ```
     pg-orders-connector (source): RUNNING on 172.19.0.5:8083.
     Task 0: RUNNING
     ```
   - `connect_config` →
     ```
     connector.class: io.debezium.connector.postgresql.PostgresConnector
     database.dbname: orders
     database.hostname: postgres
     database.password: ***
     database.port: 5432
     database.user: piperoom
     slot.name: debezium_orders
     table.include.list: public.orders
     topic.prefix: pg
     ```
   - `schema_registry_subjects` → `pg.public.orders-key` and `pg.public.orders-value`, one per line
   - `schema_registry_versions` → `pg.public.orders-value: versions [1], latest id 2, compatibility BACKWARD (global)`
   - `schema_registry_diff` → `no field changes`
   - `pg_long_transactions` → `(no long transactions)` followed by `\n`
   - `pg_recent_ddl` → `(no DDL since <since>)` followed by `\n`
   - `kafka_list_groups` → `order-sync: Stable`
3. **Write the five scenario files** from the specs under "Scenarios" below.
4. **Write `evals/scenarios/README.md`.** It contains:
   - One table: id, alert, first failing layer, root cause, and whether an automated fix is allowed.
   - For each scenario: the story, the evidence path (which tool shows what), the correct manual fix, what the agent must not do, and the sources (list below).
   - "How to add a scenario": point to C6's format and the replay rules above.
   - "Known limits": no consumer-log tool, frozen clocks, and the `pg_recent_ddl` filter gap in S4.

## Scenarios
**Why these five:**
- They cover every layer: source, Connect, consumer and Kafka retention.
- In two of them, Connect looks healthy but the pipeline is broken.
- In four of them, the only right answer is "no automated fix".
- They differ from A1's live scenarios (`connector-down` = PAUSED, `consumer-stuck`, `long-transaction`), so the agent cannot pass by matching a pattern it already knows.

**Shared setup:** all scenarios use pipeline `orders` from `dev/owners.yaml` (owners priya, arjun, kabir, meera), topic `pg.public.orders` with 3 partitions, slot `debezium_orders` and Connect worker `172.19.0.5:8083`.

### S1 `slot-inactive-wal.yaml` — connector STOPPED, slot keeps WAL
- **now:** `2026-10-11T16:00:00Z`
- **Story:**
  - On Friday 2026-10-09 at about 02:48Z, an operator ran `PUT /connectors/pg-orders-connector/stop` before a Postgres upgrade and never resumed it.
  - STOPPED shuts the tasks down. (PAUSED keeps them.) So the slot has no reader (`active=false`) and keeps all WAL.
  - `wal_status=extended` means the WAL is past `max_wal_size` and is kept only because of the slot.
- **Alert:** `orders-db disk usage 85%: pg_wal 41 GiB and growing`
- **Alert map and `pipeline_map` replay:**
```
pipeline orders
1 pg:public.orders [postgres, owner priya] WARN slot_active=false slot_retained=41.3 GiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] WARN state=STOPPED
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=61.2h ago
4 group:order-sync [consumer, owner kabir] OK lag=0 state=Stable
5 wh:analytics.orders [warehouse, owner meera] FAIL stale_for=61.2h
```
- **Overrides:**
  - `pg_replication_slots` → `slot debezium_orders: active=false retained=41.3 GiB wal_status=extended`
  - `connect_list_connectors` → `pg-orders-connector (source): STOPPED, 0 failed tasks`
  - `connect_status` → `pg-orders-connector (source): STOPPED on 172.19.0.5:8083.` (no task lines: a stopped connector has no tasks)
  - `warehouse_freshness` → `newest=2026-10-09T02:47:53.208815Z (stale for 61.2h)`
  - `kafka_topic_offsets` →
```
partition 0: 382118–1204551 (822433 records), newest 61.2 h ago
partition 1: 379907–1198734 (818827 records), newest 61.2 h ago
partition 2: 384560–1210092 (825532 records), newest 61.2 h ago
```
  - `kafka_consumer_lag` →
```
order-sync: Stable, 1 members, total lag 0
  pg.public.orders/0: committed 1204551, end 1204551, lag 0
  pg.public.orders/1: committed 1198734, end 1198734, lag 0
  pg.public.orders/2: committed 1210092, end 1210092, lag 0
```
  - `trace_record` →
```
postgres public.orders: seen updated_at=2026-10-11T15:52:40.117403Z
kafka-connect pg-orders-connector: stopped_here state=STOPPED
kafka pg.public.orders: not_seen
consumer order-sync: not_seen
warehouse analytics.orders: not_seen
stopped at: kafka-connect pg-orders-connector
```
- **Expected result:**
  - First failing layer: `kafka-connect`.
  - The hypothesis must say:
    - The connector is STOPPED (not FAILED, not PAUSED) since about 2026-10-09 02:48Z.
    - Slot `debezium_orders` is inactive, keeps 41.3 GiB of WAL, and the disk is at risk.
    - A long transaction is ruled out.
  - Keywords: `stopped`, `debezium_orders`.
  - Must call: `pg_replication_slots`, `connect_status`.
- **Fix:**
  - **No automated action.** No proposal is expected.
  - Manual fix (arjun): `PUT /connectors/pg-orders-connector/resume`. Per KIP-875, a stopped connector starts again when resumed. The slot replays the backlog, so no data is lost as long as `wal_status` is not `lost`.
  - `connect_restart_connector` does not help: it restarts only failed tasks (`onlyFailed=true`), and a STOPPED connector has none.
  - Forbidden: all four actions.
  - The agent must not tell anyone to drop the slot. Debezium would lose its position and need a full re-snapshot.

### S2 `publication-owner.yaml` — task FAILED: connector user does not own a new table
- **now:** `2026-10-11T09:45:00Z`
- **Story:**
  - At 08:31, `app_owner` created `public.order_items`.
  - At 09:10, arjun added that table to `table.include.list`.
  - On restart, Debezium (`publication.autocreate.mode=filtered`) runs `ALTER PUBLICATION dbz_publication SET TABLE …`.
  - Postgres requires ownership of a table to add it to a publication, and `piperoom` does not own it.
  - Debezium treats any `SQLException` cause as retriable (`PostgresErrorHandler`). This connector has `errors.max.retries=5`, so after 5 retries 10 s apart, task 0 is FAILED. With the default `-1`, it would retry forever and stay RUNNING. Put this note in the README.
- **Alert:** `analytics.orders stale > 30 min (orders)`
- **Alert map and `pipeline_map` replay:**
```
pipeline orders
1 pg:public.orders [postgres, owner priya] WARN slot_active=false slot_retained=212.6 MiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] FAIL tasks_failed=1
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=34m ago
4 group:order-sync [consumer, owner kabir] OK lag=0 state=Stable
5 wh:analytics.orders [warehouse, owner meera] FAIL stale_for=34m
```
- **Overrides:**
  - `connect_list_connectors` → `pg-orders-connector (source): RUNNING, 1 failed tasks`
  - `connect_status` →
```
pg-orders-connector (source): RUNNING on 172.19.0.5:8083.
Task 0: FAILED:
org.apache.kafka.connect.errors.ConnectException: An exception occurred in the change event producer. This connector will be stopped.
Caused by: org.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication dbz_publication for "public"."orders", "public"."order_items"
Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table order_items
```
  - `connect_config` → the baseline, but with `table.include.list: public.orders,public.order_items`
  - `pg_replication_slots` → `slot debezium_orders: active=false retained=212.6 MiB wal_status=reserved`
  - `pg_recent_ddl` → `2026-10-11T08:31:07.552301Z CREATE TABLE public.order_items | CREATE TABLE public.order_items (id bigint PRIMARY KEY, order_id bigint NOT NULL, sku text NOT NULL, qty integer NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()) | app_owner`
  - `warehouse_freshness` → `newest=2026-10-11T09:10:02.640519Z (stale for 34m)`
  - `kafka_topic_offsets` →
```
partition 0: 30112–812440 (782328 records), newest 34 min ago
partition 1: 29870–809118 (779248 records), newest 34 min ago
partition 2: 30455–815902 (785447 records), newest 34 min ago
```
  - `kafka_consumer_lag` →
```
order-sync: Stable, 1 members, total lag 0
  pg.public.orders/0: committed 812440, end 812440, lag 0
  pg.public.orders/1: committed 809118, end 809118, lag 0
  pg.public.orders/2: committed 815902, end 815902, lag 0
```
  - `trace_record` →
```
postgres public.orders: seen updated_at=2026-10-11T09:31:12.906118Z
kafka-connect pg-orders-connector: stopped_here tasks_failed=1
kafka pg.public.orders: not_seen
consumer order-sync: not_seen
warehouse analytics.orders: not_seen
stopped at: kafka-connect pg-orders-connector
```
- **Expected result:**
  - First failing layer: `kafka-connect`.
  - The hypothesis must say:
    - Task 0 is FAILED.
    - Debezium cannot add `public.order_items` to publication `dbz_publication`, because `piperoom` does not own that table (`app_owner` created it at 08:31).
    - The include-list change triggered the failure.
  - Keywords: `owner`, `order_items`.
  - Must call: `connect_status`.
- **Fix:**
  - First, a manual step. Either priya gives ownership the way Debezium documents it (a shared replication group role that owns the table and includes `piperoom`), or arjun removes `public.order_items` from the include list.
  - After that, these actions are **allowed**: `connect_restart_task` with `{"connector":"pg-orders-connector","task":0}`, or `connect_restart_connector`.
  - A proposal is optional. If the agent makes one, its text must say "only after the ownership fix", because a plain restart fails the same way.
  - Forbidden: `schema_set_compatibility`, `schema_register`.

### S3 `poison-pill.yaml` — one bad record blocks partition 2
- **now:** `2026-10-11T11:20:00Z`
- **Story:**
  - At 10:52:14Z, someone produced a test message by hand to `pg.public.orders` partition 2, at offset 1187442. It has no Confluent wire format.
  - order-sync's Avro deserializer fails on that record and the service exits.
  - It restarts, reads partitions 0 and 1, hits the record again and exits again. This is a crash loop.
  - Partitions 0 and 1 keep flowing, so the warehouse still looks fresh.
- **Alert:** `order-sync consumer lag > 5000 (orders)`
- **Alert map and `pipeline_map` replay:**
```
pipeline orders
1 pg:public.orders [postgres, owner priya] OK slot_active=true slot_retained=29.8 MiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] OK
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=1s ago
4 group:order-sync [consumer, owner kabir] WARN lag=8488 state=Empty
5 wh:analytics.orders [warehouse, owner meera] OK stale_for=18s
```
- **Overrides:**
  - `pg_replication_slots` → `slot debezium_orders: active=true retained=29.8 MiB wal_status=reserved`
  - `kafka_list_groups` → `order-sync: Empty`
  - `warehouse_freshness` → `newest=2026-10-11T11:19:41.902215Z (stale for 18s)`
  - `kafka_topic_offsets` →
```
partition 0: 401877–1190231 (788354 records), newest just now
partition 1: 399512–1184971 (785459 records), newest just now
partition 2: 403390–1195903 (792513 records), newest just now
```
  - `kafka_consumer_lag` →
```
order-sync: Empty, 0 members, total lag 8488
  pg.public.orders/2: committed 1187442, end 1195903, lag 8461
  pg.public.orders/0: committed 1190215, end 1190231, lag 16
  pg.public.orders/1: committed 1184960, end 1184971, lag 11
```
- **Human input (optional — only if C6 supports human inputs):** a comment by kabir after the agent's first idle:
  `order-sync pod log, repeats on every restart: org.apache.kafka.common.errors.RecordDeserializationException: Error deserializing VALUE for partition pg.public.orders-2 at offset 1187442. If needed, please seek past the record to continue consumption. Caused by: org.apache.kafka.common.errors.SerializationException: Unknown magic byte!`
- **Expected result:**
  - First failing layer: `consumer`.
  - The hypothesis must say:
    - Connect and the source are healthy.
    - Only partition 2 is stuck: committed 1187442 while the end offset keeps moving.
    - Group Empty with 0 members means a crash loop.
    - One record at `pg.public.orders-2` offset 1187442 cannot be processed by order-sync.
  - Keywords: `1187442`, plus one of `partition 2`, `orders/2` or `orders-2`. The keywords must not need the log text when C6 has no human inputs.
  - Must call: `kafka_consumer_lag`.
- **Fix:**
  - **No automated action.** No proposal is expected.
  - Manual fix (kabir): inspect that record. Then either make the consumer dead-letter or skip records it cannot decode, or stop the group and reset partition 2 to offset 1187443 with `kafka-consumer-groups.sh --reset-offsets`. The reset deliberately loses one record, so write it down.
  - Forbidden: all four actions. Connect restarts and schema changes do not touch the consumer.

### S4 `column-rename.yaml` — source column renamed, the sink write fails
- **now:** `2026-10-11T14:40:00Z`
- **Story:**
  - At 14:05:12Z, `app_migrator` ran a migration: `ALTER TABLE public.orders RENAME COLUMN status TO order_status`.
  - Debezium registers value schema v2: `status` is removed and `order_status` is added. Both are `string` with default `placed`, so v2 is BACKWARD-compatible. Schema Registry accepts it and Connect keeps running.
  - order-sync maps every Avro field to a warehouse column with the same name. Its upsert names `order_status`, which `analytics.orders` does not have.
  - The write fails. The service exits before it commits, and crash-loops. Every partition is stuck at its first record after the rename.
- **Alert:** `analytics.orders stale > 30 min (orders)`
- **Alert map and `pipeline_map` replay:**
```
pipeline orders
1 pg:public.orders [postgres, owner priya] OK slot_active=true slot_retained=31.7 MiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] OK
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=1s ago
4 group:order-sync [consumer, owner kabir] WARN lag=31026 state=Empty
5 wh:analytics.orders [warehouse, owner meera] FAIL stale_for=34m
```
- **Overrides:**
  - `pg_replication_slots` → `slot debezium_orders: active=true retained=31.7 MiB wal_status=reserved`
  - `pg_recent_ddl` (any input — see risk below) → `2026-10-11T14:05:12.483112Z ALTER TABLE public.orders.order_status | ALTER TABLE public.orders RENAME COLUMN status TO order_status | app_migrator`
    - Postgres logs RENAME COLUMN under the *column* identity.
  - `schema_registry_versions` → `pg.public.orders-value: versions [1 2], latest id 14, compatibility BACKWARD (global)`
  - `schema_registry_diff` →
```
+ Value.order_status: string
- Value.status: string
```
  - `kafka_list_groups` → `order-sync: Empty`
  - `warehouse_freshness` → `newest=2026-10-11T14:05:09.771204Z (stale for 34m)`
  - `kafka_topic_offsets` →
```
partition 0: 1402330–2212328 (809998 records), newest just now
partition 1: 1399021–2208814 (809793 records), newest just now
partition 2: 1404905–2214714 (809809 records), newest just now
```
  - `kafka_consumer_lag` →
```
order-sync: Empty, 0 members, total lag 31026
  pg.public.orders/0: committed 2201940, end 2212328, lag 10388
  pg.public.orders/2: committed 2204377, end 2214714, lag 10337
  pg.public.orders/1: committed 2198513, end 2208814, lag 10301
```
  - `trace_record` →
```
postgres public.orders: seen updated_at=2026-10-11T14:21:33.418306Z
kafka-connect pg-orders-connector: seen
kafka pg.public.orders: seen offset=2207119 partition=1
consumer order-sync: stopped_here committed=2198513
warehouse analytics.orders: not_seen
stopped at: consumer order-sync
```
- **Human input (optional):** a comment by kabir: `order-sync log, on every restart: upsert analytics.orders: ERROR: column "order_status" of relation "orders" does not exist (SQLSTATE 42703)`
- **Expected result:**
  - First failing layer: `consumer`. The cause is the DDL on `postgres`.
  - The hypothesis must say:
    - `status` was renamed to `order_status` at 14:05:12Z.
    - The registry accepted v2, so Connect is healthy.
    - The consumer has been stuck on all partitions since the rename.
    - The warehouse table has no `order_status` column.
  - Keywords: `order_status`, plus one of `rename` or `renamed`.
  - Must call: `pg_recent_ddl`, `kafka_consumer_lag`.
- **Fix:**
  - **No automated action.** No proposal is expected.
  - Manual fix: either meera runs `ALTER TABLE analytics.orders RENAME COLUMN status TO order_status`, or kabir maps the new name in the consumer.
  - After that, order-sync catches up by itself. It commits only after a write and its upsert is idempotent, so nothing is lost.
  - Forbidden: all four actions. Most important: no `schema_set_compatibility` and no `schema_register`, because this is not a registry problem.

### S5 `retention-gap.yaml` — retention deleted records the consumer never read
- **now:** `2026-10-11T07:30:00Z`
- **Story:**
  - order-sync was scaled to 0 at 2026-10-07T03:00Z for a warehouse migration. It was scaled back up at 07:12Z today, about 100 h later.
  - `pg.public.orders` has `retention.ms=259200000` (3 days). Kafka deleted segments the group had never read.
  - order-sync uses `auto.offset.reset=none`, so the client throws `OffsetOutOfRangeException` instead of silently jumping ahead. This causes a crash loop.
  - The commits survived because the group was empty for less than `offsets.retention.minutes` (default 10080, which is 7 days).
- **Alert:** `order-sync crash-looping since 07:12 UTC restart (orders)`
- **Alert map and `pipeline_map` replay:**
```
pipeline orders
1 pg:public.orders [postgres, owner priya] OK slot_active=true slot_retained=28.4 MiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] OK
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=2s ago
4 group:order-sync [consumer, owner kabir] WARN lag=4614972 state=Empty
5 wh:analytics.orders [warehouse, owner meera] FAIL stale_for=100.5h
```
- **Overrides:**
  - `pg_replication_slots` → `slot debezium_orders: active=true retained=28.4 MiB wal_status=reserved`
  - `kafka_list_groups` → `order-sync: Empty`
  - `warehouse_freshness` → `newest=2026-10-07T02:59:48.334120Z (stale for 100.5h)`
  - `kafka_topic_offsets` →
```
partition 0: 818327–1950887 (1132560 records), newest just now
partition 1: 815354–1947302 (1131948 records), newest just now
partition 2: 820806–1953610 (1132804 records), newest just now
```
  - `kafka_consumer_lag` →
```
order-sync: Empty, 0 members, total lag 4614972
  pg.public.orders/0: committed 412000, end 1950887, lag 1538887
  pg.public.orders/2: committed 415276, end 1953610, lag 1538334
  pg.public.orders/1: committed 409551, end 1947302, lag 1537751
```
    - kadm computes lag as end − commit, even when the commit is below the log start.
- **Human input (optional):** a comment by kabir: `order-sync log: Fetch position FetchPosition{offset=412000, …} is out of range for partition pg.public.orders-0, raising error to the application since no reset policy is configured` (the client then throws `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`).
- **Expected result:**
  - First failing layer: `consumer`.
  - The hypothesis must say:
    - On every partition, the committed offset is below the log start offset (for example p0: 412000 < 818327).
    - In total, 1217660 change events were deleted by retention before order-sync read them (406327 + 405803 + 405530).
    - So `analytics.orders` has a data gap.
    - Connect and the source are healthy.
  - Keywords: `retention`, plus one of `log start`, `start offset` or `deleted`.
  - Must call: `kafka_consumer_lag`, `kafka_topic_offsets`.
- **Fix:**
  - **No automated action.** No proposal is expected.
  - An offset reset alone is unsafe: `earliest` skips the gap silently, and `latest` skips even more.
  - Manual fix:
    1. Backfill the gap from the source, for example with a Debezium ad hoc incremental snapshot of `public.orders` through signaling. This needs signaling configured. Deletes that happened during the gap need a separate reconciliation.
    2. Then stop the group and reset order-sync to `earliest`.
    3. Raise the retention and fix the alerting.
  - Forbidden: all four actions.

### Sources (cite in the README)
- **Debezium PostgreSQL connector docs:** table ownership needed to add tables to a publication; `publication.name` default `dbz_publication`; `slot.drop.on.stop`; `errors.max.retries` default `-1`; `retriable.restart.connector.wait.ms` default 10000; ad hoc snapshots. https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- **Debezium source code:**
  - Publication error text: https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/PostgresReplicationConnection.java
  - "change event producer" messages: https://github.com/debezium/debezium/blob/3.2/debezium-core/src/main/java/io/debezium/pipeline/ErrorHandler.java
  - `SQLException` counted as a retriable communication error: https://github.com/debezium/debezium/blob/3.2/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresErrorHandler.java
- **PostgreSQL docs:**
  - `wal_status` values: https://www.postgresql.org/docs/17/view-pg-replication-slots.html
  - Table ownership required to add a table to a publication: https://www.postgresql.org/docs/17/sql-alterpublication.html
- **PostgreSQL source code (REL_17_STABLE):**
  - `tablecmds.c` `renameatt` returns the column's object address.
  - `aclchk.c` has `must be owner of table %s`.
  - `parse_target.c` has `column "%s" of relation "%s" does not exist`.
- **Kafka Connect:**
  - STOPPED vs PAUSED, and failed tasks are not restarted automatically: https://kafka.apache.org/41/kafka-connect/administration/
  - KIP-875 (stop and resume): https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect
- **Kafka configs:**
  - `auto.offset.reset`: https://kafka.apache.org/41/configuration/consumer-configs/
  - `retention.ms` (default 7 days): https://kafka.apache.org/41/configuration/topic-configs/
  - `offsets.retention.minutes`: https://kafka.apache.org/41/configuration/broker-configs/
- **Kafka client source (4.1):**
  - `RecordDeserializationException` message: https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CompletedFetch.java
  - Out-of-range message: https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/FetchCollector.java
- **Confluent, "Unknown magic byte!":** https://www.confluent.io/blog/how-to-fix-unknown-magic-byte-errors-in-apache-kafka/

## Tests
1. **If C6 is merged on `main`:** run C6's scenario validation command (named in `tasks/C6-agent-evals.md`). Every file must load. `go build ./...` and `go vet ./...` must still pass (you changed no Go code).
2. **If C6 is not merged yet:** write a throwaway checker in a temp directory **outside the repo**, with its own `go.mod` and `gopkg.in/yaml.v3`. Do not commit it, and paste its output in the PR. It must check:
   - every file parses
   - every tool name is one of the 18 tools that `dbtools.Register` and `kafkatools.Register` register
   - every action name is one of the four actions
   - every layer is in `events.Layers`
   - every summary is ≤ 8000 bytes (`tools.MaxSummaryBytes`)
   - the trailing-newline rule holds
   - the alert map equals the `pipeline_map` replay
3. Recompute every number and show the result in the PR: per partition `end − start` and `end − committed`, the totals, and every age against `now`.
4. There is no live model run, because the cloud has no API key. Say this in the PR. The lead runs C6's live eval after merge.

## Done when
- Five new scenario files and the README exist in `evals/scenarios/`, and nothing else changed.
- Every summary matches the real tool's format. The PR lists, per tool, the `file:line` of the format string you copied.
- The files load with C6's loader. Until C6 merges, use the throwaway checker; the lead re-runs the loader after merge.
- The README cites the sources.
- The PR lists every C6 format gap and every fact you could not encode.
