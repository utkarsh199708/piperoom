# Write roles for approved fixes

Piperoom is **read-only by default**: the agent proposes fixes, people approve them, but a fully
approved fix never runs until you turn write mode on. This page lists every write call of the P3 fix
actions and the connector lifecycle actions, except the four source admin actions in [source-admin.md](source-admin.md), with the exact,
minimal credential for each, for the day you set `PIPEROOM_WRITE_MODE=on`. The read-only grants are in
[permissions.md](permissions.md); keep them too. The switch stops the executor, it does not take write
rights away from a role that has them (`cmd/server/writemode.go`). Grant exactly what is below and nothing
more, on **dedicated** service accounts, never a human or application credential.

## The two settings

Only two variables turn writes on. Both are read through `internal/config/config.go`.

| Variable | Default | What it does |
|---|---|---|
| `PIPEROOM_WRITE_MODE` | `off` | `off`: no write client is constructed at all, an approved proposal is recorded but never runs, and the room shows `blocked: "write_mode_off"`. `on`: the write clients below are built. Any other value stops the start (`ParseWriteMode`). One switch per process. |
| `SOURCE_DB_WRITE_URL` | empty | A Postgres URL for the source database as a **separate write role**. Empty = no source writer: the `pg_*` actions still validate and preview, but `Execute` refuses with "no write credentials". Wiring refuses a value that logs in as the read-only `SOURCE_DB_URL` role (`internal/wiring/writemode.go` `checkSourceWriteURL`). |

The Kafka and Connect write clients reuse the same `KAFKA_*` and `CONNECT_*` connection settings as the
readers; there is no separate write URL for them.

## Source Postgres (`SOURCE_DB_WRITE_URL`)

The source writer is a second pool with its own role (`internal/clients/postgres/writer.go`). It refuses a
role whose sessions start read-only (`default_transaction_read_only = on`) and a hot standby. It caps the
pool at 2 connections and applies `lock_timeout` (5 s) and `statement_timeout` (10 s) inside each write
transaction.

Three actions write through it (layer `postgres`); the four source admin actions of the same pool
(`pg_set_replica_identity`, `source_grant_select`, `source_end_session`, `source_create_heartbeat_table`)
are in [source-admin.md](source-admin.md):

| Action | SQL | Minimal grant | Code |
|---|---|---|---|
| `pg_add_publication_table` | `ALTER PUBLICATION <publication.name> ADD TABLE <schema.table>` | Own the publication **and** own the table (PostgreSQL requires both for `ADD TABLE`; only a superuser can skip it). There is no `GRANT` for publications. | `internal/actions/pgactions/add_publication_table.go`; `internal/clients/postgres/writer.go` (`AddPublicationTables`) |
| `pg_drop_replication_slot` | `SELECT pg_drop_replication_slot($1)` | Superuser, or the `REPLICATION` attribute. Slot ownership alone is not enough: PostgreSQL 17 restricts the replication-slot functions to superusers and roles with `REPLICATION`, so no slot-owner-only grant works. The action drops only the connector's inactive logical slot. | `internal/actions/pgactions/drop_replication_slot.go`; `internal/clients/postgres/writer_dropslot.go`, `writer.go` |
| `pg_snapshot_signal` | `INSERT INTO <signal.data.collection> (id, type, data) VALUES ($1, 'execute-snapshot', $2)` | `INSERT` on the signal table only (plus `USAGE` on its schema and `CONNECT` on the database). | `internal/actions/pgactions/snapshot_signal.go`; `internal/clients/postgres/writer.go` (`InsertSnapshotSignal`) |

Role template:

```sql
CREATE ROLE piperoom_writer LOGIN PASSWORD '<generated, from your secret store>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 2;
GRANT CONNECT ON DATABASE <db> TO piperoom_writer;

-- pg_add_publication_table: the role must own the publication and each table it may add.
ALTER PUBLICATION <publication.name> OWNER TO piperoom_writer;
ALTER TABLE <schema>.<table> OWNER TO piperoom_writer;   -- repeat per source table

-- pg_drop_replication_slot: superuser or the REPLICATION attribute. There is no
-- slot-owner-only grant: PostgreSQL 17 restricts pg_drop_replication_slot to superusers
-- and roles with REPLICATION, so owning the slot (or being a member of its owner) is
-- not enough on its own.
ALTER ROLE piperoom_writer REPLICATION;

-- pg_snapshot_signal: INSERT on the signal table only.
GRANT USAGE ON SCHEMA <signal_schema> TO piperoom_writer;
GRANT INSERT ON <signal_schema>.<signal_table> TO piperoom_writer;
```

Notes:

- **Table ownership is the real cost of `pg_add_publication_table`.** An owner can grant itself any
  privilege on the table, so ownership is effectively full control of that one table. If that is too much,
  leave `pg_add_publication_table` ungranted (it will refuse at `Execute`) and let the DBA run the one-off
  `ALTER PUBLICATION ... ADD TABLE` by hand. Owning the publication alone is not enough: PostgreSQL requires
  owning the table too (the `ALTER PUBLICATION` documentation). The action never uses `FOR ALL TABLES` or
  `ADD TABLES IN SCHEMA` (the latter is superuser-only); it refuses a `FOR ALL TABLES` publication.
- **`REPLICATION` is broad.** It also lets the role create slots and stream through the replication
  protocol, which means it can decode every data change in the database (all of the WAL), whatever its
  table grants. There is no narrower slot-ownership grant: PostgreSQL 17 restricts `pg_drop_replication_slot`
  (and the other replication-slot functions) to superusers and roles with the `REPLICATION` attribute, so
  owning the slot, or being a member of its owner, does not by itself permit the call. The slot is usually
  owned by Debezium's own replication user.
- **Do not grant table or schema `SELECT` to the writer**, except the `SELECT ... WITH GRANT OPTION` that
  `source_grant_select` needs on the captured tables ([source-admin.md](source-admin.md)). It needs no
  other read, and it must not read data the read-only role cannot. The writer's re-reads (`pg_publication`,
  `pg_publication_tables`, `pg_replication_slots`) are readable by any role.
- The signal table (`signal.data.collection`) must already exist and be in the publication; creating it and
  putting it in the publication is a DBA step, not one of these grants. `pg_snapshot_signal` refuses a signal
  table that is also a source table (`internal/actions/pgactions/snapshot_signal.go`).

## Kafka Connect

Connect's REST API usually has no per-endpoint authorization, so scope it with the network and, where
available, a proxy. Two new write calls are added on top of the four in [permissions.md](permissions.md#kafka-connect):

| Action (layer `kafka-connect`) | HTTP call | Minimal grant | Code |
|---|---|---|---|
| `connect_pause_connector` | `PUT /connectors/{name}/pause` | allow the call on the pipeline's connectors only | `internal/clients/connect/fix.go` (`PauseConnector`); `internal/actions/connect_pause.go` |
| `connect_update_connector_config` (N keys, one approval) | `PATCH /connectors/{name}/config` with one or more allow-listed keys | allow the call on the pipeline's connectors only | `internal/clients/connect/fix.go` (`UpdateConnectorConfigKeys`); `internal/actions/connector_config.go` |

`connect_update_connector_config` needs **Kafka Connect 3.5 or later** (KIP-477 added `PATCH` on config).
An older Connect answers 405 and the action reports it; there is no `PUT` fallback
(`internal/clients/connect/fix.go`; #614).

In write mode, add exactly these to the Connect allow-list alongside the four existing calls:

- `PUT /connectors/{name}/pause`
- `PATCH /connectors/{name}/config`

The N-key form may change only the allow-listed keys and values of `connect_update_connector_config`
(`defaultConnectorConfigAllowList` in `internal/actions/connector_config.go`), and it never carries a
secret: a credential-looking key or value is refused by `looksSecret`/`valueLooksSecret`. Connect restarts
the connector's tasks when its config changes, so the update is not free.

### Connector lifecycle and source offsets (Q5a, Q5b, Q5c)

Four more write actions stop a connector, clear or move a source connector's stored offsets, and create or delete
a connector (`RegisterLifecycle` in `internal/actions/connect_lifecycle.go`). Wiring registers them in both write
modes (`registerLifecycle` in `internal/wiring/actions_q5.go`, row Q5d, P0 ruling #606): with write mode off they
hold a read-only Connect value, so the agent can propose and preview them with the read calls below, and `Execute`
refuses before any Connect call because no lifecycle writer exists. The write calls below are made only with
`PIPEROOM_WRITE_MODE=on`, after the approvals. The read tool `connect_source_offsets` is live today on a Connect client
that implements the offsets read (`internal/tools/kafkatools/kafkatools.go`, the `connect.SourceOffsetReader`
check), so its read call belongs in the read-only allow-list ([permissions.md](permissions.md#kafka-connect)).

Every call below, with the Kafka Connect version that first serves it. The versions of the KIP-875 calls come from
the Apache Kafka documentation: the [3.5 Connect REST reference](https://kafka.apache.org/35/documentation.html#connect_rest)
documents `GET /connectors/{name}/offsets` and `PUT /connectors/{name}/stop`, the
[3.6 reference](https://kafka.apache.org/36/documentation.html#connect_rest) adds `DELETE` and `PATCH` on
`/connectors/{name}/offsets` and requires a `STOPPED` connector for both (the constants `kip875ReadVersion` and
`kip875ModifyVersion` and the comment above them in `internal/clients/connect/lifecycle.go`; the known-issues
entries `connect-offsets-read-api` and `connect-offsets-alter-api` in
`internal/tools/runbooks/library/known-issues/plugin-mismatch.yaml`). The design is
[KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect).

| HTTP call | Kafka Connect | Client method | Used by | Code |
|---|---|---|---|---|
| `GET /` | any (Connect's root resource; Piperoom checks no version) | `ServerInfo` | `connect_source_offsets`, only to name the running version when the offsets endpoint is missing | `lifecycle.go`; `internal/tools/kafkatools/connect_offsets.go` (`unsupportedNote`) |
| `GET /connectors/{name}/offsets` | **3.5+** | `SourceOffsets` | `connect_source_offsets`; `connect_reset_source_offsets` at proposal (`Validate`), in `Preview` (the stored offsets are the rollback), before and after the write in `Execute` | `lifecycle.go`; `internal/actions/connect_stop_reset.go` (`readStoredOffsets`, `afterWrite`) |
| `PUT /connectors/{name}/stop` | **3.5+** | `StopConnector` | `connect_stop_connector` | `lifecycle.go`; `connect_stop_reset.go` (`stopConnector.Execute`) |
| `DELETE /connectors/{name}/offsets` | **3.6+**, connector `STOPPED` | `ResetSourceOffsets` | `connect_reset_source_offsets` with no position (clear every offset) | `lifecycle.go`; `connect_stop_reset.go` (`resetSourceOffsets.Execute`) |
| `PATCH /connectors/{name}/offsets` | **3.6+**, connector `STOPPED` | `AlterSourceOffsets` | `connect_reset_source_offsets` with a position; Piperoom sends one `{partition, offset}` entry (the client caps a request at 1000 entries and 1 MiB) | `lifecycle.go` (`maxOffsetEntries`, `maxAlterBodyBytes`); `connect_stop_reset.go` |
| `PUT /connector-plugins/{class}/config/validate` | older than KIP-875; Piperoom checks no version | `ValidateConfig` | `connect_create_connector` in `Preview` and again right before the write; it creates nothing | `internal/clients/connect/connect.go`; `internal/actions/connect_create_delete.go` (`createConnector.validate`) |
| `POST /connectors` (body `{"name", "config"}`) | older than KIP-875; Piperoom checks no version | `CreateConnector` | `connect_create_connector` | `lifecycle.go`; `connect_create_delete.go` (`createWithRetry`) |
| `DELETE /connectors/{name}` | older than KIP-875; Piperoom checks no version | `DeleteConnector` | `connect_delete_connector` | `lifecycle.go`; `connect_create_delete.go` (`deleteWithRetry`) |
| `PUT /connectors/{name}/resume` | resuming a **`STOPPED`** connector needs 3.5+, since that state comes with KIP-875 | `Resume` | the undo of `connect_stop_connector` is a `connect_resume_connector` proposal (already in the allow-list above) | `connect.go`; `connect_stop_reset.go` (`stopConnector.Preview`, `Rollback`) |

The actions also read with calls already in the read-only allow-list: `GET /connectors` (does the connector exist;
`listed` in `connect_lifecycle.go`), `GET /connectors/{name}/status` (state and type; the stop polls it until
`STOPPED`) and `GET /connectors/{name}/config` (the delete's plaintext-credential check, the create's host check
over the pipeline's other connectors and its read-back of the created config; `connect_create_delete.go`).
`connect_reset_source_offsets`' preview may also ask the source Postgres for a planner row estimate of the tables a
following snapshot would read, a read on the existing read role (`tableEstimate` in `connect_stop_reset.go`).

What an older or busy Connect answers, and what Piperoom makes of it (`lifecycle.go`):

- **Endpoint missing.** A `405`, or a `404` while `GET /connectors/{name}` finds the connector, becomes
  `ErrNotSupported` with "Kafka Connect 3.5 (or 3.6) or later (KIP-875) adds this endpoint"; a `404` for a connector
  that does not exist stays "not found" (`kip875Error`). `connect_source_offsets` adds the version the worker reports
  on `GET /`. On a 3.5 worker a reset therefore passes the proposal check (it can read the offsets) and fails at
  execution, naming 3.6.
- **Not stopped.** Connect 3.6 to 4.2 answer `400` "Connectors must be in the STOPPED state before their offsets can
  be modified"; that, or a `409` that is not a rebalance, becomes `ErrConnectorNotStopped` (`notStopped`). The action
  asks for a `connect_stop_connector` step earlier in the same proposal with `stop_first: true` (`checkStopStep`,
  `requireStoppedNow`, `resetWriteErr` in `connect_stop_reset.go`).
- **Rebalance.** A `409` whose message names a rebalance becomes `ErrRebalancing`, a retry, never "already exists"
  (`rebalancing`, `createError`). Create and delete retry it up to three times inside a 25 s write budget
  (`writeCtx` in `connect_lifecycle.go`; `createWithRetry`, `deleteWithRetry`); every single request is bounded at
  10 s (`writeTimeout` in `internal/clients/connect/fix.go`).
- **Name taken.** Any other `409` on `POST /connectors` is `ErrConnectorExists`.
- **Error bodies.** Connect echoes request values in its errors, so a body is redacted, stripped of unprintable
  characters and cut to 512 bytes before it reaches an error, a log or the model; the create config is never logged
  or put in a provenance receipt, which holds the method and path only (`safeErr`, `sanitizeBody`, `createError` in
  `lifecycle.go`; `doBody` in `connect.go`).

Every lifecycle write refuses a connector outside the room's pipeline before any request (`checkWriteScope` in
`internal/clients/connect/fix.go`, called by each method in `lifecycle.go`).

When Q5d registers them, add exactly these to the Connect proxy allow-list, on the pipeline's connectors only:

- `PUT /connectors/{name}/stop`
- `DELETE /connectors/{name}/offsets` and `PATCH /connectors/{name}/offsets`
- `PUT /connector-plugins/{class}/config/validate` (read-only, but it is a `PUT`, so a method-based proxy rule must
  allow it), limited to the plugin classes your pipelines use if the proxy can
- `POST /connectors` and `DELETE /connectors/{name}`, only if you want Piperoom to create and delete connectors at
  all; the stop and reset actions do not need them

`POST /connectors` carries the name in the body, not the path, so a path-based proxy rule cannot hold it to the
pipeline's names; Piperoom's own checks do (the name must be one of the pipeline's `connectors` in `owners.yaml`
and absent; `createConnector.CheckScope`, `Validate` in `connect_create_delete.go`).

## Kafka

The Kafka write client (`internal/clients/kafka/writer.go`) uses its own connections but the same principal
and security settings as the reader (`KAFKA_*`). Add these to the read ACLs in [permissions.md](permissions.md#kafka):

| Action | Kafka write | Minimal ACL | Code |
|---|---|---|---|
| `kafka_reset_consumer_offsets` (layer `consumer`) | `OffsetCommit` (`kadm.CommitOffsets`) | `Read` on the specific consumer group | `internal/clients/kafka/writer.go` (`ResetGroupOffsets`); `internal/actions/kafka_reset_offsets.go` |
| `dlq_replay` (layer `kafka`) | produce to the pipeline's CDC topics | `Write` on `<topic_prefix>.<source_table>` | `internal/clients/kafka/writer.go` (`Produce`); `internal/actions/dlq_replay.go` |
| `connect_snapshot_table` via `signal_topic` (layer `kafka-connect`, but writes to Kafka) | produce one signal record to `signal.kafka.topic` | `Describe` + `Write` on the signal topic | `internal/clients/kafka/signal.go` (`SendSnapshotSignal`); `internal/actions/snapshot_table.go` |
| any produce | idempotent producer | `IdempotentWrite` on `Cluster` (KIP-679) | `internal/clients/kafka/writer.go` (`NewWriter`) |

ACL summary for the principal, in addition to the read ACLs:

| Resource | Operation | Why |
|---|---|---|
| Group `<consumer group>` (literal, one per consumer in `owners.yaml`) | `Read` | commit the reset (`kafka_reset_consumer_offsets`) |
| Topic `<topic_prefix>.<source_table>` | `Write` | produce the replayed DLQ records (`dlq_replay`) |
| Topic `<signal.kafka.topic>` (the connector's one-partition signal topic) | `Describe`, `Write` | produce the snapshot signal (`connect_snapshot_table`) |
| Cluster | `IdempotentWrite` | required by the producer's idempotent writes |

Notes:

- `Read` on a group is the minimal Kafka ACL for `OffsetCommit`, but it also permits the principal to join
  the group as a consumer member (`JoinGroup`/`SyncGroup`/`Heartbeat` are under `Read` too). The action only
  ever describes and commits, but the credential is the boundary: keep it to the specific groups in `owners.yaml`.
- `dlq_replay` also reads whole DLQ records on the read-only client (`kafka.NewReplayReader` over the reader
  connections, `internal/wiring/actions_p3.go`), so the principal needs `Read` on the DLQ topic
  (`watch.dlq_topics`) for its preview and execute. That is a read of record values the read-only tools
  otherwise never make; the preview shows only the first three keys, redacted (`internal/actions/dlq_replay.go`).
- The signal topic must already exist with exactly one partition (`connect_snapshot_table` and
  `SendSnapshotSignal` refuse anything else); creating it and pointing the connector's `signal.kafka.topic`
  at it is a DBA step.
- Keep every topic and group grant to the pipeline's own names: the Kafka writer refuses, before any I/O, a
  topic or group outside the room's pipeline (`checkWriteScope`, `internal/clients/kafka/writer.go`).

## Review checklist

- [ ] Write mode stays off until your approvers have used read-only rooms; then `PIPEROOM_WRITE_MODE=on`.
- [ ] `SOURCE_DB_WRITE_URL` is a separate write role, never the read-only `SOURCE_DB_URL` role, and never has `default_transaction_read_only = on`.
- [ ] The source write role owns only the publication and the source tables `pg_add_publication_table` may add; it has `INSERT` on the signal table only; it has `REPLICATION`; and, only for a [source admin action](source-admin.md) you enable: `SELECT ... WITH GRANT OPTION` on the captured tables it does not already own (`source_grant_select`), membership in `pg_signal_backend` (`source_end_session`), `CREATE` on the captured tables' schema (`source_create_heartbeat_table`). Each of those may stay ungranted. Nothing else.
- [ ] The Connect proxy allow-list adds only `PUT /connectors/{name}/pause` and `PATCH /connectors/{name}/config`, on the pipeline's connectors.
- [ ] When the lifecycle actions are registered (Q5d): the allow-list adds `PUT /connectors/{name}/stop`, `DELETE`/`PATCH /connectors/{name}/offsets` and `PUT /connector-plugins/{class}/config/validate`, plus `POST /connectors` and `DELETE /connectors/{name}` only if creates and deletes are wanted; the offsets calls need Kafka Connect 3.6 or later.
- [ ] The Kafka principal gains only `Read` on the pipeline's groups, `Write` on its CDC topics and the signal topic, and `IdempotentWrite` on the cluster.
