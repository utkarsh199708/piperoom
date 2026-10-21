# Write roles for approved fixes

Piperoom is **read-only by default**: the agent proposes fixes, people approve them, but a fully
approved fix never runs until you turn write mode on. This page lists every write call of the P3 fix
actions and the exact, minimal credential for each, for the day you set `PIPEROOM_WRITE_MODE=on`. The
read-only grants are in [permissions.md](permissions.md); keep them too. The switch stops the executor,
it does not take write rights away from a role that has them (`cmd/server/writemode.go`). Grant exactly
what is below and nothing more, on **dedicated** service accounts, never a human or application credential.

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

Three actions write through it (layer `postgres`):

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
  protocol. There is no narrower slot-ownership grant: PostgreSQL 17 restricts `pg_drop_replication_slot`
  (and the other replication-slot functions) to superusers and roles with the `REPLICATION` attribute, so
  owning the slot, or being a member of its owner, does not by itself permit the call. The slot is usually
  owned by Debezium's own replication user.
- **Do not grant table or schema `SELECT` to the writer.** It does not need it, and it must not read data
  the read-only role cannot. The writer's re-reads (`pg_publication`, `pg_publication_tables`,
  `pg_replication_slots`) are readable by any role.
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
- [ ] The source write role owns only the publication and the source tables `pg_add_publication_table` may add; it has `INSERT` on the signal table only; it has `REPLICATION` and nothing else.
- [ ] The Connect proxy allow-list adds only `PUT /connectors/{name}/pause` and `PATCH /connectors/{name}/config`, on the pipeline's connectors.
- [ ] The Kafka principal gains only `Read` on the pipeline's groups, `Write` on its CDC topics and the signal topic, and `IdempotentWrite` on the cluster.
