# Read tool scope: what each tool returns, and what lies outside the room's pipeline

Task T2 (P0), on top of F4 and F11 (`docs/security/findings.md`). Against `main` @ `b81248b` plus
`a2/dlq-d4` (PR #103, which added `dlq_peek` and changed `kafkatools` and `scope.go`).

## The rule

A tool call in a room is scoped to the room's pipeline (`tools.ScopeFor`; the pipeline comes from
the alert, never from the model). A scoped call returns only objects that belong to that pipeline:

- its source tables and its sink tables (`owners.yaml`);
- the replication slot and publication its source connector's config names
  (`slot.name`, `publication.name`, with Debezium's defaults `debezium` and `dbz_publication`),
  or the slot that `owners.yaml` `watch.slot` declares;
- its connectors, its topics (`<topic_prefix>.<source_table>`), its declared DLQ topics and the
  DLQ topic its sink connector's config names, and its consumer groups;
- its schema subjects (`<topic>-key`, `<topic>-value`).

Objects outside the pipeline are dropped from `Summary` and from `Data`. A list tool adds one line
with a count only, never a name: `N other <kind> not shown (not part of this room's pipeline)`
(`tools.NotShown`). The filter runs in the query, with parameters, wherever the system can filter
(the Postgres catalog views); Kafka, Connect and the Schema Registry have no server-side filter,
so those lists are filtered in Go before the result is built. Unscoped calls (CLI and dev, the
explicit `tools.WithUnscoped` opt-out) keep their old output.

**Fail closed.** When a tool needs the connector config to know which slot or publication is the
pipeline's and cannot read it (Connect down, connector missing, an invalid `slot.name`), a scoped
call returns an error and no data. It never falls back to the unscoped list. `owners.yaml`
`watch.slot` keeps `pg_replication_slots` working while Connect is down.

## Inventory

"Before" is `main` + `a2/dlq-d4` (after F4 and F11). "Outside the scope" is what a scoped call
returned, or read into the process, that is not the pipeline's.

| Tool | Returns (scoped call, after T2) | Outside the scope before T2 | T2 change | Where it filters |
|---|---|---|---|---|
| `pg_replication_slots` | the pipeline's slots: name, plugin, active, LSNs, `wal_status`, retained WAL, `safe_wal_size`; a missing slot is reported as `not found`; count of other slots | **every slot on the server**: other pipelines' slots, physical slots, slots of other databases | scoped; slot names from `watch.slot`, else the connector configs (`Scope.Slots`); unreadable config fails closed | SQL `slot_name = ANY($1)`, other slots `count(*)` (`postgres.ReplicationSlotsNamed`); in-memory fallback for other clients |
| `pg_long_transactions` | sessions of the source database that hold or wait for a relation lock on a pipeline source table: pid, user, application, state, age, wait event, statement verb, `has_xid`; count of other long transactions and of the writers among them; the oldest 5 other **writers** anonymised: pid, `same_database` (a bool), state, age, `age(backend_xid)`, wait event type, nothing else | **every long transaction in every database of the server**: other pipelines', other users', other databases' sessions | scoped by `pg_locks`; other writers anonymised (they hold back every logical slot's `restart_lsn`); readers only counted; a client without the filter fails closed | SQL CTE over `pg_stat_activity` + `pg_locks` + the pipeline's table oids; others `count(*)`, the oldest other writers by an allow-listed `SELECT` (`postgres.LongTransactionsOnTables`); state and wait type checked against Postgres's vocabularies in the tool |
| `pg_publication_tables` | the pipeline's publication (from its connector config), the connector role, membership and SELECT for each pipeline source table; count of the publication's other member tables | Summary and Data were already limited to source tables, but the client read **every member table name** of the publication into the process (all tables for `FOR ALL TABLES`) | the client reads only the pipeline's tables; other members are counted in SQL | SQL `unnest($2,$3)` join, `count(*)` (`postgres.PublicationSourceTables`); in-memory fallback for other clients |
| `pg_recent_ddl` | DDL rows whose `object_identity` is a pipeline source table | rows are scoped (F11), but a row's `query` is the audit trigger's `current_query()`: a multi-statement DDL that also touched another table shows that table's DDL and literals, up to 300 characters (residual 9) | none | SQL `object_identity = $2` per table (`RecentDDLTable`); in-memory fallback |
| `pg_row` | key and timestamp values of one pipeline source-table row | none (F4) | none | `CheckSourceTable` before the query |
| `warehouse_freshness` | max timestamp of one pipeline sink table | none (F11) | none | `CheckSinkTable` before the query |
| `warehouse_row` | timestamp values of one pipeline sink-table row | none (F4) | none | `CheckSinkTable` before the query |
| `pipeline_map` | the pipeline's nodes and edges with health | the consumer group node's `lag` fact (and its health) was the **group's total lag over every topic it reads**, other pipelines' topics included | the group lag counts only the pipeline's topics (from the connector config, plus `owners.yaml` while Connect is down); a group with partitions but none of the pipeline's topics shows `lag: unknown (no partition of this pipeline's topics)` and health unknown, never 0 | Go, `topology.pipelineLag`; slots are read in full inside the builder but only the pipeline's slot reaches the graph |
| `trace_record` | one record's path through the pipeline's own stages | none (connector must stream the table; the group lag read uses only the record's partition) | none | `CheckSourceTable`, `connectorForTable` |
| `connect_list_connectors` | the pipeline's connectors: type, state, failed task count; count of other connectors | none in output (F11 filters); every connector name is read from Connect | count line | Go after `ListConnectors` (Connect has no filter); `Status` only for the pipeline's |
| `connect_status` | one pipeline connector's state, tasks, condensed failure trace | the connector is scoped (F11), but the trace text is the worker's: its exception and `Caused by:` lines can name other subjects and topics (residual 10); a trace subject is diffed only when in scope | none | `CheckConnector` before the call |
| `connect_config` | key settings of one pipeline connector, secrets and transform literals masked | none (F11); see residual 3 | none | `CheckConnector` before the call |
| `dlq_peek` | the pipeline sink's DLQ or a declared DLQ: counts, positions, error-header classes | records written by other connectors are dropped and counted (D4) | none | `CheckConnector` / `CheckDLQTopic`; record filter in Go |
| `schema_registry_subjects` | the pipeline's subjects (`<topic>-key`/`-value`), optional prefix; count of other subjects | none in output (F11 filters); every subject name is read from the registry | count line, counted before the prefix filter so a prefix cannot probe names | Go after `Subjects` (no filter in the client contract) |
| `schema_registry_versions` | versions, latest id and compatibility of one pipeline subject | none (F11); see residual 4 | none | `CheckSubject` before the call |
| `schema_registry_diff` | field diff of one pipeline subject's versions | none (F11) | none | `CheckSubject` before the call |
| `kafka_topic_offsets` | offsets of one pipeline topic | none (F11) | none | `CheckTopic` before the call |
| `kafka_find_by_key` | record positions in one pipeline topic | none (F11) | none | `CheckTopic` before the call |
| `kafka_topic_config` | key configuration of one pipeline topic | none (F11) | none | topic derived from `owners.yaml` |
| `kafka_list_groups` | the pipeline's consumer groups and state; count of other groups | none in output (F11 filters); every group name is read from Kafka | count line, counted before the topic filter | Go after `ConsumerGroups` (Kafka has no filter); `GroupLag` only for the pipeline's |
| `kafka_consumer_lag` | one pipeline group: state, members, lag of the pipeline's partitions only; count of other partitions | none (F11 filters partitions and recomputes the total) | count line | Go after `GroupLag` |
| `runbook_lookup` | embedded runbook text | nothing pipeline-specific (no client) | none | n/a |

## Residuals (by design, or for a later task)

1. **Counts.** A scoped list tool says how many other slots, sessions, connectors, groups,
   subjects, partitions or publication members exist. It never names them.
2. **Kafka, Connect and the Schema Registry list everything to the process.** Their APIs have no
   name filter, so `ListConnectors`, `ConsumerGroups` and `Subjects` return every name to
   Piperoom; the names are dropped before any result is built and never reach the model, the room
   log or the UI. The real boundary is still the credentials' ACLs (F7).
3. **`connect_config` shows the pipeline connector's own settings**, such as
   `table.include.list`, `database.hostname` and `database.dbname`. If a connector captures more
   tables than `owners.yaml` lists, those table names are shown: they belong to the pipeline's
   connector.
4. **`schema_registry_versions`** reports the global compatibility level when the subject has
   none: a registry-wide setting, but it is the level in force for this subject.
5. **Subjects named only in a connector config** (for example a record-name subject strategy) are
   not added to the scope: the scope stays `<topic>-key`/`-value`, which is narrower, not wider.
6. **`pg_long_transactions` shows other writers, anonymised.** A writer on another table or in
   another database of the server holds back every logical slot's `restart_lsn` (seen on real
   Postgres: `restart_lsn` stuck, retained WAL growing). So the oldest 5 writers outside the
   pipeline's tables are listed with pid, `same_database`, state, age, `age(backend_xid)` and
   wait event type: enough for the agent to name the cause and for a DBA to act on the pid. Their
   user, application, client address, database name and query (even the verb) are never selected.
   Readers (no xid) cannot pin a slot and stay a count. The other-writer lines are guaranteed to
   fit in Summary: the count line, at most 5 writer lines and the "more" line come first (a bounded
   block well under 2 KB), and the pipeline's own sessions follow, the 30 oldest only, with
   "(K more sessions on this pipeline's tables, in Data)"; truncation can only cut those. A
   session is the pipeline's when it holds
   or waits for a lock on a pipeline table in the source database; a clone of the database
   (`CREATE DATABASE … TEMPLATE`) has the same table oids, so the `datname` check keeps it out.
7. **`dlq_peek` counts are for the whole DLQ topic**, which other connectors may share (D4 says so
   in the summary).
8. **Scope comes from `owners.yaml` and the connector configs.** A wrong `owners.yaml` or a
   connector config that names another pipeline's slot widens the scope to that object.
9. **`pg_recent_ddl` shows the audit trigger's `current_query()`** (`dev/postgres-init/02-ddl-audit.sql`).
   A row is the pipeline's table, but its query text is the whole statement string the client
   sent: a multi-statement DDL that also changed another table shows that table's DDL and any
   literals in it (cut to 300 characters in Summary). Pre-existing; a fix is to store only the
   command tag and object, or to cut the text at the first statement.
10. **`connect_status` shows the pipeline connector's failure trace.** The exception and
    `Caused by:` lines are the worker's text and can name other subjects and topics (for example
    a registry error about another subject). Only an in-scope subject is ever read from the
    registry. Pre-existing.

## Tests

- `internal/tools/dbtools/scope_meta_test.go`: fakes with two pipelines' slots, sessions and
  publication members (`FOREIGNMARK` in every foreign name); a scoped call shows only its own, in
  Summary and marshalled Data; the count line names nothing; other writers are listed oldest
  first, capped at 5, with exactly the allow-listed fields (`TestOtherWriterAllowList`); config
  unreadable, connector missing, no Connect client and an invalid `slot.name` fail closed with
  no source read.
- `internal/tools/kafkatools/scope_count_test.go`: the same for the cluster-wide lists.
- `internal/tools/scope_slots_test.go`: `Scope.Slots` (declared slot, config slot, default, fail
  closed) and `NotShown`.
- `internal/topology/scope_lag_test.go`: group lag of a group that also reads another pipeline's
  topic.
- `-tags integration`, on a throwaway `postgres:16 -c wal_level=logical`
  (`PIPEROOM_SCOPE_PG_URL`, else `SOURCE_DB_URL`):
  `internal/clients/postgres/scoped_integration_test.go` (the three SQL filters; exact
  other/writer counts with a REPEATABLE READ reader; a cloned database whose writer on the
  clone's copy of the pipeline table is not listed and leaks no login or application name) and
  `internal/tools/dbtools/scope_integration_test.go` (two publications and two slots; the scoped
  slot and publication tools show only their own; unreadable config fails closed; an
  idle-in-transaction writer in another database is listed anonymised, its marker login,
  application name and database nowhere in Summary or Data).
