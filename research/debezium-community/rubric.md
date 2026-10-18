# Scoring Debezium Zulip threads against Piperoom (main @ eab2da9d)

One label per thread (stream + topic). Read the opening messages for the problem and the
closing messages for the answer. Label from what the thread says, never from guesses.

## 1. kind
- `incident`: a pipeline that was running misbehaves (task failed, lag, slot/WAL growth, missing or
  duplicate data, snapshot stuck, schema error at runtime, poison record).
- `setup`: a first-time configuration or deployment problem (cannot connect, permissions at install,
  which property to set).
- `question`: how-to, semantics, design advice with nothing broken.
- `dev`: Debezium's own development (PRs, CI, releases, code review, roadmap), feature requests.
- `other`: announcements, meetings, off-topic.

## 2. scope (only for incident and setup)
- `source`: postgres | mysql | oracle | sqlserver | mongodb | db2 | other | unknown.
  **Piperoom reads a Postgres source only.**
- `runtime`: connect | server | embedded | flink | unknown. **Piperoom assumes Kafka Connect.**
- In scope = source `postgres` (or `unknown` when the failure is in a layer Piperoom reads anyway:
  Connect, Kafka, Schema Registry, consumer lag, Postgres/Snowflake warehouse) AND runtime `connect`/`unknown`.

## 3. diagnose (in-scope incidents and setup only)
Would Piperoom's read tools have shown the cause? `yes` | `partial` | `no`, plus the tools.
Read tools: connect_status, connect_config, connect_list_connectors, debezium_metrics,
pg_replication_slots, pg_wal, pg_publication_tables, pg_recent_ddl, pg_long_transactions, pg_row,
kafka_consumer_lag, kafka_topic_offsets, kafka_topic_config, kafka_read, kafka_find_by_key,
kafka_acls, kafka_cluster_health, kafka_partition_rates, kafka_list_groups, dlq_peek,
schema_registry_subjects, schema_registry_versions, schema_registry_diff, warehouse_freshness,
warehouse_load_errors, warehouse_row, trace_record, reconcile, what_changed, pipeline_map,
runbook_lookup, similar_incidents.
`no` when the cause lives where no tool looks: Debezium/JVM logs only, connector code bugs,
network/DNS, Kubernetes, cloud IAM, the database server's own config files.

## 4. fix
- `today`: one of the six actions on main ends it: connect_restart_task, connect_restart_connector,
  connect_resume_connector, connect_update_connector_config (ONE key), schema_set_compatibility,
  schema_register.
- `after_p3`: needs a P3 action: connect_pause_connector, multi-key connector config,
  kafka_reset_consumer_offsets, dlq_replay, connect_snapshot_table (incremental snapshot signal),
  pg_add_publication_table, pg_drop_replication_slot.
- `human`: needs something no action does: upgrade Debezium, a code fix, a DB-side change
  (REPLICA IDENTITY, wal_level, max_slot_wal_keep_size, grants), redeploying the consumer,
  restoring lost data.
- `none`: nothing to fix (it was a misunderstanding) or the thread never found the fix.

## 5. community_resolved
`yes` if the thread was marked resolved or a reply clearly worked; `no` if it ended without an
answer; `unclear` otherwise.

## Output (one JSON line per thread)
{"stream","topic","kind","source","runtime","in_scope":bool,"failure_class","diagnose",
 "tools":[...],"fix","action","community_resolved","evidence":"<= 20 words, paraphrased"}

`evidence` is a paraphrase, never a quote with names. failure_class is one of: task_failed,
schema_serialization, slot_wal_growth, publication_missing_table, snapshot, offsets_position,
consumer_lag, dlq_poison, duplicates_or_loss, performance, connectivity_auth, upgrade, bug, other.
