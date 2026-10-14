---
id: retention-loss
title: Topic retention deleted records before the consumer read them
keywords: retention, retention.ms, retention.bytes, deleted, offset, out of range, auto.offset.reset, log start, data loss, gap, backfill, incremental snapshot, execute-snapshot
---
## Symptoms
- A consumer was down or behind for longer than the topic keeps data.
- Its committed offsets are below the partitions' start offsets: the records in between were deleted unread.
- What happens next depends on `auto.offset.reset`: `latest` (the default) jumps to the end, `earliest` to the oldest record still kept, `none` raises an error to the application.
- Either way the warehouse misses those changes: rows keep older values or are missing.

## Find it with Piperoom tools
- `kafka_topic_offsets` — each partition's start offset
- `kafka_consumer_lag` — committed offset per partition; compare it with the start offset
- `kafka_list_groups` — group state (crash-looping with `none`, Stable otherwise)
- `warehouse_freshness` — how stale the sink is
- `trace_record` — a row changed in the gap is seen in Postgres but nowhere downstream
- `warehouse_row` — the warehouse still holds an older version of that row

## Safe checks
- `kafka-consumer-groups.sh --describe --group <group>` shows committed and end offsets.
- `kafka-configs.sh --describe --entity-type topics --entity-name <topic>` shows the topic's retention settings.
- `retention.ms` defaults to 604800000 (7 days); `retention.bytes` defaults to -1 (no size limit).

## Fixes
- Manual — raise `retention.ms` (or `retention.bytes`) on the topic well above the worst expected consumer outage.
- Manual — backfill the gap from the source with a Debezium `execute-snapshot` signal (an incremental snapshot of the affected tables; signalling must be configured).
- Manual — after the backfill is in the topic, stop the group and reset its offsets with `kafka-consumer-groups.sh --reset-offsets`.

## Risks
- Resetting offsets to `earliest` or `latest` hides the gap instead of filling it.
- An incremental snapshot re-emits rows that exist now; deletes during the gap need their own reconciliation.
- Longer retention uses more broker disk.

## Sources
- https://kafka.apache.org/41/configuration/topic-configs/ — `retention.ms`, `retention.bytes`
- https://kafka.apache.org/41/configuration/consumer-configs/ — `auto.offset.reset`
- https://kafka.apache.org/41/operations/basic-kafka-operations/ — `kafka-consumer-groups.sh`, `kafka-configs.sh`
- https://debezium.io/documentation/reference/stable/configuration/signalling.html — `execute-snapshot` signal, incremental snapshots
