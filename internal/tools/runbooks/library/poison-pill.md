---
id: poison-pill
title: One bad record blocks a partition (poison pill)
keywords: poison, pill, deserialization, deserialize, recorddeserializationexception, dead letter queue, dlq, errors.tolerance, errors.deadletterqueue.topic.name, skip offset, crash loop, one partition
---
## Symptoms
- The consumer fails on one record, exits or stops before committing, and reads the same record again after restart.
- Lag grows on one partition only; the committed offset of that partition does not move while its end offset does.
- Other partitions may keep flowing, so the warehouse can still look fresh.
- A Java consumer reports a `RecordDeserializationException`, which names the partition and offset of the record.
- Seen on the Piperoom dev stack (`consumer-stuck`): the group flapped between `Empty` and `PreparingRebalance` and the consumer logged `cannot process order 1648 at pg.public.orders/2@799 (poison pill)` on every restart, while Connect stayed healthy.

## Find it with Piperoom tools
- `kafka_consumer_lag` — one partition with a stuck committed offset and growing lag
- `kafka_list_groups` — group `Empty` or rebalancing while it crash-loops
- `kafka_find_by_key` — where a given key's records sit in the topic
- `trace_record` — a row on the stuck partition stops at the consumer
- `pg_row` — the source row of that key, for comparison
- `connect_status` — RUNNING shows the producer side is fine

## Safe checks
- The committed offset of the stuck partition is the record the consumer cannot process.
- `kafka-consumer-groups.sh --describe --group <group>` confirms which partition is stuck.
- For a Kafka Connect sink, the default `errors.tolerance=none` stops the task on the first bad record.

## Fixes
- Manual — fix the consumer so it sends records it cannot process to a dead-letter topic and continues.
- Manual — skip exactly one record: stop the group, then `kafka-consumer-groups.sh --reset-offsets --group <group> --topic <topic>:<partition> --to-offset <offset+1> --execute`.
- Manual — for a Kafka Connect sink, set `errors.tolerance=all` with `errors.deadletterqueue.topic.name` (sink connectors only) and `errors.log.enable=true`.
- Action `connect_restart_task` — for a sink connector, after its error-handling config is fixed.

## Risks
- An offset reset skips the record for good; write down which record was skipped.
- `errors.tolerance=all` without a dead-letter topic or logging drops bad records silently.
- An offset reset needs the group to have no active members.

## Sources
- https://kafka.apache.org/41/kafka-connect/user-guide/ — Error Reporting in Connect
- https://kafka.apache.org/41/configuration/kafka-connect-configs/ — `errors.tolerance`, `errors.deadletterqueue.topic.name`, `errors.log.enable`
- https://kafka.apache.org/41/javadoc/org/apache/kafka/common/errors/RecordDeserializationException.html — the exception and its partition and offset
- https://kafka.apache.org/41/operations/basic-kafka-operations/ — `kafka-consumer-groups.sh --reset-offsets`
