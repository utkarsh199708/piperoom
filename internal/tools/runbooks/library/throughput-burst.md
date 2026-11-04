---
id: throughput-burst
title: A burst of changes; the connector lags the source and does not scale out
keywords: burst, bulk update, backfill, batch job, high volume, connector lag, source lag, millisecondsbehindsource, throughput, slow, scale, tasks.max, one task, max.batch.size, max.queue.size, poll.interval.ms, queue full, producer, linger.ms, compression
---
## Symptoms
- A bulk update, backfill or batch job writes many rows at once; the connector lags the database for minutes or hours.
- Retained WAL on the source grows while the connector works through the backlog.
- Raising `tasks.max` changes nothing: a Debezium Postgres connector reads one replication slot with exactly one task.
- Adding topic partitions does not speed up the source either; it only spreads the records the one task produces.
- The task stays RUNNING the whole time: this is lag, not a failure.

## Find it with Piperoom tools
- `connect_status` — the task RUNNING (rules out a failure)
- `connect_config` — `max.batch.size`, `max.queue.size`, `poll.interval.ms` and any `producer.override.*` keys
- `pg_replication_slots` — retained WAL growing during the burst, shrinking after it
- `kafka_topic_offsets` — how fast the CDC topic's end offsets move
- `warehouse_freshness` — how stale the sink is

## Safe checks
- Debezium's streaming metrics (the Debezium metrics read, when configured) show `MilliSecondsBehindSource` and the queue's remaining capacity: a queue that is often full means the producer side is the limit; an empty queue means reading the WAL is.
- Compare the burst's row count with the normal rate to estimate when the backlog clears.
- Check the size of the rows changed: wide rows or large TOAST columns cost more per event than the count suggests.

## Fixes
- Action `connect_tune_throughput` — change the connector's batch, queue and producer settings within bounds; the preview lists each key with the value now and the value it sets.
- Manual — raise `max.batch.size` and `max.queue.size` together (the queue must stay larger than the batch); lower `poll.interval.ms` only when the queue is often empty.
- Manual — producer side: `producer.override.linger.ms`, `producer.override.batch.size` and `producer.override.compression.type` (the worker's `connector.client.config.override.policy` must allow overrides).
- Manual — for planned backfills, run them in chunks, or snapshot the table with an incremental snapshot instead of streaming every update.
- Manual — when one connector carries unrelated busy tables, split them over two connectors, each with its own slot and publication.

## Risks
- Larger batches and queues use more worker heap; watch the worker's memory after raising them.
- Every extra connector adds a replication slot that keeps WAL while it is down.
- A tuning change restarts the connector's tasks.

## Sources
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-max-batch-size — `max.batch.size`, `max.queue.size`, `poll.interval.ms`
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-streaming-metrics — `MilliSecondsBehindSource`, queue capacity
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-tasks-max — one task per connector
- https://kafka.apache.org/documentation/#connectconfigs_connector.client.config.override.policy — producer overrides
- https://kafka.apache.org/documentation/#producerconfigs_linger.ms — `linger.ms`, `batch.size`, `compression.type`
