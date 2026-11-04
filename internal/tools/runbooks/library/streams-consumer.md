---
id: streams-consumer
title: Kafka Streams application lagging or failing after a connector restart or a burst
keywords: kafka streams, streams, streams application, state store, restore, standby, num.standby.replicas, deserialization exception handler, logandfail, streamsuncaughtexceptionhandler, application.id
---
## Symptoms
- After the connector was restarted, caught up a backlog or re-snapshotted, a Kafka Streams application lags, rebalances repeatedly or stops.
- A snapshot or replay re-emits rows (op `r`), so a Streams application that is not idempotent double-counts.
- With the default deserialization handler (`LogAndFailExceptionHandler`) one record it cannot read stops the stream thread.
- After an instance restart the application restores its state stores from the changelog topics before it processes again; with large stores that takes minutes and reads as lag.
- The consumer group of a Streams application is its `application.id`.

## Find it with Piperoom tools
- `kafka_consumer_lag` — the application's group (its `application.id`) and its lag per partition
- `kafka_list_groups` — the group's state: Stable, or PreparingRebalance again and again
- `kafka_topic_offsets` — the input topic's end offsets, to see whether the backlog is upstream or in the application
- `kafka_find_by_key` — the record at the offset the application stopped on
- `connect_status` — rules the connector in or out

## Safe checks
- Read the application's logs for the stream thread's last exception and for state restoration progress.
- Compare the lag curve with the connector's catch-up: lag that falls steadily after a restart is restoration or backlog, not a failure.
- Check `num.standby.replicas`: with 0, every restart rebuilds the state stores from the changelog.

## Fixes
- Action `connect_restart_task` — only when the connector side failed; nothing on the Streams side is fixed by Piperoom actions.
- Manual — set a `default.deserialization.exception.handler` that logs and continues (or sends to a dead letter topic) so one bad record does not stop the application.
- Manual — set a `StreamsUncaughtExceptionHandler` that replaces the thread, so a transient error does not kill the instance.
- Manual — set `num.standby.replicas` to 1 or more so a restarted instance takes over warm state.
- Manual — make the application idempotent on the record key, or filter snapshot reads (op `r`) when a re-snapshot is planned.

## Risks
- A handler that continues on bad records skips data; send skipped records somewhere visible.
- Standby replicas double the state store disk and changelog reads.
- Resetting the application (`kafka-streams-application-reset`) reprocesses everything from the chosen offsets.

## Sources
- https://kafka.apache.org/documentation/streams/developer-guide/config-streams — `default.deserialization.exception.handler`, `num.standby.replicas`, `application.id`
- https://kafka.apache.org/documentation/streams/architecture — state stores, changelogs and standby replicas
- https://kafka.apache.org/documentation/streams/developer-guide/app-reset-tool — the reset tool
