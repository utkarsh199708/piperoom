---
id: broker-restart
title: Kafka broker restarted or rolled; connector or task stuck, not ready or failed
keywords: broker restart, broker, rolling restart, roll, not ready, unassigned, timeout, expiring records, delivery.timeout.ms, not present in metadata, notenoughreplicas, min.insync.replicas, coordinator, rebalance, stuck, connect worker
---
## Symptoms
- After a broker restart or a rolling update, a source task is FAILED or the connector stays UNASSIGNED or not ready.
- Typical traces: `TimeoutException: Expiring N record(s)`, `Topic ... not present in metadata after 60000 ms`, `NotEnoughReplicasException`, or the worker logging that it lost the group coordinator.
- Producer errors are not covered by `errors.tolerance`: a source task whose sends time out fails, whatever its tolerance setting.
- The database side is healthy and the slot keeps WAL while the task is down.
- Kafka Connect's own internal topics (config, offset, status) live on the same brokers; while they are unavailable, workers cannot rebalance or store offsets.

## Find it with Piperoom tools
- `connect_status` — the FAILED or UNASSIGNED task and its trace (a producer timeout, not a data error)
- `connect_list_connectors` — every connector of the worker group in the same state, which points at Kafka rather than one connector
- `kafka_topic_config` — `min.insync.replicas` and the replication of the CDC topic
- `kafka_topic_offsets` — whether the CDC topic's end offsets move again
- `pg_replication_slots` — WAL kept while the task is down

## Safe checks
- Check the brokers first: every broker back, no under-replicated or offline partitions (the cluster health read shows them when available).
- Compare `min.insync.replicas` with the number of in-sync replicas during the roll: with `acks=all`, sends fail when fewer replicas are in sync than required.
- Kafka Connect: `GET /connectors/{name}/status` for each task's state and worker.
- Check the worker log around the restart for coordinator and rebalance messages before restarting anything.

## Fixes
- Action `connect_restart_connector` — once every broker is back and the CDC and internal topics have their replicas; it restarts only the failed tasks (`onlyFailed=true`).
- Action `connect_restart_task` — the same for one task.
- Manual — roll brokers one at a time and wait for under-replicated partitions to reach 0 before the next one.
- Manual — give the producer more room through a roll with `producer.override.delivery.timeout.ms` (needs the worker's `connector.client.config.override.policy` to allow it).
- Manual — keep the internal topics at replication factor 3 with `min.insync.replicas` 2 so one broker down does not stop Connect.

## Risks
- Restarting while partitions are still offline fails again in the same way.
- A longer delivery timeout hides a broker that does not come back; keep it bounded.
- Restarting every connector at once after a roll adds a burst of load to brokers that are still catching up.

## Sources
- https://kafka.apache.org/documentation/#producerconfigs_delivery.timeout.ms — producer delivery timeout
- https://kafka.apache.org/documentation/#brokerconfigs_min.insync.replicas — `min.insync.replicas` with `acks=all`
- https://kafka.apache.org/documentation/#connect_rest — status and restart endpoints, `onlyFailed`
- https://kafka.apache.org/documentation/#connect_errorreporting — what `errors.tolerance` covers
- https://kafka.apache.org/documentation/#basic_ops_restarting — rolling restarts
