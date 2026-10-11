---
id: consumer-lag
title: Consumer lag growing (slow processing, rebalances, stopped consumer)
keywords: consumer, lag, slow, rebalance, rebalancing, max.poll.interval.ms, max.poll.records, group.instance.id, static membership, cooperativestickyassignor, group.protocol, session.timeout.ms
---
## Symptoms
- Total lag of the group rises while the source connector is RUNNING and the topic keeps getting records.
- The warehouse goes stale; how stale depends on how far behind the group is.
- The group state is `Empty` (no members), flaps through `PreparingRebalance`, or is `Stable` but too slow.
- Seen on the Piperoom dev stack (`consumer-lag`): with the consumer stopped, `order-sync` showed `state Empty, members 0` and lag grew with the load while Connect stayed RUNNING.

## Find it with Piperoom tools
- `kafka_list_groups` — the group's state
- `kafka_consumer_lag` — members, total lag and the worst partitions
- `kafka_topic_offsets` — end offsets still moving, so the producer side is fine
- `warehouse_freshness` — how stale the sink is
- `connect_status` — RUNNING rules out the source side

## Safe checks
- `kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group>` shows current offset, end offset and lag per partition.
- Add `--members` to see which members own which partitions.
- If all partitions lag evenly, the consumer is slow or down; if one partition is stuck, see the poison-pill runbook.

## Fixes
- Manual — start or scale the consumer; members beyond the partition count stay idle.
- Manual — when one batch takes longer than `max.poll.interval.ms` (default 300000 ms), the member leaves the group and a rebalance starts: process faster, lower `max.poll.records` (default 500), or raise the interval.
- Manual — `session.timeout.ms` decides how fast a dead member is noticed; keep it and the heartbeat settings at their defaults unless rebalances show a reason.
- Manual — static membership (`group.instance.id`) avoids a rebalance when a member restarts within the session timeout.
- Manual — `partition.assignment.strategy` with `CooperativeStickyAssignor` moves only the partitions that must move during a rebalance.
- Manual — `group.protocol` `consumer` selects the newer group protocol; `classic` is the default.

## Risks
- Raising `max.poll.interval.ms` also delays noticing a consumer that is truly stuck.
- Lag that grows past topic retention loses data; see the retention runbook.
- There are no Piperoom actions for consumers: every fix here is done by the consumer owner.

## Sources
- https://kafka.apache.org/41/configuration/consumer-configs/ — `max.poll.interval.ms`, `max.poll.records`, `session.timeout.ms`, `group.instance.id`, `partition.assignment.strategy`, `group.protocol`
- https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html — poll loop, liveness and rebalances
- https://kafka.apache.org/41/operations/basic-kafka-operations/ — `kafka-consumer-groups.sh --describe`
