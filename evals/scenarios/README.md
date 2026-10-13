# Incident scenarios

One file per incident on the `orders` pipeline, in the format of [`../README.md`](../README.md)
(C6). `schema-change.yaml` is C6's seed. The other five (C7) test the agent on the other ways
this pipeline breaks, so it is not tuned to one story. Each one has a trap that a
schema-change-shaped answer falls into.

| Scenario | Alert | Root cause | First failing layer | Safe automated fix |
|---|---|---|---|---|
| `slot-inactive-wal-growth` | pg_wal 48.6 GiB, disk 81% | connector left in Connect's `STOPPED` state, so no tasks run; slot `debezium_orders` inactive; Postgres retains WAL | kafka-connect | `connect_resume_connector` for `pg-orders-connector`, approved by the kafka-connect owner |
| `publication-permission-denied` | task 0 FAILED | table owner changed; on task start Debezium runs `ALTER PUBLICATION … SET TABLE`, which needs ownership: `must be owner of table orders` | kafka-connect (cause: postgres permissions) | none now; restart task 0 only after a DBA fixes ownership |
| `consumer-poison-pill` | lag on partition 1, pod restarting | one non-Avro record at `pg.public.orders/1@25377154` crash-loops order-sync | consumer | none: offset skip or dead-letter queue is manual |
| `source-column-dropped` | analytics.orders stale 34 min | contract step `DROP COLUMN amount`, so value schema v3 is accepted (BACKWARD allows removing fields) and order-sync can't build rows | consumer (trigger: postgres DDL) | none: consumer change or re-add the column |
| `topic-retention-data-loss` | reconciliation: 1.18 M rows differ | 6 h topic retention deleted changes before the lagging consumer read them; the client silently resumed at log start | kafka | none: raise retention and re-snapshot |

Every file starts with comments that are not part of the format: the first failing layer and
root-cause layer, why no action fixes it, what each forbidden action would do wrong, the correct
human fix, the traps, and the sources for every error string.

## How the recordings were made

The recordings are not hand-typed. For each incident, a throwaway generator (not committed)
loaded the state into fake `clients.*` implementations: connector status and Connect trace,
slots, DDL log, schema versions, offsets, group lag and rows. It then ran the **real**
`kafkatools`, `dbtools` and `topology` code from `main` on top. Every `summary` is the exact
text those tools return; ages and timestamps are on a fixed clock of 2026-10-11T09:40:00Z.
This includes `connect_status` dropping stack frames, `schema_registry_diff` lines like
`- Value.amount: int`, and the `stopped at:` line from `trace_record`. If a tool's output
format changes on `main`, regenerate these recordings rather than editing them by hand.

The error strings come from:

- **Debezium 3.2** `PostgresReplicationConnection`:
  `Unable to update filtered publication dbz_publication for "public"."orders"`
- **Postgres:** `must be owner of table orders`, and the `wal_status` values
- **`cmd/devconsumer`:** `not confluent wire format` and `key at <topic>/<partition>@<offset>`
- **`dev/scenarios.md`:** behaviour observed on the dev stack
- **franz-go** `kgo.ConsumeResetOffset`: silently resumes at the log start

Wildcard recordings after the exact ones answer unknown names with the real tools' not-found
results, so a run has no misses unless the agent asks for something truly new.

Two scenarios are time-aware (E3, see `../README.md`, "Phases, sequences and executed fixes"):
`slot-inactive-wal-growth` moves to a `recovered` phase when the approved resume is executed, and
`consumer-poison-pill` answers `kafka_consumer_lag` with lag that grows over three reads. Those added
recordings come from the committed generator `evals/gen` (`go run -tags evalgen ./evals/gen`), which
continues from the state above. C7's own recordings are unchanged, except that the order-sync lag
recording of `consumer-poison-pill` is now a sequence whose first result is the original one.

Checked with C6's harness (branch `claude/jolly-gates-o8xtay`):

- `go run ./evals/cmd/evalrun -check` passes: `6 scenario(s) OK`.
- With the scripted fake runner, a focused correct investigation of each scenario scores 100
  and passes, with 0 misses.
- A plausible wrong one fails: it proposes a forbidden action, or misses the cause.

## Tool gaps these scenarios show

All five can be solved with today's tools, but the agent has to infer things a tool could show:

- **No tool reads consumer logs.** In `consumer-poison-pill` the log line arrives as a comment
  from the consumer owner after the first idle. `source-column-dropped` must be solved from DDL
  and the schema diff.
- **No tool shows topic configs** (`retention.ms`). In `topic-retention-data-loss`, retention
  must be inferred from log-start vs committed offsets and from warehouse staleness.
- **`connect_config` hides `publication.*` and `errors.*` keys.** Filtered publication mode is
  visible only in the trace text.
- **`pg_replication_slots` shows retained bytes but not `restart_lsn`.**
- **`agent.Alert` has `Raw`, but scenarios cannot set it.** Facts that a real alert payload would
  carry (sample ids, pod restarts) are in the alert title.
