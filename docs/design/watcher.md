# Piperoom watcher: built-in CDC trouble detection (v2 after review)

Base: local `main` is `0c3c243` (PR #42 and PR #43 merged; the task's `8680bcb` is older). #48 read from `origin/a3/read-scope`, #49 from `origin/a4/model-endpoint`. Line numbers are from these trees.

## 1. Problem and evidence

A room opens only when something outside sends an alert (`OpenAlertRoom`, `internal/room/service_room.go:132`; `POST /api/alerts/{source}`, `docs/api.md`) or a person posts. Nothing in the server watches the pipeline. The research says this is the costly gap: 68% of teams need 4+ hours to detect, 74% hear it from business users, and one connector sat FAILED for nine days until a disk alarm at 94% (`reports/CDC pipeline incident frequency and cost.md:13,28`). WAL growth from a stuck slot is the top Debezium failure. Buyers asked for a "built-in freshness watcher" (`reviews/product-feedback-2026-10-13.md:21`, T4). The GTM plan wants shadow mode before the tool speaks (`reviews/gtm-plan-2026-10-13.md:25,35`).

The checks exist in `devctl health` (`cmd/devctl/health.go:93-168`) and the clients exist in the server: `SourceDB.ReplicationSlots`/`LongTransactions` (`internal/clients/clients.go:111-119`), `ConnectClient.Status`/`Config` (`:22-23`), `KafkaAdmin.GroupLag`, `Warehouse.Freshness`, `SchemaRegistryClient.Compatibility`. Missing: a loop, memory of past values, and a path into a room.

## 2. Goals / non-goals

Goals (v1): detect the two failures the research names, connector down and stuck slot, with plain code; one room per pipeline per incident; log-only by default (shadow mode); the agent investigates only when a room opens; honest about what we cannot see.

Non-goals: auto-fix (never); a model call to detect; quiet hours; Prometheus endpoint; CloudWatch; disk free space. v2 (not this sprint): `freshness`, `consumer_lag`, `long_tx`, once the cost and correctness questions in §9 are settled. `compat_none` and `unreachable` are log lines only.

## 3. Design

**Package** `internal/watch` (new owner row, §6). `watch.New(Deps)`: client interfaces, `*owners.Owners`, a `RoomSink` interface (`OpenAlertRoom`, `OpenRoomForPipeline`, `LastRoomForKey`), a clock, a logger. `Run(ctx)` is one goroutine.

**v1 signals** (all read-only, each with evidence as a small JSON map):

| id | reads | fires when | severity |
|---|---|---|---|
| `connector_down` | `Connect.Status` per connector | connector not `RUNNING`, or zero tasks, or any task not `RUNNING`, for `for` (devctl rule, `health.go:114`) | `FAILED` sev2; `PAUSED`/`STOPPED` sev3 (often maintenance, but WAL still grows); 404 → `connector_missing` sev2 |
| `slot_inactive` | one `ReplicationSlots` per tick, filtered to the pipeline's slot | `active=false` for `for` (matches `devctl break slot-inactive`, `dev/scenarios.md`) | sev2 |
| `slot_wal` | same rows | `retained ≥ fire` for `for`, hysteresis to `clear` | see below |

`slot_wal` severity: `wal_status lost|unreserved` → sev1 at once (`lost` means a re-snapshot, 10+ hours per the research). "Slot invalidation in N h" (from `safe_wal_size`, below) < `eta_sev1` for 2×`for` → sev1. Otherwise sev2. If the slot is `active=true`, the connector is RUNNING, and `Connect.Config` has no `heartbeat.interval.ms` (our own `dev/connectors/pg-orders-connector.json` has none): evidence says "no heartbeat configured: idle-database WAL growth is likely" and severity is capped at sev3. This stops the "first room is about an idle database" failure (B3).

**Rate and ETA (B2).** `RetainedWALBytes` is `pg_current_wal_lsn() − restart_lsn` (`internal/clients/postgres/postgres.go:67`): WAL generated since the slot's restart point, not bytes on disk and not free space. So:
- Rate = least-squares slope over the last 1 h of samples (fast) and the last 6 h (slow); both reported. No rate or ETA from fewer than 3 samples spanning 30 min. Zero or negative slope → "not growing", no ETA.
- Primary ETA: `safe_wal_size / slow_rate` = "slot invalidated in N h". `safe_wal_size` (PG13+, `pg_replication_slots`) is the exact number of bytes still writable before the slot goes `unreserved`. Contract: `SafeWALBytes *int64` on `clients.ReplicationSlot` (`clients.go:121-129`) and one column in the query (`postgres.go:62-69`). NULL when `max_slot_wal_keep_size=-1` → no ETA.
- Optional ETA: `wal_headroom_bytes` in YAML ("free space WAL may use", set by the operator). Evidence says "rests on the configured headroom". Postgres cannot see RDS free storage; CloudWatch stays "later".

**Loop (I4, nits).** `time.NewTimer` reset each tick to `interval + rand.N(2j) − j` (`math/rand/v2`, verified with `go doc`). One goroutine, so no overlap; a tick that overruns `interval − 5 s` is skipped with a warn line. Per tick: source checks run **once** (one `ReplicationSlots`; `LongTransactions` only in v2), rows fanned out to pipelines; then Connect `Status`+`Config` per connector in parallel. Each call has `context.WithTimeout(check_timeout)`, which must not exceed the role's `statement_timeout = '10s'` (`docs/pilot/permissions.md:43`). The watcher uses ≤ 1 source connection in v1 (≤ 2 in v2), inside the pilot's `CONNECTION LIMIT 5` (`permissions.md:41`). Per-layer errors are isolated (same shape as `runHealth`, `health.go:181-220`).

**State (I4, B4).** Per (pipeline, signal): ring of the last 6 h of samples, `breaching_since`, `clearing_since`, `state ∈ {ok, firing, acked}`, `last_error`. An erroring tick is "unknown" and leaves the timers alone. Fire when `now − breaching_since ≥ for`; resolve when `now − clearing_since ≥ for` with value < `clear`. `acked` = the driver closed the room while the signal still fires; it goes back to `ok` only when the signal clears, so a closed room is not re-opened by the same incident. Set from `Deps.OnRoomClosed(ctx, roomID, pipelineID, dedupKey)`, a new hook beside `OnRoomOpened` (`internal/room/service.go:57-61`).

**Restart.** In-memory state is lost, which is fine for samples (rates warm up again, 30 min). Before opening, the watcher calls `LastRoomForKey(pipeline, dedupKey)`; if that room is closed less than `cooldown` (default 60 min) ago, it logs and stays quiet. If the room is still open, `OpenAlertRoom` joins it (`joinAlertRoomLocked`, `service_room.go:268`). Plainly: after a restart the watcher joins but **does not wake the agent**; `Load` does not start agents (`service.go:189`) and join does not call `ensureAgentLocked`. The humans see the join note.

**Output (B1).** Dedup key is `watcher:<pipeline>` — **one open watcher room per pipeline**. On the first firing signal: `alertin.Alert{Source:"watcher", Status: firing, Severity, PipelineID, Title: "orders: connector pg-orders FAILED 4m; slot debezium_orders 1.3 GB, +410 MB/h", DedupKey:"watcher:orders", Raw: evidence}` → `OpenAlertRoom`. But first `OpenRoomForPipeline(pipelineID)` (new Service method): if any room for this pipeline is already open, including one from Alertmanager, the watcher posts its evidence there as a join instead of opening a second room. Later signals and re-fires join with their evidence in the join text. While firing, at most one "still firing, now X" join every 15 min. When all signals clear: the same alert with `Status: resolved`; the service posts the resolved note and leaves the room open (`service_room.go:268-280`), or returns `ErrResolvedAlertNoRoom`, which is logged and dropped.

Storm cap: if the same signal fires on ≥ 3 pipelines in one tick (one Connect worker died), open one room on the first pipeline with all affected pipelines in the evidence; log the rest. `max_open_watcher_rooms` defaults to `PIPEROOM_MAX_AGENTS` (3, PR #43), so the watcher never queues more agents than can run. #49's per-room token budget applies unchanged; the watcher never calls a model. `Raw` is our own JSON but still passes through the untrusted alert wrapper (`startAgentLocked`, `service_room.go:210`); fine.

**Agent visibility (I5).** The agent reads evidence only from `Alert.Raw` at start. System `agent.message` events are not fed to the inbox (`feedInbox`, `service.go:612-660`). So re-fires and joins are seen by humans, not the agent, in v1. Humans can paste or `@agent`. A `Kind:"alert_update"` human input (A4 contract) is deferred to v2.

**Slack.** The title (with numbers) is mirrored by the existing bridge; raw is never rendered (`internal/slackbridge/render.go`). Evidence stays in the room.

**Logs.** One `slog` line per transition and one per "still firing" with fields `mode pipeline signal value threshold since room`. This is the shadow-mode metric. Debug line per tick; warn per skipped tick; one clear line when the slot query fails with "recovery is in progress" or an unknown `wal_status` column (I8).

## 4. Thresholds and config

```yaml
pipelines:
  - id: orders
    # ...existing fields...
    watch:                          # absent = defaults, mode from env
      enabled: true
      slot: debezium_orders         # optional; default: connector config slot.name
      wal_headroom_bytes: 50GiB     # optional; turns on "disk full in N h"
      signals:                      # every key optional
        connector_down: {for: 3m}
        slot_inactive:  {for: 5m}
        slot_wal:       {fire: 1GiB, clear: 768MiB, for: 10m, eta_sev1: 6h}
        # v2, parsed but ignored in v1:
        freshness:      {sla: 30m, clear: 15m, for: 5m, interval: 10m, tz: UTC}
        consumer_lag:   {fire: 1000, clear: 500, for: 10m, members_expected: true}
        long_tx:        {age: 10m, for: 5m}
        compat_none:    {action: log}
        unreachable:    {for: 5m, action: log}
```

Each signal accepts `enabled: false` and `action: log|room` (defaults: `room` for the v1 three, `log` for the rest). `slot_wal` numbers are exposed through one `owners → topology.Thresholds` mapping so `pipeline_map` and the watcher cannot drift (`internal/topology/builder.go:38-46`). `dev/owners.yaml` sets `connector_down` and `slot_inactive` to `for: 30s` and leaves `slot_wal` at its defaults (fire 1GiB): a healthy dev slot retains 0.8-1.1 MB under load (`dev/scenarios.md`), so a small `fire` would warn forever. With the 15 s minimum interval, `for: 30s` is up to 3 intervals after the first observation (I4).

Env (`internal/config/config.go`): `PIPEROOM_WATCH_MODE` = `off|log|rooms`, **default `log`**; `rooms` is an explicit step in `docs/pilot/setup.md`. `PIPEROOM_WATCH_INTERVAL` (60 s, min 15 s). `PIPEROOM_WATCH_CHECK_TIMEOUT` (10 s, max = `statement_timeout`). `PIPEROOM_WATCH_COOLDOWN` (60 m). `--demo` forces `off`.

## 5. Safety

Read-only: only read methods; source pools are `default_transaction_read_only` (`postgres.go:31`). No new grants: `pg_replication_slots` is readable by any role; Connect is GETs; all in `permissions.md:14-21`. Scoped: object names come only from `owners.Pipeline` via `tools.WithPipeline`/`ScopeFor` (#48, `scope.go:47,102`); never a free name. Never fixes anything. Off per pipeline and per signal, plus the global mode. Evidence (I6): no `Username`, no `Query` (`clients.Transaction`, `clients.go:139-145`); task trace = first line only (exception class and message), capped at 300 chars; `kafkatools` has an unexported `condenseTrace` (`connect_tools.go:215-232`); W2 exports it or copies the one-line rule. `setup.md` says: `SOURCE_DB_URL` must be the primary, PostgreSQL 13+ (replicas error on `pg_current_wal_lsn()`).

## 6. Contract changes (lead)

1. `config.go`: four `PIPEROOM_WATCH_*` fields; `config_test` asserts names and defaults.
2. `internal/alertin/alertin.go:42-51`: `Watcher Source = "watcher"`, not in `Sources` (no HTTP route). Comment on `AlertSource` in `events.go:105-108` lists it.
3. `clients.ReplicationSlot`: `SafeWALBytes *int64` + query column. `clients.Transaction`: `HasXID bool` and `LongTransactions` filters `backend_xid IS NOT NULL` (only writers hold `restart_lsn`; `postgres.go:161-171` today returns BI reads too). v2 uses it, but it is a one-line fix worth doing now.
4. `room.Deps.OnRoomClosed`; `Service.OpenRoomForPipeline(id) (roomID string, ok bool)`; `Service.LastRoomForKey(pipelineID, dedupKey) (roomID string, closedAt *time.Time, ok bool)` (store query; `ClosedAt` exists on the snapshot, `snapshot.go:69`).
5. `docs/api.md`: `alert_source: "watcher"`, key shape `watcher:<pipeline>`, resolved rule, join-on-open-room rule.
6. `AGENTS.md` §2: row `A7 — internal/watch/**` (or A3).
7. `owners.yaml` schema as §4.
8. v2 only: `KafkaAdmin.EndOffsets` (no record fetch; `TopicOffsets` opens a client per topic, `kafka.go:131,395`), `agent.HumanInput{Kind:"alert_update"}`, `SourceDB.OldestPending`.

## 7. Tests and evals

Unit (no Docker), fake clients with scripted series and a fake clock. Tables: slope over 1 h/6 h windows, warm-up returns "unknown", zero/negative slope, ETA from `safe_wal_size` vs headroom vs neither; time-based `for` (breach `for − 1 s` → no fire; error tick in the middle → still fires on time; hover between `clear` and `fire` → no flap); heartbeat cap to sev3; `wal_status lost` → sev1 at once; ETA sev1 only after 2×`for`; `acked` after `OnRoomClosed` and reset on clear; cooldown skip; storm cap; per-check deadline (a hanging fake does not delay the others; overrun skipped); `Run` returns within 1 s of cancel and `runtime.NumGoroutine()` returns to the pre-start count. `watchMode(cfg, demo)` unit-tested. Service-level with `memstore`: fire opens one room; second signal joins the same room; an open Alertmanager room absorbs the watcher join; resolved posts the note; `Load` then re-fire joins and does not start an agent (asserted); mode `log` opens nothing but logs transitions. Integration (`-tags integration`): `devctl break slot-inactive` → `connector_down` then `slot_inactive` fire in one room within 3 intervals; `fix` → resolved. Eval (C6): `watcher-connector-down` scenario with the evidence JSON as agent input, graded on naming the connect layer.

## 8. PR split

| PR | Owner | Scope | Done when |
|---|---|---|---|
| W1 contracts | Lead (config, alertin, events comment, api.md, AGENTS.md) + A3 (`owners.Watch`, defaults, size/duration parsing, topology mapping) + A4 (`SafeWALBytes`, `HasXID`) | ~350 lines | `go test ./internal/owners ./internal/config ./internal/clients/...` cover defaults, bad units, `enabled:false`, the four env names, the new columns against the integration Postgres |
| W2 watcher core | A7 (new) `internal/watch/**` | three signals, state machine, slope/ETA, loop, fakes, tests; no server wiring | all §7 unit tests green without Docker; cancel test passes |
| W3 wiring | A5 (`cmd/server`, `internal/room`) | `OnRoomClosed`, `OpenRoomForPipeline`, `LastRoomForKey`; start watcher after `svc.Load`, stop before `Close`; `watchMode` | `memstore` service tests in §7 green; `watchMode` unit test |
| W4 demo + docs | A1 (`dev/owners.yaml`, integration test), C6 (eval), C14/lead (`setup.md`: mode steps, primary PG13+, headroom) | — | integration test green against the dev stack |

## 9. Risks and open decisions

- **Shadow mode may stay shadow.** Default `log` means a pilot must flip `rooms`. Proposal: `setup.md` makes it the step after the first week of clean logs; the lead decides.
- **Free disk stays invisible.** `safe_wal_size` is exact but NULL when `max_slot_wal_keep_size=-1` (RDS default). Then we show rate and, if configured, headroom ETA only. CloudWatch `FreeStorageSpace` is a new grant; later.
- **Agent misses updates** (I5) until `alert_update` lands. Accepted for v1.
- **One room per pipeline** hides two unrelated incidents on the same pipeline at once. Rare; the join text carries both.
- **v2 costs.** `Freshness` is `SELECT max(col)` (`warehouse.go:68`, `snowflake.go:71-75`): on Snowflake a 60 s poll defeats auto-suspend; `TIMESTAMP_NTZ` as IST wall time is off by 5.5 h forever. v2 needs `freshness.interval ≥ auto-suspend`, an index on `sink_ts_column`, and a `tz` field. `consumer_lag` must use "min lag in window never below `clear`" plus `members_expected` for Flink-style consumers. Decide the v2 order after the first pilot's logs.
- **Cost.** Every room is an Opus run bounded by #49's budget; `max_open_watcher_rooms` = 3 bounds the count.
- **F7**: one watcher per process per credential set; fine for the pilot wedge.

## Changes after review

- B1: dedup key is now `watcher:<pipeline>`; later signals join; `OpenRoomForPipeline` absorbs into an open Alertmanager room; storm cap; `max_open_watcher_rooms` = `PIPEROOM_MAX_AGENTS`.
- B2: `disk_bytes` → `wal_headroom_bytes`; primary ETA from `safe_wal_size` (new `SafeWALBytes`); slope over 1 h/6 h, ≥ 3 samples over 30 min; ETA sev1 holds 2×`for`; `lost|unreserved` stays sev1.
- B3: `PIPEROOM_WATCH_MODE` default `log`; heartbeat check caps idle-WAL to sev3.
- B4: `OnRoomClosed` + `acked` state; cooldown via `LastRoomForKey`; doc says restart joins but does not wake the agent.
- B5: source checks once per tick; ≤ 1 connection in v1; `LongTransactions` filters `backend_xid`.
- I1–I3: moved to v2 with the fixes named in §9 (not dropped; the research does not name them as the top failures).
- I4: time-based `breaching_since`; error ticks are unknown; demo arithmetic fixed to 3 intervals.
- I5: gap stated; `alert_update` deferred.
- I6: evidence drops username/query; trace first line only.
- I7: `action: log` for `compat_none`/`unreachable`; PAUSED sev3, FAILED sev2; zero tasks unhealthy; `connector_missing`.
- I8: primary PG13+ in `setup.md`; one clear log line.
- I9: every Done-when is a test.
- I10: v1 = `connector_down`, `slot_inactive`, `slot_wal`.
- Nits: timer with jitter, transition-only open, 15-min "still firing", owners→topology mapping, `events.go` comment, `check_timeout ≤ statement_timeout`, log fields for shadow metrics.
- Not changed: evidence also as a system message in the room. Kept because it is the only place humans see numbers that are not in the title; it is not mirrored to Slack (C12 stays optional).