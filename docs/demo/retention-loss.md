# Live demo script: the `retention-loss` incident

About 10 minutes. A topic's retention is cut, Kafka deletes records a consumer never read, and the watcher opens a
**sev1** room at once. The agent has to work out that records are *gone*, not slow, and must not paper over the
gap. The honest ending differs from `schema-change`: nothing in the stack can bring the deleted rows back, so the
demo ends on a **backfill plan**, not on a green dashboard.

Everything here was written from the repo, not from a recorded run (`dev/scenarios.md` §12 says "not yet
observed"). Each claim cites its file. **Anything marked ⚠ needs rehearsal.** **Do the rehearsal below on the demo
machine the day before, and replace the timings with what you see.**

## Roles

Same people and layers as `docs/demo/schema-change.md` (`dev/users.yaml`, `dev/owners.yaml` `layers:`).

| Role | Who | Job in the demo |
|---|---|---|
| Presenter / driver | `arjun` (owns `kafka-connect`, `kafka`) | Runs the commands, drives the room, steers the agent |
| Consumer owner | `kabir` (owns `consumer`) | Second browser. `order-sync` is his group: he comments, and approves any `consumer`-layer step (offset reset) |
| Watcher / postgres owner | `priya` (owns `postgres`, `schema-registry`) | Third browser. Comments on the source-side evidence |
| Hand-off target | `meera` (owns `warehouse`) | Accepts the hand-off |
| Operator (off screen) | anyone | Holds the fallbacks; watches the server log |

One person can play all of them with separate browser profiles, as in `schema-change.md`.

## Before the audience arrives (T-30 min)

Follow steps 1 to 5 of `docs/demo/schema-change.md` unchanged (stack lock, `dev/demo-room-db.yml`, `devctl up`,
server with `dev/demo.env`, web app). The reasons, in particular why the room database is separate from the one
`devctl reset` wipes, are there. What is specific to this scenario:

1. **Nothing to turn on for the signal.** `offset_below_log_start` is on by default with action `room` and no `for`
   delay (`internal/owners/watch.go:295`, `:255`), and `retention-loss` declares it (`cmd/devctl/scenarios.go:43-47`).
   `dev/owners.yaml` does not override it. The room is created on the first watcher tick that sees the condition, so it
   is faster than `schema-change`: no 30 s signal window, up to one 15 s interval (`dev/demo.env`). ⚠ confirm.
2. **Write mode must be on** for the agent's proposals to execute (`PIPEROOM_WRITE_MODE=on` in `dev/demo.env`).
3. **Rehearse once:** `go run ./cmd/devctl demo --rehearse retention-loss --owner demo` (`cmd/devctl/rehearse.go:105`
   lists the scenarios; the argument is optional and defaults to `schema-change`). It breaks, waits for the room,
   resets, and prints `PASS: break -> room in <time>`. Note that time, open the room, **close it**.
4. The rehearsal ends with a reset. Check `go run ./cmd/devctl health`. Do not run a second `break` without
   another `reset`.
5. ⚠ **The demo reset restores the topic.** `devctl reset` runs `docker compose down -v`
   (`schema-change.md`), which recreates the topic with default retention. Confirm with the rehearsal that
   `pg.public.orders` shows no `retention.ms` override afterwards (the agent's `kafka_topic_config` tool reads it).

## The break

`go run ./cmd/devctl break retention-loss --owner demo` (`cmd/devctl/scenarios_retention.go`). It:

1. stops the `devconsumer` container and waits for `order-sync` to be `Empty` with 0 members (up to 60 s), so its
   committed offsets stay where they are;
2. sets `retention.ms=1000` and `segment.ms=1000` on `pg.public.orders` and inserts 50 orders, which Debezium
   streams so the active segment rolls;
3. waits up to 90 s until some partition's committed offset is below its log start (the broker checks retention
   every 1 s, `dev/docker-compose.yml:40`), then logs `broken: ... partitions_below=[...]`.

Use `break`, not `demo retention-loss`: `demo` resets first and takes minutes (`cmd/devctl/demo.go`).
⚠ The break time (expected: seconds to well under 90 s) has not been measured.

## Timed beats (about 10 minutes)

| Time | Beat | Say | Do / click |
|---|---|---|---|
| 0:00 | Healthy pipeline | "Postgres, Debezium, Kafka, a consumer, a warehouse. Right now it works." | `go run ./cmd/devctl status`; everything green |
| 0:45 | Break it | "Someone set the topic's retention to one second instead of days. Our consumer happened to be down while the topic rolled." | `go run ./cmd/devctl break retention-loss --owner demo`. Wait for the `broken:` line |
| 1:30 | The room opens, sev1 | "No page. The watcher noticed a consumer's position points at records Kafka already deleted. That is data loss, so it is sev1 straight away." | Room list, open the new room as arjun. Expected room label: `group order-sync committed below log start on pg.public.orders/<p> (<n> records gone)` (`internal/watch/lag_signals.go:507`) ⚠ the exact title and severity shown in the UI need rehearsal |
| 2:15 | The agent investigates | "Read-only. Topic offsets, consumer lag, topic config, the row in Postgres and in the warehouse." | Point at the steps. Tools the runbook names: `kafka_topic_offsets`, `kafka_consumer_lag`, `kafka_list_groups`, `warehouse_freshness`, `trace_record`, `warehouse_row`, plus `kafka_topic_config` (`internal/tools/runbooks/library/retention-loss.md`; `evals/scenarios/topic-retention-data-loss.yaml`) |
| 3:30 | The finding | "Not a slow consumer. The committed offset is *below* the log start: those records no longer exist. Connect is healthy; restarting it would do nothing." | Hypothesis card. The right finding names retention deleting records before `order-sync` read them (eval `expect.root_cause`, layer `kafka`). Point at `retention.ms=1000` and the committed-vs-start offsets in the cited steps |
| 4:15 | Many eyes, one driver | "Kabir owns the consumer and wants to know if it can just resume." | As kabir: comment on the lag step, send a suggestion ("Can we reset to latest?"). As arjun: forward it. Expected: the agent says no, that skips more (eval forbids `reset* latest`) |
| 5:15 | The honest proposal | "The agent cannot restore deleted data, and it says so." | Open the proposal(s). See *What the agent can propose* below |
| 6:45 | Approvals | "Different layers, different owners." | Approve as the owners of the layers the proposal names |
| 7:45 | Stack back to normal | "Retention is back to defaults, the consumer is running again. The rows deleted during the gap are still missing." | `go run ./cmd/devctl fix retention-loss --owner demo` (see *Recovery*), then `go run ./cmd/devctl health` |
| 8:30 | Hand-off | "The hand-off lists the open work: backfill the window." | As arjun: hand-off to meera; meera reads and accepts. A hand-off summary that claims no data was lost would be wrong; check it states the gap |
| 9:30 | Close | "Everything is in the log." | Close the room with a one-line resolution that mentions the backfill |

## What the agent can propose (set expectations before the demo)

Verified from the repo:

- There is **no action that restores topic retention**. The registered Kafka actions are `kafka_reset_consumer_offsets`
  and `dlq_replay` (`internal/actions/kafka_reset_offsets.go`); there is `connect_snapshot_table` for incremental
  snapshots (`internal/actions/snapshot_table.go`). The eval says the same: "Safe automated fix: none"
  (`evals/scenarios/topic-retention-data-loss.yaml`).
- `kafka_reset_consumer_offsets` needs the group to have no members (the break leaves it `Empty`), moves to
  `earliest`, and is a `consumer`-layer action (kabir approves). The runbook's risk is that resetting "hides the gap
  instead of filling it".
- The backfill the runbook recommends is a Debezium `execute-snapshot`. `connect_snapshot_table` with
  `via: signal_topic` needs the Kafka signal channel on the connector; the plain dev connector does not enable it
  (only the `missed-table-kafka-signal` scenario does, `dev/scenarios.md`). ⚠ **Rehearse what the agent proposes**:
  it may propose a snapshot that Validate refuses, or only describe the backfill in text. Either is acceptable for the
  story; do not promise the audience an executed backfill.

⚠ Which of these the live agent actually proposes has not been observed.

## Recovery

- **For the demo:** `go run ./cmd/devctl fix retention-loss --owner demo` deletes the `retention.ms` and
  `segment.ms` overrides, starts `devconsumer` and waits for `order-sync` to be `Stable` with 1 member (up to 60 s)
  (`cmd/devctl/scenarios_retention.go`). It is idempotent. The consumer then resumes at the earliest offset
  still on the topic (`cmd/devctl/scenarios_retention.go` header; `auto.offset.reset=earliest` per
  `dev/scenarios.md` §12).
- **What it does not do:** it does not restore the deleted rows. `dev/scenarios.md` §12 says the real fix is a
  backfill. Say this on stage. `go run ./cmd/devctl health` going green means the pipeline flows again, not that
  the warehouse is complete. Whether `health` is green right after the fix, and whether the warehouse count differs
  from Postgres, is ⚠ not verified.
- If the agent's own approved step already did part of the work, `fix` is still safe to run.

## Fallbacks

Same structure as `schema-change.md`; the ones that change:

- **The agent is slow / no API key / WebSocket drops / room does not open:** use the matching sections of
  `docs/demo/schema-change.md` unchanged. Manual room: `POST /api/rooms` with `{"title":"orders: consumer offsets below log start","severity":"sev1","pipeline_id":"orders"}`.
  `--demo` (scripted fake agent) plays the `schema-change` story only, not this one; say so rather than use it.
  ⚠ confirm.
- **The break fails** (`order-sync` did not leave the group, or offsets did not fall below the log start within
  90 s): run `go run ./cmd/devctl fix retention-loss --owner demo`, then `reset`, and break again. Check
  `docker compose -f dev/docker-compose.yml logs kafka --tail 50`.
- **The agent says "the consumer is just lagging"**: steer it ("Compare the committed offsets with the log start
  offsets for every partition"); the eval lists this as the main trap (Stable group that looks merely slow).
  In this dev break the group is `Empty`, not `Stable` (`scenarios_retention.go`), so this should be rarer than in the eval.
- **The watcher opens two rooms or none:** one room per pipeline; a closed room has a 30 s cooldown
  (`dev/demo.env`). Close stale rooms.
- **An approval or the executor fails:** read the error; fall back to `devctl fix retention-loss`.

## After the demo

- Close any open room.
- If still broken: `go run ./cmd/devctl fix retention-loss --owner demo`. Then `reset` before a second break.
- `go run ./cmd/devctl unlock --owner demo`; stop the server and web app; `devctl down`; `docker compose -f dev/demo-room-db.yml down -v`.

## Needs rehearsal (summary)

1. Time from `break` to the room, and the room's title and severity in the UI.
2. Whether the agent proposes a reset, a snapshot, both, or text only; whether Validate accepts each on the dev stack.
3. Break duration; state of `health` and the warehouse after `fix`.
4. That `devctl reset` clears the retention override.
5. That the hand-off summary mentions the unrecovered gap.
