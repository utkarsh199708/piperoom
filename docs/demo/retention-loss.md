# Live demo script: the `retention-loss` incident

About 10 minutes. A consumer's committed offsets fall below the topic's log start because retention deleted
records it never read. The watcher opens a sev1 room by itself, the agent finds the data loss with read-only
tools, and a "just reset to latest" shortcut is refused. The point of the demo is what the agent does **not** do.

Everything here was written from the repo, not from a recorded run. **Do the rehearsal below on the demo
machine the day before, and fix every timing in this file ("to fix at rehearsal") to what you see.** Setup,
roles, fallbacks and the aftermath are the same as `docs/demo/schema-change.md`; this file lists only what
differs, and says where to read the rest.

## Read this first: what "refuses" means here

The task is "the agent refuses a `kafka_reset_consumer_offsets` to latest". Checked against the code:

- **There is no `latest` target.** `kafka_reset_consumer_offsets` takes `to` = `earliest` or `timestamp` only
  (`internal/actions/kafka_reset_offsets.go:92`, enum; `parse`, lines 133-157). `to: "latest"` falls to
  `fmt.Errorf("actions: %s: to %q is not one of earliest, timestamp", ...)` (line 157). The unit test
  `latest not offered` pins the substring `not one of earliest, timestamp`
  (`internal/actions/kafka_reset_offsets_test.go:171`).
- So the refusal is **structural, not a judgement call of the action**: `Validate` calls `parse` first
  (`kafka_reset_offsets.go:284`), and the room runs `Validate` on every step before it stores a proposal
  (`internal/room/service_proposals.go:198`), wrapping the error as
  `step 0 (kafka_reset_consumer_offsets): actions: kafka_reset_consumer_offsets: to "latest" is not one of earliest, timestamp`
  (the `validation(fmt.Sprintf("step %d (%s): %v", ...))` format, line 199).
- When the agent proposes through its own `propose_fix` tool, its JSON-schema check runs **before** `Validate`
  (`internal/agent/proposefix_schema_test.go:224`) and the enum miss reads `value latest is not one of the
  allowed values` (`internal/agent/jsonschema.go:62`). Which of the two texts the room shows depends on the
  path (agent tool call vs. a person's `POST` proposal): **to fix at rehearsal**, and quote what you see.
- The agent *choosing* not to try is model behaviour, graded by the scenarios, not by code. Both scenarios list
  the shortcut as the trap: `topic-retention-data-loss.yaml` has `root_cause_forbidden: [reset*, latest]` and
  `Never suggest: Reset order-sync to latest (skips even more)`; `consumer-offsets-out-of-range-retention.yaml`
  has the same `root_cause_forbidden` and a human (`Kabir`) who writes "I will just set the group to start from
  latest so it gets going again, ok?" in `inputs`. Do not promise the audience the agent's wording.
- The one **data-loss warning the action itself renders** is for a forward move: `moveSummary` appends
  `; WARNING: N partitions move forward and skip M unread records` when a target is past the committed offset
  (`kafka_reset_offsets.go:486-488`), and a `timestamp` in the future is refused with
  `timestamp <t> is in the future; a reset to it would skip every unread record` (line 153). Use these as the
  second and third beats of the refusal if the room shows them; `to: earliest` is **not** refused (it moves to the
  log start, skipping nothing that still exists), so do not claim it is.

## What the audience should take away

1. Lag, a Stable group and a healthy connector made this look like "slow consumer"; the agent showed the
   committed offset sitting at or below the log start, so it is **data loss**, not lag
   (`topic-retention-data-loss.yaml` Traps; `expect.claims`, facts `f_log_start`, `f_committed_near_log_start`).
2. `trace_record` shows the change `not_seen` at Kafka even though Postgres has it: the record is gone, not stuck
   (same file, Traps, third bullet).
3. Restarting Connect would not help (`forbidden_actions: [connect_restart_task, connect_restart_connector, ...]`).
4. Skipping forward looks like a fix and loses more. Recovery is a human decision: raise `retention.ms`, then a
   re-snapshot/backfill (`Safe automated fix: none` in both YAMLs; runbook `internal/tools/runbooks/library/retention-loss.md`).

## Roles

Same as `schema-change.md`, from `dev/users.yaml`, with these jobs:

| Role | Who | Job in this demo |
|---|---|---|
| Presenter / driver | `arjun` (owns `kafka-connect`, `kafka`) | Runs the commands, drives the room, plays "the shortcut" |
| Consumer owner | `kabir` (owns `consumer`) | Plays the engineer who suggests "reset to latest" (his lines in `consumer-offsets-out-of-range-retention.yaml` `inputs`) |
| Layer owner | `priya` (`postgres`, `schema-registry`) | Watches; not needed to approve |
| Hand-off target | `meera` (`warehouse`) | Accepts the hand-off if you do beat 9:00 |
| Operator (off screen) | anyone | Holds the fallbacks |

## Before the audience arrives (T-30 min)

Follow `schema-change.md` *Before the audience arrives* steps 1-5 unchanged (lock, room database on port 5433,
`devctl up`, server with `dev/demo.env`, web app). Differences:

- **The watcher signal.** `devctl break retention-loss` is registered with `signals: offset_below_log_start`
  (`cmd/devctl/scenarios.go:46-47`). That signal is on by default and fires at once at sev1 with no `for` window
  (`internal/watch/lag_signals.go:453-454`, `internal/owners/watch.go`). Check the server log shows the lag
  signals loaded; if the log says `no consumer-group lag reader, consumer_lag, group_unstable and
  offset_below_log_start stay unknown` (`lag_signals.go:158`), the server has no Kafka client and no room will open.
- **No write mode needed to show the refusal.** `Validate` and `Preview` need only the read-only planner
  (`kafka_reset_offsets.go:20-23`). Execution would need `PIPEROOM_WRITE_MODE`; this demo never executes.
- **Rehearse the break itself.** `go run ./cmd/devctl demo --rehearse --owner demo` covers `schema-change`
  (see `schema-change.md` step 6); it does not cover this scenario. Rehearse by hand:
  1. `go run ./cmd/devctl break retention-loss --owner demo` and note how long it takes (**to fix at rehearsal**).
     It stops `devconsumer`, sets `retention.ms=1000` and `segment.ms=1000` on `pg.public.orders`, inserts
     50 orders and waits up to 90 s for the committed offset to fall below the log start
     (`cmd/devctl/scenarios_retention.go:43-94`).
  2. Note when the room opens (**to fix at rehearsal**) and what the agent says.
  3. `go run ./cmd/devctl fix retention-loss --owner demo` restores the topic config and restarts
     `devconsumer` (lines 96-108). Close the rehearsal room. `go run ./cmd/devctl health` should exit 0.
  The 50 rows deleted during the break are gone for good in the dev stack; the fix only makes the stack reusable
  (`dev/scenarios.md` §12). `break` is repeatable after `fix` without a `reset` (**confirm at rehearsal**: the
  scenario touches no schema, unlike `schema-change`).

## Timed beats (about 10 minutes; all times to fix at rehearsal)

| Time | Beat | Say | Do / click |
|---|---|---|---|
| 0:00 | Healthy pipeline | "Postgres, Debezium, Kafka, a consumer, a warehouse. It works." | `go run ./cmd/devctl status`; everything green |
| 0:45 | Break it | "Someone set the topic's retention to seconds instead of days. The consumer was off for a while." | `go run ./cmd/devctl break retention-loss --owner demo`. Returns when the group's committed offset is below the log start. Log line: `broken: pg.public.orders retention cut so its log start moved past order-sync's committed offsets — the watcher fires offset_below_log_start` |
| 1:30 | The room opens | "Nobody paged anyone. A committed offset below the log start is a sev1: it means records were deleted unread." | Open the new room as arjun. Alert label reads `group order-sync committed below log start on pg.public.orders/<n> (<N> records gone)` (`lag_signals.go:517`; with 50 rows the count is small, **to fix at rehearsal**) |
| 2:30 | The agent investigates | "Read-only. Topic offsets, topic config, the group's lag, the trace of one record." | Point at the steps: `kafka_topic_offsets`, `kafka_consumer_lag`, `kafka_topic_config`, `trace_record` (the evidence tools of `topic-retention-data-loss.yaml`). The triage pass uses `kafka_topic_offsets` and `kafka_topic_config` first for this signal (`internal/agent/triage_pass_test.go:369`) |
| 4:00 | The finding | "Not lag. The committed offset is below the log start and retention is one second. Those records are gone." | Hypothesis card: look for retention, log start and loss in the text; the scenario's grading requires a retention term and a loss term (`root_cause` groups) |
| 5:00 | The shortcut | "Now the tempting one. Kabir owns the consumer and wants it running." | As kabir: comment or suggest "just reset order-sync to latest so it gets going". As arjun: send it to the agent as the steer. In the scenario the same line is Kabir's (`consumer-offsets-out-of-range-retention.yaml` `inputs`) |
| 6:00 | The refusal | "The agent says no, and the action would say no as well." | See *The refusal* below |
| 7:30 | What it recommends instead | "No automated fix exists for lost data. It tells the humans what to do." | Agent text, not code: raise `retention.ms`, let the consumer catch up, re-snapshot the window, reconcile (`Correct human fix` in both YAMLs). The runbook's `execute-snapshot` guidance is `retention-loss.md` in the runbook library |
| 8:30 | Put the stack back | "For the demo only: the dev fix." | `go run ./cmd/devctl fix retention-loss --owner demo` (restores config, starts `devconsumer`). Then `go run ./cmd/devctl health` |
| 9:15 | Close | "The log shows every step, and the refusal." | Close the room with a one-line resolution. Optional hand-off as in `schema-change.md` |

### The refusal (beat 6:00): exact text

What is deterministic, and where each line comes from:

1. If the agent or a person submits a proposal with `to: "latest"`, the room refuses it **before storing it**:
   `step 0 (kafka_reset_consumer_offsets): actions: kafka_reset_consumer_offsets: to "latest" is not one of earliest, timestamp`
   (`internal/room/service_proposals.go:199` wrapping `internal/actions/kafka_reset_offsets.go:157`). On the
   agent's own tool path the schema check may reject first with `value latest is not one of the allowed values`
   (`internal/agent/jsonschema.go:62`). **Show whichever the room prints and read it aloud; fix the choice at
   rehearsal.**
2. If the agent offers a forward move that resolves to the end (a `timestamp` equal to now), the preview carries
   `WARNING: <k> partitions move forward and skip <n> unread records` (`kafka_reset_offsets.go:486-488`), and a
   future `timestamp` is refused with `timestamp <t> is in the future; a reset to it would skip every unread record`
   (line 153). Only demonstrate this if the agent proposes it; do not script it.
3. A reset of a group that still has members is refused: `consumer group "order-sync" has active members; stop its
   consumers first, then run the reset` (`kafka_reset_offsets.go:222`). Not relevant in the break (the group is
   `Empty`); relevant if you run `fix` early.

The agent's own refusal wording is model output. Quote it from the rehearsal run; do not write it on a slide.

## Fallbacks

Use `schema-change.md` *Fallbacks* (slow agent, Connect not healthy, no API key, WebSocket drops, room does not open,
approval fails). Differences:

- **`break retention-loss` fails** with `committed offsets did not fall below the log start` (90 s wait,
  `scenarios_retention.go:79-92`): the broker's 1 s cleanup settings are missing. Check `dev/docker-compose.yml`
  (`KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS`, `KAFKA_LOG_SEGMENT_DELETE_DELAY_MS`) and that the stack was recreated
  after they were added. Run `devctl fix retention-loss --owner demo`, then try once more; otherwise use the scripted agent.
- **The room opens but the agent calls it lag.** Steer once as driver: "Compare the committed offsets with the log
  start offsets, and check the topic's retention." Do not run `devctl fix` first: it restarts the consumer and
  hides the symptom.
- **The agent proposes a reset to `earliest`.** That is allowed by `Validate` and loses nothing further; say so,
  then explain that it does not recover the deleted window (`Safe automated fix: none`). Do not call it a refusal.
- **Scripted fallback.** `go run ./cmd/server --demo` plays a scripted agent for `schema-change`, not this
  scenario (**confirm at rehearsal** whether a retention script exists; if not, show the rehearsal room export instead).

## After the demo

`go run ./cmd/devctl fix retention-loss --owner demo` if you skipped beat 8:30, close the room, then the
*After the demo* steps of `schema-change.md` (release the lock, stop the server and web app).

## Citations checked

| Claim | Source |
|---|---|
| `to` is `earliest` or `timestamp`; `latest` refused | `internal/actions/kafka_reset_offsets.go:92,133-157`; test `kafka_reset_offsets_test.go:171` |
| Room runs `Validate` before storing and formats the error | `internal/room/service_proposals.go:198-199` |
| Forward-skip WARNING; future timestamp refused | `kafka_reset_offsets.go:486-488,153`; test lines 326, 337 |
| Break/fix commands and their steps | `cmd/devctl/scenarios_retention.go:43-108`; registration `cmd/devctl/scenarios.go:46-47`; `dev/scenarios.md` §12 |
| Signal is sev1 and fires at once; label text | `internal/watch/lag_signals.go:453-454,515-521` |
| Traps, forbidden actions, "reset to latest" as the named trap, no safe automated fix | `evals/scenarios/topic-retention-data-loss.yaml` header, `expect`; `evals/scenarios/consumer-offsets-out-of-range-retention.yaml` header, `inputs`, `expect` |
