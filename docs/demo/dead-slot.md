# Live demo script: the `dead-slot` incident

About 8 minutes. A connector was retired on purpose and nobody dropped its replication slot, so Postgres keeps
every WAL segment. The watcher opens a room by itself, the AI agent shows the slot is dead weight, the postgres
layer owner approves one irreversible action, and the slot is dropped.

The point of this demo, and the reason it differs from `schema-change`: **the stack state looks exactly like a
connector that should be resumed.** Only a human saying "this connector is retired" makes dropping the slot the
right answer (`dev/scenarios.md` §9 "Why this is not §5"; `evals/scenarios/dead-slot-wal.yaml` header). The demo
shows the agent obeying that context, and the approval ceremony guarding an action that cannot be undone.

Everything here was written from the repo, not from a recorded run. Where a step could not be checked from the
code it says **needs rehearsal**. **Do the rehearsal below on the demo machine the day before, and fix the
timings in this file to what you see.** This script is built on `docs/demo/schema-change.md`; read it first for
the shared set-up and fallback reasoning.

## Known status (read this first)

`dev/scenarios.md` §9, "Live status (KAN-411, 2026-10-22)": the first live run of `devctl fix dead-slot` dropped
the slot and then failed, because the connector's stored offset pointed into the dropped slot. `fixDeadSlot` was
reworked (`cmd/devctl/scenarios_p3.go:366`) and is unit-tested against a fake stack
(`cmd/devctl/scenarios_p3_test.go`), but **it has not been re-run on a live stack**. This demo depends on it in
two places: the clean-up after the show, and the fallback "Fix by hand". Until someone has run it live and
replaced that paragraph in `dev/scenarios.md`, treat both as **needs rehearsal**, and keep `devctl reset`
(1-3 minutes) as the sure way back to a clean stack.

The agent's own fix, `pg_drop_replication_slot`, does not depend on `fixDeadSlot`. It is covered by
`internal/actions/pgactions/drop_replication_slot_test.go` and an `integration`-tagged test next to it.

## Roles

| Role | Who (`dev/users.yaml`, `dev/owners.p3.yaml`) | Job in the demo |
|---|---|---|
| Presenter / driver | `arjun` (owns `kafka-connect`, `kafka`) | Runs the commands, drives the room, tells the agent the connector is retired, hands off at the end |
| Watcher | `kabir` (owns `consumer`) | Second browser. Comments on a step, sends a suggestion. Shows "many people, one driver" |
| Layer owner | `priya` (owns `postgres`, `schema-registry`) | Third browser. The only approver this fix needs: the action's layer is `postgres` (`internal/actions/pgactions/drop_replication_slot.go`, `Layer()`) |
| Hand-off target | `meera` (owns `warehouse`) | Accepts the hand-off at the end |
| Operator (off screen) | anyone | Holds the fallbacks below; watches the server log |

`arjun` cannot approve this fix: he owns neither `postgres` nor a fallback for it. That is the "different people,
different layers" beat, and it is why `priya` has the key moment. One person can play all four with four browser
profiles (as in `schema-change.md`).

## Before the audience arrives (T-30 min)

Follow `docs/demo/schema-change.md` steps 1-3 and 5 (stack lock, room database on port 5433, `devctl up`, web app)
unchanged. These steps differ:

1. **Owners file.** `dead-slot` needs the server started with `OWNERS_FILE=dev/owners.p3.yaml`
   (`cmd/devctl/main.go` usage text; `dev/scenarios.md` "These four scenarios need the server started with
   `OWNERS_FILE=...`"). On the baseline `dev/owners.yaml` the room is not set up for this scenario. That file sets
   `slot_inactive` and `connector_down` to `for: 30s` (`dev/owners.p3.yaml`), which is what makes the room open
   within a minute.
2. **A write role for the source database. Needs rehearsal.** The drop runs through `SOURCE_DB_WRITE_URL`, a role
   that is **not** the read-only `SOURCE_DB_URL` role (`internal/wiring/writemode.go`, `checkSourceWriteURL`).
   `dev/demo.env` sets `PIPEROOM_WRITE_MODE=on` but **sets no `SOURCE_DB_WRITE_URL`**, and no file in `dev/` creates
   such a role. Without it the approval goes through and `Execute` fails with "no write credentials"
   (`drop_replication_slot.go`, `errNoWriteCredentials`; `docs/pilot/write-roles.md`). The drop needs `REPLICATION`
   (or superuser) on the role (`docs/pilot/write-roles.md`, "Source Postgres"). A role must exist **after the last
   `devctl reset`**, because reset wipes the database. Suggested, not yet run:
   ```
   docker compose -f dev/docker-compose.yml exec -T postgres psql -U piperoom -d orders -c \
     "CREATE ROLE piperoom_writer LOGIN REPLICATION PASSWORD 'piperoom-dev'"
   export SOURCE_DB_WRITE_URL='postgres://piperoom_writer:piperoom-dev@localhost:5432/orders?sslmode=disable'
   ```
   (Dev-stack-only credentials, as in `AGENTS.md` §3. The `debezium` role of `connector-auth` is not an option:
   it exists only after that scenario's break.) Whether the dev Postgres accepts this login from the host, and
   whether the writer's start-up checks pass, is **unverified**.
3. **Start the server** with the demo profile plus the owners file and the writer URL:
   ```
   export ANTHROPIC_API_KEY=...        # from your shell, never from a file
   set -a; . dev/demo.env; set +a
   export OWNERS_FILE=dev/owners.p3.yaml
   export SOURCE_DB_WRITE_URL=...      # step 2
   go run ./cmd/server
   ```
   The log shows `server: watcher starting` with mode `rooms` (`schema-change.md`). If wiring refuses
   `SOURCE_DB_WRITE_URL`, the log names the reason (same role as the read URL, or a read-only role).
4. **Rehearse once:** `go run ./cmd/devctl demo --rehearse dead-slot --owner demo`. It accepts any scenario
   (`cmd/devctl/rehearse.go`, `parseDemoFlags`), resets, loads a short burst, runs `breakDeadSlot`, waits for a
   room on pipeline `orders`, resets again, and prints `PASS: break -> room in <time>`. Note that time. The
   rehearsal ends with a reset, which **wipes the writer role from step 2**: create it again afterwards, and
   restart the server so it picks up nothing stale (the server's rooms are in the 5433 database and survive).
   Open the rehearsal room and run the whole beat table below against it if you can; **close the room** when done
   (the next run refuses to start while one is open).
5. After the rehearsal's final reset: `go run ./cmd/devctl health` (exit 0). Do **not** run a second `break`
   without another `reset`: `breakDeadSlot` needs the slot to exist and refuses otherwise
   (`cmd/devctl/scenarios_p3.go:315`).

## Timed beats (about 8 minutes)

Timings are estimates from the watcher settings, not observations. **Replace them after the rehearsal.**

| Time | Beat | Say | Do / click |
|---|---|---|---|
| 0:00 | Healthy pipeline | "Postgres, Debezium, Kafka, a consumer, a warehouse. Right now it works." | Terminal: `go run ./cmd/devctl status`. Everything green |
| 0:45 | The platform team retires the feed | "Platform retired the orders connector for a new design. They stopped it and walked away. Nobody dropped its replication slot." | Terminal: `go run ./cmd/devctl break dead-slot --owner demo`. It stops the connector (Connect REST `stop`), waits for the slot to go inactive, inserts 100 rows, and logs `retained_before_bytes` / `retained_after_bytes` (`breakDeadSlot`). Seconds. Optional, to make the WAL number bigger: `go run ./cmd/devctl load --rate 20` in a second terminal (`dev/scenarios.md` §5/§9), stopped with Ctrl+C before the fix |
| 1:15 | The room opens | "Nothing failed. No alert from the application. The watcher saw a stopped connector and a slot that is not being read." | Room list in the browser. Open the new room as arjun. Expect it roughly a minute after the break (30 s signal window, 15 s polls, start-up; `dev/owners.p3.yaml`, `dev/demo.env`): **use your rehearsal time**. The title and severity come from the watcher; `connector_down` for STOPPED is sev3 (`internal/watch/signals.go`, `classifyConnectors`), `slot_inactive` is sev2 (`slotInactiveReading`), so expect both signals in one room (`docs/api.md`, one watcher room per pipeline) |
| 2:00 | The agent investigates | "Read-only. Connect status, the slot, the topology, the consumer group." | Point at the timeline: tool calls such as `connect_status`, `pg_replication_slots`, `pipeline_map`. Do not read the output. Point at `slot debezium_orders: active=false` and the retained WAL (`evals/scenarios/dead-slot-wal.yaml` shows the shape of these results; the real numbers are small, see below) |
| 3:00 | **The trap** | "Look at this state. Connector stopped, slot inactive, WAL growing. Everything a *paused* feed looks like. If I say nothing, the safe answer is to resume it. The agent cannot know the connector is retired. I do." | As arjun (the driver), send a steer: `The orders connector was retired on purpose by the platform team and will be re-created later with a new snapshot. It is not coming back. Priya is worried about disk.` The steer is delivered when the agent is idle (`evals/scenarios/dead-slot-wal.yaml`, last comment; `docs/api.md`, steer). Why: `internal/agent/prompt.go:95` tells the agent to propose a drop only when the alert, a driver message or the map says the connector is retired, and otherwise to run the resume checks |
| 3:45 | Many eyes, one driver | "Kabir owns the consumer. He wants to check it is not affected." | As kabir: comment on a step, send the agent a suggestion (for example "Is order-sync lagging?"); show kabir cannot steer directly. As arjun: send it on to the agent. Skip if running short |
| 4:30 | The proposal | "The agent cannot drop anything. It can only propose, and this one says it cannot be undone." | Open the proposal: one step, `pg_drop_replication_slot {connector: pg-orders-connector, slot: debezium_orders}`, required layer `postgres`. Show the preview: "NOT reversible ... needs a new snapshot", current state "inactive ... retains N of WAL" (`drop_replication_slot.go`, `Preview`; the preview appears as `proposal.previewed`, `docs/api.md`) |
| 5:15 | The approval | "Only the postgres owner can say yes to this. I am the driver and I cannot." | As arjun: point at the `postgres` approval, which he cannot give (the server answers `403 not_layer_owner`, `docs/api.md`; how the UI shows this is **needs rehearsal**). As priya: approve `postgres`. The executor runs the step |
| 6:00 | Result | "Slot gone. Postgres can recycle that WAL at the next checkpoints. The connector stays stopped, which is what retired means." | The room shows `action.executed` with output beginning `dropped replication slot debezium_orders of connector pg-orders-connector ...` (`Execute`). Terminal: `docker compose -f dev/docker-compose.yml exec -T postgres psql -U piperoom -d orders -c "select slot_name, active from pg_replication_slots"` shows no `debezium_orders` row. **Do not run `devctl health` here: it will show kafka-connect red, because the connector is intentionally STOPPED.** Say so before anyone asks |
| 6:45 | Hand-off | "Shift change. The agent writes the summary, the driver role moves." | As arjun: start a hand-off to meera. As meera: read the summary, accept. Driver badge moves (`schema-change.md`, same beat) |
| 7:45 | Close | "The log has every step, including who approved an irreversible action." | Close the room with a one-line resolution, for example "Retired connector's slot dropped; feed to be re-created with a new snapshot" |

## What the room and the agent should show

Everything below is from the code or the committed eval fixture; **none of it has been observed live**.

- **Stack state after the break** (`dev/scenarios.md` §9, "Expected symptoms (not yet observed)"): connector
  `pg-orders-connector` STOPPED with no tasks; slot `debezium_orders` `active=false`; `order-sync` Stable and lag 0;
  the warehouse goes stale. Nothing is FAILED, there is no trace and no DDL (`dead-slot-wal.yaml`, "Traps").
- **The WAL number is small on the dev stack.** The eval fixture says 1.4 GiB over 3.2 hours; the real break
  inserts 100 rows, so `retained_after_bytes` is in the kilobytes to low megabytes (`dev/scenarios.md` §5 saw
  about 1 MB). Do not say "gigabytes" on stage. Say what retained WAL does over days. Watcher thresholds do not
  help here: `slot_wal` stays at its 1 GiB default (`dev/owners.p3.yaml`), so **`slot_wal` will not fire**; the two
  signals that fire are `connector_down` and `slot_inactive`.
- **Triage first look.** `PIPEROOM_TRIAGE_PASS` defaults to on (`internal/config/config.go`), so the room's first
  steps may come from a bounded first look before the main agent starts (`docs/api.md`, "Triage pass";
  `internal/agent/triage_pass.go` has a `slot_inactive` branch). Whether it says anything about retirement is
  **needs rehearsal**.
- **Root cause wording.** With the steer, the agent should name the orphaned slot (postgres layer) as the cause,
  not the stopped connector (`prompt.go:95`: "If retired, the root cause is the orphaned slot (postgres layer)").
  Exact wording: **needs rehearsal**.
- **One proposal, one step.** The eval's expected fix is `[[pg_drop_replication_slot]]` with
  `{connector: pg-orders-connector, slot: debezium_orders}`, and it forbids `connect_resume_connector`,
  `connect_restart_task` and `connect_restart_connector` (`dead-slot-wal.yaml`, `expect`). The eval does not set
  `force`; it is only for `wal_status` lost (`prompt.go:95`), which will not be the case here.
- **Refusal rules the action enforces** (good to mention if asked): refused while the slot is active, refused for a
  slot that is not the connector's `slot.name`, refused for a physical slot, and `Execute` re-reads the slot in the
  drop's own transaction (`drop_replication_slot.go`).

## Fallbacks

Decide at the beat, not in front of the audience. For the agent being slow, Connect being unhealthy, no API key,
the WebSocket dropping and the room not opening, use the matching sections of `docs/demo/schema-change.md`,
with these changes: the manual room has `"title":"orders-db: slot debezium_orders inactive"` and the same
`pipeline_id` `orders`; the scripted `--demo` agent plays `schema-change`, not this incident, so **do not fall
back to `go run ./cmd/server --demo` for this demo** (the committed script is the schema-change one,
`schema-change.md`; whether it has a dead-slot script is not checked here).

### The agent proposes a resume instead of the drop

This is the trap working, and it is the most likely surprise: the agent may reach a proposal before your steer
is delivered (the steer is read when the agent goes idle).

- Do not approve it. As arjun (owner of `kafka-connect`) reject the `connect_resume_connector` proposal with a
  reason ("retired on purpose"). Say "this is why a fix needs the layer owner".
- Then send the steer from the 3:00 beat. The agent should propose the drop next. If it asks a question instead,
  answer it as the driver.
- Never approve the resume: it restarts a retired feed from a position nobody wants
  (`dead-slot-wal.yaml`, "Forbidden").

### The steer is not enough (the agent still will not propose a drop)

- Name the slot and the action in a second steer: `Propose pg_drop_replication_slot for debezium_orders on
  pg-orders-connector.` The model is allowed to refuse; check its reasoning in the room.
- Last resort, keeping the ceremony: as driver, create the proposal yourself with `POST /api/rooms/{id}/proposals`
  (`docs/api.md`, driver-authored fix, P3c) with the same step. It is validated like an agent proposal. Say that
  you did it.

### The approval goes through and the action fails

- Read the failure in the room (`action.executed` with `ok: false`). Likely causes, all from `drop_replication_slot.go`:
  - `no write credentials`: `SOURCE_DB_WRITE_URL` was not set or the server was started without it. Restart the
    server with it (server restarts are safe: rooms are in the 5433 database).
  - `in use by a consumer` / `ErrSlotActive`: something resumed the connector. `curl -X PUT $CONNECT_URL/connectors/pg-orders-connector/stop`
    (`cmd/devctl` uses the same call), then ask the agent to propose again.
  - A permission error from Postgres: the writer role lacks `REPLICATION` (`docs/pilot/write-roles.md`).
- Fix by hand and say the executor refused:
  `docker compose -f dev/docker-compose.yml exec -T postgres psql -U piperoom -d orders -c "select pg_drop_replication_slot('debezium_orders')"`.
  This is the same call the action makes.

### Fix by hand with devctl (**needs rehearsal; KAN-411**)

`go run ./cmd/devctl fix dead-slot --owner demo` drops the slot if it exists, recreates it, deletes the connector's
stored offsets, sets `snapshot.mode=when_needed`, resumes, and waits for a marker row in the warehouse and lag 0
(`fixDeadSlot`). **It is not live-verified** (see *Known status*). Use it only after the show. If you need a clean
stack live, `go run ./cmd/devctl reset --owner demo` (1-3 minutes) is the proven path.

### The pipeline looks broken after the fix

Expected, not a failure: the agent's fix drops the slot and leaves the connector STOPPED, the feed is retired
(`dead-slot-wal.yaml`: "Not reversible: the re-created connector starts from a new snapshot"). The warehouse
stays stale. If the audience asks "so is it fixed?": the disk risk is; restoring the feed is a separate decision.

## After the demo

- Close any open room.
- Put the stack back: `go run ./cmd/devctl fix dead-slot --owner demo` (needs rehearsal, above). If it fails or
  the stack is unhealthy, `go run ./cmd/devctl reset --owner demo` is the known-good way, and the next `break`
  needs the reset anyway when the agent already dropped the slot (`breakDeadSlot` refuses without a slot). Confirm
  with `go run ./cmd/devctl health` (exit 0).
- A reset removes the writer role: re-create it before the next run (*Before the audience arrives*, step 2).
- Release the lock, stop the server and the web app, and tear down as in `schema-change.md`
  (`devctl unlock`, `devctl down`, `docker compose -f dev/demo-room-db.yml down -v`).

## Needs rehearsal (checklist)

- [ ] A source write role: create it, confirm the server starts with `SOURCE_DB_WRITE_URL`, confirm the drop runs.
- [ ] `devctl demo --rehearse dead-slot`: record the break-to-room time and fix beat 1:15.
- [ ] Both watcher signals (`connector_down` sev3, `slot_inactive` sev2) land in one room; what the title says.
- [ ] Whether the first agent turn (or the triage first look) proposes a resume before the steer is delivered, and
      how long until the drop proposal appears after the steer.
- [ ] The preview text and the `action.executed` output as the UI renders them.
- [ ] `devctl fix dead-slot` on a live stack (KAN-411); then replace the "Live status" paragraph in `dev/scenarios.md`.
- [ ] What `devctl status` and the topology show for a STOPPED connector with no slot.
