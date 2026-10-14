# Live demo script: the `schema-change` incident

About 12 minutes. One engineer breaks the pipeline, a room opens by itself, an AI agent finds the
cause with read-only tools, the layer owners approve a fix, the fix runs, the driver role moves.

Everything here was written from the repo, not from a recorded run. **Do the rehearsal below on the
demo machine the day before, and fix the timings in this file to what you see.**

## Roles

| Role | Who (`dev/users.yaml`) | Job in the demo |
|---|---|---|
| Presenter / driver | `arjun` (owns `kafka-connect`, `kafka`) | Runs the commands, drives the room, steers the agent, approves for kafka-connect |
| Watcher 1 | `kabir` (owns `consumer`) | Second browser. Comments on a step, sends a suggestion. Shows "many people, one driver" |
| Layer owner | `priya` (owns `postgres`, `schema-registry`) | Third browser. Approves the schema-registry part of the fix |
| Hand-off target | `meera` (owns `warehouse`) | Accepts the hand-off at the end |
| Operator (off screen) | anyone | Holds the fallbacks below; watches the server log |

One person can play all four with four browser profiles (or private windows), logging in as each user
(`POST /auth/dev-login`, which the login screen does for you). Fewer people means more window switching;
say who you are each time you switch.

## Before the audience arrives (T-30 min)

1. Docker running. Take the stack lock: `go run ./cmd/devctl lock --owner demo`
2. Start the stack: `go run ./cmd/devctl up --owner demo`
3. Start the server with the demo profile (`dev/demo.env`: watcher in `rooms` mode, 15 s interval, 30 s cooldown):
   ```
   export ANTHROPIC_API_KEY=...        # from your shell, never from a file
   set -a; . dev/demo.env; set +a
   go run ./cmd/server
   ```
   (PowerShell: see the header of `dev/demo.env`.) Log should show `server: watcher starting` with mode `rooms`.
4. Start the web app (`cd web && npm run dev`) and open it as arjun, kabir and priya.
5. **Rehearse once:** `go run ./cmd/devctl demo --rehearse --owner demo`. It checks the profile, resets,
   breaks, waits for the room, resets, and prints `PASS: break -> room in <time>`. Note that time.
   Then **close the rehearsal room** in the UI (the next run refuses to start while one is open).
6. Confirm `go run ./cmd/devctl health` is green (exit 0). Leave the stack lock held under `demo`.

## Timed beats

| Time | Beat | Say | Do / click |
|---|---|---|---|
| 0:00 | Healthy pipeline | "Postgres, Debezium, Kafka, a consumer, a warehouse. Right now it works." | Terminal: `go run ./cmd/devctl status`. Everything green |
| 1:00 | Break it | "A developer ran a migration: `amount` changed from integer to numeric. Nobody told the data team." | Terminal: `go run ./cmd/devctl demo schema-change --owner demo`. It resets, loads traffic, breaks, prints how to watch. Takes about a minute |
| 2:30 | The room opens | "Nobody paged anyone. The watcher saw the connector fail and opened a room." | Room list in the browser. Open the new room as arjun. Expect it within ~1 min of the break (30 s signal window + 15 s polls + start-up; use your rehearsal time) |
| 3:00 | The agent investigates | "The agent can only read. Connect status, the schema registry, the replication slot, the consumer lag, the warehouse." | Point at the timeline: each tool call is a step with its result. Do not read the output aloud |
| 4:30 | The finding | "It says the connector task failed because the new schema is not compatible with the registered one. The consumer is healthy, so the problem is upstream." | Point at the hypothesis card and the steps it cites |
| 5:00 | Many eyes, one driver | "Kabir owns the consumer and wants to check something." | As kabir: comment on a step, then send the agent a suggestion (for example "Is the consumer group lagging?"). Show that kabir cannot steer directly |
| 6:00 | Driver steers | "I'm the driver, so I decide what the agent hears." | As arjun: send kabir's suggestion on to the agent; add one steer of your own |
| 7:00 | The proposal | "The agent cannot fix anything. It can only propose." | Open the proposal: steps, risk, rollback, required layers (`schema-registry`, `kafka-connect`). Show both approvals pending |
| 8:00 | Approvals | "A fix needs every layer owner. Different people, different layers." | As priya: approve schema-registry. As arjun: approve kafka-connect. The executor runs the steps |
| 9:00 | Recovery | "Connector task running again, schema registered, rows reach the warehouse." | Terminal: `go run ./cmd/devctl health`. The room shows the result of the run |
| 10:00 | Hand-off | "Shift change. The agent writes the summary, the driver role moves." | As arjun: start a hand-off to meera. As meera: read the summary, accept. Driver badge moves |
| 11:30 | Close | "Everything is in the log, and you can replay the room from any point." | Close the room with a one-line resolution |

After the demo: `go run ./cmd/devctl demo --fix schema-change --owner demo` (safe if the room already fixed
it; the fix is idempotent), then `go run ./cmd/devctl unlock --owner demo`.

## Fallbacks

Decide at the beat, not in front of the audience. The operator says "give me a moment" and fixes it; the
presenter keeps talking about the architecture.

### The agent is slow (no step after ~60 s, or the hypothesis takes more than ~2 min)

- The room's agent status says what it is doing (`working`, `queued` if all agent slots are busy). If it says
  `queued`, check the server for other running rooms; close stale ones.
- Fill the time: walk through the timeline and the topology, explain read-only tools and the untrusted-data wrapping.
- Steer it: as driver, send "Check the connector task trace and the schema registry subject first."
- Still nothing after 3 min: say so, then show the rehearsal room from the day before if you kept it, or the
  scripted run (see *No API key* below).
- Do **not** run `devctl demo --fix` early. That makes the agent's finding "everything is already fixed".

### Connect is not healthy (`devctl up` hangs, or `devctl health` shows kafka-connect red *before* the break)

- `go run ./cmd/devctl status` and `docker compose -f dev/docker-compose.yml logs connect --tail 50`.
- Connect needs 30–90 s after Kafka. Wait, then re-check. If still red: `go run ./cmd/devctl reset --owner demo`
  (about 2–3 min; do this in the pre-flight, not live).
- If the break itself fails with `task 0 did not fail` (`devctl demo` exits non-zero): `go run ./cmd/devctl demo --fix schema-change --owner demo`,
  then run the demo command again. The lock is released on a failed break; take it again if needed.
- Live and nothing recovers: skip to the *No API key* path (scripted agent) and say the live stack is down.

### No API key (or the model API is unreachable)

- The room still opens (the watcher does not need the model); the agent just never answers. Check the server
  log for the model error before assuming anything else.
- Quick check before the demo: `echo ${ANTHROPIC_API_KEY:+set}` prints `set`. Never print the key itself.
- Fallback: restart the server as `go run ./cmd/server --demo`. It runs a **scripted fake agent** with an
  in-memory store: no stack, no key. **`--demo` turns the watcher off**, so open the room by hand:
  `POST /api/rooms` from the UI's new-room form (pipeline `orders`). Tell the audience it is scripted.
  Skip beat 1 (break) and beats that use the real stack (`devctl health`).
- If the key works but the room did not open, open it by hand instead (see *The room does not open*).

### The WebSocket drops (timeline frozen, "reconnecting" banner)

- The client replays from the last event it saw, then goes live. Wait ~10 s; nothing is lost.
- Still frozen: reload the page. The room is rebuilt from the event log (`after=0`), so no state is lost.
- The server restarted? Check the terminal. The log is in Postgres, so the room comes back as it was.
  An agent turn that was in flight may need a fresh steer.
- Persistent: use another browser profile; sessions are per profile.

### The room does not open (more than 2 min after the break)

- Server log: look for `watch:` lines. No lines: the profile was not exported (`PIPEROOM_WATCH_MODE` is not `rooms`);
  restart the server with the profile. Lines but no room: a room for `orders` is already open (the watcher joins
  it) or one was closed within the 30 s cooldown.
- Open it by hand so the demo goes on: as arjun, `curl -X POST -H 'X-User: arjun' -H 'Content-Type: application/json' -d '{"title":"Warehouse orders stale","severity":"sev2","pipeline_id":"orders"}' http://127.0.0.1:8090/api/rooms`
  (or the UI). The agent starts the same way.

### An approval or the executor fails

- Read the failure in the room; do not retry blindly. If the proposal is stuck, apply the fix by hand:
  `go run ./cmd/devctl fix schema-change --owner demo`, and say "the executor refused; here is the manual fix
  the agent recommended".
- A reject by a layer owner is a feature: say so, and let the agent propose again.

## After the demo

- Close any open room (the next `--rehearse` refuses to run while one is open).
- `go run ./cmd/devctl demo --fix schema-change --owner demo` then `go run ./cmd/devctl unlock --owner demo`.
- Stop the server and the web app. `go run ./cmd/devctl down --owner demo` if you are done with the stack.
