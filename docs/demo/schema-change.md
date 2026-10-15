# Live demo script: the `schema-change` incident

About 10 minutes. One engineer breaks the pipeline, a room opens by itself, an AI agent finds the
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

**One rule: `devctl reset` runs `docker compose down -v`, which deletes the dev Postgres, including the
`piperoom` database the server would keep its rooms in.** So the server uses its own Postgres
(`dev/demo-room-db.yml`, port 5433, a separate compose project that reset does not touch), and the order
below matters: room database first, server second, and the server is started **once**, after the last
`reset` you plan before the show. Resets during the rehearsal are fine because the server's rooms are not
in the wiped database.

1. Docker running. Take the stack lock and keep it until the very end: `go run ./cmd/devctl lock --owner demo`
2. Room database: `docker compose -f dev/demo-room-db.yml up -d --wait`
3. Pipeline stack: `go run ./cmd/devctl up --owner demo`
4. Start the server with the demo profile (`dev/demo.env`: watcher in `rooms` mode, 15 s interval, 30 s cooldown,
   room database on port 5433):
   ```
   export ANTHROPIC_API_KEY=...        # from your shell, never from a file
   set -a; . dev/demo.env; set +a
   go run ./cmd/server
   ```
   (PowerShell: see the header of `dev/demo.env`.) The log shows `server: watcher starting` with mode `rooms`.
   Do not restart the server after this unless a fallback says so.
5. Start the web app (`cd web && npm run dev`) and open it as arjun, kabir and priya.
6. **Rehearse once:** `go run ./cmd/devctl demo --rehearse --owner demo`. It checks the profile, **refuses to run if the
   profile's room database is the one reset wipes**, resets the pipeline, breaks it, waits for the room, resets the
   pipeline again, and prints `PASS: break -> room in <time>`. Note that time. It keeps your lock. The server and
   its rooms are untouched by the resets, so the room it opened is really there: open it, then **close it** in the UI
   (the next run refuses to start while one is open).
7. The rehearsal ended with a reset, so the pipeline is clean and the schema is back to integer. Confirm with
   `go run ./cmd/devctl health` (exit 0). Do **not** run a second `break` or `demo` without another `reset`: `break`
   needs a fresh seeded schema, and a reset takes 1–3 minutes, which you do not want live. While a reset runs the
   watcher cannot reach Connect; this has not been observed on a real run, so if it opens a room during a reset, close it.

## Timed beats (about 10 minutes)

| Time | Beat | Say | Do / click |
|---|---|---|---|
| 0:00 | Healthy pipeline | "Postgres, Debezium, Kafka, a consumer, a warehouse. Right now it works." | Terminal: `go run ./cmd/devctl status`. Everything green |
| 0:45 | Break it | "A developer ran a migration: `amount` changed from integer to numeric. Nobody told the data team." | Terminal: `go run ./cmd/devctl break schema-change --owner demo`. It runs the ALTER, inserts two rows and returns when task 0 has FAILED (seconds). Use `break`, **not** `demo schema-change`: that one resets first and takes minutes |
| 1:15 | The room opens | "Nobody paged anyone. The watcher saw the connector fail and opened a room." | Room list in the browser. Open the new room as arjun. Expect it about a minute after the break (30 s signal window + 15 s polls + start-up; use your rehearsal time) |
| 2:00 | The agent investigates | "The agent can only read. Connect status, the schema registry, the replication slot, the consumer lag, the warehouse." | Point at the timeline: each tool call is a step with its result. Do not read the output aloud |
| 3:15 | The finding | "It says the connector task failed because the new schema is not compatible with the registered one. The consumer is healthy, so the problem is upstream." | Point at the hypothesis card and the steps it cites |
| 3:45 | Many eyes, one driver | "Kabir owns the consumer and wants to check something. I'm the driver, so I decide what the agent hears." | As kabir: comment on a step, send the agent a suggestion (for example "Is the consumer group lagging?"); show kabir cannot steer directly. As arjun: send that suggestion on to the agent |
| 5:30 | The proposal | "The agent cannot fix anything. It can only propose." | Open the proposal: steps, risk, rollback, required layers (`schema-registry`, `kafka-connect`). Show both approvals pending |
| 6:30 | Approvals | "A fix needs every layer owner. Different people, different layers." | As priya: approve schema-registry. As arjun: approve kafka-connect. The executor runs the steps |
| 7:30 | Recovery | "Connector task running again, schema registered, rows reach the warehouse." | Terminal: `go run ./cmd/devctl health`. The room shows the result of the run |
| 8:15 | Hand-off | "Shift change. The agent writes the summary, the driver role moves." | As arjun: start a hand-off to meera. As meera: read the summary, accept. Driver badge moves |
| 9:30 | Close | "Everything is in the log, and you can replay the room from any point." | Close the room with a one-line resolution |

If the agent's own fix (beat 6:30) works you do not need `devctl fix`. After the demo see *After the demo*.

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
- Do **not** run `devctl fix` early. That makes the agent's finding "everything is already fixed".

### Connect is not healthy (`devctl up` hangs, or `devctl health` shows kafka-connect red *before* the break)

- `go run ./cmd/devctl status` and `docker compose -f dev/docker-compose.yml logs connect --tail 50`.
- Connect needs 30–90 s after Kafka. Wait, then re-check. If still red: `go run ./cmd/devctl reset --owner demo`
  (about 2–3 min; do this in the pre-flight, not live).
- If the break itself fails with `task 0 did not fail` (`devctl break` exits non-zero): `go run ./cmd/devctl fix schema-change --owner demo`,
  then `go run ./cmd/devctl reset --owner demo` (1–3 min, the server and its rooms are not affected) and break again.
- Live and nothing recovers: skip to the *No API key* path (scripted agent) and say the live stack is down.

### No API key (or the model API is unreachable)

- The room still opens (the watcher does not need the model); the agent just never answers. Check the server
  log for the model error before assuming anything else.
- Quick check before the demo: `echo ${ANTHROPIC_API_KEY:+set}` prints `set`. Never print the key itself.
- Fallback: restart the server as `go run ./cmd/server --demo`. It runs a **scripted fake agent** with an
  in-memory store: no stack, no key, no room database. **`--demo` turns the watcher off**, so open the room by hand:
  `POST /api/rooms` from the UI's new-room form (pipeline `orders`). Tell the audience it is scripted.
  Skip beat 1 (break) and beats that use the real stack (`devctl health`).
- If the key works but the room did not open, open it by hand instead (see *The room does not open*).

### The WebSocket drops (timeline frozen, "reconnecting" banner)

- The client replays from the last event it saw, then goes live. Wait ~10 s; nothing is lost.
- Still frozen: reload the page. The room is rebuilt from the event log (`after=0`), so no state is lost.
- The server restarted? Check the terminal. The log is in the room database (port 5433, not touched by `reset`), so the room comes back as it was.
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
- If the pipeline is still broken: `go run ./cmd/devctl fix schema-change --owner demo`. The ALTER stays in place
  (amount is numeric), so the next `break` needs `go run ./cmd/devctl reset --owner demo` first.
- Release the lock you took in pre-flight: `go run ./cmd/devctl unlock --owner demo`.
- Stop the server and the web app. `go run ./cmd/devctl down --owner demo` if you are done with the stack, and
  `docker compose -f dev/demo-room-db.yml down -v` to forget the demo rooms.
