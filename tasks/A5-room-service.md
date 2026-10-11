# A5 — Room service: event store, room logic, HTTP API, WebSocket, server

Read `AGENTS.md` first. You own: `migrations/**`, `internal/store/**`, `internal/room/**`,
`internal/httpapi/**`, `internal/ws/**`, `cmd/server/**`.

## Goal

The multiplayer core. Implement `docs/api.md` exactly, on top of an append-only event log in Postgres.

## 1. Migrations (`migrations/`, goose SQL files; database `piperoom`)

- `rooms(id text pk, title, severity, pipeline_id, status, opened_at, closed_at, last_seq bigint not null default 0,
  driver_id text null, driver_lease_until timestamptz null)`
- `room_events(room_id text, seq bigint, type text, actor jsonb, payload jsonb, at timestamptz,
  primary key (room_id, seq))`
- `proposals`, `approvals`, `suggestions`, `handoffs` — enough to enforce rules; the log stays the source of truth.
- Run with `goose -dir migrations postgres "$PIPEROOM_DB_URL" up` (goose is installed).

## 2. Event store (`internal/store`) — implements `events.Store`

- `Append`: one transaction — `UPDATE rooms SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq`,
  insert the event, `SELECT pg_notify('room_events', room_id || ':' || seq)`. Seq has no gaps.
- `Subscribe`: replay with `Since`, then live. **One** shared `LISTEN` connection per process fans
  out to subscribers (a hub), not one connection per subscriber. On each notify, read `Since(lastSent)`
  so nothing is skipped or repeated even if notifications are merged or lost. Reconnect LISTEN on error
  and re-read from the last seq.
- Also write `memstore` (in-memory `events.Store`) for unit tests across your packages.

## 3. Room logic (`internal/room`)

A `Service` with methods matching the endpoints. It enforces every rule in `docs/api.md`:
driver-only actions, 30-min driver lease (renewed by driver actions), layer ownership for approvals
(users from `dev/users.yaml`, which you parse), one decision per layer per proposal, closed rooms reject writes.

- **Snapshot:** fold events into `RoomSnapshot` (pure function `Apply(snapshot, event) snapshot`,
  table-tested). Keep an in-memory cache per open room, rebuilt from the log on start.
- **Agent supervisor:** when a room opens, start `agent.Run` in a goroutine with:
  - `Sink` → `store.Append(..., events.AgentActor, ...)`
  - `Inbox` → fed from new events: `steer.sent`, `comment.added` with `mentions_agent`,
    `approval.*` and `action.executed` (converted to `agent.HumanInput`)
  - `Proposals` → your proposal creation (validates via `actions.Registry`)
  - stop it when the room closes; if `Run` returns an error, emit `agent.status` `error`; do not restart in a loop.
- **Executor:** when every required layer approved, run the steps in order via `actions.Registry`
  (`Validate` again, then `Execute`), emit `action.executed` per step, stop at the first failure.
  Never run a step twice (idempotency key = proposal id + step index, stored).
- **Hand-off:** `POST /handoffs` calls `agent.WriteHandoff` with the room events, emits `handoff.created`;
  accept moves the driver role (`control.granted`) and emits `handoff.accepted`.
- **Trace:** `POST /trace` calls `topology.Builder.Trace` and emits `trace.pinned`.

## 4. HTTP API (`internal/httpapi`) — `chi`

Exactly `docs/api.md`. `X-User` auth middleware; JSON error shape; request body limit 1 MB;
CORS for `http://localhost:5173`; `slog` request logging (no bodies). Handlers are thin — logic lives in `room`.

## 5. WebSocket (`internal/ws`) — `github.com/coder/websocket`

`/api/rooms/{id}/ws?user=&after=`: replay then live from `store.Subscribe`; presence hub in memory
(member list broadcast on change and every 20 s timeout → `away`); per-connection send buffer of 256 —
if full, close with a policy-violation status (the client reconnects with `after=`). Every goroutine
ends when the connection or server context ends.

## 6. `cmd/server`

Wires everything from `config.Load()`: store, room service, actions + tools registries (A2/A3 `Register`
functions), topology builder, agent, HTTP. Flags:
- `--demo`: no real agent. A **scripted fake agent** replays the mockup story (steps 1–6, hypothesis,
  a proposal needing `schema-registry` + `warehouse`) with realistic delays. A6 builds the UI against this,
  so make it good and deterministic.
- Until A2/A3/A4 merge, `--demo` must work on its own (no imports that do not exist yet; use the contracts).

## Tests

- Unit (memstore + fakes): every rule in `docs/api.md` (non-driver steer → suggestion; approve without
  owning the layer → 403; double approval → 409; lease expiry lets others take control; executor
  stops at first failed step and never re-runs a step).
- `Apply` table tests for every event type.
- Integration (tag `integration`, real Postgres): 20 goroutines append concurrently → seqs 1..N with
  no gaps; a subscriber that connects mid-stream sees every event exactly once.
- WS test with `httptest.Server`: replay + live + reconnect with `after`.

## Done when

`go run ./cmd/server --demo` + two browser tabs as different users (A6) show the same room live:
comments, suggestions → driver sends, approvals by the right owners, executor results, hand-off.
