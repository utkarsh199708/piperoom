# Piperoom HTTP + WebSocket API (v0.1)

CONTRACT FILE — owned by the reviewer. A5 implements it, A6 consumes it.
Change requests go in your PR description, not in this file.

Base URL in dev: `http://localhost:8090`; the server listens on `127.0.0.1:8090` by default (security F1 — no
real login yet). All bodies are JSON. Times are RFC 3339 **UTC** (`Z`), also when read back from Postgres.
Arrays in responses are always JSON arrays (`[]`), never `null`.

## Auth (MVP)

No SSO yet. Every request sends `X-User: <user id>`. Users come from `dev/users.yaml`:

```yaml
users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka]          # layers this user can approve
  - id: priya
    name: Priya
    layers: [postgres, schema-registry]     # the source team owns its table's schemas
  - id: meera
    name: Meera
    layers: [warehouse]
  - id: kabir
    name: Kabir
    layers: [consumer]
```

Unknown or missing `X-User` → `401`. The WebSocket takes `?user=<id>` instead (browsers cannot set headers on WS).
`dev/users.yaml` (who may approve a layer) and the `layers:` map in `dev/owners.yaml` (owner shown on the pipeline
map) must agree; the server checks this at start-up and refuses to start on a mismatch. Real mode requires
`USERS_FILE`; the built-in users are for `--demo` only. Real login is designed in `docs/design/auth.md` (C9).

## Errors

```json
{ "error": { "code": "not_driver", "message": "Only the driver can send steers." } }
```

| HTTP | code examples |
|---|---|
| 400 | `bad_request`, `validation` |
| 401 | `unauthenticated` |
| 403 | `not_driver`, `not_layer_owner`, `not_recipient` (hand-off accept by someone other than `to`) |
| 404 | `not_found` |
| 413 | `too_large` (signed alert body over 1 MB) |
| 409 | `already_decided`, `room_closed`, `lease_held`, `stale_handoff` |

`lease_held`: `control/grant` called by a non-driver while another user's lease is still live. All other
driver-only endpoints answer `403 not_driver`. `stale_handoff`: the hand-off's creator is no longer the driver,
or the hand-off was voided by a newer one. A second `send` of an already-sent suggestion → `409 already_decided`.

## Objects

### Event
Exactly `events.Event` from `internal/events/events.go`:
```json
{ "room_id": "r_01J...", "seq": 12, "type": "comment.added",
  "actor": { "kind": "human", "id": "kabir", "name": "Kabir" },
  "payload": { "comment_id": "c_01J...", "step_id": "s_2", "text": "Wrong service?", "mentions_agent": false },
  "at": "2026-10-11T10:59:03Z" }
```

### RoomSnapshot
Derived state for room lists and a fast first paint. **It does not contain the timeline, comments,
trace or activity feed** — to render a room, the client replays the full event log (WebSocket with
`after=0`, see below) and folds it with the same reducer:
```json
{
  "id": "r_01J...", "title": "Warehouse orders stale", "severity": "sev2",
  "pipeline_id": "orders", "status": "open", "opened_at": "...", "closed_at": null, "resolution": "",
  "last_seq": 42,
  "driver": { "id": "arjun", "name": "Arjun", "lease_until": "..." },
  "agent_status": { "state": "working", "detail": "step 6" },     // "idle" for a new room
  "hypothesis": { "text": "...", "based_on_steps": ["s_3","s_4"], "confidence": "high", "confirmed": false },
  "open_proposals": [ { "proposal_id": "p_01J...", "title": "...", "steps": [], "risk": "...", "rollback": "...",
                        "required_layers": ["schema-registry","kafka-connect"],
                        "approvals": { "schema-registry": "approved", "kafka-connect": "pending" } } ],
  "pending_suggestions": [ { "suggestion_id": "g_01J...", "from": {"id":"kabir","name":"Kabir"}, "text": "..." } ],
  "members": [ { "id": "priya", "name": "Priya", "role": "watching" } ]
}
```
`closed_at` and `resolution` are set by `room.closed`. `required_layers` is always the set of the steps' layers
(the server derives it; the proposer cannot choose it).
IDs are prefixed ULIDs: `r_` room, `s_` step, `c_` comment, `g_` suggestion, `p_` proposal, `h_` hand-off.

### Topology (pipeline map)
```json
{ "pipeline_id": "orders",
  "nodes": [ { "id": "pg:public.orders", "kind": "source_table", "name": "public.orders", "layer": "postgres",
               "owner": "priya", "health": "warn", "facts": { "slot_retained": "12.4 GB" } } ],
  "edges": [ { "from": "pg:public.orders", "to": "connect:pg-orders-connector" } ] }
```
`kind`: `source_table | connector | topic | consumer_group | sink_table`. `health`: `ok | warn | fail | unknown`.

## REST endpoints

| Method | Path | Body | Result | Notes |
|---|---|---|---|---|
| GET | `/api/me` | – | `{id,name,layers}` | |
| GET | `/api/rooms` | – | `[RoomSnapshot]` | open rooms first |
| POST | `/api/rooms` | `{title, severity, pipeline_id}` | `RoomSnapshot` | creator becomes driver; agent starts; unknown `pipeline_id` → `400 validation` |
| POST | `/api/alerts/webhook` | any JSON | `RoomSnapshot` | generic alert; maps `title`/`severity`/`pipeline_id` if the body is an object; the raw body is stored verbatim in `room.created.alert`; unknown pipeline → room opens with a system message and proposals are disabled; driver = none until someone takes control. **Dedup:** the same alert (same `dedup_key`, else a byte-identical body) for the same pipeline while its room is open joins that room → `200` with that `RoomSnapshot` (a resolved repeat is noted in the room; it never closes the room); a new room → `201`. A **resolved** alert (`status: "resolved"`) that matches no open room — no `dedup_key` and no byte-identical open match, or its room is already closed — opens nothing: `202` with `{"ignored": true, "reason": "resolved alert with no open room"}` (nothing stored, no agent, no hook). Webhook auth: see `docs/design/webhook-auth.md` (C9) |
| POST | `/api/alerts/{source}` | the sender's own JSON (`source` = `alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic`) | `RoomSnapshot` | **No `X-User`**: the sender's signature or token is the auth (`internal/alertin/README.md`); a source with no secret configured answers `401`. Same dedup / resolved / room rules as `/api/alerts/webhook`, keyed on the source's own dedup key: new room `201`; repeat while its room is open `200` (join); resolved with no open room `202 {"ignored": true, "reason": "resolved alert with no open room"}`; a verified non-alert (PagerDuty ping, ack) `202 {"ignored": true, "reason": ...}`. Errors: bad / missing / stale signature or disabled source `401 unauthenticated` (checked before the body is read); body over 1 MB `413 too_large`; verified but unparseable `400 validation`; unknown `{source}` `404 not_found`. Title, severity and pipeline come from the alert (label, `PAGERDUTY_SERVICES` map, owners match, or `DEFAULT_ALERT_PIPELINE`); the raw body is stored in `room.created.alert`. `room.created` also records `alert_source` (`webhook` or the source name) and `dedup_key`. |
| GET | `/api/rooms/{id}` | – | `RoomSnapshot` | |
| GET | `/api/rooms/{id}/events?after=0&limit=500` | – | `{events:[Event], last_seq}` | limit max 1000; `events` is `[]` when nothing is after `after` |
| POST | `/api/rooms/{id}/comments` | `{step_id?, text}` | `Event` | `@agent` in text sets `mentions_agent` |
| POST | `/api/rooms/{id}/steer` | `{text}` | `Event` | driver → `steer.sent`; others → `steer.suggested` |
| POST | `/api/rooms/{id}/suggestions/{sid}/send` | – | `Event` | driver only; emits `steer.sent` with `suggestion_id` |
| POST | `/api/rooms/{id}/control/request` | – | `Event` | |
| POST | `/api/rooms/{id}/control/grant` | `{to}` | `Event` | driver only, or anyone if no driver / lease expired |
| POST | `/api/rooms/{id}/close` | `{resolution}` | `Event` | driver only |
| POST | `/api/proposals/{pid}/approve` | `{layer}` | `Event` | caller must own `layer`; when all layers approved the server runs the steps in order and emits `action.executed` per step |
| POST | `/api/proposals/{pid}/reject` | `{layer, reason}` | `Event` | |
| POST | `/api/rooms/{id}/handoffs` | `{to, stay_as_watcher}` | `Event` | driver only; server asks the agent for the summary (can take up to 3 min; this route is exempt from the 60 s request timeout) |
| POST | `/api/handoffs/{hid}/accept` | – | `Event` | only the `to` user; moves driver role |
| GET | `/api/pipelines/{pid}/topology` | – | `Topology` | unknown pipeline → `404 not_found` |
| POST | `/api/rooms/{id}/trace` | `{table, key}` | `Event` | runs a record trace; emits `trace.pinned`; table not in the pipeline or empty key → `400 validation` |

Driver lease: 30 minutes, renewed by any driver action. So that clients folding the log see the same lease as
the server, a renewal that happens when less than 25 minutes remain is written to the log as `control.granted`
with `to` = the current driver and the new `lease_until` (at most one such event per 5 minutes). A driver whose
lease expired and who was not replaced is still treated as the driver: their next action renews the lease.

Proposals: a step's action must implement `actions.Scoped`; the server calls `Validate` and then
`CheckScope(room's pipeline, params)` when the proposal is created and again right before `Execute`. Params are
frozen at creation; approvals bind to those exact steps.

## WebSocket

`GET /api/rooms/{id}/ws?user=<id>&after=<seq>`

Server → client messages:
```json
{ "type": "event", "event": { ...Event } }
{ "type": "presence", "members": [ { "id": "priya", "name": "Priya", "state": "viewing" } ] }
{ "type": "error", "error": { "code": "...", "message": "..." } }
```
Client → server:
```json
{ "type": "presence", "state": "viewing" }    // send every 20 s; "away" when tab hidden
```
The server marks a member `away` after 45 s without any client message (2× the heartbeat plus slack).
Rules: the server first replays every event with `seq > after`, then streams live events. Events are never skipped or repeated. **On first open of a room the client connects with `after=0`** (full replay); on reconnect it passes its highest seen seq. Presence is not stored in the event log.
