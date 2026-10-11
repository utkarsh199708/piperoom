# Piperoom HTTP + WebSocket API (v0)

CONTRACT FILE — owned by the reviewer. A5 implements it, A6 consumes it.
Change requests go in your PR description, not in this file.

Base URL in dev: `http://localhost:8090`. All bodies are JSON. Times are RFC 3339 UTC.

## Auth (MVP)

No SSO yet. Every request sends `X-User: <user id>`. Users come from `dev/users.yaml`:

```yaml
users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka, schema-registry]   # layers this user can approve
  - id: priya
    name: Priya
    layers: [postgres]
  - id: meera
    name: Meera
    layers: [warehouse]
  - id: kabir
    name: Kabir
    layers: [consumer]
```

Unknown or missing `X-User` → `401`. The WebSocket takes `?user=<id>` instead (browsers cannot set headers on WS).

## Errors

```json
{ "error": { "code": "not_driver", "message": "Only the driver can send steers." } }
```

| HTTP | code examples |
|---|---|
| 400 | `bad_request`, `validation` |
| 401 | `unauthenticated` |
| 403 | `not_driver`, `not_layer_owner` |
| 404 | `not_found` |
| 409 | `already_decided`, `room_closed`, `lease_held` |

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
Derived state, so the UI can render without replaying everything:
```json
{
  "id": "r_01J...", "title": "Warehouse orders stale", "severity": "sev2",
  "pipeline_id": "orders", "status": "open", "opened_at": "...",
  "last_seq": 42,
  "driver": { "id": "arjun", "name": "Arjun", "lease_until": "..." },
  "agent_status": { "state": "working", "detail": "step 6" },
  "hypothesis": { "text": "...", "based_on_steps": ["s_3","s_4"], "confidence": "high", "confirmed": false },
  "open_proposals": [ { "proposal_id": "p_01J...", "title": "...", "steps": [], "risk": "...", "rollback": "...",
                        "required_layers": ["schema-registry","warehouse"],
                        "approvals": { "schema-registry": "approved", "warehouse": "pending" } } ],
  "pending_suggestions": [ { "suggestion_id": "g_01J...", "from": {"id":"kabir","name":"Kabir"}, "text": "..." } ],
  "members": [ { "id": "priya", "name": "Priya", "role": "watching" } ]
}
```
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
| POST | `/api/rooms` | `{title, severity, pipeline_id}` | `RoomSnapshot` | creator becomes driver; agent starts |
| POST | `/api/alerts/webhook` | any JSON | `RoomSnapshot` | generic alert; maps `title`/`severity`/`pipeline_id` if present; driver = none until someone takes control |
| GET | `/api/rooms/{id}` | – | `RoomSnapshot` | |
| GET | `/api/rooms/{id}/events?after=0&limit=500` | – | `{events:[Event], last_seq}` | limit max 1000 |
| POST | `/api/rooms/{id}/comments` | `{step_id?, text}` | `Event` | `@agent` in text sets `mentions_agent` |
| POST | `/api/rooms/{id}/steer` | `{text}` | `Event` | driver → `steer.sent`; others → `steer.suggested` |
| POST | `/api/rooms/{id}/suggestions/{sid}/send` | – | `Event` | driver only; emits `steer.sent` with `suggestion_id` |
| POST | `/api/rooms/{id}/control/request` | – | `Event` | |
| POST | `/api/rooms/{id}/control/grant` | `{to}` | `Event` | driver only, or anyone if no driver / lease expired |
| POST | `/api/rooms/{id}/close` | `{resolution}` | `Event` | driver only |
| POST | `/api/proposals/{pid}/approve` | `{layer}` | `Event` | caller must own `layer`; when all layers approved the server runs the steps in order and emits `action.executed` per step |
| POST | `/api/proposals/{pid}/reject` | `{layer, reason}` | `Event` | |
| POST | `/api/rooms/{id}/handoffs` | `{to, stay_as_watcher}` | `Event` | driver only; server asks the agent for the summary |
| POST | `/api/handoffs/{hid}/accept` | – | `Event` | only the `to` user; moves driver role |
| GET | `/api/pipelines/{pid}/topology` | – | `Topology` | |
| POST | `/api/rooms/{id}/trace` | `{table, key}` | `Event` | runs a record trace; emits `trace.pinned` |

Driver lease: 30 minutes, renewed by any driver action.

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
Rules: the server first replays every event with `seq > after`, then streams live events. Events are never skipped or repeated. On reconnect, the client passes its highest seen seq. Presence is not stored in the event log.
