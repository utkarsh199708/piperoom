# Piperoom HTTP + WebSocket API (v0.1)

CONTRACT FILE — owned by the reviewer. A5 implements it, A6 consumes it.
Change requests go in your PR description, not in this file.

Base URL in dev: `http://localhost:8090`; the server listens on `127.0.0.1:8090` by default (security F1 — no
real login yet). All bodies are JSON. Times are RFC 3339 **UTC** (`Z`), also when read back from Postgres.
Arrays in responses are always JSON arrays (`[]`), never `null`.

## Auth

Dev mode (the default, live since auth PR 2): the web app logs in with `POST /auth/dev-login {user}` and a
session cookie (see **Login** below); `curl` may instead send `X-User: <user id>` when it sends no cookie. SSO (OIDC)
is auth PR 3. Users come from `dev/users.yaml`:

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

No cookie and no `X-User` → `401 session_expired`; an `X-User` naming no known user → `401 unauthenticated`. The
WebSocket uses the session cookie (or `X-User` for non-browser clients in dev); `?user=` is gone.
`dev/users.yaml` (who may approve a layer) and the `layers:` map in `dev/owners.yaml` (owner shown on the pipeline
map) must agree; the server checks this at start-up and refuses to start on a mismatch. Real mode requires
`USERS_FILE`; the built-in users are for `--demo` only. Real login is designed in `docs/design/auth.md` (C9).

### Login (auth PRs 2–4, design `docs/design/auth.md`)

PRs 2 and 3 are live (dev sessions, Host allow-list, proxy guard, OIDC login, identities, roles, `/api/people`); items marked PR 4 are not yet.

- **Mode.** `PIPEROOM_AUTH_MODE` = `dev` (default) or `oidc`. In `oidc` mode `X-User` is ignored and a request with
  no session answers `401 session_expired`; the unsigned `POST /api/alerts/webhook` is not mounted (`404`), so
  pilots send alerts to `/api/alerts/generic`. In `dev` mode `POST /auth/dev-login {user}` picks a `users.yaml` user;
  `curl` with `X-User` and no cookie still works. Dev mode refuses to start on a non-loopback listen address unless
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` (loud warnings); `--allow-remote` is a deprecated alias for one release.
- **Session.** A server-side session in a cookie: `__Host-piperoom_session` in `oidc` mode (https),
  `piperoom_session` (no `Secure`) in `dev` mode over plain http. The cookie wins over `X-User` when both name the
  same user.
- **Routes.** `GET /auth/login?return=/path` → IdP redirect; `GET /auth/callback` (finds the login row by the
  login cookie, then checks `state` and nonce; single use; on success `302` to `return`; on failure `302` to
  `/?login_error=<code>` with the same code as the JSON error — `login_expired`, `unauthenticated`,
  `not_provisioned`, `rate_limited`, `idp_unavailable` — never a JSON page in the browser); `POST /auth/logout` (`204`; revokes this session and closes this session's WebSockets);
  `POST /auth/dev-login {user}` (dev mode only).
- **Host allow-list.** `Host` must be a loopback name (any port), a value in `PIPEROOM_ALLOWED_HOSTS`, or the host
  of `PIPEROOM_PUBLIC_URL` (exact) — in `oidc` mode always, in `dev` mode only with
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`; otherwise `421 bad_host`. The signed `POST /api/alerts/{source}` routes are exempt (their signature is the auth); the unsigned dev webhook is not. Unknown paths under `/auth/` answer a JSON `404 not_found`. A request carrying more than one `X-User` value in dev mode → `403 proxy_identity`. Cross-origin browser POSTs get Go's
  `CrossOriginProtection` `403` (plain text, not the JSON error shape).
- **Login gate.** Login works only for a `users.yaml` user, a principal named in `owners.yaml` (`layers:`, a pipeline
  `owners:` block, or `auditors:`), or a member of `PIPEROOM_AUTH_VIEWER_GROUPS`; else `403 not_provisioned`.
- **Roles.** `member` (a `users.yaml` user, or a `user:`/`group:` in `layers:` or a pipeline `owners:` block as
  approver or backup) may act; rights to approve still come from layer ownership. `viewer` (`PIPEROOM_AUTH_VIEWER_GROUPS`, or an `auditors:`-only principal): GET (not export) and
  WebSocket only; every non-GET under `/api` → `403 viewer`. An `auditors:` principal may also export.
- **Solo mode.** With `PIPEROOM_SOLO_USER`, that user may approve every layer; each approve also sends
  `confirm: "<pipeline_id>/<layer>"`, and the approval records `grant: "solo"`.
- **Tenants** (`docs/design/tenancy.md`). With a `tenants:` block in `owners.yaml`, a session sees only the rooms
  of pipelines in its tenants (a member of `tenants.<t>.members`, or an approver or backup of any layer of one of
  the tenant's pipelines; the solo user sees all). Anything outside them answers as if it did not exist, **404 not
  403**: every `/api/rooms/{id}/...` route including the WebSocket (refused before the upgrade) → `404 not_found`
  "not found"; `POST /api/proposals/{pid}/approve|reject` and `POST /api/handoffs/{hid}/accept` → `404 not_found`
  before any `409`/`403`; `GET /api/pipelines/{pid}/topology` → `404 not_found`; `GET /api/rooms` and
  `GET /api/shadow/metrics` leave those rooms out; `POST /api/rooms` → `400 validation` "unknown pipeline";
  `POST /api/alerts/webhook` → `404 not_found`; `control/grant` and hand-offs refuse a target who cannot see the
  room like an unknown person (`400 validation`). A room whose pipeline is not in `owners.yaml` is in no tenant.
  `tenants:` grants no login and no role. Without a `tenants:` block nothing is filtered.
- **Proxy guard (dev mode).** `POST /auth/dev-login` on a request carrying `X-User`, or a request with a session
  cookie plus an `X-User` naming a different user → `403 proxy_identity`. A reverse proxy needs
  `PIPEROOM_AUTH_MODE=oidc` (or the git tag `pre-auth`).

## Errors

```json
{ "error": { "code": "not_driver", "message": "Only the driver can send steers." } }
```

| HTTP | code examples |
|---|---|
| 400 | `bad_request`, `validation`; after auth PR 3: `login_expired` (login cookie or row missing/expired at `/auth/callback`) |
| 401 | `unauthenticated`; after auth PR 2: `session_expired` (no, revoked or expired session). Its body also carries the server's mode: `{"error":{"code":"session_expired","message":"…","auth_mode":"dev"\|"oidc"}}` — the web app shows the dev picker or goes to `/auth/login?return=…`. A WebSocket whose session ends gets an `error` frame with code `session_expired` and is then closed with close reason `session_expired` |
| 403 | `not_driver`, `not_layer_owner`, `not_recipient` (hand-off accept by someone other than `to`); after auth PRs: `viewer`, `proxy_identity`, `not_allowed` (export; also: label or grade a shadow room without being a member of its pipeline), `not_provisioned` (login gate) |
| 404 | `not_found` |
| 413 | `too_large` (signed alert body over 1 MB) |
| 409 | `already_decided`, `room_closed`, `lease_held`, `stale_handoff`; with several server replicas: `conflict` (another replica changed the room between the check and the write three times in a row; retry); after auth PR 4: `params_mismatch`; shadow mode: `room_open` (shadow route on an open room, or `?include=shadow` on an open room), `not_labelled` (shadow data read before the blind label), `not_revealed` (grade before `shadow.revealed`: the run is still going during the grace period) |
| 421 | `bad_host` (after auth PR 2: `Host` not on the allow-list) |
| 429 | after auth PR 3: `rate_limited` (`/auth/login` over 300 per minute per client) |
| 503 | after auth PR 3: `idp_unavailable` (new logins while the IdP is down; live sessions keep working) |

Text a person types (titles, comments, steers, resolutions, trace keys, approve/reject layer and reason) that contains a NUL character or invalid UTF-8 is refused with `400 validation` ("text contains characters that cannot be stored") and nothing is written. Text from the agent, tools and actions is cleaned instead (bad characters become U+FFFD).

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

### Agent usage (`agent.usage`)
One event per model request the agent sends (design `docs/design/cost.md`), actor `agent`: every investigation
turn, the hand-off summary, and, as a second event, a server-side compaction pass. Counts only, never prompt,
tool or model text. A request refused by a budget is never sent and gets no event; a request still in flight when
the room closes still gets one. In a shadow room these events go to the shadow log like every other agent event.
Clients that do not show cost ignore the type (the web reducer only advances `last_seq` for it).
```json
{ "model": "claude-opus-5-5", "role": "main",          // triage | main | handoff | compaction | judge (reserved)
  "input_tokens": 1200, "cache_creation_input_tokens": 3000, "cache_read_input_tokens": 41000,
  "output_tokens": 900, "latency_ms": 8123,             // 0 for compaction (its time is in the request's)
  "cost_usd": 0.0412 }                                  // omitted when PIPEROOM_MODEL_PRICES has no price for model
```

### Usage
The totals and per-model rows of `GET /api/rooms/{id}/usage` and `GET /api/usage` (finished calls only):
```json
{ "calls": 31, "unpriced_calls": 0, "input_tokens": 1200, "cache_creation_input_tokens": 90000,
  "cache_read_input_tokens": 700000, "output_tokens": 21000, "tokens": 812200,
  "cost_usd": 4.11 }        // null when no call in the row was priced; with unpriced_calls > 0 it is a lower bound
```

### RoomSnapshot
Derived state for room lists and a fast first paint. **It does not contain the timeline, comments,
trace or activity feed** — to render a room, the client replays the full event log (WebSocket with
`after=0`, see below) and folds it with the same reducer:
```json
{
  "id": "r_01J...", "title": "Warehouse orders stale", "severity": "sev2",
  "pipeline_id": "orders", "status": "open", "opened_at": "...", "closed_at": null, "resolution": "",
  "shadow": false,                 // true when the room's pipeline had shadow enabled at open (room.created.shadow): the agent ran silently, no proposals; see Shadow mode
  "last_seq": 42,
  "driver": { "id": "arjun", "name": "Arjun", "lease_until": "..." },
  "agent_status": { "state": "working", "detail": "step 6" },     // "idle" for a new room; states: working | idle | done | waiting_approval | queued ("waiting for an agent slot (N ahead)" when PIPEROOM_MAX_AGENTS runs are busy); in a shadow room the agent writes no status to the room log, so it stays "idle"
  "hypothesis": { "text": "...", "based_on_steps": ["s_3","s_4"], "confidence": "high", "confirmed": false },
  "open_proposals": [ { "proposal_id": "p_01J...", "title": "...", "steps": [], "risk": "...", "rollback": "...",   // after auth PR 4 also "params_hash"
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
  "edges": [ { "from": "pg:public.orders", "to": "connect:pg-orders-connector" },
             { "from": "group:order-sync", "to": "wh:analytics.orders", "mapping": "inferred" },
             { "from": "connect:pg-orders-connector", "to": "sr:pg.public.orders-value", "kind": "depends_on" } ] }
```
Node `kind`: `source_table | connector | topic | dlq_topic | consumer_group | sink_table | schema_subject`. `health`: `ok | warn | fail | unknown`.
Edge `kind`: `data` (records flow `from` → `to`) or `depends_on` (`from` needs `to` to work; no records flow, so `to` is
upstream of `from` when looking for a root cause). Edge `mapping`: `mapped` (proven from connector config, committed
group offsets, Schema Registry subjects, or an explicit owners.yaml declaration) or `inferred` (not proven; implied by
owners.yaml or a naming convention; never a cross-product shown as proven). Both fields are omitted when they hold the
default (`data`, `mapped`), so a client that ignores them reads every edge as proven data flow, as before.
A `schema_subject` node (`id` `sr:<subject>`, `layer` `schema-registry`) is a Schema Registry subject a connector or
consumer group serializes or deserializes with; only `depends_on` edges point at it. Its `facts` carry `compatibility`,
`latest_version` and `registered_recently` (`true | false | unknown`; the registry API has no registration time, so
this is what the server saw change between its own readings), plus `new_version_seen` (`<age> ago`, `health` `warn`)
when `registered_recently` is `true` and `version_deleted_seen` (`<age> ago`, `health` `warn`) when the latest version
went down (a deletion, not a registration). A subject a converter config names but nothing registered has
`exists=false` with `health` `warn`; an unreachable registry gives `error=unreachable` with `health` `unknown`.
With no Schema Registry configured (`SCHEMA_REGISTRY_URL` set to empty or `off`) the graph has no `schema_subject`
nodes and the schema tools answer "Schema Registry not configured".
A `topic` node whose name the connector config would make illegal (not a valid Kafka topic name) is a placeholder named
after the source table, with `health` `unknown` and the fact `topic_name=invalid`: the config itself is a likely cause.
A `dlq_topic` node (`id` `dlq:<topic>`) is a dead-letter topic of the pipeline; its `layer` and `owner` are the
writer's (`kafka-connect` for a sink connector, `consumer` for a declared consumer group), edges lead into it only,
and `facts` carry `records` (end minus start offsets, summed), `newest_record`, `writer` and `exists`. A `connector`
node may carry `errors_tolerance`, `dlq` and `config_risk` facts. The agent's `dlq_peek` tool returns DLQ record
positions and Kafka Connect error headers only, never a key or value; per-record header text appears in the
summary only (by the pipeline's `watch.dlq_peek.headers` mode) and `tool_result.data` holds counts and positions
(design: `docs/design/dlq.md`).

## REST endpoints

| Method | Path | Body | Result | Notes |
|---|---|---|---|---|
| GET | `/api/people` | – | `[{id, name, email?}]` | after auth PR 3: the people a driver may grant control or hand off to: `users.yaml` users plus members with a live session (`LookupPerson`). `email` only for IdP users; pickers show `id` or `email` next to `name`. Members only: a viewer gets `403 viewer` (the list holds emails and viewers cannot grant or hand off anyway) |
| GET | `/api/me` | – | `{id,name,layers}` | after auth PR 3 also `role` (`member`\|`viewer`) and `groups_missing` (the IdP sent no groups claim); `layers` is computed per request; `solo: true` when `PIPEROOM_SOLO_USER` is this user (omitted otherwise) |
| GET | `/api/rooms` | – | `[RoomSnapshot]` | open rooms first; with `tenants:` only the rooms of your tenants (see Tenants above) |
| POST | `/api/rooms` | `{title, severity, pipeline_id}` | `RoomSnapshot` | creator becomes driver; agent starts; unknown `pipeline_id` → `400 validation` |
| POST | `/api/alerts/webhook` | any JSON | `RoomSnapshot` | generic alert; maps `title`/`severity`/`pipeline_id` if the body is an object; the raw body is stored verbatim in `room.created.alert`; unknown pipeline → room opens with a system message and proposals are disabled; driver = none until someone takes control. **Dedup:** the same alert (same `dedup_key`, else a byte-identical body) for the same pipeline while its room is open joins that room → `200` with that `RoomSnapshot` (a resolved repeat is noted in the room; it never closes the room); a new room → `201`. A **resolved** alert (`status: "resolved"`) that matches no open room — no `dedup_key` and no byte-identical open match, or its room is already closed — opens nothing: `202` with `{"ignored": true, "reason": "resolved alert with no open room"}` (nothing stored, no agent, no hook). Webhook auth: see `docs/design/webhook-auth.md` (C9) |
| POST | `/api/alerts/{source}` | the sender's own JSON (`source` = `alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic`) | `RoomSnapshot` | **No `X-User`**: the sender's signature or token is the auth (`internal/alertin/README.md`); a source with no secret configured answers `401`. Same dedup / resolved / room rules as `/api/alerts/webhook`, keyed on the source's own dedup key: new room `201`; repeat while its room is open `200` (join); resolved with no open room `202 {"ignored": true, "reason": "resolved alert with no open room"}`; a verified non-alert (PagerDuty ping, ack) `202 {"ignored": true, "reason": ...}`. Errors: bad / missing / stale signature or disabled source `401 unauthenticated` (checked before the body is read); body over 1 MB `413 too_large`; verified but unparseable `400 validation`; unknown `{source}` `404 not_found`. Title, severity and pipeline come from the alert (label, `PAGERDUTY_SERVICES` map, owners match, or `DEFAULT_ALERT_PIPELINE`); the raw body is stored in `room.created.alert`. `room.created` also records `alert_source` (`webhook` or the source name) and `dedup_key`. With a `tenants:` block in `owners.yaml` the `200`/`201` body is `{"room_id": "r_…", "created": true\|false}` instead of the `RoomSnapshot`: a source secret is shared by every tenant, so the reply carries no room content. |
| GET | `/api/rooms/{id}` | – | `RoomSnapshot` | |
| GET | `/api/rooms/{id}/events?after=0&limit=500` | – | `{events:[Event], last_seq}` | limit max 1000; `events` is `[]` when nothing is after `after` |
| POST | `/api/rooms/{id}/comments` | `{step_id?, text}` | `Event` | `@agent` in text sets `mentions_agent` |
| POST | `/api/rooms/{id}/steer` | `{text}` | `Event` | driver → `steer.sent`; others → `steer.suggested` |
| POST | `/api/rooms/{id}/suggestions/{sid}/send` | – | `Event` | driver only; emits `steer.sent` with `suggestion_id` |
| POST | `/api/rooms/{id}/control/request` | – | `Event` | |
| POST | `/api/rooms/{id}/control/grant` | `{to}` | `Event` | driver only, or anyone if no driver / lease expired |
| POST | `/api/rooms/{id}/close` | `{resolution}` | `Event` | driver only; a shadow room refuses an empty resolution with `400 validation` |
| POST | `/api/proposals/{pid}/approve` | `{layer}`; after auth PR 4 `{layer, params_hash}`, plus `confirm: "<pipeline_id>/<layer>"` in solo mode | `Event` | caller must own `layer`; when all layers approved the server runs the steps in order and emits `action.executed` per step (`{proposal_id, step_index, ok, output, params_hash, outcome_unknown?}`; `outcome_unknown: true` with `ok: false` when the server running the step stopped before it recorded the result: the step is never run again, check the target before retrying). After auth PR 4: missing `params_hash` → `400 validation`, different from the stored one → `409 params_mismatch`; `approval.given` carries `params_hash` and `identity` (`grant: "solo"` in solo mode) |
| POST | `/api/proposals/{pid}/reject` | `{layer, reason}`; after auth PR 4 `{layer, reason, params_hash}` | `Event` | same hash rule as approve |
| GET | `/api/rooms/{id}/export` | – | the room as one JSON document | header `X-Piperoom-Export-SHA256` = SHA-256 of the RFC 8785 (JCS) form without the top-level `integrity` member. Today any user may export. After auth PR 4: only an approver or backup for any layer of the room's pipeline (in dev mode: `users.yaml` `layers`) or an `auditors:` principal; the driver is not on the list; others → `403 not_allowed`. Proposals, approvals and action results then carry `params_hash`, and approvals also `identity`. `?include=shadow` (closed rooms only, else `409 room_open`; `409 not_labelled` before the blind label) adds a top-level `"shadow": {run, events}` member (with `data`) after `"schema"`; it is inside the hashed document, so the hash differs from the plain export. Same access rule |
| POST | `/api/rooms/{id}/handoffs` | `{to, stay_as_watcher}` | `Event` | driver only; server asks the agent for the summary (can take up to 3 min; this route is exempt from the 60 s request timeout) |
| POST | `/api/handoffs/{hid}/accept` | – | `Event` | only the `to` user; moves driver role |
| GET | `/api/pipelines/{pid}/topology` | – | `Topology` | unknown pipeline → `404 not_found` |
| POST | `/api/rooms/{id}/trace` | `{table, key}` | `Event` | runs a record trace; emits `trace.pinned`; table not in the pipeline or empty key → `400 validation` |
| POST | `/api/rooms/{id}/shadow/blind` | `{actual_layer, root_cause_terms}` | `Event` (`shadow.labelled`) | closed shadow rooms only; once per room (`409 already_decided`); caller must be a member of the room's pipeline: the solo user, or an approver or backup for any layer of that pipeline in `owners.yaml` (in dev mode also a `users.yaml` user whose `layers:` grant a layer the pipeline's `owners:` block does not replace), the same rule as the export minus auditors; else `403 not_allowed`; viewers `403 viewer`; `actual_layer` one of the six layers, `root_cause_terms` 1–20 strings of 1–200 chars → else `400 validation`; the event carries `identity` with the qualifying `grant` |
| GET | `/api/rooms/{id}/shadow?include=data,room` | – | `{run: ShadowRun, events: [Event + "shadow": true]}` | closed shadow rooms only; `409 not_labelled` until the blind label; any logged-in user, viewers included (`include=data` too, as with `tool_result.data` on `GET /events`); `events` have their own `seq` and are never fed to the room reducer; `tool_result.data` is omitted unless `include=data`; `include=room` adds `room: {created, labelled, graded}` for the eval builder. `ShadowRun` = `{room_id, status, model, pipeline_map, started_at, first_hypothesis_at, final_hypothesis_at, finished_at, finished_after_close, final_hypothesis, tool_calls, tokens, revealed_at}` (null times when unset; `status` ∈ `queued\|running\|hypothesised\|no_hypothesis\|error\|budget\|interrupted`) |
| POST | `/api/rooms/{id}/shadow/grade` | `{layer_correct, root_cause, notes?}` | `Event` (`shadow.graded`) | closed, labelled and revealed shadow rooms (before `shadow.revealed` → `409 not_revealed`); same caller rule as `blind`; `root_cause` ∈ `yes\|partial\|no`, `notes` ≤ 2000 chars; re-grade allowed, last wins |
| GET | `/api/rooms/{id}/usage` | – | `{room_id, pipeline_id, charged: {tokens, cost_usd}, limit: {tokens}, totals: Usage, by_model: [Usage + model]}` | model spend of the room (design `docs/design/cost.md`). `charged` is what the room budget checks: finished calls plus the estimate of a call in flight, so it can run ahead of `totals`; `limit.tokens` is `PIPEROOM_ROOM_TOKEN_BUDGET` (0 = no cap; a shadow run's own cap is not shown). Any logged-in user who may see and read the room, viewers included (the same numbers are in its `agent.usage` events). A room the caller cannot see (tenancy, `docs/design/tenancy.md`) answers `404 not_found` exactly like a missing room; a shadow room answers `409 room_open` while it is open. `by_model` is sorted by model id |
| GET | `/api/usage?pipeline=&day=` | – | `{pipeline_id, day, charged: {tokens, cost_usd}, limit: {tokens, cost_usd}, totals: Usage, by_model: [Usage + model]}` | model spend of one pipeline on one UTC day, shadow runs included. `pipeline` required (at most 200 bytes, valid UTF-8, no NUL); `day` is `YYYY-MM-DD` (UTC, default today) between `2000-01-01` and `2999-12-31`; else `400 validation`. Visibility is checked first: a pipeline `owners.yaml` does not define, or one outside the caller's tenants, answers `404 not_found` ("pipeline not found") before any role check. Then only an approver or backup for any layer of that pipeline (in dev mode also a `users.yaml` user whose `layers:` grant one), the solo user, or an `auditors:` principal (the export rule of `docs/design/auth.md` §3.4); others `403 not_allowed`. Auditors see only their tenants' pipelines. Rooms with no pipeline id, or a pipeline `owners.yaml` does not know, are counted in buckets this route cannot read; their room route still shows them. `limit` holds `PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET` and `PIPEROOM_PIPELINE_DAILY_COST_USD` (0 = off). A day with no spend answers zeros |
| GET | `/api/shadow/metrics?pipeline=&since=` | – | `{rooms, labelled, graded, layer_correct, root_cause: {yes, partial, no}, by_status: {…}, median_time_to_first_s, median_time_to_final_s, median_human_mttr_s, total_tokens, tokens_available, per_room: [...]}` | over **every closed shadow room** matching the filters (errors included); `since` RFC 3339 on `closed_at`; medians `-1` when no room has the value; `tokens_available` false when every run reports 0 tokens (demo, plain runner); `per_room` rows: `{room_id, pipeline_id, alert_source, status, opened_at, closed_at, labelled, graded, layer_correct, root_cause, time_to_first_s, time_to_final_s, human_mttr_s, tool_calls, tokens, finished_after_close}`; "N of M matched" = `layer_correct` of `rooms` |

Driver lease: 30 minutes, renewed by any driver action. So that clients folding the log see the same lease as
the server, a renewal that happens when less than 25 minutes remain is written to the log as `control.granted`
with `to` = the current driver and the new `lease_until` (at most one such event per 5 minutes). A driver whose
lease expired and who was not replaced is still treated as the driver: their next action renews the lease.

Proposals: a step's action must implement `actions.Scoped`; the server calls `Validate` and then
`CheckScope(room's pipeline, params)` when the proposal is created and again right before `Execute`. Params are
frozen at creation; approvals bind to those exact steps.

### Watcher alerts (`alert_source: "watcher"`)

The built-in watcher (`internal/watch`, wired in `cmd/server/watch.go`, design in `docs/design/watcher.md`)
opens rooms **inside the process**. It has no HTTP route and no secret: `POST /api/alerts/watcher` is
`404 not_found`, like any unknown `{source}`. It starts after the room service loads and stops before it
closes; `--demo` forces it off.

- `room.created.alert_source` is `"watcher"`. `dedup_key` is `watcher:<pipeline>`, for example
  `watcher:orders`. There is **one open watcher room per pipeline**; every signal (connector down, slot
  inactive, slot WAL, DLQ growth, config risk) shares that key. `room.created.alert` holds the watcher's evidence JSON.
- **Join-on-open-room.** Before it opens a room, the watcher asks whether any room for that pipeline is
  already open, including one opened by Alertmanager or a person. If so it posts its evidence into that room
  as a join and opens nothing. A later signal or re-fire on the same pipeline joins the same room the same
  way. While a signal keeps firing, at most one "still firing" join is posted every 15 minutes.
- **Resolved.** When every signal of the pipeline has cleared, the watcher sends the same alert with
  `status: "resolved"`. The service posts a resolved note in the open room and leaves the room **open**; a
  resolved alert with no open room opens nothing and is dropped (same rule as the signed sources above).
- **DLQ growth** (docs/design/dlq.md). `dlq_growth` fires when a dead-letter topic of the pipeline (from the
  sink's `errors.deadletterqueue.topic.name`, or `watch.dlq_topics` in `owners.yaml`) gains records; the
  evidence holds counts, rates and offsets only, never record content. `config_risk` (default `action: log`)
  reports a connector set to drop records silently (`errors.tolerance=all` with no DLQ and no error log, or on a
  source connector) or a sink whose configured DLQ topic does not exist. Records that were in the DLQ before
  the watcher started never fire. A pipeline with no DLQ topics costs the watcher no Kafka call.
- **After a restart** the watcher joins a room that is still open but does not wake the agent. A room closed
  less than `PIPEROOM_WATCH_COOLDOWN` ago (default 60 min) is not re-opened by the same incident.
- Modes: `PIPEROOM_WATCH_MODE` = `off`, `log` (default: log transitions, open nothing) or `rooms`.

### Spend limits (design `docs/design/cost.md`)

`PIPEROOM_ROOM_TOKEN_BUDGET` (per room, default 2 000 000), `PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET` and
`PIPEROOM_PIPELINE_DAILY_COST_USD` (per pipeline per UTC day, both `0` = off) are kept in the room database, so a
restart does not reset them. The caps are soft: each model request is checked before it is sent, and requests in
flight can run past a cap by what they use beyond their estimates: a room by one request plus a hand-off, a pipeline
by up to `PIPEROOM_MAX_AGENTS` requests plus in-flight hand-offs. A spent cap stops the agent with an `agent.message` and an idle
`agent.status` whose detail names the cap and the setting to raise (the room cap keeps its text: `token budget
reached (N of M); a person must raise PIPEROOM_ROOM_TOKEN_BUDGET or continue manually`). If the spend store
cannot be reached while any cap is set, the agent stops the same way with a detail that says so; with no cap set it
goes on and the server logs a warning. The hand-off summary is counted but never refused. Cost is estimated from
`PIPEROOM_MODEL_PRICES`; a cost cap needs that table.

In a shadow room, steer, suggestion and comment events are logged but never start or wake an agent (the run is one-shot and takes no input).

### Shadow mode (`room.created.shadow: true`) — contract fixed, server in shadow PR 1

A pipeline with `shadow: {enabled: true}` in `owners.yaml` opens shadow rooms from every alert route
(dev webhook, signed sources, watcher); `POST /api/rooms` never does. In a shadow room the agent runs
once with read tools only, proposes nothing, and writes nothing to the room log: its events live in a
separate shadow log. When the driver closes the room (resolution required) and the run has ended, the
server appends `shadow.revealed` (system actor; numbers only). A member of the room's pipeline then
labels blind (`POST …/shadow/blind`), reads the run (`GET …/shadow`) and grades it
(`POST …/shadow/grade`). `GET /api/shadow/metrics` aggregates. Env: `PIPEROOM_SHADOW_GRACE` (default
`0`: the run is cancelled at close; a duration lets it finish), `PIPEROOM_SHADOW_TOKEN_BUDGET`
(`500000`; `0` = the room default `PIPEROOM_ROOM_TOKEN_BUDGET`, never unlimited), `PIPEROOM_SHADOW_MAX_TURNS` (`20`). Shadow runs share `PIPEROOM_MAX_AGENTS`. Events after
`room.closed` in a shadow room: `shadow.revealed`, `shadow.labelled`, `shadow.graded` (one per
re-grade), with the payloads in `internal/events/events.go`. The Slack mirror stops at `room.closed` and
never shows shadow output.

## WebSocket

`GET /api/rooms/{id}/ws?after=<seq>`

The WebSocket uses the session cookie (dev: or `X-User` for non-browser clients); no credentials → `401`.

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
