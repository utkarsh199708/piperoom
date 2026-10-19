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

`admin: true` on a users.yaml entry makes that user an **admin**: the one role that may delete a room
(`DELETE /api/rooms/{id}`, see Room limits). It is false when left out, grants nothing else, and is not in
`owners.yaml`: an IdP user who maps to no users.yaml entry is never an admin.

A users.yaml entry may also carry `slack_id: U0...` (the person's Slack member id). It is used only by the Slack
bridge's allow-listed paging (`internal/slackbridge`, `PagingOptions`): the bridge pages a person on Slack only when
their entry names a `slack_id`; it is never shown in the UI or returned by `/api/me` or `/api/people`, and an entry
without one means the person cannot be paged. Implemented by A3 (owners lookup) and A5 (`cmd/server` wiring of
`slackbridge.PagingOptions`), consumed by C12 #130.

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
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` (loud warnings); `--allow-remote` is a deprecated alias for one release. From R9
  it also needs `PIPEROOM_DEV_ACK=dev-login-has-no-password` (`config.ParseDevAck`): the image sets only the switch,
  so the operator must state in their own deployment that dev login has no password. Any other non-empty value stops
  the start. Not needed on a loopback address, in `oidc` mode (ignored there with one warning), or with `--demo`.
- **Session.** A server-side session in a cookie: `__Host-piperoom_session` in `oidc` mode (https),
  `piperoom_session` (no `Secure`) in `dev` mode over plain http. The cookie wins over `X-User` when both name the
  same user.
- **Routes.** `GET /auth/login?return=/path` → IdP redirect; `GET /auth/callback` (finds the login row by the
  login cookie, then checks `state` and nonce; single use; on success `302` to `return`; on failure `302` to
  `/?login_error=<code>` with the same code as the JSON error — `login_expired`, `unauthenticated`,
  `not_provisioned`, `rate_limited`, `idp_unavailable` — never a JSON page in the browser); `POST /auth/logout` (`204`; revokes this session and closes this session's WebSockets);
  `POST /auth/dev-login {user}` (dev mode only); `GET /auth/users` (dev mode only, below).
- **Login picker (`GET /auth/users`, R0; server R4, web R3).** Dev mode only: the users the dev login screen may
  pick from, so the web app stops shipping its own copy of `dev/users.yaml`. No session, cookie or `X-User` needed
  (it is called before login); the Host allow-list and the `/auth/` JSON 404 apply as to every `/auth/` route.
  Answer `200` with `[{id, name, layers}]`: every `users.yaml` user (in `--demo` without a users file, the built-in
  demo users), in file order; `layers` as in the file (`[]` when none). Nothing else of the entry is returned: no
  `email`, `sub` or `admin`, and never anyone known only to the IdP or `owners.yaml`. With `PIPEROOM_SOLO_USER` set
  the list is that one user. In `oidc` mode the route is **not mounted** and answers `404 not_found` like any
  unknown `/auth/` path, so the people directory is never readable without a login (the web app learns the mode
  from the `401 session_expired` body's `auth_mode`).
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
- **Verified people (design §2.1).** Every person the API returns carries `verified`: `true` for a `users.yaml`
  person, `false` for someone known only to the IdP (a derived `u_` id). This covers event `actor`s (agent and system
  actors are `true`), `control.granted.to` and `handoff.created.to`, the snapshot's `driver`,
  `pending_suggestions[].from` and `members[]`, `/api/me`, `/api/people` and WebSocket presence `members`. In events
  (`events.Actor`, also inside the export) the key is **omitted when false** (`omitempty`), so events written before
  the field, and exports of old rooms, are unchanged; a missing `verified` counts as `false` everywhere. The other
  objects always carry `true` or `false`. Clients show an unverified person with a badge and their id or email next
  to the name. The web badge ships in R3 (#135). An
  IdP name that looks like a `users.yaml` person is shown as `"<name> (<email>)"`, or `"<name> (<id>)"` without a
  verified email, and the login is logged, not refused. "Looks like": the whole name, or the name with its bracketed
  parts removed, equals a `users.yaml` id, name or email after the design §2.1 skeleton (default-ignorable code points
  dropped, compatibility forms, Unicode case folding, Cyrillic/Greek/Armenian/Cherokee/Lisu look-alikes); also any
  name that mixes Latin with another script, and any name that ends in the server's own suffix shape (`(…@…)` or
  `(u_…)`). A name with extra words (`Priya S.`) is not suffixed and relies on the badge.

## Errors

```json
{ "error": { "code": "not_driver", "message": "Only the driver can send steers." } }
```

| HTTP | code examples |
|---|---|
| 400 | `bad_request`, `validation`; after auth PR 3: `login_expired` (login cookie or row missing/expired at `/auth/callback`) |
| 401 | `unauthenticated`; after auth PR 2: `session_expired` (no, revoked or expired session). Its body also carries the server's mode: `{"error":{"code":"session_expired","message":"…","auth_mode":"dev"\|"oidc"}}` — the web app shows the dev picker or goes to `/auth/login?return=…`. A WebSocket whose session ends gets an `error` frame with code `session_expired` and is then closed with close reason `session_expired` |
| 403 | `not_driver`, `not_layer_owner`, `not_recipient` (hand-off accept by someone other than `to`); after auth PRs: `viewer`, `proxy_identity`, `not_allowed` (export; also: label or grade a shadow room without being a member of its pipeline; delete a room without `admin: true`; F8: `POST /api/rooms/{id}/label` without being a member of its pipeline), `not_provisioned` (login gate) |
| 404 | `not_found` |
| 413 | `too_large` (signed alert body over 1 MB) |
| 409 | `already_decided`, `room_closed`, `lease_held`, `stale_handoff`; with several server replicas: `conflict` (another replica changed the room between the check and the write three times in a row; retry); after auth PR 4: `params_mismatch`; shadow mode: `room_open` (shadow route on an open room, or `?include=shadow` on an open room), `not_labelled` (shadow data read before the blind label), `not_revealed` (grade before `shadow.revealed`: the run is still going during the grace period); room limits: `already_extended` (a second `extend`), `log_full` (the room log is over its cap and its reserve; see Room limits); R0: `handoff_in_progress` (a hand-off request of the room is in flight); F9: `no_hypothesis` (a verdict before the room has a hypothesis), `already_nudged` (a second nudge of the same proposal and layer within 10 minutes; the message says how long ago, e.g. "nudged 4 min ago"); F8: `room_open` also answers `POST /api/rooms/{id}/label` on an open room; #459: `client_id_reused` (an idempotency key sent again with another route or body); P2: `not_scheduled` (`POST /api/proposals/{pid}/cancel` when no `proposal.scheduled` of the proposal is pending) |
| 421 | `bad_host` (after auth PR 2: `Host` not on the allow-list). R4: a browser `GET` outside `/api` and `/auth` that sends `Accept: text/html` gets a short HTML page instead (what happened and which of `PIPEROOM_PUBLIC_URL` / `PIPEROOM_ALLOWED_HOSTS` to set; the Host is HTML-escaped and clipped to 100 bytes; its own CSP `default-src 'none'`, no script). Every other request gets the JSON body, whose message never echoes the Host |
| 429 | after auth PR 3: `rate_limited` (`/auth/login` over 300 per minute per client); R4: `too_many_connections` (WebSocket, before the upgrade; see WebSocket); P3: `too_many_proposals` (a sixth open driver-authored proposal in one room) |
| 500 | `internal_error`; `archive_failed` (`DELETE /api/rooms/{id}?archive=true` could not write the archive, so nothing was deleted) |
| 503 | after auth PR 3: `idp_unavailable` (new logins while the IdP is down; live sessions keep working) |

Every text field a person types (for example titles, comments, steers, resolutions, trace keys, labels, verdict notes, decline and delete reasons, checklist notes, shadow grades, approve/reject layer and reason) that contains a NUL character or invalid UTF-8 is refused with `400 validation` ("text contains characters that cannot be stored") and nothing is written. Such text that contains a bidirectional control character (U+202A–U+202E, U+2066–U+2069) is refused the same way with `400 validation` ("text contains bidirectional control characters (U+202A-U+202E, U+2066-U+2069)") (#358). Text from the agent, tools and actions is cleaned instead (bad characters become U+FFFD, bidirectional control characters are dropped). Alert room titles, watcher notes, a label's `cause` and `fix_that_worked`, and a close resolution (which the web app pre-fills from agent text) have bidirectional control characters dropped instead of refused.

`lease_held`: `control/grant` called by a non-driver while another user's lease is still live. All other
driver-only endpoints answer `403 not_driver`. `stale_handoff`: the hand-off's creator is no longer the driver,
or the hand-off was voided by a newer one. A second `send` of an already-sent suggestion → `409 already_decided`.

Setup rooms (U0; see **Setup rooms** under REST endpoints) add: `400 not_in_plan` (a proposal step that is not in the
current `setup.plan`), `400 address_refused` (a target address on the denylist of design §2.6), `400 class_lowered`
(a class below the one the server derived), `400 window_too_long` (a maintenance window over 4 h or more than 24 h
ahead), `400 not_setup_room` (a setup route on an incident room); `403 not_tenant_member` (`POST /api/setup` for a
tenant the caller is not in), `403 not_admin` (a T2/T3 approval on a team server by someone who is neither a tenant
admin nor an owner of an overlapping pipeline); `404 setup_disabled` (every setup route and `/api/runners/ws` while
`PIPEROOM_SETUP_ENABLED` is off); `409 pipeline_exists` (`POST /api/setup` for a pipeline id `owners.yaml` already
has), `409 no_runner` (a route that needs the paired runner while none is connected), `409 runner_paired` (a second
pairing while the first runner is still attached), `409 overlap` (`pipeline_register` refused by the overlap check;
the message names the overlap kind and the caller's own tenant's pipelines only), `409 not_verified`
(`pipeline_register` without a green `setup.verified` for the current plan); `429 pairing_closed` (the pairing rate
limit of design §2.1: pairing is closed for 10 minutes and audited).

## Objects

### Event
Exactly `events.Event` from `internal/events/events.go`:
```json
{ "room_id": "r_01J...", "seq": 12, "type": "comment.added",
  "actor": { "kind": "human", "id": "kabir", "name": "Kabir", "verified": true },
  "payload": { "comment_id": "c_01J...", "step_id": "s_2", "text": "Wrong service?", "mentions_agent": false },
  "at": "2026-10-11T10:59:03Z" }
```
`actor.verified` is decided when the event is written (see **Verified people** under Login).

`room.created` (`events.RoomCreatedPayload`) carries, besides `title`, `severity`, `pipeline_id`, `alert`,
`alert_source`, `dedup_key`, `shadow` and `mode` (U0: `"setup"` for a setup room, absent otherwise), the server's
verification of the alert **sender** (R3 #135 contract change
request; A5 implements):

- `alert_verified` (bool, omitted when false): `true` when the server checked the sender before opening the room, a
  signed route's signature or token (`internal/alertin`) or the built-in watcher. `false` for the unsigned dev
  webhook (`POST /api/alerts/webhook`) and for rooms not opened by an alert. It vouches for the sender, not for the
  alert's text.
- `alert_verified_by` (string, omitted when empty): how the sender was checked, one of `bearer_token`, `hmac`,
  `hmac_timestamp`, `header_token` or `watcher`. Empty when `alert_verified` is false.

A client reads `alert_verified === true` and keeps any `alert_source` table only for the label.

### Agent usage (`agent.usage`)
One event per model request the agent sends (design `docs/design/cost.md`), actor `agent`: every investigation
turn, the hand-off summary, and, as a second event, a server-side compaction pass. Counts only, never prompt,
tool or model text. A request refused by a budget is never sent and gets no event; a request still in flight when
the room closes still gets one. In a shadow room these events go to the shadow log like every other agent event.
Clients that do not show cost ignore the type (the web reducer only advances `last_seq` for it).
```json
{ "model": "claude-opus-5-5", "role": "main",          // triage | main | handoff | compaction | judge (reserved) | postmortem (F2) | first_look (F6, the triage pass)
  "input_tokens": 1200, "cache_creation_input_tokens": 3000, "cache_read_input_tokens": 41000,
  "output_tokens": 900, "latency_ms": 8123,             // 0 for compaction (its time is in the request's)
  "cost_usd": 0.0412 }                                  // omitted when PIPEROOM_MODEL_PRICES has no price for model
```

### F-wave events (F0; payloads in `internal/events/events.go`)

Every type below is additive: a client that does not know a type ignores it (the web reducer only advances
`last_seq`), and every new field on an existing payload is omitted when empty, so events written before F0 and
their export hashes are unchanged. Text fields from the agent or a tool are untrusted: clients render them as text,
never as HTML or Markdown.

- **Evidence receipts (F3, implemented by A2/A3 clients and A4).** `agent.tool_result` may carry
  `provenance: [{kind, target, text, params_redacted?, at, took_ms}]`, the reads behind the summary (≤ 16;
  `kind` ∈ `sql | http | kafka | registry`; `text` is the statement or request with placeholders, never values,
  ≤ 2 KiB; each `params_redacted` item ≤ 200 bytes; `target` names a system, never credentials). It is outside
  `data`, so a cut `data` keeps its receipt. `agent.hypothesis` may carry `unbacked_terms: [string]` (≤ 20, each
  ≤ 80 bytes): names and numbers in `text` that no cited step's summary contains; they never block the hypothesis.
  `handoff.created.summary` carries `violations: [{rule, field, detail}]`, the hand-off checks still failing
  after the agent's self-correction pass; shown to the recipient before Accept, never blocking. `[]` means
  checked and clean; no field (summaries written before F3) means not checked.
- **Fix ceremony (F4).** `ProposalStep` gains `verify?` (one sentence, what the agent checks after the step;
  part of the hashed proposal, omitted when empty). `proposal.previewed {proposal_id, steps: [{step_index,
  preview?: {target, current, expected, downstream?, reversible, rollback?, dry_run?: {ok, source, text,
  skipped?}}, error?}], at}` (system actor, once per proposal after `proposal.created`, informational, **not** part
  of `params_hash`, may be absent; implemented by A5 with A2's `actions.Preview`). `agent.verification
  {proposal_id, checks: [{layer, name, ok, detail}], data_flow?: {checked, fresh, sinks: [{table, newest?,
  baseline?, passed, passed_by?}], detail?}, recovered, verdict, repeat?}` (agent actor, after the last step of a
  proposal ran) replaces the plain-text `[verification]` `agent.message` (implemented by A4).
- **Post-mortem (F2, implemented by A5; model pass by A4).** `postmortem.drafted {source: "handoff" | "model" |
  "events", summary_ref?, summary?, violations?}` and `postmortem.failed {reason: "summary_failed" |
  "internal_error"}` are written by the system actor **after `room.closed`**, like `shadow.revealed`: clients must
  accept events after `room.closed`. Never written for an open room or a shadow room. `summary` has the
  `HandoffSummary` shape; `violations` are rule names only.
- **Hypothesis verdicts and nudges (F9, implemented by A5).** `hypothesis.verdict {verdict: "confirmed" |
  "disputed", note?, hypothesis_seq}` (actor: the member who decided). `agent.hypothesis.confirmed` is always
  `false` as written by the agent: clients and the snapshot derive it (at least one `confirmed` and no `disputed`
  verdict on the current hypothesis seq); a new `agent.hypothesis` clears every verdict. `approval.nudged
  {proposal_id, layer}` (actor: the member who nudged); ids only.
- **Triage (F6, implemented by A4).** `agent.triage {status: "ok" | "failed", reason?, headline?, suspected_layer?,
  confidence?, evidence_refs, next_check?, model, took_ms}` (agent actor), the triage pass's one-line first look,
  labelled "triage, unverified" wherever shown. `reason` ∈ `timeout | model_error | budget | refused | no_tools |
  off`; `evidence_refs` is the pass's own step ids, never `null`. A room may hold several (a re-fire re-runs the pass).
- **Labels and checklists (F8, implemented by A5).** `room.labelled {actual_layer?, cause, fix_that_worked?,
  fix_proposal_id?, agent_helped?, identity?}` (a closed room's human label; re-labelling appends, the last wins).
  `checklist.step_done {step_id, tool_use_id, hit, item, done, note?}` (a person ticked or unticked item `item` of
  `hits[hit].items` in the `runbook_lookup` result `tool_use_id`).

### P-wave events (P0; payloads in `internal/events/events.go`, items in `tasks/P0-contracts.md`)

Additive like the F wave: unknown types are ignored by clients, and every new field on an existing payload is
omitted when empty, so events written before P0 and their export hashes are unchanged.

- **Approval grants (item 4; P2b, A5).** `approval.given` gains `grant` and `policy`. `grant` says what gave the
  right to approve: `owner` (the actor owns the layer: owners.yaml principal or backup, or the users.yaml `layers`
  list in dev mode), `solo` (`PIPEROOM_SOLO_USER`), `fallback` (nobody owns the layer, see `fallback_layers`, and
  the actor was the room's driver when the event was appended) or `policy` (the pipeline's `auto_approve:` list,
  after an uncancelled `proposal.scheduled`; actor: the system; `policy: "auto_approve"`; no `identity`).
  `identity.grant` keeps the principal detail (`user:priya`, `backup:user:meera`, `solo`, `fallback`). An event
  without `grant` was written before P2: read `solo` from `identity.grant`, anything else as `owner`.
- **Scheduled proposals (item 4; P2b).** `proposal.scheduled {proposal_id, run_at, policy: "auto_approve",
  cancel_until}` (system actor, after `proposal.previewed`, at most once per proposal): unless cancelled, at
  `run_at` the system appends one `approval.given {grant: "policy"}` per `required_layers` entry, in that order,
  and the executor runs the steps. `cancel_until` equals `run_at` in this wave. The proposal is cancellable until
  the policy starts (its first approval is logged); a cancel after `cancel_until` still wins if it lands first.
  `proposal.cancelled {proposal_id,
  by, reason}` (actor: the person in `by`) stops it; the proposal stays open for manual approval. A manual
  `approve-all` before `run_at` stops the timer without an event. Both count against the room log caps like
  `approval.given` (the 2% reserve applies; neither is exempt like `room.closed`). Never scheduled for a proposal
  with `origin: "driver"`, a preview that failed or has `dry_run.ok: false`, write mode off, a shadow room, or
  `PIPEROOM_AUTO_APPROVE_DELAY=0`.
- **Sole-member driver (item 4; P2b).** `control.granted` gains `reason`: `"sole_member"` when the first member to
  join a driverless alert room (no `control.requested` pending; never a viewer; never in a shadow room) became its
  driver without asking. Actor: the system. Absent for every other grant.
- **Unowned layers and driver-authored proposals (items 4, 5; P2b, P3c).** `proposal.created` gains
  `fallback_layers` (the `required_layers` nobody owns: the driver approves them with `grant: "fallback"`; absent
  when every layer has an owner) and `origin`: `"agent"` or `"driver"` (`POST /api/rooms/{id}/proposals`); absent
  reads as `"agent"`. Neither is part of `params_hash`.
- **Pipeline kinds (item 8; P4).** The layer set grows by `orchestrator` and `transform` (batch pipelines:
  `events.LayersOf("batch")` = `orchestrator, transform, warehouse`; `cdc` = the six of before). Wherever a field
  says "one of the layers" (`approval.*.layer`, `room.labelled.actual_layer`, `agent.triage.suspected_layer`,
  topology `layer`), these two are valid too.

### U-wave events (U0; payloads in `internal/events/events.go`, design `docs/design/setup-agent.md`)

Additive like the F and P waves: unknown types are ignored by clients, and every new field on an existing payload is
omitted when empty, so events written before U0 and their export hashes are unchanged. Every `setup.*` event is
audit: **no event ever carries a secret value, a pairing code or a runner token**; the runner label stands for the
runner, a name for a secret, a hash for the canary nonce.

- **Setup rooms (`room.created.mode: "setup"`).** `POST /api/setup` opens a room with `mode: "setup"` next to
  `shadow` (never both) and the `SetupRequest` as `alert`. The agent runs in setup mode (`agent.Alert.Mode`); the
  room scopes tools and actions to the provisional pipeline (`owners.Provisional`) until `pipeline.registered`.
- **Layer `environment` (`events.LayerEnvironment`).** `events.LayersOf("cdc")` gains `environment` **at the end**
  (postgres, kafka-connect, schema-registry, kafka, consumer, warehouse, environment); `events.Layers` has nine
  entries. It is the layer of the setup actions (compose, service, manifest, plugin, `pipeline_register`). An
  `owners.yaml` that does not name it leaves it unowned (the driver approves it with `grant: "fallback"` in solo mode;
  a tenant admin with `grant: "admin"` otherwise); every existing owners file still loads. `environment` is valid
  for **approvals, `required_layers`, `fallback_layers` and owners entries only**. It is never an incident layer
  (`events.IncidentLayers()`): the triage `suspected_layer` enum the model sees is unchanged, and
  `room.labelled.actual_layer`, `shadow.labelled.actual_layer`, the scenario, learned-example, agreement and
  `similar_incidents` validators mean the eight incident layers (they switch from `events.Layers` to
  `events.IncidentLayers()` in their owners' rows; until then they accept `environment` too, which only widens
  what a label may say).
- **Runner lifecycle.** `setup.pairing {outcome: ok | bad_code | expired | rate_limited | replaced | refused, source,
  runner_label?}` for every pairing attempt (system actor); `setup.runner_attached {runner_label, os, arch, tools,
  project_dir, catalogue_version, protocol_version}` (the runner's `Hello`, after the person confirmed on its
  terminal); `setup.runner_lost {runner_label, last_seen}` (silent 90 s; every in-flight step is recorded with
  `outcome_unknown: true`); `setup.runner_refused {op, reason, proposal_id?, step_index?}` (`reason` is a
  `protocol.Refuse*` code, never runner text).
- **Plan and bounds (agent actor).** `setup.plan {plan_id, supersedes?, phases: [{name, steps: [{index, op, target,
  tier, layer, verify?}]}]}`: `target`, `tier` and `layer` are filled by the server from the resolved endpoint, never
  from the model; a proposal whose step is not in the current plan is refused (`agent.ErrNotInPlan`, `400
  not_in_plan`). `setup.blocked {reason: attempts | steps | wall_clock | budget | runner_lost | refused, step_index?,
  question}`: a bound stopped the run and the agent is idle until a steer.
- **Driver clicks (actor: the driver, also in `by`).** `setup.target_registered {kind, address, class, by}`,
  `setup.pipeline_extended {add_connectors?, add_tables?, add_consumer_groups?, add_sink_tables?, by}`,
  `setup.maintenance_window {start, end, by}`. `setup.target_classed {target, class, reason, by: "server" |
  "person"}` records every class decision; a class only rises.
- **Secrets, names only.** `setup.secret_set {name, actor}` (the runner's terminal or `POST
  /api/rooms/{id}/setup/secret`); `setup.credentials_registered {pipeline_id, names, runner_label}` (the runner's
  `Credentials` frame landed in `PIPEROOM_SECRETS_DIR/<pipeline>/`). The values are in no event, log, action result
  or prompt.
- **Verification (system actor, the ladder runner only).** `setup.verified {plan_id, nonce_hash, rungs: [{name, ok,
  step_id?, receipt_id?, note?}], first_event_latency_ms?}` is written only by the server's ladder runner: the
  agent's `Sink` refuses the type, so no model output can claim a pipeline verified. `setup.awaiting_change {table,
  example_sql}` is rung 8 (the person's own change).
- **Receipts.** `action.executed` gains `receipt` (`events.SetupReceipt`: `{kind: exec | http | sql | file, argv?,
  target, exit_code, at, took_ms, out_mac}`) for a step of a `Receipted` action: the exact argv with every secret as
  `${secret:name}`, the target, and an HMAC of the output under a key only the runner holds; never credentials or raw
  output. Absent for every other action.
- **Policies and grants.** `proposal.scheduled.policy` gains `"setup_local"` (a T1 step on a local-class target,
  countdown `PIPEROOM_SETUP_LOCAL_DELAY`, cancellable like `auto_approve`); `approval.given.grant` gains `"admin"` (a
  tenant admin, `tenants.<t>.admins`, approving a T2 or T3 step of a setup room on a team server).
- **Registration.** `pipeline.registered {pipeline_id, tenant?, path | pull_request_url, watched_by: "server" |
  "local", by, approved_by}` (system actor) after the `pipeline_register` step ran and the server reloaded; the room
  then closes with resolution `pipeline live` and `room.labelled` gets `cause: setup`.

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
  "driver": { "id": "arjun", "name": "Arjun", "verified": true, "lease_until": "..." },
  "agent_status": { "state": "working", "detail": "step 6" },     // "idle" for a new room; states: working | idle | done | waiting_approval | queued ("waiting for an agent slot (N ahead)" when PIPEROOM_MAX_AGENTS runs are busy) | log_full (the room log hit its cap; the agent stopped for good); in a shadow room the agent writes no status to the room log, so it stays "idle"
  "auto_close_at": "...",          // open-room limit: when the server closes this open room (opened_at + PIPEROOM_MAX_OPEN_DAYS, or room.extended.close_at, or a later room.retention_warning.close_at); null when the limit is off or the room is closed
  "extended": false,               // the room's one extension was used (room.extended)
  "log_cap": "ok",                 // ok | warning (room.log_cap_warning: 90% of a cap) | full (agent.status log_full)
  "hypothesis": { "text": "...", "based_on_steps": ["s_3","s_4"], "confidence": "high", "confirmed": false },
  "open_proposals": [ { "proposal_id": "p_01J...", "title": "...", "steps": [], "risk": "...", "rollback": "...",   // after auth PR 4 also "params_hash"
                        "required_layers": ["schema-registry","kafka-connect"],
                        "approvals": { "schema-registry": "approved", "kafka-connect": "pending" } } ],
  "pending_suggestions": [ { "suggestion_id": "g_01J...", "from": {"id":"kabir","name":"Kabir","verified":true}, "text": "..." } ],
  "members": [ { "id": "priya", "name": "Priya", "verified": true, "role": "watching" } ]
}
```
`closed_at` and `resolution` are set by `room.closed`. `required_layers` is always the set of the steps' layers
(the server derives it; the proposer cannot choose it).
#### F-wave snapshot fields (F0; implemented by A5 in `internal/room/snapshot.go`)

Additive: each field is absent until the task that implements it lands, and clients must treat an absent field
as its empty value. The shapes are fixed here.
```json
  "closed_by": { "kind": "human", "id": "priya", "name": "Priya", "verified": true },  // F10: closed rooms only, the actor of room.closed (a system actor for retention closes)
  "hypothesis": { "text": "...", "based_on_steps": ["s_3"], "confidence": "high", "confirmed": true,
                  "seq": 17,                                  // F9: seq of the agent.hypothesis event
                  "verdicts": [ { "verdict": "confirmed",     // F9: latest verdict per person on this seq, in seq order
                                  "by": { "id": "kabir", "name": "Kabir", "verified": true },
                                  "note": "", "at": "..." } ],
                  "unbacked_terms": ["order_id"] },           // F3: copied from the agent.hypothesis payload
  "triage": { "status": "ok", "headline": "...", "suspected_layer": "kafka-connect", "confidence": "medium",
              "evidence_refs": ["s_1","s_2"], "at": "..." },  // F6: the newest agent.triage, or null
  "labelled": false,                                          // F8: a room.labelled exists
  "checklist_done": { "<tool_use_id>:<hit>:<item>": { "by": { "id": "kabir", "name": "Kabir", "verified": true }, "at": "..." } },  // F8: open rooms; last event per key wins, done=false removes the key
  "postmortem": { "status": "drafted", "source": "handoff" },  // F2: status none | drafting | drafted | failed; source? handoff | model | events; reason? summary_failed | internal_error
  "insight": { "first_hypothesis_at": null, "time_to_first_hypothesis_s": -1, "final_confidence": "",
               "steps": 0, "tool_calls": 0, "tool_errors": 0,
               "usage": { "calls": 0, "unpriced_calls": 0, "tokens": 0, "cost_usd": null, "latency_ms_total": 0 } },  // F11
  "open_proposals": [ { "...": "...", "preview": { "proposal_id": "p_…", "steps": [ … ], "at": "..." } } ]  // F4: the proposal.previewed payload, absent until it exists
```
- `closed_by` (F10, implemented by A5): set from `room.closed`'s actor in `Apply`; absent on open rooms.
- `hypothesis.seq`, `hypothesis.verdicts` (F9, implemented by A5): `confirmed` is **derived**: at least one
  `confirmed` and no `disputed` verdict on `seq`; the agent-written `confirmed` is ignored. A new
  `agent.hypothesis` resets `verdicts` to `[]`. `hypothesis.unbacked_terms` (F3, implemented by A5): `[]` when none.
- `triage` (F6, implemented by A5): folded from the newest `agent.triage`; `agent.hypothesis` does not clear it.
  `GET /api/rooms` rows carry it.
- `labelled`, `checklist_done` (F8, implemented by A5).
- `postmortem` (F2, implemented by A5): `drafting` from `room.closed` until `postmortem.drafted` or
  `postmortem.failed` (at most 3 min); `none` on open rooms and shadow rooms.
- `insight` (F11, implemented by A5): folded from the room log (`agent.step.started`, `agent.tool_call`,
  `agent.tool_result`, `agent.hypothesis`, `agent.usage`). `time_to_first_hypothesis_s` is whole seconds from
  `opened_at` to the first `agent.hypothesis`, `-1` before one exists; `tokens` = input + cache creation + cache
  read + output; `cost_usd` is `null` while no call is priced and a lower bound when `unpriced_calls > 0` (the
  `Usage` rule); zeros in a shadow room. `GET /api/rooms` rows carry it.
- `open_proposals[].preview` (F4, implemented by A5): the `proposal.previewed` payload of that proposal.

#### P-wave snapshot fields (P0; implemented by A5)

Additive, same rule as the F-wave fields: absent until the task lands, and an absent field reads as its empty value.
```json
  "mode": { "write": false, "solo": false, "auto_approve": ["connect_restart_task"] },   // P1a (write), P2b (solo, auto_approve)
  "open_proposals": [ { "...": "...",
                        "origin": "agent",                                   // P3c: "agent" | "driver" (proposal.created.origin; absent reads as "agent")
                        "fallback_layers": ["consumer"],                     // P2b: proposal.created.fallback_layers; [] when every layer has an owner
                        "blocked": "write_mode_off",                         // P1a: every layer approved, nothing ran because write mode is off; absent otherwise
                        "scheduled": { "run_at": "...", "cancel_until": "..." } } ]  // P2b: while a proposal.scheduled is pending; absent otherwise
```
- `mode` (item 1, item 4): `write` is the server's `PIPEROOM_WRITE_MODE` (`--demo` forces `true`); `solo` is
  **derived per caller**: true when the caller may approve every layer of the room's pipeline kind
  (`events.LayersOf(kind)` without the setup layer `environment`, U0: no incident action targets it, so an owners
  file from before the U wave keeps its solo mode), as owner or backup, or is `PIPEROOM_SOLO_USER`; `auto_approve` is the pipeline's
  `auto_approve:` list from `owners.yaml` (`[]` when none, or when `PIPEROOM_AUTO_APPROVE_DELAY=0`). `GET /api/rooms`
  rows do not carry `mode` (room view only).
- `open_proposals[].blocked` (item 1, P1a): `"write_mode_off"` when every required layer approved and the executor
  did not run the steps because write mode is off. The proposal stays open with every required layer approved; no `action.executed` is written.
  It is the only value in this wave.
- `open_proposals[].scheduled` (item 4, P2b): from the pending `proposal.scheduled`; cleared by
  `proposal.cancelled`, by the policy's approvals, or by any human `approval.*` on the proposal; the room view
  (`GET /api/rooms/{id}`) leaves it out once `run_at` is more than 45 s in the past; the proposal stays open for manual
  approval and can still be cancelled; WebSocket snapshot frames still carry it (P2b-4b, #652).
- `open_proposals[].fallback_layers`, `open_proposals[].origin` (items 4, 5): copied from `proposal.created`.

#### U-wave snapshot fields (U0; implemented by A5)

Additive, same rule: absent until the row lands, and an absent field reads as its empty value. Present only on a
setup room (`room.created.mode: "setup"`).
```json
  "setup": { "mode": true,
             "runner": { "label": "arjun-laptop", "connected": true, "tools": { "docker": "27.1.1", "kubectl": "" } },  // setup.runner_attached / setup.runner_lost; null before pairing
             "plan_id": "pl_…", "phase": "stack", "step_index": 3,              // the current setup.plan and the step the agent is on
             "bounds": { "attempts_per_step": 3, "steps_per_plan": 12, "wall_clock_s": 2700, "token_budget": 2000000 },
             "targets": [ { "kind": "postgres", "address": "db.acme:5432", "class": "shared" } ],   // the request's plus every setup.target_registered, classes after setup.target_classed
             "provisional": { "topic_prefix": "orders", "tables": ["public.orders"], "connectors": ["orders-src"] },  // owners.Provisional after every setup.pipeline_extended
             "maintenance_window": { "start": "...", "end": "..." },             // the latest setup.maintenance_window, absent when none or past
             "verified": [ { "name": "connector_running", "ok": true } ],       // rungs of the latest setup.verified for plan_id; [] before one exists
             "blocked": { "reason": "attempts", "question": "..." },            // the latest setup.blocked while the agent is idle on it; absent otherwise
             "watched_by": "server" }                                           // after pipeline.registered: "server" | "local"; "" before
```
`GET /api/rooms` rows carry `setup.mode`, `setup.runner.connected` and `setup.phase` only. Nothing in the snapshot is
a secret value, a pairing code or a token: `runner.tools` are versions, `targets[].address` is what the person typed.

IDs are prefixed ULIDs: `r_` room, `s_` step, `c_` comment, `g_` suggestion, `p_` proposal, `h_` hand-off, `hr_` hand-off request (R0).

### AgentStats (F11, implemented by A5)
The answer of `GET /api/pipelines/{pid}/agent-stats`: the agent's track record on one pipeline over closed rooms.
Numbers only (no titles or hypothesis text).
```json
{ "pipeline_id": "orders", "window_days": 90, "generated_at": "…",
  "rooms": 23, "hypothesised": 21, "labelled": 17, "graded": 17,
  "layer_correct": 15, "root_cause": {"yes": 11, "partial": 4, "no": 2},
  "by_confidence": [ {"confidence": "high", "runs": 12, "correct": 11, "accuracy": 0.917},
                     {"confidence": "medium", "runs": 4, "correct": 2, "accuracy": 0.5},
                     {"confidence": "low", "runs": 1, "correct": 0, "accuracy": 0} ],
  "median_time_to_first_hypothesis_s": 48, "median_human_mttr_s": 720,
  "median_cost_usd": 0.42, "cost_available": true, "enough_data": true,
  "shadow": {"rooms": 6, "graded": 5, "layer_correct": 4} }
```
`by_confidence[]` uses the field names of `evals.ReliabilityBucket` (`confidence, runs, correct, accuracy`;
`probability` omitted); `correct` = graded `root_cause: yes` (`partial` is not correct). Medians are `-1` when no room
has the value; `cost_available` is false when every room is unpriced; `enough_data` = `graded >= 10` (clients show
counts, never a rate, below it). A graded room needs a human grade of the hypothesis: `room.labelled` (F8)
carries no grade, so a labelled live room counts in `labelled` only, and `graded`, `layer_correct`, `root_cause`
and `by_confidence` come from shadow rooms until a grade for live rooms is added by a later contract change.

### Topology (pipeline map)
```json
{ "pipeline_id": "orders",
  "nodes": [ { "id": "pg:public.orders", "kind": "source_table", "name": "public.orders", "layer": "postgres",
               "owner": "priya", "health": "warn", "facts": { "slot_retained": "12.4 GB" } } ],
  "edges": [ { "from": "pg:public.orders", "to": "connect:pg-orders-connector" },
             { "from": "group:order-sync", "to": "wh:analytics.orders", "mapping": "inferred" },
             { "from": "connect:pg-orders-connector", "to": "sr:pg.public.orders-value", "kind": "depends_on" } ] }
```
Node `kind`: `source_table | connector | topic | dlq_topic | consumer_group | sink_table | schema_subject`, and for a
`batch` pipeline (P0 item 8, builder P4a) `dag` (id `dag:<dag_id>`, layer `orchestrator`), `task` (id
`task:<dag_id>.<task_id>`, layer `orchestrator`, one per task instance of the latest run, its state in `facts`) and
`model` (id `model:<name>`, layer `transform`), drawn `dag → task → model → sink_table` with data edges. `health`:
`ok | warn | fail | unknown`. `partial` (P4, omitted when false): an optional reader the pipeline's kind needs is not
configured or not supported (no Airflow or dbt reader), so its nodes are left out; clients say the map is incomplete
rather than healthy.
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
| GET | `/rooms/{id}` | – | `302` | R4. Deep link (the room link Slack posts); outside `/api`, so no JSON. Signed in, or in dev mode: `302 /#/rooms/{id}`. Signed out in `oidc` mode: `302 /auth/login?return=/rooms/{id}`, and the callback lands back on the link. `{id}` must match `^[A-Za-z0-9_-]{1,64}$`; anything else is never redirected and falls to the web UI or `404`. `HEAD` answers the same |
| GET | `/healthz` | – | `{status: "ok", version, write_mode}` | outside `/api`, no auth, exempt from the Host allow-list (probes). `version` (R0; server R9) is the build version linked in with `-ldflags "-X piperoom/internal/version.Version=<v>"` (`internal/version`), `"dev"` for a plain `go build`. `write_mode` (P0 item 1; server P1a) is `"off"` or `"on"`: whether fully approved proposals run (`PIPEROOM_WRITE_MODE`, default `off`; `--demo` forces `on`). Liveness only: it checks nothing else. `GET /readyz` is unchanged (`{status, checks}`, `503` when the store does not answer) |
| GET | `/metrics` | – | Prometheus text format (0.0.4): counters/histogram `piperoom_http_requests_total{method,route,code}`, `piperoom_http_request_duration_seconds{method,route,code}` (histogram); counter `piperoom_sole_member_grants_total` (P2b; driverless alert rooms whose first member this replica made the driver; no labels); counter `piperoom_approvals_total{grant}` (P2b; `grant` = `owner` \| `solo` \| `fallback` \| `policy`, all four series present from start; per replica); gauges `piperoom_ws_connections`, `piperoom_agent_slots_in_use`, `piperoom_agent_rooms_queued`, `piperoom_rooms_open`, `piperoom_goroutines`, `piperoom_uptime_seconds` (all per replica) | R9 (#142). Outside `/api`, **no auth**, skips the Host allow-list like `/healthz` so a scraper can reach pods by IP; a deployment that publishes the server port beyond the cluster restricts `/metrics` at its proxy or ingress. `route` is the chi route pattern (`/api/rooms/{id}`, or `unmatched`), never a concrete path; `code` is the class (`2xx`…`5xx`); no label holds a room or user id. Gauges come from in-memory state (no store call per scrape) |
| GET | `/api/people` | – | `[{id, name, email?, verified, layers}]` | F9 (implemented by A5): `layers` are the layers the person may approve (`LayersFor`; for IdP members from their session groups), `[]` when none. After auth PR 3: the people a driver may grant control or hand off to: `users.yaml` users plus members with a live session (`LookupPerson`). `email` only for IdP users; pickers show `id` or `email` next to `name`. `verified` is `true` for `users.yaml` users only. Members only: a viewer gets `403 viewer` (the list holds emails and viewers cannot grant or hand off anyway) |
| GET | `/api/me` | – | `{id,name,layers,verified}` | after auth PR 3 also `role` (`member`\|`viewer`) and `groups_missing` (the IdP sent no groups claim); `layers` is computed per request (for `PIPEROOM_SOLO_USER` it is every entry of `events.Layers`, eight since P0; every layer of the kinds owners.yaml uses, cdc when there are no pipelines (P4a-4)); `solo: true` when `PIPEROOM_SOLO_USER` is this user (omitted otherwise); `admin: true` when this user has `admin: true` in users.yaml (omitted otherwise); `verified` as for event actors |
| GET | `/api/rooms` | – | `[RoomSnapshot]` | open rooms first; with `tenants:` only the rooms of your tenants (see Tenants above). Rows carry `triage` (F6) and `insight` (F11) like `GET /api/rooms/{id}`; the same list is pushed by `GET /api/rooms/ws` (F6, see WebSocket) |
| POST | `/api/rooms` | `{title, severity, pipeline_id}` | `RoomSnapshot` | creator becomes driver; agent starts; unknown `pipeline_id` → `400 validation`. R4: `title` at most 200 characters (Unicode code points), else `400 validation` (the message gives the length and the limit, never the text). An alert's title is clipped to 200 with `…` instead, so an alert is never refused for it |
| POST | `/api/alerts/webhook` | any JSON | `RoomSnapshot` | generic alert; maps `title`/`severity`/`pipeline_id` if the body is an object; without a valid `pipeline_id` (the `alertin.ValidPipelineID` shape), a top-level string `dag_id` selects the one `kind: batch` pipeline whose `dags` lists it (exact, case-sensitive match; a DAG listed by two pipelines, or by none, is ignored and the body resolves as if it had no `dag_id`), the same rule as the signed routes (P4a-3, P4a-4); the raw body is stored verbatim in `room.created.alert`; unknown pipeline → room opens with a system message and proposals are disabled; driver = none until someone takes control. **Dedup:** the same alert (same `dedup_key`, else a byte-identical body) for the same pipeline while its room is open joins that room → `200` with that `RoomSnapshot` (a resolved repeat is noted in the room; it never closes the room); a new room → `201`. A **resolved** alert (`status: "resolved"`) that matches no open room — no `dedup_key` and no byte-identical open match, or its room is already closed — opens nothing: `202` with `{"ignored": true, "reason": "resolved alert with no open room"}` (nothing stored, no agent, no hook). Webhook auth: see `docs/design/webhook-auth.md` (C9) |
| POST | `/api/alerts/{source}` | the sender's own JSON (`source` = `alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic`) | `RoomSnapshot` | **No `X-User`**: the sender's signature or token is the auth (`internal/alertin/README.md`); a source with no secret configured answers `401`. Same dedup / resolved / room rules as `/api/alerts/webhook`, keyed on the source's own dedup key: new room `201`; repeat while its room is open `200` (join); resolved with no open room `202 {"ignored": true, "reason": "resolved alert with no open room"}`; a verified non-alert (PagerDuty ping, ack) `202 {"ignored": true, "reason": ...}`. Errors: bad / missing / stale signature or disabled source `401 unauthenticated` (checked before the body is read); body over 1 MB `413 too_large`; verified but unparseable `400 validation`; unknown `{source}` `404 not_found`. Title, severity and pipeline come from the alert, in this order: a `pipeline`/`pipeline_id` label or field; a `dag_id` label (Alertmanager, Grafana) or field (generic) → the one batch pipeline whose `dags` lists it (P4a-3; an unknown id is skipped, a DAG listed by two pipelines is not guessed); the `PAGERDUTY_SERVICES` map; an owners match; or `DEFAULT_ALERT_PIPELINE`. The dev `POST /api/alerts/webhook` maps `pipeline_id` only; the raw body is stored in `room.created.alert`. `room.created` also records `alert_source` (`webhook` or the source name), `dedup_key`, and `alert_verified: true` with `alert_verified_by` (see Event under Objects; R3 #135, A5 implements). With a `tenants:` block in `owners.yaml` the `200`/`201` body is `{"room_id": "r_…", "created": true\|false}` instead of the `RoomSnapshot`: a source secret is shared by every tenant, so the reply carries no room content. |
| GET | `/api/rooms/{id}` | – | `RoomSnapshot` | |
| GET | `/api/rooms/{id}/events?after=0&limit=500` | – | `{events:[Event], last_seq}` | limit max 1000; `events` is `[]` when nothing is after `after` |
| POST | `/api/rooms/{id}/comments` | `{step_id?, text, client_id?}` | `Event` | `@agent` in text sets `mentions_agent`. R4: `text` at most 4000 characters (code points), else `400 validation`. `client_id`: optional idempotency key, see Idempotent posts (`409 client_id_reused`) |
| POST | `/api/rooms/{id}/steer` | `{text, client_id?}` | `Event` | driver → `steer.sent`; others (also when no one drives or the lease expired) → `steer.suggested`, kept for the next driver (F5, implemented by A5; existing behaviour: a suggestion stays in `pending_suggestions` across `control.granted` and never wakes the agent; viewers `403 viewer`, closed rooms `409 room_closed`). R4: `text` at most 4000 characters (code points), else `400 validation`. `client_id`: optional idempotency key, see Idempotent posts (`409 client_id_reused`) |
| POST | `/api/rooms/{id}/suggestions/{sid}/send` | – | `Event` | driver only; emits `steer.sent` with `suggestion_id` and `from` (the suggester's actor; the event's actor is the driver). The agent reads a forwarded suggestion as the suggester's words, wrapped as untrusted data, not as a driver instruction |
| POST | `/api/rooms/{id}/suggestions/{sid}/decline` | `{reason?}` | `Event` (`steer.declined`) | R0; server R11. Driver only (`403 not_driver`); open rooms only (`409 room_closed`); unknown `sid`, or one of another room, `404 not_found`; a suggestion that is no longer pending (sent or declined) `409 already_decided`, the same answer as a second `send`, so a retry after a lost response is safe to treat as done. `reason` optional, at most 500 characters, typed-text rule above (else `400 validation`). The suggestion leaves `pending_suggestions`; the agent never sees it |
| POST | `/api/rooms/{id}/control/request` | – | `Event` | |
| POST | `/api/rooms/{id}/control/grant` | `{to}` | `Event` | driver only, or anyone if no driver / lease expired |
| POST | `/api/rooms/{id}/close` | `{resolution}` | `Event` | driver only; a shadow room refuses an empty resolution with `400 validation`. A full room log never refuses `room.closed` |
| POST | `/api/rooms/{id}/hypothesis` | `{verdict: "confirmed"\|"disputed", note?}` | `Event` (`hypothesis.verdict`) | F9 (server F9b, implemented by A5; web F9c, A6). Any member (viewers `403 viewer`); open rooms only (`409 room_closed`); a shadow room `409 room_open`; no hypothesis yet `409 no_hypothesis`; the same person repeating the same verdict on the same hypothesis `409 already_decided`; `note` ≤ 500 characters, typed-text rule. The event carries `hypothesis_seq`; a new `agent.hypothesis` clears every verdict. The agent receives it as a comment and wakes. Renews the driver lease only when the caller is the driver |
| POST | `/api/rooms/{id}/nudge` | `{proposal_id, layer}` | `Event` (`approval.nudged`) | F9 (implemented by A5). Any member (viewers `403 viewer`); open rooms (`409 room_closed`); the proposal must be open and `layer` pending on it (`409 already_decided`); one per proposal and layer per 10 minutes (`409 already_nudged`). Mirrored to the Slack thread when the bridge is on (C12 follow-up) |
| POST | `/api/rooms/{id}/label` | `{actual_layer?, cause, fix_that_worked?, fix_proposal_id?, agent_helped?}` | `Event` (`room.labelled`) | F8 (implemented by A5). Closed rooms only (`409 room_open`); caller = member of the room's pipeline (the `shadow/blind` rule), else `403 not_allowed`; viewers `403 viewer`; `actual_layer` one of the layers (`events.Layers`, nine since U0) or absent; `cause` 1–500 characters, `fix_that_worked` ≤ 500, typed-text rule; `fix_proposal_id` must be a proposal of this room with an executed step (`400 validation`); re-label allowed, last wins. A shadow room still needs `shadow/blind` first (`409 not_labelled`) so the blind label stays blind. The event carries `identity` like `shadow.labelled` |
| POST | `/api/rooms/{id}/checklist` | `{step_id, tool_use_id, hit, item, done, note?}` | `Event` (`checklist.step_done`) | F8 (implemented by A5). Open rooms only (`409 room_closed`); any member (viewers `403 viewer`); `tool_use_id` must be a `runbook_lookup` result of this room and `hit`/`item` within its `data` (`400 validation`); `note` ≤ 300 characters, typed-text rule; idempotent (the same state again still appends) |
| GET | `/api/rooms/{id}/postmortem.md` | – | `text/markdown` | F2 (server F2b, implemented by A5; web F2a, A6). The room rendered as a post-mortem draft: timeline, root cause, actions + approvers, verification, open items, risks, agent cost; see `docs/design/postmortem.md` for the section list. Same access as `GET /api/rooms/{id}/events` (viewers included; tenancy `404 not_found`). Open rooms answer the draft so far. `?download=1` adds `Content-Disposition: attachment`. Every string from the log is Markdown-escaped; clients show it as text, never as HTML. Own timeout like the export. Headers `Cache-Control: no-store`, `X-Content-Type-Options: nosniff` |
| POST | `/api/rooms/{id}/extend` | – | `Event` (`room.extended`) | driver only (`403 not_driver`); open rooms only (`409 room_closed`); once per room (`409 already_extended`); `400 validation` when `PIPEROOM_MAX_OPEN_DAYS=0`. Moves the auto-close deadline by `PIPEROOM_MAX_OPEN_DAYS` days and renews the driver lease. See Room limits |
| DELETE | `/api/rooms/{id}?reason=&archive=` | – (send `Content-Type: application/json` like every non-GET) | `{room_id, pipeline_id, deleted_by, deleted_at, reason, event_count, archived, archive_removed}` | admin only (users.yaml `admin: true`): any other member `403 not_allowed`, a viewer `403 viewer`, checked first, before the `archive` value and before the room is looked up. Then tenancy: a room whose pipeline the admin may not see (the live room's pipeline, or the pipeline in its audit row) answers `404 not_found` like a room that never existed. `reason` 1–500 characters, else `400 validation`. **A delete is an erase**: it hard-deletes the room (open or closed) with its events, proposals, approvals, steps, suggestions, hand-offs, Slack thread state and shadow rows, keeps no copy, and writes one content-free audit row (`room_deletions`). `archive=true` writes the room's export to `PIPEROOM_ARCHIVE_DIR` first (`400 validation` when the server has no archive directory, or for a value other than `true`/`false`; a failed write is `500 archive_failed` and nothing is deleted). `event_count` is the number of events deleted; `archived` is true when an export of the room is in the archive directory. An erase (no `archive=true`) also removes an export of the room left in `PIPEROOM_ARCHIVE_DIR` by an earlier failed attempt or retention run, after the database delete commits; `archive_removed` is then true. Idempotent: a room already deleted, by an admin or by retention, answers `200` with its audit row (`deleted_by: "system"` for retention) and changes nothing; an id that never existed is `404 not_found`. Open WebSockets of the room get a last `room.deleted` frame and close (see WebSocket) |
| POST | `/api/proposals/{pid}/approve` | `{layer}`; after auth PR 4 `{layer, params_hash}`, plus `confirm: "<pipeline_id>/<layer>"` in solo mode | `Event` | caller must own `layer`; when all layers approved the server runs the steps in order and emits `action.executed` per step (`{proposal_id, step_index, ok, output, params_hash, outcome_unknown?}`; `outcome_unknown: true` with `ok: false` when the server running the step stopped before it recorded the result: the step is never run again, check the target before retrying). After auth PR 4: missing `params_hash` → `400 validation`, different from the stored one → `409 params_mismatch`; `approval.given` carries `params_hash` and `identity` (`grant: "solo"` in solo mode); `identity.email` is the session's verified IdP email, else the `users.yaml` `email:` (omitted when neither exists), so a decision is tied to the actor id and a mailbox, never to the name alone. `approval.rejected`, `shadow.labelled` and `shadow.graded` carry the same `identity.email`. P2 (server P2b): `approval.given` also carries `grant` (`owner`, `solo`, `fallback`; see P-wave events); the room's driver may approve a layer in the proposal's `fallback_layers` (`grant: "fallback"`), checked when the event is appended. P1 (server P1a): with write mode off the last approval records as usual and the steps do not run (snapshot `open_proposals[].blocked: "write_mode_off"`, no `action.executed`). Before every step the executor checks again (#638; `docs/design/replicas.md` §5). First, in replica mode only (with a coordinator; without one the refresh always succeeds), it refreshes its claim, which holds only while the proposal is open: if the refresh fails (the proposal is no longer open, or the claim was declared dead) the step is skipped with no `action.executed`, and the stale-claim sweep records the outcome. Then `recheckStep` re-reads the proposal and its approvals; the step runs only when the recomputed hash equals the stored one and every `approved` row's `params_hash`, and every `required_layers` entry has an `approved` row (any grant). Otherwise the step does not run and `action.executed` has `ok: false` with one of these outputs: `params hash mismatch`, `params hash mismatch (cannot recompute: …)`, `not approved by every required layer`, `cannot re-read the proposal before running the step: …`, `cannot read the approvals before running the step: …` |
| POST | `/api/proposals/{pid}/reject` | `{layer, reason}`; after auth PR 4 `{layer, reason, params_hash}` | `Event` | same hash rule as approve; the caller must be allowed the layer as for `approve`, so the room's driver may reject a `fallback_layers` entry (P2b-3b), checked when the event is appended |
| POST | `/api/proposals/{pid}/approve-all` | `{params_hash, confirm?}` | `[Event]` (the `approval.given` events, in `required_layers` order) | P0 item 4; server P2b. The caller must be allowed **every** `required_layers` entry (owner or backup, `PIPEROOM_SOLO_USER`, or the driver for a `fallback_layers` entry), else `403 not_layer_owner` naming the first layer the caller lacks, and nothing is recorded. Same `params_hash` rule as `approve` (`409 params_mismatch`); in solo mode `confirm: "<pipeline_id>/*"` once per call (`400 validation` otherwise). Then one `approval.given` per `required_layers` entry not yet approved, in that order, each with its `grant`, under one lock, and the steps run as after the last `approve`. Stops a pending policy countdown without a `proposal.cancelled`. Open rooms only (`409 room_closed`); a proposal already decided (rejected, or every layer approved) `409 already_decided` |
| POST | `/api/proposals/{pid}/cancel` | `{reason?}` | `Event` (`proposal.cancelled`) | P0 item 4; server P2b. The driver or an owner of any `required_layers` entry (`403 not_layer_owner` otherwise; viewers `403 viewer`); only while a `proposal.scheduled` of the proposal is pending (`409 not_scheduled` otherwise, also after the policy ran). Cancellable until the policy starts (its first approval is logged); a cancel after `cancel_until` still wins if it lands first. `reason` ≤ 500 characters, typed-text rule. The proposal stays open for manual approval |
| POST | `/api/rooms/{id}/proposals` | `{title, risk, rollback, steps: [{action, layer, params, description?}]}` | `Event` (`proposal.created`, `origin: "driver"`) | P0 item 5; server P3c. A driver-authored fix: driver only (`403 not_driver`), open rooms only (`409 room_closed`), never in a shadow room (`400 validation`: a shadow room has no proposals). Validated exactly like an agent proposal: a registered action, `layer` equal to the action's `Layer()`, `Validate`, `CheckScope` against the room's pipeline, `params_hash`, then the preview (`proposal.previewed`); a refused step answers `400 validation` naming the step index. At most 5 open driver-authored proposals per room (`429 too_many_proposals`). The agent hears of it (`HumanInput` kind `proposal_created`) and verifies after it runs (`proposal_executed`). The auto-approve policy never applies to it. There is no free-form command: only registered actions with their params |
| GET | `/api/actions` | – | `[{name, layer, describe, params_schema, preview}]` | P0 item 5; server P3c, web P3d. The write actions the agent proposes from (the full registry, also when write mode is off), in registration order, for any logged-in member (viewers included). `describe` is the action's `Describe()`, `params_schema` its `ParamsSchema()` (a JSON Schema object), `preview` whether it implements `actions.Preview`. Never includes client state, connection settings or credentials. The P3d form is generated from `params_schema` |
| GET | `/api/rooms/{id}/export` | – | the room as one JSON document | header `X-Piperoom-Export-SHA256` = SHA-256 of the RFC 8785 (JCS) form without the top-level `integrity` member. Today any user may export. After auth PR 4: only an approver or backup for any layer of the room's pipeline (in dev mode: `users.yaml` `layers`) or an `auditors:` principal; the driver is not on the list; others → `403 not_allowed`. Proposals, approvals and action results then carry `params_hash`, and approvals also `identity`. `?include=shadow` (closed rooms only, else `409 room_open`; `409 not_labelled` before the blind label) adds a top-level `"shadow": {run, events}` member (with `data`) after `"schema"`; it is inside the hashed document, so the hash differs from the plain export. Same access rule |
| POST | `/api/rooms/{id}/handoffs` | `{to, stay_as_watcher}` | `Event` | driver only; server asks the agent for the summary (can take up to 3 min; this route is exempt from the 60 s request timeout). R0 (server R4): after its checks the server appends `handoff.requested` (see Hand-off requests below) and answers with the final `handoff.created`. While a request of the room is in flight, every further POST answers `409 handoff_in_progress`, from the driver or anyone else, before any other check but visibility. A request that ends without a hand-off appends `handoff.failed` and answers the matching error (`403 not_driver`, `409 room_closed`, else `500 internal_error`) |
| POST | `/api/handoffs/{hid}/accept` | – | `Event` | only the `to` user; moves driver role |
| GET | `/api/pipelines` | – | `[{id, title, shadow, kind}]` | R0; server R4, web R3. The pipelines this server knows: every pipeline of `owners.yaml`, in file order (with a `tenants:` block, only those of the caller's tenants; the solo user sees all). `title` is the pipeline's `title:` (`""` when unset); `shadow` is `true` when its `shadow:` block is enabled (rooms from alerts open as shadow rooms). `kind` (P0 item 8; server P4a-4 (A5)) is the pipeline's `kind:`, `"cdc"` or `"batch"` (`"cdc"` when the file names none). Any logged-in user, viewers included. No other field (connections, owners, watch settings) is returned. `[]` when there are none: clients must not fall back to a made-up pipeline id such as `orders`; the new-room form then has no pipeline to offer |
| GET | `/api/pipelines/{pid}/topology` | – | `Topology` | unknown pipeline → `404 not_found` |
| GET | `/api/pipelines/{pid}/agent-stats?days=90` | – | `AgentStats` | F11 (server F11a, implemented by A5; web F11b, A6). The agent's track record on one pipeline over the last `days` (1–365, default 90, else `400 validation`) of **closed** rooms: counts, graded hit rate, reliability by confidence (`by_confidence[]` with the field names of `evals.ReliabilityBucket`), medians of time to first hypothesis, human MTTR and cost, plus the shadow subset. Visibility first (`404 not_found` for an unknown pipeline or one outside the caller's tenants), then any logged-in user, viewers included. `enough_data` is false under 10 graded rooms: clients show counts, never a rate. Cached 2 minutes per pipeline. Sources: `shadow.labelled/graded` and F8's `room.labelled`; before F8 only shadow rooms are graded |
| POST | `/api/rooms/{id}/trace` | `{table, key}` | `Event` | runs a record trace; emits `trace.pinned`; table not in the pipeline or empty key → `400 validation` |
| POST | `/api/rooms/{id}/shadow/blind` | `{actual_layer, root_cause_terms}` | `Event` (`shadow.labelled`) | closed shadow rooms only; once per room (`409 already_decided`); caller must be a member of the room's pipeline: the solo user, or an approver or backup for any layer of that pipeline in `owners.yaml` (in dev mode also a `users.yaml` user whose `layers:` grant a layer the pipeline's `owners:` block does not replace), the same rule as the export minus auditors; else `403 not_allowed`; viewers `403 viewer`; `actual_layer` one of the layers (`events.Layers`), `root_cause_terms` 1–20 strings of 1–200 chars → else `400 validation`; the event carries `identity` with the qualifying `grant` |
| GET | `/api/rooms/{id}/shadow?include=data,room` | – | `{run: ShadowRun, events: [Event + "shadow": true]}` | closed shadow rooms only; `409 not_labelled` until the blind label; any logged-in user, viewers included (`include=data` too, as with `tool_result.data` on `GET /events`); `events` have their own `seq` and are never fed to the room reducer; `tool_result.data` is omitted unless `include=data`; `include=room` adds `room: {created, labelled, graded}` for the eval builder. `ShadowRun` = `{room_id, status, model, pipeline_map, started_at, first_hypothesis_at, final_hypothesis_at, finished_at, finished_after_close, final_hypothesis, tool_calls, tokens, revealed_at}` (null times when unset; `status` ∈ `queued\|running\|hypothesised\|no_hypothesis\|error\|budget\|interrupted`) |
| POST | `/api/rooms/{id}/shadow/grade` | `{layer_correct, root_cause, notes?}` | `Event` (`shadow.graded`) | closed, labelled and revealed shadow rooms (before `shadow.revealed` → `409 not_revealed`); same caller rule as `blind`; `root_cause` ∈ `yes\|partial\|no`, `notes` ≤ 2000 chars; re-grade allowed, last wins |
| GET | `/api/rooms/{id}/usage` | – | `{room_id, pipeline_id, charged: {tokens, cost_usd}, limit: {tokens}, totals: Usage, by_model: [Usage + model]}` | model spend of the room (design `docs/design/cost.md`). `charged` is what the room budget checks: finished calls plus the estimate of a call in flight, so it can run ahead of `totals`; `limit.tokens` is `PIPEROOM_ROOM_TOKEN_BUDGET` (0 = no cap; a shadow run's own cap is not shown). Any logged-in user who may see and read the room, viewers included (the same numbers are in its `agent.usage` events). A room the caller cannot see (tenancy, `docs/design/tenancy.md`) answers `404 not_found` exactly like a missing room; a shadow room answers `409 room_open` while it is open. `by_model` is sorted by model id |
| GET | `/api/usage?pipeline=&day=` | – | `{pipeline_id, day, charged: {tokens, cost_usd}, limit: {tokens, cost_usd}, totals: Usage, by_model: [Usage + model]}` | model spend of one pipeline on one UTC day, shadow runs included. `pipeline` required (at most 200 bytes, valid UTF-8, no NUL); `day` is `YYYY-MM-DD` (UTC, default today) between `2000-01-01` and `2999-12-31`; else `400 validation`. Visibility is checked first: a pipeline `owners.yaml` does not define, or one outside the caller's tenants, answers `404 not_found` ("pipeline not found") before any role check. Then only an approver or backup for any layer of that pipeline (in dev mode also a `users.yaml` user whose `layers:` grant one), the solo user, or an `auditors:` principal (the export rule of `docs/design/auth.md` §3.4); others `403 not_allowed`. Auditors see only their tenants' pipelines. Rooms with no pipeline id, or a pipeline `owners.yaml` does not know, are counted in buckets this route cannot read; their room route still shows them. `limit` holds `PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET` and `PIPEROOM_PIPELINE_DAILY_COST_USD` (0 = off). A day with no spend answers zeros |
| GET | `/api/shadow/metrics?pipeline=&since=` | – | `{rooms, labelled, graded, layer_correct, root_cause: {yes, partial, no}, by_status: {…}, median_time_to_first_s, median_time_to_final_s, median_human_mttr_s, total_tokens, tokens_available, per_room: [...]}` | over **every closed shadow room** matching the filters (errors included); `since` RFC 3339 on `closed_at`; medians `-1` when no room has the value; `tokens_available` false when every run reports 0 tokens (demo, plain runner); `per_room` rows: `{room_id, pipeline_id, alert_source, status, opened_at, closed_at, labelled, graded, layer_correct, root_cause, time_to_first_s, time_to_final_s, human_mttr_s, tool_calls, tokens, finished_after_close}`; "N of M matched" = `layer_correct` of `rooms` |

Driver lease: 30 minutes, renewed by any driver action. So that clients folding the log see the same lease as
the server, a renewal that happens when less than 25 minutes remain is written to the log as `control.granted`
with `to` = the current driver and the new `lease_until` (at most one such event per 5 minutes). A driver whose
lease expired and who was not replaced is still treated as the driver: their next action renews the lease.

Lease expiry (R0; server R11, Slack paging C12 in R11, web R3): when the lease has passed with no renewal and no new
driver, the system actor appends `control.lease_expired {driver, lease_until}` once for that lease (`lease_until`
equals the `control.granted` that set it; with several replicas only the replica holding the room writes it). It is
the room's "nobody is driving" signal and moves nothing: the driver stays `snapshot.driver`, their next action
renews the lease with a `control.granted` to themselves (which ends the signal), and anyone may take control with
`control/grant`. Not written for a closed room or a room with no driver. A client shows the signal from the last
`control.lease_expired` whose `lease_until` equals the driver's current `lease_until`.

Paging on lease expiry (C12 #130): `PIPEROOM_SLACK_PAGE_NO_DRIVER` is a comma-separated list of the on-call Slack
member IDs (each `^[UW][A-Z0-9]{2,}$`, for example `U012ABC,W0XYZ99`) the Slack bridge pages when
`control.lease_expired` is written; empty (the default) means no paging on lease expiry, and a value that is not a
Slack member ID stops the server at start (`config.ParseSlackMemberIDs`). Read by `config.Load()` into
`Config.SlackPageNoDriver`; consumed by `cmd/server` when building `slackbridge.PagingOptions` (`NoDriver`; A5
follow-up), C12 #130.

Proposals: a step's action must implement `actions.Scoped`; the server calls `Validate` and then
`CheckScope(room's pipeline, params)` when the proposal is created and again right before `Execute`. Params are
frozen at creation; approvals bind to those exact steps.

Write mode (P0 item 1; server P1a): `PIPEROOM_WRITE_MODE` is `off` by default (`config.ParseWriteMode`; any value
but `off`/`on` stops the start). Off: the agent still proposes from the full action registry, proposals are
created, previewed, approved and recorded as usual, but the executor refuses to queue or run a step, so no step runs and no
`action.executed` is written (snapshot `open_proposals[].blocked: "write_mode_off"`, `mode.write: false`,
`/healthz` `write_mode: "off"`). On: today's behaviour. `--demo` forces on (fake clients only).
`deploy/pilot.env.example`, `deploy/docker-compose.pilot.yml`, the Helm values and `dev/` set `on` explicitly, so
installs from before the P wave keep running fixes.

### Hand-off requests (R0; server R4, web R3)

`handoff.requested {request_id, to, stay_as_watcher, expires_at}` (actor: the driver) is appended when
`POST /api/rooms/{id}/handoffs` passes its checks, before the summary is written. The request is **in flight** until
the first of: `handoff.created` with the same `request_id` (the normal end), `handoff.failed {request_id, reason}`
(system actor; `reason` ∈ `summary_failed | not_driver | room_closed | internal_error`, never error text), or
`expires_at` (request time + 3 min, the summary deadline; covers a server that stopped mid-request). The server and
clients use this same rule. `handoff.created` gains `request_id` (absent on events from before R0).

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
- Modes: `PIPEROOM_WATCH_MODE` = `off`, `log` (default: log transitions, open nothing) or `rooms`. F6 plans `rooms` as
  the default; until F6d lands the default stays `log`. The watcher keeps at most `PIPEROOM_WATCH_MAX_OPEN_ROOMS`
  rooms open at once (default 6; F6, implemented by A7), independent of `PIPEROOM_MAX_AGENTS`.
- **Alert updates (F6, implemented by A5 and A4).** A join carries the evidence JSON; the service feeds it, and a
  re-fire of the same alert on any route, to the agent as `HumanInput{kind: "alert_update"}`: the server's rendering
  of the evidence (`agent.AlertEvidenceText`), wrapped as untrusted data. It never wakes an idle agent and never
  resets the turn guard; at most one pending `alert_update` per room (a newer one replaces it). It is not an event.
- **Triage pass (F6, implemented by A4 and A5).** Before an agent slot is taken, a bounded first look
  (`PIPEROOM_TRIAGE_PASS`, model `PIPEROOM_TRIAGE_PASS_MODEL`, deadline `PIPEROOM_TRIAGE_PASS_TIMEOUT`) writes its
  own steps and one `agent.triage`; not in shadow rooms or rooms restored after a restart.

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

### Room limits (retention, caps, deletion) — design `docs/design/retention.md`

- **Open-room limit.** `PIPEROOM_MAX_OPEN_DAYS` (default `14`, `0` = off). 24 h before a room's deadline
  (`snapshot.auto_close_at`) the system actor appends `room.retention_warning {close_at, open_days}`, once per
  deadline. At `close_at` the system actor closes the room with the resolution
  `retention: open longer than N days` (a normal `room.closed`; the closed-room retention below applies from then on).
  No room is closed less than 24 h after its warning: a room already past its deadline when the limit first sees it
  (the limit was just turned on or lowered) is warned with `close_at` 24 h later.
  The driver may extend once: `POST /api/rooms/{id}/extend` appends `room.extended {days, close_at}`; the new
  deadline is the shown `auto_close_at` plus `PIPEROOM_MAX_OPEN_DAYS` days (a new warning comes 24 h before it).
- **Closed-room retention.** `PIPEROOM_RETENTION_DAYS` (empty or `0` = keep forever) deletes closed rooms whose
  `closed_at` is older. A pipeline's `retention_days:` in `owners.yaml` (1–36500) sets that pipeline's own value;
  when `PIPEROOM_RETENTION_DAYS` is set too, the **shorter** wins. A bad value stops the server.
- **Per-room log caps.** `PIPEROOM_ROOM_MAX_EVENTS` (default `20000`) and `PIPEROOM_ROOM_MAX_BYTES` (default
  `256MiB`, the sum of the stored payload JSON sizes). At 90% of either cap the system actor appends one
  `room.log_cap_warning {events, max_events, bytes, max_bytes}`. At 100% events of the agent actor (status, steps,
  tool calls and results, messages, hypotheses) are refused, the agent stops and the system actor writes
  `agent.status {state: "log_full"}`; a steer or `@agent` comment no longer starts it in that room. Every other
  event (human comments, steers, suggestions, control, approvals and rejections, action results,
  hand-offs, traces, system notes) may go up to 2% over the caps; past that the route answers `409 log_full`.
  `room.closed` is always accepted, so a full room can still be closed. Rooms created before the upgrade that
  added the caps get their byte count in the background after the server starts; until a room is counted only
  its event cap applies (`docs/design/retention.md` section 5.2).
- **Data size.** `PIPEROOM_EVENT_MAX_DATA_BYTES` (default `64KiB`). A larger `agent.tool_result.data` is stored as
  `{"truncated": true, "original_bytes": N, "preview": "<start of the JSON text>"}` and the payload's `truncated`
  is true; a larger `action.executed.output` is cut and ends with `… [truncated: N bytes]`.
- **Deletion.** `DELETE /api/rooms/{id}?reason=` (admin, see REST endpoints) erases a room; add `archive=true` to keep
  a copy first. `devctl rooms delete <id> --reason <text> --as <admin> [--archive]` calls this route.
- **Audit.** Every deleted room leaves one content-free `room_deletions` row (room id, pipeline, deleted_by,
  deleted_at, reason, event_count): an admin delete with the admin and their reason, a retention purge with
  `deleted_by: "system"` and `reason: "retention: closed N days"`, in the purge's transaction.
- **Archive.** With `PIPEROOM_ARCHIVE_DIR` (an absolute directory) set, retention (always) and an admin delete with
  `archive=true` first write the
  room's export (the `GET /api/rooms/{id}/export` document, with its `integrity` hash) to
  `<dir>/<pipeline_id>/<room_id>.json`, mode `0600`. If that write fails the room is not deleted: retention logs
  the error and tries again on its next run; `DELETE …&archive=true` answers `500 archive_failed`.

### Idempotent posts (#459; server A5, web A6)

`POST /api/rooms/{id}/comments` and `POST /api/rooms/{id}/steer` take an optional `client_id`: an idempotency key
the client generates once per user action (a UUID v4 is recommended) and sends again, unchanged, on every retry of
that action. It is 1–64 characters of `[A-Za-z0-9_-]`, compared exactly (case-sensitive), else `400 validation`. It
must not be derived from user data: it is visible to everyone who can read the room (event payload, WebSocket,
export). Another user's request with the same value is a different key.

- **Key.** `(room, the caller's user id, client_id)`, across both routes, for the life of the room. The server enforces
  it in the store, so it holds with several replicas and for two requests racing each other: exactly one event is
  appended.
- **Order.** Authentication, viewer, tenancy; then the JSON body and the `client_id` format (`400 validation`); then
  the repeat check; then the route's own checks (text required, typed-text rule, 4000 characters, `room_closed`,
  `log_full`, driver). So a used key answers `200` or `409 client_id_reused` even when its new body would fail
  validation, and the retry of an action that landed still answers `200` after the room closed, the log filled or the
  driver changed (a steer that landed as `steer.sent` stays that event).
- **Repeat.** A request whose key was already used, with the same route and the same body, appends nothing, wakes
  nothing, renews no lease and answers `200` (the same status as the first answer) with the original `Event` as
  stored: same `room_id`, `seq`, `type` and payload values; compare by `seq`, not by bytes. Same body means `text`
  equal byte for byte (no trimming or normalisation) and the same `step_id` (absent and `""` are the same).
- **Reuse.** The same key with another route or another body answers `409 client_id_reused` and appends nothing.
- **Race.** The request that loses the race answers as a repeat: `200` with the winner's event, or
  `409 client_id_reused` if its body differs. A refused request (any error, nothing appended) uses no key; its retry
  is checked as a new request.
- **Payload.** The event carries the key: `comment.added`, `steer.suggested` and `steer.sent` have `client_id`
  (omitted when the request had none; a `steer.sent` that forwards a suggestion has none, because the forward is the
  driver's action, not the suggester's request). Clients reconcile their pending copies by `(actor.id, client_id)`, the event's actor being the caller, never by `client_id` alone (every room reader sees it, so another user can send the same value) and never by matching text.
- **Without a key** nothing changes: every request appends, so a client that cannot tell whether a request landed
  must not re-send it without a key. A server without this feature ignores the field, so a client retries
  automatically only when the answer's event carries `client_id`.

### Admin: settings and connection tests (onboarding phase 1, design `docs/design/onboarding.md`)

Read-only views of what the server runs with, and live tests of its connections. **Admin only**
(`admin: true` in users.yaml): any other member gets `403 not_allowed`, a viewer `403 viewer`, checked before
anything else. Phase 1 changes no configuration: the files and environment stay the source of truth, and nothing
here writes them. **No response ever contains a secret value**: DSNs, passwords, user names and tokens are shown as
`set`/`unset` with where they come from; URLs and brokers are shown without user info, query or fragment.

| Method | Route | Body | Response | Notes |
|---|---|---|---|---|
| GET | `/api/admin/config` | – | `AdminConfig` | what the server runs with now |
| POST | `/api/admin/connections/test` | `{"id": "<connection id>"}` | `ProbeResult` | `404 not_found` for an unknown id; `429 rate_limited` after 10 tests per minute per admin; the test itself never answers 5xx for a failed check, a failed check is `ok: false` |
| GET | `/api/admin/config/export` | – | `text/yaml` attachment `piperoom-config.yaml` | the owners file, then `---`, then the users file, as read at start (they hold no secrets by design: connection settings are `env:`/`file:` references) |

`AdminConfig`:

```json
{
  "source": "file",
  "owners_file": "owners.yaml",
  "users_file": "users.yaml",
  "connections": [
    {
      "id": "process-wide.kafka",
      "scope": "process-wide",
      "pipeline_id": "",
      "kind": "kafka",
      "used_by": ["orders"],
      "settings": [
        {"key": "kafka_brokers", "display": "localhost:29092", "secret": false, "from": "env KAFKA_BROKERS"},
        {"key": "kafka_sasl_password", "display": "", "secret": true, "set": true, "from": "env KAFKA_SASL_PASSWORD"}
      ]
    }
  ],
  "pipelines": [
    {
      "id": "orders", "title": "Orders CDC", "tenant": "default",
      "connections": "process-wide",
      "connectors": ["pg-orders-connector"], "consumer_groups": ["order-sync"],
      "source_tables": ["public.orders"], "sink_tables": ["analytics.orders"],
      "owners": [{"layer": "postgres", "principals": ["user:priya"]}],
      "shadow": false
    }
  ],
  "people": [{"id": "priya", "name": "Priya", "layers": ["postgres", "schema-registry"], "admin": false}]
}
```

- `source` is `file` in phase 1. `owners_file` / `users_file` are base names only.
- **Connections.** One per kind (`source_db`, `kafka`, `connect`, `schema_registry`, `warehouse`) for the
  process-wide settings (`id` `process-wide.<kind>`, `scope` `process-wide`) and one per kind for each pipeline
  with a `connections:` block (`id` `pipeline.<pipeline id>.<kind>`, `scope` `pipeline`, `pipeline_id` set). A kind
  whose required setting (the DSN, brokers or URL) is empty is left out. `used_by` lists the pipelines whose client
  set the connection serves, in owners order.
- **Settings.** One row per `owners.ConnectionKeys()` key of the kind that is set or required (so a process-wide
  secret row always has `set: true`; `set: false` appears for a pipeline reference whose variable or file is
  missing). `secret` is true for
  every key that may hold a credential (DSNs, user names, passwords); then `display` is `""` and `set` says whether
  a value is present. For URLs and `kafka_brokers`, `display` is scheme, host, port and path only. For literal keys
  (`owners.IsLiteralKey`) and TLS file paths, `display` is the value. `from` is `env NAME`, `file /path` or `value`
  for a pipeline reference, and `env NAME` for a process-wide setting.
- **Pipelines and people** mirror owners.yaml and users.yaml; `owners` lists each layer's principals as written
  (`user:<id>`, `group:<name>`); `connections` is `own` or `process-wide`.

`ProbeResult` (`internal/connprobe.Result`):

```json
{"kind": "source_db", "ok": false, "took_ms": 212, "checks": [
  {"id": "reach", "status": "ok", "ms": 4},
  {"id": "login", "status": "ok", "ms": 31},
  {"id": "read_only", "status": "ok", "ms": 2, "detail": "default_transaction_read_only = on"},
  {"id": "read_slots", "status": "fail", "ms": 3, "problem": "role piperoom_ro cannot read pg_replication_slots",
   "fix": "GRANT SELECT ON pg_catalog.pg_replication_slots TO piperoom_ro;"}]}
```

Check ids are stable per kind. Every kind starts with `config` (the settings are present and parse; a pipeline
reference that cannot be resolved is a failed `config` check naming the key and its reference, never a value), then
`reach` and `login`; later checks are `skip` when one of those three fails. Kind-specific ids: `source_db`
`read_only`, `statement_timeout`, `read_slots`, `read_publications`, `select_tables`, `can_write`; `warehouse`
`read_only` (Postgres only), `select_sink`; `kafka`, `connect` and `schema_registry` as their probes document.

`status` is `ok`, `fail`, `warn` (works, but should change, e.g. the login can write) or `skip` (not run because an
earlier check failed). `ok` is false when any check failed. `problem`, `fix` and `detail` are fixed text with object
names only, never a server's raw error. Every dial goes through an address guard that refuses link-local and cloud
metadata addresses; phase 1 allows loopback and private addresses because it tests only operator-configured
connections. A test is logged (`admin: connection test`, actor, id, ok), never with settings.

### Setup rooms (U0; server A5, runner A9, web A6; design `docs/design/setup-agent.md`)

Every route below exists only while `PIPEROOM_SETUP_ENABLED=on` (which needs `PIPEROOM_WRITE_MODE=on`; the server
refuses to start otherwise); with setup off they answer `404 setup_disabled` and `/api/runners/ws` is not served.
Every `POST` under `/api/rooms/{id}/setup/` is **driver only** (`403 not_driver`; viewers `403 viewer`), open setup
rooms only (`409 room_closed`; an incident room `400 not_setup_room`), under the same CSRF rule as every non-GET, and
renews the driver lease. The model never calls them: it may ask for a click in a message.

| Method | Path | Body | Result | Notes |
|---|---|---|---|---|
| POST | `/api/setup` | `SetupRequest` (`events.SetupRequest`: `{pipeline_id, tenant?, title, source_kind, target, topic_prefix, tables, connectors?, targets: [{kind, address, class?}], repos?}`) | `201 {room: RoomSnapshot, code, expires_at, command}` | opens a setup room (`room.created {mode: "setup"}`, the creator is driver, `control.granted`) and returns a single-use pairing code: 8 characters of `protocol.CodeAlphabet`, valid 10 min, bound to `(tenant, room, creator)`; `command` is the `piperoom-setup --server <origin> --code <code>` line to paste. With a `tenants:` block `tenant` is required and the caller must be a member (`403 not_tenant_member`); without one it must be absent. `pipeline_id` must match the owners id rule and must not exist (`409 pipeline_exists`); `targets[].address` goes through the denylist of design §2.6 (`400 address_refused`); classes are floors and never below `shared` once registered. Any member, viewers `403 viewer`. The code is never logged and never in an event (`setup.pairing` carries the outcome only) |
| GET | `/api/runners/ws` | first frame `{type: "pair", pair: {code}}` or `{type: "pair", pair: {token}}` | WebSocket (`internal/setup/protocol` frames) | the runner's socket; see **Runner socket** under WebSocket. No session cookie: the code or token is the credential. TLS required unless loopback. Rate-limited (5 attempts per code, 20 per source address, 100 in all per 10 min, then `429 pairing_closed` for 10 min, audited); constant-time compare; one runner per room (`409 runner_paired`; a lost runner may be replaced, `setup.pairing {outcome: "replaced"}`) |
| POST | `/api/rooms/{id}/setup/targets` | `{kind, address, class?}` | `Event` (`setup.target_registered`) | adds a target the ops may reach. The server resolves `address` and refuses loopback of another host, link-local, cloud metadata endpoints, multicast, unspecified and its own addresses (`400 address_refused`); `class` is a floor combined with `max` against the derived class and never below `shared` (`400 class_lowered`); the runner re-checks the resolved IP at dial time. Also emits `setup.target_classed` |
| POST | `/api/rooms/{id}/setup/targets/{t}/class` | `{class}` | `Event` (`setup.target_classed {by: "person"}`) | raise only: `local` → `shared` → `production`; a lower value `400 class_lowered`; `{t}` is the target's address as registered (`404 not_found` otherwise) |
| POST | `/api/rooms/{id}/setup/pipeline` | `{add_connectors?, add_tables?, add_consumer_groups?, add_sink_tables?}` | `Event` (`setup.pipeline_extended`) | extends the provisional pipeline; every name follows the owners.yaml rules for that list (`400 validation` names the entry); the room re-derives the tool and action scope. Hosts are targets, not here |
| POST | `/api/rooms/{id}/setup/secret` | `{name, value}` | `204` | `name` matches `^[a-zA-Z_][a-zA-Z0-9_]*$`, `value` ≥ 6 bytes and ≤ 4 KiB (`400 validation`); forwarded to the paired runner in one `secret` frame (`409 no_runner` when none) and **stored nowhere on the server**; the route is excluded from request logging (a test asserts no log line carries the body); emits `setup.secret_set {name, actor}`. The value is in no event, log or prompt |
| POST | `/api/rooms/{id}/setup/credentials` | – (no body) | `202` | asks the paired runner (`register_credentials {request_id, pipeline_id, names}` frame; `409 no_runner`) to register the read credentials it generated (design §3.7). The values travel in the runner's `credentials {request_id, secrets}` frame after the person's `y` on the runner's terminal, never in this request. The server accepts that frame only while this request is pending (one per room; `409 already_decided` while one is), once, with exactly the requested names, else `setup.runner_refused {reason: "credentials"}`; the pipeline id is the room's, never the runner's; names and the pipeline id are validated before any path is built; each value is written through an `os.Root` at `PIPEROOM_SECRETS_DIR` (no symlink followed) as `<pipeline_id>/<name>`, 0600, temp file then rename; then `setup.credentials_registered {pipeline_id, names, runner_label}`. A refusal on the terminal is `setup.runner_refused {reason: "credentials"}` |
| POST | `/api/rooms/{id}/setup/maintenance-window` | `{start, end}` | `Event` (`setup.maintenance_window`) | RFC 3339 times; `end − start` ≤ 4 h and `start` ≤ 24 h ahead, `end` in the future (`400 window_too_long`, `400 validation`). A T3 step whose guard row needs a window is created and executed only inside the latest window; a later window replaces it |
| POST | `/api/rooms/{id}/setup/verify` | – | `202` | asks the server's ladder runner to run the verification ladder now (the person's equivalent of the agent's `setup_request_verification`); `409 no_runner` when no runner is paired; `409 already_decided` while a ladder run is in flight. Its outcome is `setup.verified`, written by the ladder runner only |
| GET | `/api/pipelines` | – | rows gain `setup: {room_id, target, registered_at, watched_by}` | present for a pipeline a setup room registered (the owners.yaml `setup:` block); absent otherwise |

## WebSocket

`GET /api/rooms/{id}/ws?after=<seq>`

The WebSocket uses the session cookie (dev: or `X-User` for non-browser clients); no credentials → `401`.
R4: at most 200 live connections per room, 20 per user and 5000 in all (room and room-list sockets together), per
server process (with several replicas the caps apply on each). Over any cap the request answers
`429 too_many_connections` before the upgrade, with a message naming which cap was hit; a closed connection frees its
slot.

Server → client messages:
```json
{ "type": "event", "event": { ...Event } }
{ "type": "presence", "members": [ { "id": "priya", "name": "Priya", "verified": true, "state": "viewing", "since": "2026-10-11T10:52:00Z" } ] }
{ "type": "error", "error": { "code": "...", "message": "..." } }
```
Client → server:
```json
{ "type": "presence", "state": "viewing" }    // send every 20 s; "away" when tab hidden
```
The server marks a member `away` after 45 s without any client message (2× the heartbeat plus slack).
`since` (F9, implemented by A5) is when the member entered its current state on the server (connect time for a new
member; with several connections, the earliest among those in the winning state), RFC 3339 UTC; clients show
`away 12m` from it, never from a client-measured time.
Rules: the server first replays every event with `seq > after`, then streams live events. Events are never skipped or repeated. **On first open of a room the client connects with `after=0`** (full replay); on reconnect it passes its highest seen seq. Presence is not stored in the event log.

Stale position (#287): `after` greater than the last seq of the room's stored event log would start the stream past
the log head and skip the events in between, so the server refuses it. The server checks the store, never a cached
room snapshot, which can trail an append in flight or another replica. It accepts the upgrade (a browser cannot read the status of a
refused upgrade), sends one frame `{ "type": "error", "error": { "code": "stale_position", "message": "after N is past
the room's last event M" } }` and closes with status 1008 and close reason `stale_position`. Nothing is replayed. The
client drops its room state and reconnects with `after=0` (full replay), once, silently; the error frame is shown only
on a second `stale_position` in a row (which `after=0` cannot cause, so it means a client bug), not retried. Clients
tell this close apart by its reason, like `session_expired` and `room deleted`: the other 1008 closes (`replay
timeout`, `read timeout`, `send buffer full`) keep reconnecting with the highest seen seq. `after` that is not a
non-negative integer still answers `400 bad_request` before the upgrade.

Room deleted: when an admin deletes the room, each open connection gets one last frame
`{ "type": "event", "event": { "room_id": "r_…", "seq": <last_seq + 1>, "type": "room.deleted", "actor": <the admin>, "payload": { "reason": "…" }, "at": "…" } }`
and is then closed with status 1000 and close reason `room deleted`. That event is never stored; a reconnect gets `404`.
A connection that was still replaying when the room was deleted gets only the close (same status and reason).
A room removed by retention, or deleted on another replica, closes the same way (retention's frame has the system
actor and reason `retention: closed N days`); if the server cannot tell who deleted it, only the close is sent. If a
room's live feed ends while the room still exists, the connection is closed with status 1011 and reason
`subscription ended`; the client reconnects with its highest seen seq.

### Room list socket (F6, implemented by A5)

`GET /api/rooms/ws` — same auth, origin rules and per-user connection cap as `/api/rooms/{id}/ws`
(`429 too_many_connections` before the upgrade). No replay cursor: the first frame is the truth.

Server → client:
```json
{ "type": "rooms", "rooms": [RoomSnapshot…] }                 // first frame: exactly GET /api/rooms for this user
{ "type": "room",  "room": RoomSnapshot }                      // one room changed (opened, updated, closed)
{ "type": "room_deleted", "room_id": "r_…" }
{ "type": "error", "error": { "code": "session_expired", "message": "…" } }   // then close, as the room socket
```
Client → server: `{ "type": "ping" }` every 20 s (read timeout 60 s). Rules: rooms outside the caller's tenants are
never sent; a `room` frame is sent only when the room's list-relevant state changed (status, title, severity,
driver, agent state, triage, hypothesis text and confidence, open proposals and their approvals, pending suggestion
count, `closed_at`, resolution), so step churn and `last_seq` alone send nothing; at most one frame per room per
500 ms; a full send buffer closes the socket with a policy-violation status and the client reconnects (it gets a
fresh `rooms` frame). Clients fall back to polling `GET /api/rooms` when the socket cannot be opened.

### Runner socket (U0; implemented by A5, runner A9; frames in `internal/setup/protocol/protocol.go`)

`GET /api/runners/ws` — the one outbound connection a `piperoom-setup` runner holds to the server. No session
cookie and no `X-User`: the first frame carries the pairing code (first connection) or the runner token
(reconnect), both compared in constant time, both absent from every log and event. Frames are JSON `protocol.Frame`
objects, at most 1 MiB; exactly one payload field is set, the one `type` names. Served only with
`PIPEROOM_SETUP_ENABLED=on`.

Runner → server: `pair {code | token}` (first), `hello {os, arch, label, tools, project_dir, catalogue_version,
protocol_version}` (only after the person confirmed the `welcome` on the runner's terminal), `result {id, ok,
summary, data?, receipt, truncated?}`, `refused {id, op, reason, proposal_id?, step_index?}`, `resume {in_flight:
[{id, proposal_id?, step_index, state: running | done, result?}]}` (first frame after a reconnect with a token),
`credentials {request_id, secrets: [{name, value}]}` (only after a `register_credentials` and the terminal `y`;
the one frame that ever carries a secret value; the server accepts it only while a `register_credentials` with
that `request_id` is pending for the runner's room, once per request, with exactly the requested names, and
refuses anything else as `setup.runner_refused {reason: "credentials"}`; the pipeline id is the room's, never the
runner's; names and the pipeline id are validated before any path is built; each value is written through an
`os.Root` at `PIPEROOM_SECRETS_DIR`, no symlink followed, as `<pipeline>/<name>` 0600, temp file then rename),
`ping`/`pong`.

Server → runner: `welcome {room_id, title, pipeline_id, tenant?, creator, protocol_version, targets?}` (what the
runner shows before asking `y/N`), `token {token, expires_at}` (after `hello` and after every reconnect; idle expiry
30 min), `op {id, op, params, tier, class, deadline, proposal_id?, step_index, params_hash?, proposal?}`, `cancel
{id}`, `register_credentials {request_id, pipeline_id, names}` (one pending per room), `secret {name, value}` (one value the driver typed in the UI,
stored nowhere on the server), `ping`/`pong`.

Rules: ping every 20 s, a runner silent 90 s is `setup.runner_lost`; a `protocol_version` other than
`protocol.Version` is refused at `hello` (`setup.pairing {outcome: "refused"}`); one runner per room; the server
re-sends an `op` the runner never saw at most once after `resume` and never one the runner reports running or
done (`docs/design/replicas.md` §5 holds with the runner on the path); every `result` and `refused` becomes an
`action.executed` (with `receipt`) or `setup.runner_refused` in the room log.
