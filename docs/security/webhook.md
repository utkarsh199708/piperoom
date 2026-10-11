# Design: webhook security

Task C9, part 2 of 3 (see also `auth.md`, `retention.md`). Design only — no code changed.
Closes **F9** (undefined webhook auth). Written against `main` @ `270377c`; `internal/httpapi` is
unmerged, so file names are proposals for A5.

## 1. What is being protected

`POST /api/alerts/webhook` lets an outside system start a room, which starts a billed agent
investigation against production read tools. The contract today is "any JSON, no stated auth".
Without auth, anyone who can reach the endpoint can:
- open unlimited rooms (Anthropic cost, DB/Kafka load, noisy on-call);
- post a believable fake sev1 about a real pipeline (social engineering of the on-call team);
- replay a captured real alert to re-open work.

The endpoint cannot use the human login in `auth.md`: callers are machines with no browser.
It gets its own, narrower, credential model. It must also never grant more than "create or append to
a room".

## 2. Rule zero: what a webhook is allowed to influence

| Field | Taken from payload? | Why |
|---|---|---|
| `tenant_id` | **No** — comes from the integration record the credential belongs to | no cross-tenant injection |
| `pipeline_id` | Only if it is in the integration's **allowlist**; else the integration's default; else reject | stops pointing the agent at an unrelated pipeline (ties to F4/F10) |
| `severity` | Yes, clamped to `sev1..sev4`, invalid → `sev3`, and capped at the integration's `max_severity` | a low-trust source cannot page everyone as sev1 |
| `title` | Yes, trimmed, control chars stripped, ≤120 runes | it is displayed everywhere |
| everything else | Stored as `alert` on `room.created` (size-capped), **never** put in the model prompt | the F9 analysis found `agent.Alert.Raw` is not used by `firstUserMessage`; keep it that way and add a test |

Treat all webhook text as untrusted data. If a later feature wants to show it to the model, it goes
through the same `<untrusted>` wrapper as comments (A4).

## 3. Authentication

Each sender is an **integration** row: `id, tenant_id, name, kind, allowed_pipelines, default_pipeline,
max_severity, auth_mode, secret(s), enabled, created_at`. Created by an admin; not editable by the
webhook itself. Two auth modes, chosen per integration:

### 3a. Signed (preferred) — HMAC-SHA256
Headers:
```
X-Piperoom-Integration: int_01J...
X-Piperoom-Timestamp:   1760180000            (unix seconds)
X-Piperoom-Signature:   v1=<hex hmac-sha256>
```
`signature = HMAC_SHA256(secret, timestamp + "." + raw_body)`.
Server steps, in this order, all failures return the same `401` with a generic body:
1. Look up integration by header; unknown/disabled → 401.
2. Reject if `|now - timestamp| > 5 min`.
3. Recompute over the **raw bytes** (read once with `io.LimitReader`, before JSON parsing) and compare with
   `hmac.Equal`. Never compare with `==`.
4. Replay check (§5).

Why this shape: the timestamp is inside the signed string so it cannot be swapped; the separator prevents
ambiguity; a version prefix lets us move to a new scheme later.

### 3b. Bearer token — for senders that cannot sign
Some sources (Prometheus Alertmanager's webhook receiver is the common one) can send a static
`Authorization: Bearer <token>` but not compute an HMAC. Allowed, with extra rules:
- Token is 32 random bytes (base64url), shown **once** at creation; Postgres stores `sha256(token)`.
- Lookup by hash, constant-time compare, TLS required.
- Optional per-integration source-IP/CIDR allowlist (recommended; applied to the connection address,
  or to the left-most-trusted `X-Forwarded-For` only if the proxy is configured as trusted — never a raw
  client-sent header).
- No replay protection from the token itself, so the dedupe/rate rules in §5–§6 matter more here.

For vendors that ship their own signature scheme (PagerDuty v3 webhooks, GitHub-style `sha256=` headers),
add a small **adapter** per `kind` that verifies the vendor's scheme and then maps to the common alert
struct. Confirm each vendor's header names and algorithm against its current docs when writing the adapter
— do not copy them from this file.

## 4. Secret handling

- HMAC secrets must be recoverable, so they cannot be hashed. Store them encrypted (envelope
  encryption with a key from the deployment's KMS/secret manager; for the pilot, an AES-GCM key from a
  file path in config, never an env var literal).
- Rotation: an integration may hold **two** active secrets; verification tries both; admin retires the
  old one after the sender is updated. No downtime, no shared "rotate everything" flag day.
- Never log a secret, token, signature, or the request body. Log `integration_id`, outcome, body size,
  and a short request id.
- Generated secrets are ≥ 32 random bytes. Reject user-chosen short secrets.

## 5. Replay and duplicate protection

- **Replay window:** within the ±5 min window, remember `sha256(signature)` in a table/cache with 10 min
  TTL; a repeat returns `200` with the original room (idempotent), not a second room.
- **Alert dedupe (also solves noisy alert storms):** each adapter derives a `dedupe_key` (Alertmanager
  `groupKey`, PagerDuty incident key, else `sha256(title + pipeline_id)`). Unique on
  `(integration_id, dedupe_key)` while a room for it is open. A repeat appends an `agent`-free
  system comment event ("alert fired again, N times") instead of opening a new room.
- A re-fire after the room is **closed** opens a new room and links the previous room id in `alert`.

## 6. Abuse and cost limits

| Limit | Default | On exceed |
|---|---|---|
| request body | 256 KB (`http.MaxBytesReader`) | `413` |
| per-integration rate | 30 requests/min | `429` + `Retry-After` |
| new rooms per integration | 6/hour | accept the alert as a comment on the newest open room; flag |
| open auto-created rooms per tenant | 10 | stop starting agents; room opens but agent stays `idle` until a human takes control |
| agent budget per auto room | cap on loop steps/tokens before a human confirms (A4 `Options`) | agent pauses with `waiting` status |
| global unauthenticated-failure rate | 60/min per source address | `429`; no per-attempt work beyond the lookup |

Rule of thumb: a flood must cost us a few DB rows, not Anthropic tokens. Agent start for webhook rooms is
the expensive step, so it is the one that is rate-limited hardest. Auto-started agents run with the same
read-only tools as manual rooms; a webhook can never trigger an action.

## 7. Response behaviour

- All auth failures: `401 {"error":{"code":"unauthenticated","message":"invalid credentials"}}` — no hint
  whether the integration id, timestamp, or signature was wrong.
- Success: `201` with a **minimal** body for webhook callers (`{room_id, status}`), not the full
  `RoomSnapshot` the contract returns today (it includes members and proposals that an alert sender has no
  business seeing). **Contract change request** for `docs/api.md`.
- Room is created with actor `{kind:"system", id:"webhook:<integration_id>", name:<integration name>}`,
  `driver = none` as the contract already says.
- The route is mounted **outside** the human-auth middleware and **inside** its own middleware; it must
  not honor a session cookie (otherwise a logged-in browser could be tricked into CSRF-ing it).

## 8. Inbound Slack (if/when added)

Same pattern, different verifier. Slack requests are signed with the app's signing secret over
`v0:<timestamp>:<body>`; reject timestamps older than 5 min and compare constant-time. Map the Slack user
to a Piperoom user through the authenticated identity link from `auth.md` — a Slack user id alone must
never grant approval rights. Approvals from Slack are out of scope for v1: Slack can notify and link to the
room, not approve in place. Check the current Slack signing docs when implementing.

## 9. Tests (A5 unless noted)

| Case | Expect |
|---|---|
| no credential / unknown integration | 401, identical body |
| valid HMAC, body altered by 1 byte | 401 |
| valid HMAC, timestamp 6 min old / 6 min in the future | 401 |
| same signed request sent twice | second returns same room, no new room, no new agent |
| bearer token, wrong token / hash mismatch | 401; timing test not required, `subtle.ConstantTimeCompare` used |
| `pipeline_id` not in allowlist | rejected or defaulted per config, never accepted |
| `severity: "sev0"`, 10 KB title, control chars | clamped / truncated / stripped |
| body > 256 KB | 413, nothing stored |
| 100 distinct alerts in a minute | rate-limited; rooms ≤ cap; agent starts ≤ cap |
| alert body containing "ignore previous instructions" | stored in `alert`, absent from the model's first message (A4 test on `firstUserMessage`) |
| request with a valid session cookie but no webhook credential | 401 |
| secret rotation: old and new secret both accepted, then old retired | old → 401 |
| log capture over all of the above | contains no secret, token, signature, or body |

## 10. Rollout and decisions needed

1. Leads: add the `docs/api.md` webhook section (headers, 201 minimal body, error shape) and an admin
   path to create integrations. For the pilot a CLI (`cmd/server integration create`) is enough; no
   admin UI.
2. A5: `internal/httpapi/webhook.go`, `internal/store` tables `integrations`, `webhook_replays`.
3. Decide: do we require HMAC for every production integration and allow bearer only with an IP
   allowlist? (Proposed: yes.)
4. Decide the default room/hour and per-tenant caps in §6 with the pilot customer's on-call volume in hand.
