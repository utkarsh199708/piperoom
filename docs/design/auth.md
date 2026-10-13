# Design: OIDC login, identity-bound approvals, params hash

C9, 2026-10-13, **revision 2**, against `main` @ 9a97cb2. Replaces PR #10's `auth.md`. Covers F1, F2 and feedback
T1, T5, T6. Revision 1 was reviewed in `reviews/auth-design-critique.md` (verdict: approve with changes).

## Changes in revision 2

Each row is one numbered item of the critique and the section that now answers it.

| Item | What changed | Section |
|---|---|---|
| 1 | Dev mode keeps an explicit non-loopback opt-in (`PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`) so the Docker image still starts. The image sets `PIPEROOM_AUTH_MODE` itself. The listen check is the real guard; the `X-Forwarded-For` refusal is only a hint. | §5.1 |
| 2 | Facts refreshed to `main` @ 9a97cb2 (export, signed alerts, jcs already in `go.mod`, served UI, retention). Export is limited to layer owners, the driver and auditors. `params` is hashed as stored, after the model's text is parsed. | §0, §3.4, §3.1 |
| 3 | Host allow-list middleware against DNS rebinding. | §1.5, §7 |
| 4 | The callback finds the `login_states` row by the login cookie id, then compares `state`. Single use. | §1.2 |
| 5 | Dev mode over plain http uses `piperoom_session` (no `__Host-`, no `Secure`). OIDC mode keeps `__Host-`. | §1.3 |
| 6 | `params_hash` goes on `ActionExecutedPayload` too. `title`, `risk`, `rollback` and `required_layers` are inside the hashed object. The limits (a DB admin) are stated. JCS number rule: no number above 2^53. Signed approvals are the stronger form, later. | §3.1–3.3 |
| 7 | Solo mode refuses to start when `owners.yaml` names anyone else. | §5.3 |
| 8 | `ValidateUsers` checks only layers whose principals are all `user:`. | §2.3 |
| 9 | `return` is parsed with `url.Parse`; scheme and host must be empty. | §1.1 |
| 10 | The client secret is required unless `PIPEROOM_OIDC_PUBLIC_CLIENT=1`. | §1.1, §6 |
| 11 | Missing groups claim: one Warn log per login and a clear `403` message. | §2.2 |
| 12 | `GET /auth/login` row caps; `Access-Control-Expose-Headers` goes with `cors`; `amr`/`acr` stored now. | §1.1, §1.4, §1.3 |
| Founder table | The critique's recommended defaults are written in as the proposal. | §9 |

## 0. Today (`main` @ 9a97cb2)

- `authenticate` trusts `X-User` (`internal/httpapi/server.go:191-208`). The WebSocket trusts `?user=`
  (`internal/ws/gateway.go:163-166`). Rights are `users.yaml` `layers` (`internal/room/service_proposals.go:165`,
  `users.go:145`). `approval.given` records only `Actor{kind,id,name}` (`service_proposals.go:180-182`).
- Steps are frozen at creation: `CreateProposal` marshals them into `proposals.steps` (`jsonb`,
  `service_proposals.go:112-127`, `migrations/00001_init.sql:26-37`). `approvals` has no approver column
  (`00001_init.sql:39-46`). The executor re-validates and re-scopes each step from the row
  (`service_executor.go:200-214`). Nothing proves what an approver saw.
- The model sends `params` as JSON **text** inside a string (PR #45). `decodeStepParams`
  (`internal/agent/tools_room.go:282-306`) parses it, refuses non-objects and duplicate keys, and the step reaches
  `CreateProposal` as a `json.RawMessage` object.
- Export: `GET /api/rooms/{id}/export` (a5/retention-audit, 46c3d3a) streams the whole room with a SHA-256 over the
  JCS form of the document (`internal/room/service_export.go`). **Any `users.yaml` user may call it**
  (`internal/httpapi/export.go:26-29`). Retention deletes whole closed rooms after `PIPEROOM_RETENTION_DAYS`
  (`internal/config/config.go:101-104`, `service_retention.go`).
- `github.com/gowebpki/jcs` v1.0.2 is already a direct dependency (`go.mod:11`, used by `service_export.go:17`).
  `golang.org/x/oauth2` v0.36.0 is direct (`go.mod:19`). `go-oidc` is still new.
- Listen guard: `checkListenAddr` refuses a non-loopback `PIPEROOM_HTTP_ADDR` without `--allow-remote`
  (`cmd/server/listen.go:19-31`, `main.go:89-95`). Default is `127.0.0.1:8090` (`config.go:110`).
- Docker image (PR #47): `PIPEROOM_HTTP_ADDR=0.0.0.0:8090` and `ENTRYPOINT ["/piperoom", "--allow-remote"]`
  (`Dockerfile:44-45,66`). The pilot compose binds the host side to `${PIPEROOM_BIND:-127.0.0.1}`
  (`deploy/docker-compose.pilot.yml:136`). The image serves the built UI (`PIPEROOM_WEB_DIR`), so API and UI are
  same-origin there; `cors` is used only when no web dir is set (`server.go:77-83`).
- Signed alert routes `POST /api/alerts/{source}` are merged (f4ce4b5) and sit outside `X-User`
  (`server.go:119`). The unsigned `POST /api/alerts/webhook` is still inside the `X-User` group (`server.go:135`).
- `PIPEROOM_PUBLIC_URL` is validated by `httpapi.ParsePublicURL` (`static.go:41-76`) and its host is the only extra
  WebSocket origin when the UI is served (`server.go:152-160`). `gateway.go:186-189` falls back to
  `localhost:5173` / `127.0.0.1:5173` otherwise.

## 1. Login

**OIDC authorization code + PKCE**, done by the Go server (works with Okta, Azure AD/Entra, Google, Keycloak).

Libraries (all verified by the lead with `go doc` in a scratch module): `golang.org/x/oauth2` (`GenerateVerifier`,
`S256ChallengeOption`, `VerifierOption`, `Config.Exchange`) and `github.com/coreos/go-oidc/v3/oidc` v3.21.0
(`NewProvider`, `Provider.Verifier`, `IDTokenVerifier.Verify`, `IDToken.Nonce`, `NewRemoteKeySet`; **new module**,
Apache-2.0). CSRF: `net/http.CrossOriginProtection` (Go 1.27). Hashing: `jcs.Transform` (already in `go.mod`).

### 1.1 `GET /auth/login?return=/rooms/r_x`

- Create one `login_states` row: `id`, `state`, `nonce`, `pkce_verifier`, `return_path`, `expires_at` (10 min).
  Set the login cookie (§1.3) to the row `id`. The cookie is `SameSite=Lax` because the IdP sends the user back
  with a cross-site redirect.
- Redirect to the IdP with `S256ChallengeOption` and `oidc.Nonce(nonce)`.
- `return` check: `url.Parse` it. Accept only when `Scheme == ""`, `Host == ""`, the path starts with `/` and not
  with `//` or `/\`, it has no byte below `0x20` or equal to `0x7f`, and it is at most 2 KB. Anything else becomes
  `/`. (Browsers turn `\` into `/`, so `/\evil.com` would otherwise leave the site.)
- This GET writes a row, so it is capped: one live row per login cookie (a new call replaces the old row), expired
  rows are pruned on insert, and the route is rate limited to 10 per minute per client IP. A full table answers
  `429`. Rows never outlive 10 minutes.
- Client secret: `PIPEROOM_OIDC_CLIENT_SECRET` is **required**. Okta, Entra and Google register a server app as a
  confidential client; some IdPs refuse PKCE-only backends. `PIPEROOM_OIDC_PUBLIC_CLIENT=1` allows an empty secret
  for an IdP that wants a public client. PKCE is always on, with or without the secret.

### 1.2 `GET /auth/callback?code=…&state=…`

Order matters. The row is found **through the cookie**, never by `state` value. Looking a row up by `state`
alone would let an attacker finish the flow with their own IdP account and send the victim to the callback URL,
logging the victim in as the attacker.

1. Read the login cookie. No cookie → `400 login_expired`.
2. `DELETE FROM login_states WHERE id = $1 AND expires_at > now() RETURNING …`. No row → `400 login_expired`.
   The delete makes the row single use: a replayed callback finds nothing.
3. Compare the `state` query value with the row's `state` using `subtle.ConstantTimeCompare`. Mismatch → `400`.
4. `Config.Exchange` with `VerifierOption(row.pkce_verifier)`.
5. `IDTokenVerifier.Verify` (signature, `iss`, `aud`, `exp`). Then **compare `IDToken.Nonce` with the row's nonce
   ourselves**; `Verify` does not.
6. Map the identity (§2). Create the session (§1.3). Clear the login cookie. Discard every IdP token; we keep none.
7. Redirect to `return_path`.

### 1.3 Session

Server-side `sessions` table: `id` (public, `ses_`+ULID), `token_hash` (SHA-256 of 32 random bytes), `user_id`,
`iss`, `sub`, `groups`, `groups_missing` (bool, §2.2), `auth_time`, `amr`, `acr`, `method` (`oidc` | `dev`),
`created_at`, `last_seen_at`, `expires_at`, `revoked_at`. `amr` and `acr` are stored now so a later step-up rule
needs no migration. A new token at every login. `last_seen_at` is written at most once a minute.

Cookie names depend on the mode. `__Host-` needs `Secure`, and `Secure` cookies are not reliably set on plain
`http://` except on `localhost` in Chrome and Firefox. Dev over `http://192.168.x.x:8090` would silently never get a
cookie.

| Mode | Session cookie | Login cookie | Attributes |
|---|---|---|---|
| `oidc` (https required) | `__Host-piperoom_session` | `__Host-piperoom_login` | `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, no `Domain` |
| `dev` | `piperoom_session` | `piperoom_login` | `HttpOnly`, `SameSite=Lax`, `Path=/`, no `Domain`, **no `Secure`** |

The server reads only the name of its own mode, never both. Expiry: idle 8 h, absolute 12 h (§6). Expired →
`401 session_expired`; the web app goes to `/auth/login?return=…`. Logout: `POST /auth/logout` sets `revoked_at`,
clears the cookie and closes that session's WebSockets. The IdP session stays (RP-initiated logout is later).

### 1.4 CSRF

`CrossOriginProtection` on every route. POSTs must also send `Content-Type: application/json`. Production is
same-origin (the image serves `web/dist`). In dev, Vite proxies `/api` and `/auth` to `:8090`, so dev is same-origin
too. The `cors` middleware (`server.go:215-227`) is deleted, and with it the `Access-Control-Expose-Headers` line in
`export.go:46`, which has no use once nothing is cross-origin. Note: requests with neither `Sec-Fetch-Site` nor
`Origin` are allowed by the library, so `curl` keeps working; that is why `X-User` for curl stays dev-only (§5).

### 1.5 Host allow-list (DNS rebinding)

`CrossOriginProtection` compares `Origin` with `Host`. A page at `evil.test` whose DNS now points to `127.0.0.1`
sends `Origin: evil.test`, `Host: evil.test:8090` and `Sec-Fetch-Site: same-origin`, so the check passes and
`POST /auth/dev-login` then `POST …/approve` would run. The WebSocket is safe (`OriginPatterns`); HTTP is not.

Fix: a middleware before every route (probes included) that refuses any `Host` not on the allow-list with
`421 bad_host`. The list is: `localhost`, `127.0.0.1`, `[::1]`, each with no port or the listen port; plus, in
`oidc` mode, the host of `PIPEROOM_PUBLIC_URL` (as `ParsePublicURL` returns it); plus, in dev mode with
`PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`, the host of `PIPEROOM_PUBLIC_URL` when set (a LAN demo at
`http://192.168.1.5:8090` sets it). Loopback stays on the list in every mode because the container health probe
calls `127.0.0.1` (`Dockerfile:51-55`). The same list feeds `gw.OriginPatterns`.

### 1.6 WebSocket

The session cookie is checked at upgrade; **`?user=` is removed**. `OriginPatterns` = the allow-list of §1.5
(dev adds `localhost:5173`). The 20 s presence tick re-checks the session and closes with `session_expired`.

## 2. Identity mapping

### 2.1 Stable key

- **`iss` + `sub`**, never email alone (OIDC Core §5.7).
- `users.yaml` (additive): each user may add `email:` and/or `sub:`. `sub` wins. Email matches only when
  `email_verified` is true. The first email match binds `iss`+`sub` in an `identities` table; a later login with the
  same email and a different `sub` is refused (recycled account).
- A user with no `users.yaml` entry gets id `u_` + first 12 hex of SHA-256(`iss`|`sub`), name from `name`.
- **Login gate (fail closed):** login works only if the user is in `users.yaml`, in any group named in
  `owners.yaml`, or in `PIPEROOM_AUTH_VIEWER_GROUPS`. Else `403 not_provisioned`.

### 2.2 Groups

Groups come from the claim named by `PIPEROOM_OIDC_GROUPS_CLAIM`. `groups` is not a standard OIDC claim: Okta and
Keycloak need a mapper, Azure sends object ids and **drops the claim on overage** (too many groups), Google sends
none (map Google users by `users.yaml`).

No groups claim → no group rights (correct), but it must not be silent:

- Log once per login at Warn: `auth: no groups claim in id token` with `user`, `iss` and the claim name. Never log
  the token.
- Set `sessions.groups_missing = true`. `GET /api/me` returns `groups_missing: true` so the UI can show a hint.
- An approve or reject that fails only because of this answers `403 not_layer_owner` with the message
  `no groups claim in token (IdP overage?)`.

### 2.3 Approval rights move to `owners.yaml` (one source of truth)

A layer value may be a string (today) or a list of principals `user:<id>` / `group:<name>`. Per pipeline, an optional
`owners:` block overrides it. A top-level `auditors:` list names who may export (§3.4). All additive.

```yaml
pipelines:
  - id: orders
    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:priya] }
layers:
  postgres: priya                              # today's form still valid
  kafka: [user:arjun, group:dataeng-oncall]
auditors: [group:security, user:meera]
```

Rights are derived per request from the session's user and groups. `users.yaml` `layers` is deprecated.
`ValidateUsers` (`internal/room/users.go:70-110`) keeps checking agreement **only for a layer whose principals are
all `user:`**. The server does not know group membership at start-up, so a layer that lists any `group:` is skipped
with one Info log. The check goes away when `users.yaml` `layers` is removed.

**Backup approver:** may approve at once; the event records `grant: "backup:…"` and the room gets a system message
naming the backup. Escalation timers are T6 later work.

## 3. Approval integrity (F2+)

### 3.1 What is hashed

At `CreateProposal`, after `decodeStepParams` and `a.Validate` have run and **after** `p.Steps` is marshalled for the
row (`service_proposals.go:112`), compute `params_hash` = hex SHA-256 of the **RFC 8785 (JCS)** form of:

```json
{"v":2,"room_id":"…","proposal_id":"…","title":"…","risk":"…","rollback":"…",
 "required_layers":["…"],"steps":[{"action":"…","layer":"…","params":{…},"description":"…"}]}
```

- `params` is the parsed object that is stored, never the model's raw text. `proposals.steps` is `jsonb`: Postgres
  drops whitespace and key order. JCS does not depend on either, so the hash of the in-memory object equals the
  hash of the read-back row.
- `title`, `risk`, `rollback` and `required_layers` are inside the hash because an approver reads them. Without this
  a DB edit to `rollback` would be invisible. `description` is already inside each step.
- **Number rule.** JCS writes numbers as IEEE 754 doubles. An integer above 2^53 or a decimal finer than a double
  would hash differently from what is shown. `CreateProposal` runs the export's `hashForm`
  (`internal/room/numbers.go:96`) over the steps and refuses with `400 validation` when `inexact > 0`. The
  `propose_fix` tool text tells the model to quote offsets and other large integers as strings. No action may put
  a raw offset into `params` as a number.
- Store the hash in the proposal row. Publish it in `proposal.created` and `open_proposals[]`.

### 3.2 Who checks it

- `POST /api/proposals/{pid}/approve` body becomes `{layer, params_hash}` (reject: `{layer, reason, params_hash}`).
  Missing → `400 validation`; different from the stored hash → **`409 params_mismatch`**. The web app sends the hash
  it rendered next to the params. Steps are already frozen at creation, so this echo mainly catches a stale UI. It is
  kept because it is cheap and an auditor can follow it.
- The executor recomputes the hash from the row before each `Execute`
  (next to `service_executor.go:205-214`) and refuses on mismatch: the step fails with output
  `params hash mismatch`, the proposal becomes `failed`.
- `ApprovalGivenPayload` / `ApprovalRejectedPayload` gain `params_hash` and `identity`:
  `{iss, sub, session_id, method: "oidc"|"dev", grant: "user:…"|"group:…"|"backup:…"|"solo", auth_time}`.
  `session_id` is the public id, never the token. The `approvals` table gains the same columns.
- **`ActionExecutedPayload` gains `params_hash`** (contract change, additive). Then `proposal.created` →
  `approval.given` → `action.executed` chain on one value, and the export shows the chain.
- `HumanInput` (`internal/agent/contract.go`) is unchanged.

### 3.3 What this does and does not prove

- It catches: a stale UI, a bug that rewrites steps, an accidental or scripted edit of the row between creation and
  execution, and a mismatch between what was approved and what ran.
- It does **not** catch a database admin. The executor reads the stored hash from the **same** database; an admin who
  edits `steps` can edit `params_hash` too. Only a copy held outside the database catches that: the export file plus
  its `X-Piperoom-Export-SHA256` value, taken before the edit. Pilots that need this must export on a schedule to
  storage the DB admin cannot write (open question in §9).
- The stronger form is a **signed approval**: the approver's device signs `params_hash` with a WebAuthn key or the
  IdP signs it in a step-up login. Later; the `amr`/`acr` columns prepare for it.

### 3.4 Export

- Access: `GET /api/rooms/{id}/export` answers `403 not_allowed` unless the user is (a) an approver or backup for
  any layer of the room's pipeline, (b) the room's current driver, or (c) an `auditors:` principal. Viewers
  (`PIPEROOM_AUTH_VIEWER_GROUPS`) cannot export. In dev mode (a) uses `users.yaml` `layers`. The log line in
  `export.go:57` (who exported what) stays.
- Content: `proposals[]` gains `params_hash`; `approvals[]` gains `params_hash` and `identity`; `action_results[]`
  gains `params_hash`. The `proposals[]` entry already carries the frozen `title`, `risk`, `rollback`,
  `required_layers` and `steps`, so an auditor can rebuild the §3.1 object and re-hash it. The export's own JCS
  SHA-256 over the whole document stays; its number rule is the same as §3.1.
- Retention deletes whole closed rooms after `PIPEROOM_RETENTION_DAYS`. The export is the only copy after that, so
  a pilot with an audit need exports before the cutoff.

## 4. Machine routes

- `POST /api/alerts/{source}` (merged, f4ce4b5) stays signature/token-based, outside session auth. Cookies are
  ignored there. The Host allow-list (§1.5) still applies.
- `POST /api/alerts/webhook` (unsigned, today in the `X-User` group, `server.go:135`) and the `X-User` header work
  **only in dev mode**. In OIDC mode the webhook route is not mounted (404) and `X-User` is ignored. Use
  `/api/alerts/generic` (HMAC, `GENERIC_ALERT_SECRET`).

## 5. Dev, demo, solo, migration

### 5.1 Dev mode and the listen guard

`PIPEROOM_AUTH_MODE=dev` (default). The listen check is the real guard, so it stays close to today's code:

- `PIPEROOM_HTTP_ADDR` must be loopback (`isLoopbackHost`, `listen.go:35-41`) **unless**
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`. That variable replaces the `--allow-remote` flag. With it set the server logs
  a loud Warn at start and on **every** dev login: "dev auth on a non-loopback address: anyone who can reach this
  port can act as any user".
- Requests that carry `Forwarded` or `X-Forwarded-For` are refused in dev mode with `403`. This is a hint only: a
  proxy may not add the header. It never replaces the listen check.
- `--demo` forces dev.
- Docker image (A1): `ENV PIPEROOM_AUTH_MODE=dev PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` replaces the `--allow-remote`
  entrypoint flag, with the same comment block. The compose file keeps `PIPEROOM_BIND=127.0.0.1` as the host-side
  default and adds `PIPEROOM_AUTH_MODE`, `PIPEROOM_DEV_ALLOW_NONLOOPBACK`, `PIPEROOM_OIDC_*` and
  `PIPEROOM_AUTH_*` to the pass-through list so a pilot can switch to `oidc` without an image change. The image
  states its mode explicitly so a future default change cannot silently lock the demo path.
- `PIPEROOM_AUTH_MODE=oidc`: needs issuer, client id, client secret (or the public-client flag) and an `https`
  `PIPEROOM_PUBLIC_URL`, else refuse to start. `PIPEROOM_DEV_ALLOW_NONLOOPBACK` is ignored there, with a Warn.

### 5.2 Dev login

`POST /auth/dev-login {user}` issues a **real session** (`method: "dev"`) for a `users.yaml` user. `UserSwitcher`
calls it, so dev and prod share the cookie path. `X-User` stays for curl only, dev mode only.

### 5.3 Solo mode

`PIPEROOM_SOLO_USER=<id>`: that user may approve every layer. Each approval must also send
`confirm: "<pipeline_id>/<layer>"`, typed by hand; the UI shows a solo banner; events record `grant: "solo"`.
The typed string is a speed bump, not a control, so the server refuses to **start** when:

- the solo user is not in `users.yaml`, or
- `owners.yaml` names any principal (in `layers:`, any `pipelines[].owners`, or `auditors:`) other than that user.

This keeps solo mode for a solo data engineer (P4) and makes it impossible to leave it set on a multi-owner install.
Startup logs a warning.

### 5.4 Migration

Old events keep their actor (new fields empty). One additive migration adds `sessions`, `login_states`,
`identities`, the new `approvals` columns and `proposals.params_hash`. `devctl` needs no new env var. The pilot docs
(`deploy/README.md:9-23`) are updated by A1 in PR 2.

## 6. Config fields (contract, `internal/config/config.go`)

| Env var | Default | Meaning |
|---|---|---|
| `PIPEROOM_AUTH_MODE` | `dev` | `dev` \| `oidc` |
| `PIPEROOM_DEV_ALLOW_NONLOOPBACK` | `""` | `1` lets dev mode listen on a non-loopback address (loud warnings); replaces `--allow-remote` |
| `PIPEROOM_PUBLIC_URL` | `""` (exists) | `https` origin in oidc mode; redirect URI = this + `/auth/callback`; its host joins the Host allow-list |
| `PIPEROOM_OIDC_ISSUER` | `""` | issuer URL |
| `PIPEROOM_OIDC_CLIENT_ID` | `""` | |
| `PIPEROOM_OIDC_CLIENT_SECRET` | `""` | secret, never logged; required unless the next flag is set |
| `PIPEROOM_OIDC_PUBLIC_CLIENT` | `""` | `1` allows an empty secret (PKCE only) |
| `PIPEROOM_OIDC_SCOPES` | `openid email profile` | space separated |
| `PIPEROOM_OIDC_GROUPS_CLAIM` | `groups` | `""` = no group rights |
| `PIPEROOM_AUTH_VIEWER_GROUPS` | `""` | comma separated; may log in and watch; may not export |
| `PIPEROOM_SESSION_IDLE` | `8h` | Go duration |
| `PIPEROOM_SESSION_MAX` | `12h` | Go duration |
| `PIPEROOM_SOLO_USER` | `""` | off |

## 7. Threats and mitigations

| Threat | Mitigation |
|---|---|
| Session fixation | New token at login; separate pre-login cookie; no id in URLs; `__Host-` in oidc mode |
| CSRF | `SameSite=Lax`, `CrossOriginProtection`, JSON content type, same origin |
| DNS rebinding to the loopback server | Host allow-list, `421 bad_host` (§1.5) |
| Login CSRF (attacker's code, victim's browser) | Row found by login cookie, then `state` compared; single use (§1.2) |
| WebSocket hijack | Cookie + `OriginPatterns` at upgrade |
| Token leakage in logs | Only the token's SHA-256 is stored; IdP tokens discarded; `requestLog` logs path, not query (`server.go:240`); never log cookies; a leaked `code` is useless without the PKCE verifier |
| Replayed callback | `login_states` row deleted on first use; state + nonce checked |
| `login_states` filling | One row per cookie, 10 min expiry, rate limit (§1.1) |
| Open redirect via `return` | `url.Parse`, empty scheme and host (§1.1) |
| IdP outage | Live sessions keep working; new logins get `503 idp_unavailable`; discovery retried, not fatal at start |
| Clock skew | Session expiry uses server time; ID token `exp` checked once at login; NTP required |
| Replayed approvals | Live session + CSRF pass; one decision per layer (`already_decided`); `params_hash` mismatch → `409` |
| Edited proposal row | Executor re-hash (accidents); export copy outside the DB (admin); signed approvals later (§3.3) |
| Stale groups / recycled email | Groups refresh at login (worst case 12 h); `iss`+`sub` binding in `identities` |
| Solo mode on a multi-owner install | Refuse to start (§5.3) |
| Export by a viewer (F3 data) | Export limited to owners, driver, auditors (§3.4) |

## 8. PR split

| PR | Owner | Scope | Done when |
|---|---|---|---|
| 1 | Lead (contract) | `config.go` fields (§6); `events.go` `ParamsHash` on `ProposalCreatedPayload` and `ActionExecutedPayload`, `ParamsHash` + `Identity` on approval payloads; `docs/api.md` auth, `/auth/*`, approve body, `409 params_mismatch`, `421 bad_host`, export access, WS without `?user=` | `go build ./...` passes; api.md matches this doc |
| 2 | A5 + A6 + A1 | Sessions, mode-specific cookie names, `/auth/dev-login`, `/auth/logout`, cookie WS, `?user=` removed, Host allow-list, `CrossOriginProtection`, `cors` + expose header deleted, `PIPEROOM_DEV_ALLOW_NONLOOPBACK` replaces `--allow-remote`, `X-Forwarded-For` hint, webhook dev-only. A6: Vite proxy, cookie client, `UserSwitcher` via dev-login, 401 redirect. A1: Dockerfile env, compose pass-through, `deploy/README.md` | `X-User` in oidc mode → 401; dev on `0.0.0.0` without the flag refuses start; with the flag warns; `Host: evil.test` → 421; WS without cookie → 401; cross-origin POST → 403; logout closes WS; the pilot image starts |
| 3 | A5 + A3 + A1 | OIDC login (§1.1–1.2 order), `identities`, login gate, `return` check, secret rule, rights from `owners.yaml` principals, groups + overage message, backup, solo start check, `ValidateUsers` group rule. A3: `internal/owners` lists, per-pipeline `owners:`, `auditors:`. A1: fake IdP (`httptest`) in `internal/testenv` | With the fake IdP: bad nonce/state → 401; callback without the login cookie → 400; replayed callback → 400; unmapped user → 403; group owner approves; missing groups claim → 403 with the overage message; solo + other owners → no start |
| 4 | A5 + A6 | `params_hash` compute/store/check (§3.1 object, number rule), executor re-check, `action.executed` hash, approval identity columns, export access + fields (§3.4). A6: show params + hash, send hash, solo confirm box | Changed params → `409`; DB-edited row → executor refuses; params with `2^53+1` → `400`; viewer export → `403`; export row re-hashes to `params_hash` |

## 9. Decisions (founder)

The critique recommended these defaults. They are written into this design; the founder confirms or changes them.

| # | Question | Proposed | Why |
|---|---|---|---|
| 1 | `PIPEROOM_AUTH_MODE` default | `dev`, with the loopback guard and `PIPEROOM_DEV_ALLOW_NONLOOPBACK` for containers | `go run` and `devctl` stay zero-config; remote exposure is a start failure or a loud warning, never silent |
| 2 | Session lengths | 8 h idle / 12 h absolute, both configurable | One shift; 12 h also bounds stale group membership; regulated pilots set 1 h / 8 h |
| 3 | Backup approver | Immediate, with a room system message and `grant: "backup:…"` | On-call needs it now; the audit trail shows the backup path; N-minute timer in T6 |
| 4 | Step-up before approvals | Later; `auth_time`, `amr`, `acr` stored now | No regulated pilot in the next 8 weeks; the columns make it a no-migration add |
| 5 | New module `go-oidc` v3.21.0 | Yes | Hand-rolled JWKS/JWT checking is the riskier path; API verified; Apache-2.0. `jcs` is already in `go.mod` |
| 6 | Scheduled export to storage outside the DB (§3.3) | Open | Needed only for pilots that must detect a DB admin; a cron plus the existing export route would do |
