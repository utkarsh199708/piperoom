# Design: OIDC login, identity-bound approvals, params hash

C9, 2026-10-13, **revision 5**, against `main` @ 0172826. Replaces PR #10's `auth.md`. Covers F1, F2 and feedback
T1, T5, T6. Revision 1 was reviewed in `reviews/auth-design-critique.md` (verdict: approve with changes). Revision 2
was reviewed by two critics (security, fit with `main`); revision 3 answers them. Revision 4 answers the review of
revision 3 (PR #44 at be6f3c2). Revision 5 (lead, 2026-10-13) answers three items of
`reviews/security-audit-2026-10.md`.

## Changes in revision 5

| Item | What changed | Section |
|---|---|---|
| A-ID-2 | Display-name impersonation: `verified` on every person in events and API responses; IdP names that look like a `users.yaml` person get the email (or id) appended; `identity.email` on approvals | §2.1 |
| A-AP-3 | The users → owners check of `ValidateUsers` no longer skips layers that list groups; `authorizeLayer` applies the same rule per request | §2.3 |
| A-ID-5 | Unbind revokes the live sessions of the user and of the removed `iss`+`sub` | §2.1 |

## Changes in revision 4

| Item | What changed | Section |
|---|---|---|
| 1 (blocking) | A proxy pilot that upgrades could be impersonated: the proxy forces `X-User: bob`, the image stays in dev mode, and bob calls `POST /auth/dev-login {user:"priya"}`. Now dev mode answers `403 proxy_identity` when (a) `/auth/dev-login` is called with `X-User`, or (b) a request has a session cookie and an `X-User` for a different user. Keyed on `X-User` only. Both cases are in PR 2's done-when. | §5.2, §5.1, §7, §8 |
| 2 | Viewer rule is default-deny: every non-GET under `/api` answers `403 viewer`. It now also catches `POST /api/handoffs/{hid}/accept`. Viewer row: every GET except export. | §2.1, §6, §7, §8 |
| 3 | People who get in only through an `owners.yaml` group are not in `users.yaml`, so `GrantControl` and `CreateHandoff` could not find them. PR 3 adds a person lookup backed by sessions (`LookupPerson`). | §0, §1.3, §2.1, §8 |
| 4 | `auditors:` no longer makes a member. An auditor-only principal is a viewer plus export. | §2.1, §3.4, §8 |
| 5 | The `PIPEROOM_PUBLIC_URL` row now states the §1.5 rule: in dev mode on a non-loopback address the host joins the allow-list only with `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`. | §6 |
| 6 | Line refs and the header refreshed to `main` @ 0172826: `service_proposals.go` 131-146, `OwnsLayer` check :187, `approval.given` :202-204; `service_steer.go` `GrantControl` 161-193; `docs/api.md:103`. Each was checked against `origin/main`. | §0, §3.1, `findings.md` |

## Changes in revision 3

| Item | What changed | Section |
|---|---|---|
| S1 | Roles. A **viewer** (only in `PIPEROOM_AUTH_VIEWER_GROUPS`) gets GET and the WebSocket; every POST answers `403 viewer`. So a viewer can never become driver. The driver clause is dropped from export. | §2.1, §3.4, §7 |
| S2 | The `/auth/login` IP limit is no longer a lockout: 300 per minute per client IP, with `PIPEROOM_TRUSTED_PROXIES` so `X-Forwarded-For` is trusted only from listed CIDRs. | §1.1, §6, §7 |
| S3, F2 | Loopback hosts match on **any port**. Only the `PIPEROOM_PUBLIC_URL` host is exact. `PIPEROOM_ALLOWED_HOSTS` for a proxy that rewrites `Host`. A non-loopback `PIPEROOM_BIND` needs one of the two. | §1.5, §6 |
| S4, F1 | The dev-mode `X-Forwarded-For` 403 is deleted. A reverse proxy needs `PIPEROOM_AUTH_MODE=oidc`. Proxy pilots build from git tag `pre-auth` until PR 3. `--allow-remote` stays as a deprecated alias for one release. The Dockerfile comment and the pilot docs are in A1's list. | §5.1, §5.4, §8 |
| F3 | `/api/me.layers` stays: the layers this session may approve, computed per request. The UI needs no change. | §2.3, §8 |
| F4 | Stale fact fixed: `main` has no Vite proxy. PR 2 (A6) adds one, and `cors` goes only after it. | §1.4, §8 |
| F5 | Line refs: `Dockerfile:44-47` (ENV) and `:67` (ENTRYPOINT). | §0 |
| Lead decisions (2026-10-13) | `PIPEROOM_TRUSTED_PROXIES` is kept (oidc mode only). No image pipeline: git tag `pre-auth`, pilots build from source. Members who are not owners or auditors cannot export. They answer the old open questions. | §1.1, §2.1, §3.4, §5.4, §6, §8, §9, §10 |
| Small | Nonce compared in constant time; login gate re-run per request; `group:` values should be IdP group ids; `identities` unbind; cookie wins over `X-User`; `return` checked after decoding; `LayerOwner` display rule; export row in `docs/api.md`; §9 row 6 has a default. | §1.1–1.3, §2.1–2.3, §5.2, §8, §9 |

## Changes in revision 2

Each row is one numbered item of the critique and the section that now answers it.

| Item | What changed | Section |
|---|---|---|
| 1 | Dev mode keeps an explicit non-loopback opt-in (`PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`) so the Docker image still starts. The image sets `PIPEROOM_AUTH_MODE` itself. The listen check is the real guard; the `X-Forwarded-For` refusal was only a hint (deleted in revision 3). | §5.1 |
| 2 | Facts refreshed to `main` @ 9a97cb2 (export, signed alerts, jcs already in `go.mod`, served UI, retention). Export is limited to layer owners, the driver and auditors (driver dropped in revision 3). `params` is hashed as stored, after the model's text is parsed. | §0, §3.4, §3.1 |
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

## 0. Today (`main` @ 0172826)

- `authenticate` trusts `X-User` (`internal/httpapi/server.go:191-208`). The WebSocket trusts `?user=`
  (`internal/ws/gateway.go:163-166`). Rights are `users.yaml` `layers` (`internal/room/service_proposals.go:187`,
  `users.go:145`). `approval.given` records only `Actor{kind,id,name}` (`service_proposals.go:202-204`).
- Steps are frozen at creation: `CreateProposal` marshals them into `proposals.steps` (`jsonb`,
  `service_proposals.go:131-146`, `migrations/00001_init.sql:26-37`). `approvals` has no approver column
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
  (`Dockerfile:44-47,67`). The pilot compose publishes `${PIPEROOM_BIND:-127.0.0.1}:${PIPEROOM_PORT:-8090}:8090`
  (`deploy/docker-compose.pilot.yml:136`). The image serves the built UI (`PIPEROOM_WEB_DIR`), so API and UI are
  same-origin there; `cors` is used only when no web dir is set (`server.go:77-83`).
- Dev UI: `web/vite.config.ts` has **no proxy**. Under `npm run dev` the client calls `http://localhost:8090`
  cross-origin with `X-User` (`web/src/api/client.ts:15-16,38`). The UI gates approve buttons on `/api/me.layers`
  (`web/src/context/UserContext.tsx:40`; `docs/api.md:103`).
- The pilot docs tell pilots to run the image behind an authenticating reverse proxy that sets `X-User` and forces
  `?user=` (`deploy/README.md:12-19`, `docs/pilot/setup.md:385-388`, `Dockerfile:57-62`).
- Driver: `GrantControl` lets **anyone** take the driver role when there is no driver or the 30-minute lease has
  lapsed (`internal/room/service_steer.go:161-193`). `GrantControl` and `CreateHandoff` find the target with
  `s.users.Get(toID)` (`service_steer.go:165`, `service_handoff.go:46`), and `Service.LookupUser` is the same
  `users.yaml`-only map (`service.go:198`). A person who is not in `users.yaml` cannot be found by either call (§2.1).
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
- `return` check: `url.Parse` it. Accept only when `Scheme == ""`, `Host == ""`, `Opaque == ""`, the **decoded**
  `u.Path` starts with `/` and not with `//` or `/\` (so `/%2F%2Fevil` is caught too), it has no byte below `0x20`
  or equal to `0x7f`, and the raw value is at most 2 KB. Anything else becomes `/`. The redirect at the end goes to
  the rebuilt `u.Path` + `?` + `u.RawQuery`, never to the raw string. (Browsers turn `\` into `/`, so `/\evil.com`
  would otherwise leave the site.)
- This GET writes a row, so it is capped. The real caps are: one live row per login cookie (a new call replaces the
  old row), expired rows pruned on insert, rows never older than 10 minutes. On top, a **large** per-client-IP limit
  of 300 per minute answers `429`. It is large on purpose: this route has no login, so a small limit would be an
  anonymous lockout (ten `curl` calls a minute from anywhere, and nobody can log in). Behind a TLS proxy every user
  shares one `RemoteAddr`, so the client IP is taken from `X-Forwarded-For` **only** when the peer is in
  `PIPEROOM_TRUSTED_PROXIES` (CIDR list, §6); then the rightmost address not in that list is the client. With no
  trusted proxy the peer address is the client. This is used **only in `oidc` mode**, for this rate limit and for the
  request log. In `dev` mode the limit and the log key on `RemoteAddr`. The limit is in memory, per process.
  (Lead decision, 2026-10-13: the field is kept, §9 row 7.)
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
3. Compare the `state` query value with the row's `state` using `subtle.ConstantTimeCompare`. Mismatch → `401 unauthenticated` (lead, 2026-10-13: matches the code and the §8 tests; a bad nonce, a bad token and an `azp` other than our client id also give `401`).
4. `Config.Exchange` with `VerifierOption(row.pkce_verifier)`.
5. `IDTokenVerifier.Verify` (signature, `iss`, `aud`, `exp`). Then **compare `IDToken.Nonce` with the row's nonce
   ourselves** with `subtle.ConstantTimeCompare`; `Verify` does not check the nonce. If the token has an `azp` claim it must equal our client id (OIDC Core 3.1.3.7).
6. Map the identity (§2). Create the session (§1.3). Clear the login cookie. Discard every IdP token; we keep none.
7. Redirect to `return_path`.

### 1.3 Session

Server-side `sessions` table: `id` (public, `ses_`+ULID), `token_hash` (SHA-256 of 32 random bytes), `user_id`, `name`,
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

`authenticate` re-runs the §2.1 login gate on **every** request from the session's stored `user_id` and `groups`
(`users.yaml` and `owners.yaml` are in memory, so this is cheap). A user removed from `users.yaml` or from the viewer
groups gets `401 session_expired` at once, not at the 12 h cap. Stale IdP group membership still lasts until the
next login (§7).

### 1.4 CSRF

`CrossOriginProtection` on every route. POSTs must also send `Content-Type: application/json`. Production is
same-origin (the image serves `web/dist`). Dev is **not** same-origin today: `main` has no Vite proxy and the client
calls `http://localhost:8090` cross-origin (§0). A cookie session cannot work cross-origin over plain http, so
**PR 2 (A6) adds a Vite proxy** for `/api` and `/auth` to `:8090` (with `ws: true` for the WebSocket) and makes the
client call same-origin paths. That proxy is a prerequisite inside PR 2: the `cors` middleware (`server.go:215-227`)
and the `Access-Control-Expose-Headers` line in `export.go:46` are deleted **in the same PR, after** the proxy lands,
or `npm run dev` breaks. Note: requests with neither `Sec-Fetch-Site` nor `Origin` are allowed by the library, so
`curl` keeps working; that is why `X-User` for curl stays dev-only (§5).

### 1.5 Host allow-list (DNS rebinding)

`CrossOriginProtection` compares `Origin` with `Host`. A page at `evil.test` whose DNS now points to `127.0.0.1`
sends `Origin: evil.test`, `Host: evil.test:8090` and `Sec-Fetch-Site: same-origin`, so the check passes and
`POST /auth/dev-login` then `POST …/approve` would run. The WebSocket is safe (`OriginPatterns`); HTTP is not.

Fix: a middleware before every route (probes included) that refuses any `Host` not on the allow-list with
`421 bad_host`. The list:

- `localhost`, `127.0.0.1`, `[::1]`, on **any port**. An attacker page can never make a browser send
  `Host: localhost`, so the port adds nothing. Fixing the port would break two shipped paths: Vite's proxy keeps
  `Host: localhost:5173` (`changeOrigin` is off by default), and compose with `PIPEROOM_PORT=9000` sends
  `Host: 127.0.0.1:9000` (`:136`).
- The host of `PIPEROOM_PUBLIC_URL` (as `ParsePublicURL` returns it), **exact**, with its port if it has one. In
  `oidc` mode always; in dev mode only with `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` (a LAN demo at
  `http://192.168.1.5:8090` sets it).
- Each entry of `PIPEROOM_ALLOWED_HOSTS` (comma list, §6), compared to `Host` as written. For a proxy, ingress or
  tunnel that rewrites `Host` to an internal name. Without it such a pilot gets `421` on day one and someone
  "fixes" it by deleting the middleware.

Loopback stays on the list in every mode because the container health probe calls `127.0.0.1:<port>`
(`cmd/server/healthcheck.go:20-23`). A `PIPEROOM_BIND` other than loopback (`deploy/README.md:22-23`) now needs
`PIPEROOM_PUBLIC_URL` or `PIPEROOM_ALLOWED_HOSTS`, else every request from another machine is `421`; A1 writes
this into `deploy/README.md`. The same list feeds `gw.OriginPatterns`.

### 1.6 WebSocket

The session cookie is checked at upgrade; **`?user=` is removed**. `OriginPatterns` = the allow-list of §1.5
(dev adds `localhost:5173`). The 20 s presence tick re-checks the session and closes with `session_expired`.

## 2. Identity mapping

### 2.1 Stable key

- **`iss` + `sub`**, never email alone (OIDC Core §5.7).
- `users.yaml` (additive): each user may add `email:` and/or `sub:`. `sub` wins. Email matches only when
  `email_verified` is true. The first email match binds `iss`+`sub` in an `identities` table; a later login with the
  same email and a different `sub` is refused (recycled account).
- A user with no `users.yaml` entry gets id `u_` + first 12 hex of SHA-256(`iss`|`sub`), name from `name` (format
  characters dropped), else the email if `email_verified`, else the id. The name is a label, never an identity; see
  **Display names and `verified`** below.
- **Login gate (fail closed):** login works only if the user is in `users.yaml`, or is named in `owners.yaml`
  (a `user:` or `group:` in `layers:`, in a pipeline `owners:` block as approver or backup, or in `auditors:`), or is
  in a group of `PIPEROOM_AUTH_VIEWER_GROUPS`. Else `403 not_provisioned`. The gate decides **who gets in**. The role
  below decides **what they may do**, and the two are not the same list.
- **Unbind:** an admin who moves a user to a new IdP account deletes that user's `identities` row; the next
  verified-email login binds the new `iss`+`sub`. A `devctl identities unbind <user>` helper is A1 work in PR 3.
  The same transaction revokes every live session of that user or of the removed `iss`+`sub` (security audit
  A-ID-5), and `devctl` prints how many: the unbound account stops at its next request (`401 session_expired`) and its
  WebSockets close on the next presence tick.

**Display names and `verified` (revision 5, security audit A-ID-2).** Anyone can set their own IdP name claim, so a
member who got in through a group could call themself "Priya". Three rules keep a name from passing for a person:

- **`verified`.** Every person in events and API responses carries `verified` (`events.Actor.Verified`,
  `docs/api.md`): `true` for a `users.yaml` person, whose name is the `users.yaml` name; `false` for a derived `u_`
  id. The agent and system actors are `true`. It is decided when the event is written. The UI marks `false` with a
  badge and shows the id or email next to the name.
- **Look-alike names.** The name of a person who is not in `users.yaml` is compared with every `users.yaml` id, name
  and email, at login and again on every request (so a `users.yaml` entry added later, or a session made before this
  rule, is caught too). The comparison form is a skeleton in the spirit of Unicode TR39: drop format characters,
  NFKD (fullwidth and other compatibility forms), drop combining marks, map Greek and Cyrillic capitals that look
  Latin, Unicode case folding, map the lower-case Latin, Greek and Cyrillic look-alikes to Latin (a small built-in
  table), fold `i`/`l`/`1`/`|` and `0`/`o`, `rn`→`m`, `vv`→`w`, and ignore spaces, punctuation and symbols. On a
  match the name becomes `"<name> (<email>)"` with the verified email, or `"<name> (<u_ id>)"` without one, and the
  login is logged at Warn with both ids. It is **not refused**: the person may really be called Priya, and the suffix
  plus the badge tell the two apart. A false match costs only a suffix, so the mapping errs wide.
- **Decisions carry a mailbox.** `approval.given` / `approval.rejected` `identity.email` is the session's verified
  IdP email, else the `users.yaml` `email:` of the user, beside the actor id; an audit never rests on the name.

**Roles**, computed per request from the session's user and groups:

| Role | Who | May |
|---|---|---|
| viewer | passes the gate **only** through `PIPEROOM_AUTH_VIEWER_GROUPS` and/or `auditors:` (an auditor-only principal is a viewer) | every GET **except export**, and the WebSocket. **Every non-GET under `/api` answers `403 viewer`.** No room create, no comment, no steer, no control request or grant, no hand-off create or accept. Export only if also in `auditors:` (§3.4) |
| member | in `users.yaml`, or a `user:` / `group:` listed in `layers:` or in a pipeline `owners:` block (approver or backup) | all of today's room actions: create, comment, steer, suggest, request and take control, hand off, close, trace. **Not export:** a member who is not an approver and not in `auditors:` gets `403 not_allowed` on export (§3.4) |
| approver | member whose `user:` or `group:` is listed for a layer (or is its backup) | approve or reject that layer (§2.3) |
| auditor | in `auditors:` (any role) | export (§3.4). **`auditors:` never makes a member.** A principal who is only in `auditors:` has the viewer role plus export: they can read and export, they cannot steer, comment, approve, take control or receive a hand-off |

**The viewer rule is default-deny.** It is a middleware on the HTTP method, not a list of routes: for a request whose
role is viewer, every method other than GET answers `403 viewer`. A route list missed one already: it named
`/api/rooms` and `/api/proposals` and left out `POST /api/handoffs/{hid}/accept`. With a method rule, a route added
later is closed from the first day. Two limits: `POST /auth/logout` is under `/auth`, so a viewer can still log out;
`POST /api/alerts/{source}` is a machine route outside session auth (§4), so it has no role.

Why the viewer rule is strict: on `main`, `GrantControl` lets anyone take the wheel when the room has no driver or
the lease has lapsed (§0). Without this rule a viewer waits for a quiet room, grants control to themself, then
steers the agent. Viewers can watch; they cannot touch.

**Finding a person who is not in `users.yaml`.** A member who gets in only through an `owners.yaml` group has no
`users.yaml` entry. Their id is `u_` + 12 hex (above). `GrantControl` and `CreateHandoff` call `s.users.Get(toID)`
(§0), which knows only `users.yaml`, so today they answer `400 validation` for such a member: the group owner cannot be
given control and cannot receive a hand-off. PR 3 adds `Service.LookupPerson(ctx, id)` and uses it in both calls
(and wherever a "who can I pick" list is built):

1. `users.yaml` first (always a member, no session needed).
2. Else the newest **live** `sessions` row with `user_id = id` (not revoked, not past idle or absolute expiry). The
   person is returned with `name` from that row (§1.3), and is accepted only if the §2.1 gate and role rules give them
   the **member** role for the stored `groups`.
3. Else `400 validation` ("unknown user"). A viewer or auditor-only principal is also `400 validation`, because
   control and hand-offs are member actions.

Limit, on purpose: a group-only member must have logged in and be live. A person who has never logged in, or whose
session ended, cannot be picked. That is the safe side: nobody gets the driver role without a current identity.
`LookupUser` stays `users.yaml`-only; the dev-mode `authenticate` path keeps using it.

### 2.2 Groups

Groups come from the claim named by `PIPEROOM_OIDC_GROUPS_CLAIM`. `groups` is not a standard OIDC claim: Okta and
Keycloak need a mapper, Azure sends object ids and **drops the claim on overage** (too many groups), Google sends
none (map Google users by `users.yaml`).

Group **names** can be forged where ordinary users may create groups (Entra and Okta allow this in some tenants):
anyone could make a group called `dataeng-oncall`. So a `group:` value in `owners.yaml` should be the IdP's group
**id** wherever the claim carries ids (Entra object ids), and the pilot docs say "turn self-service group creation
off" for IdPs that send names. The server compares the claim values as strings; it does not know which kind it got.

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
`ValidateUsers` (`internal/room/users.go`) checks the two files in both directions until `users.yaml` `layers` is
removed:

- **owners → users** (every owner listed must exist and list the layer): **only for a layer whose principals are all
  `user:`**. The server does not know group membership at start-up, so a layer that lists any `group:` is skipped
  with one Info log.
- **users → owners** (revision 5, security audit A-AP-3): **always**, groups or not. Every layer a `users.yaml` user
  lists must name that same user as a `user:` principal in `owners.yaml` (in `layers:`). A layer
  `consumer: [user:kabir, group:consumers]` with `mallory: layers: [consumer]` refuses to start: the server cannot
  vouch for group membership at start-up, which is exactly why a `users.yaml` claim must not stand in for it.
- `authorizeLayer` applies the same rule per request, so it holds even for a config that never went through the start
  check: with an `owners.yaml` loaded, a `users.yaml` layer grants nothing that `owners.yaml` does not grant the same
  user by name.

**`/api/me.layers` stays.** `docs/api.md:103` returns `{id,name,layers}` and the UI gates approve buttons on it
(`web/src/context/UserContext.tsx:40`). `layers` becomes "the layers this session may approve", computed per
request from `user:`, `group:`, backup and solo. The UI needs no change. `/api/me` also gains `role` (§2.1) and
`groups_missing` (§2.2).

**`Owners.Layers` and `LayerOwner`.** `owners.Layers` is `map[string]string` today (`owners.go:39`) and
`LayerOwner` (a contract method, `topology/graph.go:70-71`) feeds the owner label on topology nodes
(`topology/builder.go:194`, `demo.go:220`). The map becomes `map[string]Principals`, whose YAML form is a string
or a list. `LayerOwner` keeps its signature and returns a **display string**: the principals joined by `, `, with
the `user:` prefix dropped (`[user:arjun, group:dataeng-oncall]` shows as `arjun, group:dataeng-oncall`). It is a
label; no right comes from it.

**Backup approver:** may approve at once; the event records `grant: "backup:…"` and the room gets a system message
naming the backup. Escalation timers are T6 later work.

## 3. Approval integrity (F2+)

### 3.1 What is hashed

At `CreateProposal`, after `decodeStepParams` and `a.Validate` have run and **after** `p.Steps` is marshalled for the
row (`service_proposals.go:131`), compute `params_hash` = hex SHA-256 of the **RFC 8785 (JCS)** form of:

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
  any layer of the room's pipeline, or (b) an `auditors:` principal. **The driver is not on the list.** Revision 2
  had "the current driver" there; on `main` anyone can become driver of a quiet room (§0), so that clause was a
  door, not a rule. A driver who needs the file goes in `auditors:`. A viewer cannot export unless they are also in `auditors:`; an auditor-only principal is a viewer plus export (§2.1).
  **Accepted (lead, 2026-10-13):** a member who is not an owner, not a backup and not an auditor cannot export either.
  Today any `users.yaml` user can; from PR 4 such a member must be added to `auditors:` to export. Pilots are told
  in `deploy/README.md` (PR 2, A1 scope) so nobody finds out when the first export returns `403`. In dev mode (a)
  uses `users.yaml` `layers`. The log line in `export.go:57` (who exported what) stays.
- Content: `proposals[]` gains `params_hash`; `approvals[]` gains `params_hash` and `identity`; `action_results[]`
  gains `params_hash`. The `proposals[]` entry already carries the frozen `title`, `risk`, `rollback`,
  `required_layers` and `steps`, so an auditor can rebuild the §3.1 object and re-hash it. The export's own JCS
  SHA-256 over the whole document stays; its number rule is the same as §3.1.
- Retention deletes whole closed rooms after `PIPEROOM_RETENTION_DAYS`. The export is the only copy after that, so
  a pilot with an audit need exports before the cutoff.

## 4. Machine routes

- `POST /api/alerts/{source}` (merged, f4ce4b5) stays signature/token-based, outside session auth. Cookies are
  ignored there. **Lead decision (2026-10-13, PR #61):** the signed `POST /api/alerts/{source}` routes are exempt from the Host allow-list (§1.5): the signature is their auth, DNS rebinding cannot forge it, and in-cluster senders use internal DNS names. The unsigned dev `/api/alerts/webhook` stays under the Host check.
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
- `--allow-remote` stays for **one release as a deprecated alias** of the variable: it sets the same switch and logs
  "`--allow-remote` is deprecated, set `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`". Without the alias a pilot that runs the
  binary with the flag fails with "flag provided but not defined". The flag is removed in the release after.
- **A reverse proxy needs `PIPEROOM_AUTH_MODE=oidc`.** Dev mode has no way to trust a proxy's identity header, and
  from PR 2 the dev login is a cookie from `POST /auth/dev-login {user}` (§5.2), which a proxy cannot force. The
  revision 2 rule "refuse dev requests that carry `X-Forwarded-For`" is **deleted**: the image is dev mode and the
  pilot docs send pilots behind a proxy, so that 403 would have locked out every proxied request while giving no
  real protection (a proxy may not add the header). The listen check is the guard. What replaces it is a rule on
  `X-User` itself, which a proxy must send to work: `403 proxy_identity` (§5.2).
- `--demo` forces dev.
- Docker image (A1): `ENV PIPEROOM_AUTH_MODE=dev PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` replaces the `--allow-remote`
  entrypoint flag. The comment block at `Dockerfile:57-62` is rewritten: "do not publish this port; a reverse proxy
  needs `PIPEROOM_AUTH_MODE=oidc`". The compose file keeps `PIPEROOM_BIND=127.0.0.1` as the host-side default and
  adds `PIPEROOM_AUTH_MODE`, `PIPEROOM_DEV_ALLOW_NONLOOPBACK`, `PIPEROOM_ALLOWED_HOSTS`, `PIPEROOM_TRUSTED_PROXIES`,
  `PIPEROOM_OIDC_*` and `PIPEROOM_AUTH_*` to the pass-through list so a pilot can switch to `oidc` without an image
  change. The image states its mode explicitly so a future default change cannot silently lock the demo path.
- A1 also updates every doc that names `--allow-remote` or the proxy path: `deploy/README.md:9-23,229`,
  `docs/pilot/setup.md:63,271,311,382-390`, `docs/pilot/README.md:68-70`.
- `PIPEROOM_AUTH_MODE=oidc`: needs issuer, client id, client secret (or the public-client flag) and an `https`
  `PIPEROOM_PUBLIC_URL`, else refuse to start. `PIPEROOM_DEV_ALLOW_NONLOOPBACK` is ignored there, with a Warn.

### 5.2 Dev login

`POST /auth/dev-login {user}` issues a **real session** (`method: "dev"`) for a `users.yaml` user. `UserSwitcher`
calls it, so dev and prod share the cookie path. `X-User` stays for curl only, dev mode only.

**A proxy that forces `X-User` must not be able to use dev login.** The hole in revision 3: a proxy pilot upgrades to
PR 2. The proxy still forces `X-User: bob`, the image is still dev mode with `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`, and
bob calls `POST /auth/dev-login {user:"priya"}`. He gets priya's cookie, and a cookie beats `X-User`, so from then on
he is priya, silently. The fix is two checks in dev mode. Both answer `403 proxy_identity` with the message
`a reverse proxy needs PIPEROOM_AUTH_MODE=oidc or tag pre-auth`:

- (a) `POST /auth/dev-login` on a request that carries an `X-User` header (any value).
- (b) **any** request that carries a valid session cookie and an `X-User` that names a **different** user than the
  session's `user_id`. (A cookie that is not valid gets the normal `401 session_expired`.) The WebSocket upgrade
  counts as a request.

The key is `X-User` only, **not** `X-Forwarded-For`: a proxy may not add that header, but it has to send `X-User`.
So `curl` with no cookie keeps working: `X-User` with no cookie is read as today, `POST /auth/dev-login` without
`X-User` works, and `X-User` equal to the cookie's user passes. With no mismatch the cookie still wins over `X-User`.
Result for the upgraded proxy pilot: bob cannot get a cookie at all and every cookie he somehow holds fails (b), so he
stays bob, and the 403 message tells the operator what to do. This is not a defence against a user who can reach the
port without the proxy; the listen check and the loopback default are for that (§5.1).

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

**Proxy pilots between PR 2 and PR 3.** Today's docs send pilots behind an authenticating proxy that sets `X-User`
and forces `?user=`. PR 2 removes `?user=` and moves the dev login to a cookie the proxy cannot force; OIDC arrives
only in PR 3. So: **a pilot on the proxy path stays on the old code until PR 3 lands.** There is no image-publishing pipeline:
pilots build from source with `deploy/docker-compose.pilot.yml`. The lead puts a git tag `pre-auth` on the last
`main` commit before PR 2 merges (agents do not touch `main`), and A1 says in `deploy/README.md` that proxy pilots
build from tag `pre-auth` until PR 3 (lead decision, 2026-10-13, §9 row 8). Local, VPN and SSH-tunnel pilots are
not affected; their users pick themselves in `UserSwitcher` as today. PR 2 does not keep `?user=` as a fallback:
with dev-login any proxied user could pick any name, so the fallback would only look like protection. For a proxy
pilot that upgrades anyway, dev mode answers `403 proxy_identity` (§5.2) instead of issuing a cookie.

## 6. Config fields (contract, `internal/config/config.go`)

| Env var | Default | Meaning |
|---|---|---|
| `PIPEROOM_AUTH_MODE` | `dev` | `dev` \| `oidc` |
| `PIPEROOM_DEV_ALLOW_NONLOOPBACK` | `""` | `1` lets dev mode listen on a non-loopback address (loud warnings); replaces `--allow-remote` |
| `PIPEROOM_PUBLIC_URL` | `""` (exists) | `https` origin in oidc mode; redirect URI = this + `/auth/callback`; its host joins the Host allow-list (exact). In dev mode the host joins only with `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` on a non-loopback address (§1.5) |
| `PIPEROOM_ALLOWED_HOSTS` | `""` | comma list of extra `Host` values (`host` or `host:port`, compared as written) for a proxy or tunnel that rewrites `Host` (§1.5) |
| `PIPEROOM_TRUSTED_PROXIES` | `""` | comma list of CIDRs; `X-Forwarded-For` is read only from these peers, for the `/auth/login` rate limit and the request log (§1.1). **Used only in `oidc` mode**; dev mode keys on `RemoteAddr`. The lead adds the field in the PR 1 contract commit |
| `PIPEROOM_OIDC_ISSUER` | `""` | issuer URL |
| `PIPEROOM_OIDC_CLIENT_ID` | `""` | |
| `PIPEROOM_OIDC_CLIENT_SECRET` | `""` | secret, never logged; required unless the next flag is set |
| `PIPEROOM_OIDC_PUBLIC_CLIENT` | `""` | `1` allows an empty secret (PKCE only) |
| `PIPEROOM_OIDC_SCOPES` | `openid email profile` | space separated |
| `PIPEROOM_OIDC_GROUPS_CLAIM` | `groups` | `""` = no group rights |
| `PIPEROOM_AUTH_VIEWER_GROUPS` | `""` | comma separated; viewer role (§2.1): GET (not export) and WebSocket only, every non-GET under `/api` is `403 viewer` |
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
| `login_states` filling | One row per cookie, 10 min expiry, large per-IP limit (§1.1) |
| Anonymous login lockout (small per-IP limit on `/auth/login`) | 300/min per client IP; client IP from `X-Forwarded-For` only behind `PIPEROOM_TRUSTED_PROXIES` (§1.1) |
| Viewer becomes driver, then exports or steers | Viewer role is default-deny: every non-GET under `/api` is `403 viewer` (this includes `POST /api/handoffs/{hid}/accept`); driver dropped from the export list (§2.1, §3.4) |
| An `auditors:` entry becomes a member who can steer | `auditors:` gives export only; an auditor-only principal is a viewer (§2.1) |
| Proxy pilot upgrades; the proxy forces `X-User: bob`; bob calls `POST /auth/dev-login {user:"priya"}` and keeps priya's cookie | Dev mode: `403 proxy_identity` when `dev-login` carries `X-User`, or when a cookie and an `X-User` for a different user arrive together. Keyed on `X-User`, not `X-Forwarded-For`. The real fix is `PIPEROOM_AUTH_MODE=oidc` (§5.2) |
| Open redirect via `return` | `url.Parse`, empty scheme and host (§1.1) |
| IdP outage | Live sessions keep working; new logins get `503 idp_unavailable`; discovery retried, not fatal at start |
| Clock skew | Session expiry uses server time; ID token `exp` checked once at login; NTP required |
| Replayed approvals | Live session + CSRF pass; one decision per layer (`already_decided`); `params_hash` mismatch → `409` |
| Edited proposal row | Executor re-hash (accidents); export copy outside the DB (admin); signed approvals later (§3.3) |
| Stale groups / recycled email | Groups refresh at login (worst case 12 h); `iss`+`sub` binding in `identities` |
| Solo mode on a multi-owner install | Refuse to start (§5.3) |
| Export by a viewer or a plain member (F3 data) | Export limited to approvers, backups and auditors (§3.4) |
| Proxy rewrites `Host` | `PIPEROOM_ALLOWED_HOSTS` (§1.5); never a wildcard |

## 8. PR split

| PR | Owner | Scope | Done when |
|---|---|---|---|
| 1 | Lead (contract) | `config.go` fields (§6, including `PIPEROOM_ALLOWED_HOSTS` and `PIPEROOM_TRUSTED_PROXIES`; the lead adds `PIPEROOM_TRUSTED_PROXIES` in the lead contract commit, oidc mode only); `events.go` `ParamsHash` on `ProposalCreatedPayload` and `ActionExecutedPayload`, `ParamsHash` + `Identity` on approval payloads; `docs/api.md`: auth, `/auth/*`, approve body, `409 params_mismatch`, `421 bad_host`, `403 viewer`, `/api/me` keeps `layers` and gains `role` + `groups_missing`, a new row for `GET /api/rooms/{id}/export` with the §3.4 access rule (there is none today), WS without `?user=` | `go build ./...` passes; api.md matches this doc |
| 2 | A5 + A6 + A1 | Sessions, mode-specific cookie names, `/auth/dev-login`, `/auth/logout`, cookie WS, `?user=` removed, Host allow-list (loopback any port, public host exact, `PIPEROOM_ALLOWED_HOSTS`), `CrossOriginProtection`, `PIPEROOM_DEV_ALLOW_NONLOOPBACK` with `--allow-remote` as a deprecated alias, webhook dev-only, `403 proxy_identity` (§5.2), cookie wins over `X-User` when both name the same user. **A6 first:** Vite proxy (`/api`, `/auth`, `ws: true`), same-origin client, cookie client, `UserSwitcher` via dev-login, 401 redirect; **then** A5 deletes `cors` and the expose header. A1: Dockerfile env + comment, compose pass-through, `deploy/README.md` and the pilot docs listed in §5.1. The `deploy/README.md` text lists: pilots build from source with `deploy/docker-compose.pilot.yml`; proxy pilots build from git tag `pre-auth` (made by the lead on the last `main` commit before PR 2); from PR 4 members who are not owners or auditors cannot export and must be added to `auditors:` | `X-User` in oidc mode → 401; dev on `0.0.0.0` without the switch refuses start; with the switch or the old flag warns; `Host: evil.test` → 421; `Host: localhost:5173` and `Host: 127.0.0.1:9000` → 200; WS without cookie → 401; cross-origin POST → 403; dev: `POST /auth/dev-login` with `X-User` → `403 proxy_identity`; dev: cookie for priya plus `X-User: bob` → `403 proxy_identity`; cookie for priya plus `X-User: priya` → 200; `curl` with `X-User` and no cookie → 200; `curl` `dev-login` with no `X-User` → 200; `X-Forwarded-For` alone changes nothing; logout closes WS; `npm run dev` works through the proxy; the pilot image starts; `deploy/README.md` says: "Pilots build from source with `deploy/docker-compose.pilot.yml`. Proxy pilots build from the git tag `pre-auth` until PR 3." It also says that members who are not owners or auditors cannot export and must be added to `auditors:` |
| 3 | A5 + A3 + A1 | OIDC login (§1.1–1.2 order, nonce in constant time, `return` check on the decoded path, 300/min limit with trusted proxies), `identities` + unbind helper, login gate at login and per request, secret rule, **roles** (§2.1: default-deny `403 viewer` on every non-GET under `/api`, including `POST /api/handoffs/{hid}/accept`; `auditors:` gives export only), **`Service.LookupPerson`** backed by `sessions` (§2.1) used by `GrantControl` (`service_steer.go:165`) and `CreateHandoff` (`service_handoff.go:46`), `sessions.name`, rights from `owners.yaml` principals, `/api/me.layers` computed per request, groups + overage message, backup, solo start check, `ValidateUsers` group rule. A3: `internal/owners` `Principals` type, per-pipeline `owners:`, `auditors:`, `LayerOwner` display string. A1: fake IdP (`httptest`) in `internal/testenv`, `devctl identities unbind` | With the fake IdP: bad nonce/state → 401; callback without the login cookie → 400; replayed callback → 400; unmapped user → 403; viewer `POST …/control/grant` → `403 viewer`; viewer `POST /api/handoffs/{hid}/accept` → `403 viewer`; viewer GET and WS → 200; viewer GET export → `403 not_allowed`; auditor-only principal: GET → 200, `POST …/steer` → `403 viewer`, export → 200; a member who got in only through an `owners.yaml` group (not in `users.yaml`, live session) can be granted control and can receive a hand-off; the same call for a viewer, an auditor-only principal, a person with no live session, or an unknown id → `400 validation`; group owner approves; `/api/me.layers` lists the group's layer; missing groups claim → 403 with the overage message; removed `users.yaml` user → 401 on the next request; solo + other owners → no start |
| 4 | A5 + A6 | `params_hash` compute/store/check (§3.1 object, number rule), executor re-check, `action.executed` hash, approval identity columns, export access (approvers + auditors, no driver) + fields (§3.4). A6: show params + hash, send hash, solo confirm box | Changed params → `409`; DB-edited row → executor refuses; params with `2^53+1` → `400`; viewer export → `403`; member-only driver export → `403`; plain member (not owner, not auditor) export → `403`; auditor export → 200; export row re-hashes to `params_hash` |

## 9. Decisions

Rows 1–6: the critique recommended these defaults. They are written into this design; the founder confirms or changes them.

| # | Question | Proposed | Why |
|---|---|---|---|
| 1 | `PIPEROOM_AUTH_MODE` default | `dev`, with the loopback guard and `PIPEROOM_DEV_ALLOW_NONLOOPBACK` for containers | `go run` and `devctl` stay zero-config; remote exposure is a start failure or a loud warning, never silent |
| 2 | Session lengths | 8 h idle / 12 h absolute, both configurable | One shift; 12 h also bounds stale group membership; regulated pilots set 1 h / 8 h |
| 3 | Backup approver | Immediate, with a room system message and `grant: "backup:…"` | On-call needs it now; the audit trail shows the backup path; N-minute timer in T6 |
| 4 | Step-up before approvals | Later; `auth_time`, `amr`, `acr` stored now | No regulated pilot in the next 8 weeks; the columns make it a no-migration add |
| 5 | New module `go-oidc` v3.21.0 | Yes | Hand-rolled JWKS/JWT checking is the riskier path; API verified; Apache-2.0. `jcs` is already in `go.mod` |
| 6 | Scheduled export to storage outside the DB (§3.3) | **Not in the pilot.** Documented as a pilot-side cron (an auditor token calling the export route every N hours into write-once storage) | Needed only for pilots that must detect a DB admin; no pilot in the next 8 weeks asks for it; the route and hash header already exist, so nothing in the server has to change |

Rows 7–9 are **lead decisions, 2026-10-13**. They answer the open questions that ended revision 3. They are final and
are written into every section they touch.

| # | Question | Decision (lead, 2026-10-13) | Where |
|---|---|---|---|
| 7 | Keep `PIPEROOM_TRUSTED_PROXIES`? | **Kept.** Used only in `oidc` mode, for the `/auth/login` rate limit and the request log. Dev mode keys on `RemoteAddr`. The lead adds the config field in the PR 1 lead contract commit. | §1.1, §6, §8 (PR 1) |
| 8 | Image tag `piperoom:pre-auth`? | **No image pipeline.** Pilots build from source with `deploy/docker-compose.pilot.yml`. Every mention of the image tag becomes a git tag `pre-auth` on the last `main` commit before PR 2. Proxy pilots build from tag `pre-auth` until PR 3. PR 2's `deploy/README.md` says so. | §5.4, §8 (PR 2), §10 |
| 9 | Members who are not owners or auditors and export? | **Accepted.** They cannot export. To export, add them to `auditors:`. Said in §3.4, in PR 2's `deploy/README.md` text list (A1 scope) and in `docs/security/findings.md` F3. | §2.1, §3.4, §8 (PR 2, PR 4) |

## 10. Rejected or narrowed (revision 3)

Each line is one critic item that is not applied as written, and why. No open questions are left; the three that
remained were decided by the lead on 2026-10-13 (§9 rows 7–9).

- **Security S1, "control request/grant and room create need an `owners.yaml` principal of the room's pipeline":
  narrowed.** Those actions need the **member** role (§2.1), not an owner principal. Today every `users.yaml` user
  can drive, and an on-call engineer who owns no layer still has to steer and open rooms. The escalation in S1 came
  from viewers; viewers now cannot POST at all, which closes it. Tying the driver to owner principals would also
  make `owners.yaml` the room-membership list, which it is not.
- **Fit F1 option (a), "dev mode keeps `X-User` and `?user=` as fallbacks until PR 3": rejected for option (b).**
  With `POST /auth/dev-login {user}` any proxied user picks any name, so the proxy no longer controls identity. A
  fallback would only look like protection. Proxy pilots build from git tag `pre-auth` until PR 3 (§5.4).
- **Fit nice-to-have, "drop the IP limit": not dropped, raised.** Without any per-IP limit one client can create one
  row per call with a fresh cookie each time. 300 per minute per client IP keeps that bounded without locking anyone
  out (§1.1).
