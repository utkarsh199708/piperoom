# Design: real login (replacing `X-User`)

Task C9, part 1 of 3 (see also `webhook.md`, `retention.md`). Design only — no code changed.
Closes the design gap behind **F1** (critical) and the production notes in `threat-model.md` §4.3.
Written against `main` @ `270377c`. `internal/httpapi` (A5) is still unmerged, so file names below are
proposals for A5, not descriptions of existing code.

## 1. Goals and non-goals

**Goals**
1. The server decides who a caller is and which layers they own. The client never supplies either.
2. An approval is attributable to one authenticated human, with a recent proof of presence.
3. One deployment can serve more than one customer without rooms or data crossing over.
4. Local dev stays one command (`dev/users.yaml`), but cannot be switched on in production by accident.

**Non-goals (v1):** passwords or MFA inside Piperoom (the customer's IdP does that), fine-grained
per-room ACLs, SCIM provisioning (we re-read group claims at login instead, §5).

## 2. Decision

**OpenID Connect, Authorization Code + PKCE, handled by the Go server (backend-for-frontend).**
The browser never holds an access token. After login the server sets an opaque session cookie and
every API call and WebSocket upgrade is authenticated by that cookie.

Rejected: JWT-in-localStorage (XSS steals it; cannot be revoked); SAML-only (OIDC covers Okta, Entra,
Google Workspace; add SAML later through a broker such as Dex if a customer needs it); client-supplied
header behind a trusted proxy (works only if every deployment has one, and it silently fails open).

## 3. Login flow

```
browser ── GET /auth/login ──────────────► server: create state, nonce, PKCE verifier (stored server-side,
                                                    10 min TTL, bound to a short-lived pre-login cookie)
        ◄── 302 IdP /authorize ───────────
        ── user signs in at IdP (MFA is the IdP's job) ──
        ── GET /auth/callback?code&state ─► server: check state, exchange code with verifier,
                                                    verify ID token (iss, aud, exp, nonce, signature via JWKS),
                                                    map claims → user + tenant + layers (§4),
                                                    create session row, Set-Cookie
        ◄── 302 /  ───────────────────────
```

Routes (all new; add to `docs/api.md` through the leads): `GET /auth/login`, `GET /auth/callback`,
`POST /auth/logout`, `GET /api/me` (already exists; now returns `{id,name,tenant,layers,auth_age_s}`).

Library: `github.com/coreos/go-oidc/v3` + `golang.org/x/oauth2` (both standard choices; confirm every
symbol with `go doc` per AGENTS.md rule 9). Do not hand-roll JWT verification.

## 4. Identity, tenant and layer mapping

| Thing | Source | Notes |
|---|---|---|
| user id | `iss` + `sub` from the ID token | Stable key. Email and name are display only — emails get reused. |
| tenant | configured per issuer (`tenants.yaml`: issuer → `tenant_id`), or the `hd`/org claim | One IdP issuer = one tenant. Never taken from a request field. |
| layers a user may approve | **server-side** mapping from IdP group → layer, in the same file family as `owners.yaml` | e.g. group `data-platform-kafka` → `[kafka-connect, kafka, schema-registry]`. Unmapped groups grant nothing. |

Rules:
- Layer membership is resolved at login and again on refresh (§5), then stored on the session. The approve
  endpoint reads layers from the session, **never** from the request body. `{layer}` in the body only
  names which layer the caller is approving *as*, and must be in the session's set.
- Every room, proposal, hand-off and event row carries `tenant_id`; every query filters on the session's
  tenant (F7). A missing filter is a bug the tests in §10 must catch.
- A user with no mapped layer can still watch, comment and suggest; they can never approve.

## 5. Sessions

- Cookie: `__Host-piperoom_session`, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, no `Domain`.
- Value: 32 random bytes, base64url. Postgres stores only `sha256(value)` in a `sessions` table
  (`id_hash, user_key, tenant_id, layers, created_at, last_seen_at, expires_at, authenticated_at`),
  so a DB read leak does not leak live sessions.
- Idle timeout 8 h (an incident shift), absolute timeout 12 h, then full re-login. Both configurable
  downward only for v1.
- Re-resolve group claims when the session is older than 1 h, using the IdP's refresh token or a silent
  re-auth. If the user lost a group, their layers shrink immediately; if the IdP is unreachable, keep the
  old set until the absolute timeout (availability during an incident outranks instant revocation) and
  log a warning.
- `POST /auth/logout` deletes the row. An admin can delete all of a user's sessions (offboarding).
- No refresh/access tokens are exposed to the browser or written to logs.

## 6. CSRF and CORS

Cookies make CSRF real. Layers of defence, all required:
1. `SameSite=Lax` cookie.
2. Every state-changing request (`POST`) must carry `X-Piperoom-CSRF: 1`. Browsers cannot add a custom
   header cross-site without a CORS preflight, which we refuse.
3. Server checks `Origin` (falling back to `Referer`) equals the configured public origin on every
   `POST` and on the WebSocket upgrade.
4. CORS: allow exactly one origin (`PIPEROOM_PUBLIC_URL`), `Access-Control-Allow-Credentials: true`,
   never `*`. In dev the allowed origin is `http://localhost:5173` as the A5 task already says.

## 7. WebSocket

`GET /api/rooms/{id}/ws` authenticates with the same cookie. The `?user=<id>` parameter is removed in
production mode. The upgrade handler must:
- verify the `Origin` header against `PIPEROOM_PUBLIC_URL` (`coder/websocket` checks origin by default;
  A5 must not set `InsecureSkipVerify` or add wildcard `OriginPatterns` — add a test that a foreign origin
  gets 403);
- verify the session's tenant owns the room;
- close the socket when the session expires or is deleted (check on the 20 s presence tick).

## 8. Approvals need fresh proof

`POST /api/proposals/{pid}/approve` and `/reject` require:
- session layer set contains `layer`;
- `now - authenticated_at <= 15 min`. If older, return `401 reauth_required` and the UI sends the user
  through `/auth/login?prompt=login&return=...` (OIDC `prompt=login`, `max_age=0`). The approval is
  re-submitted after.
- the proposal's current params hash equals the hash shown to the approver (the F2 immutability fix);
  a changed proposal resets approvals.

The `approval.given` event payload records `user_key` (iss+sub), display name, `auth_time`, and a session
id *hash prefix* — enough to answer "who approved this, and how recently did they log in" months later.
Add the same actor identity to `action.executed`.

## 9. Dev mode and fail-closed rules

- New env var `PIPEROOM_AUTH` = `oidc` (default) or `dev`. `dev` keeps `X-User` / `?user=` and
  `dev/users.yaml`. (`internal/config/config.go` is a contract file: this is a **contract change
  request** — add `AuthMode`, `PublicURL`, `OIDCIssuer`, `OIDCClientID`, `OIDCClientSecretFile`,
  `TenantsFile`. The client secret is read from a file or secret manager, never a plain env var that
  ends up in process listings.)
- `dev` refuses to start unless `PIPEROOM_HTTP_ADDR` binds loopback, and logs a loud banner. A production
  deployment that forgets to set `oidc` therefore fails to boot rather than running open.
- The web `UserSwitcher` is compiled out of production builds (`import.meta.env.DEV` guard) — A6,
  see F1. In `oidc` mode `/api/me` is the only source of identity in the UI.
- Missing or invalid session → `401 unauthenticated`, same body for every cause (no user enumeration).

## 10. Threats this addresses, and tests that prove it

| Threat | Control | Test (A5 unless noted) |
|---|---|---|
| Caller claims to be another user | identity only from session | request with forged `X-User` in `oidc` mode → 401 |
| Owner of layer A approves layer B | layers from session | approve `{layer: B}` as A-owner → 403 `not_layer_owner` |
| Cross-tenant read | `tenant_id` filter everywhere | tenant X session reads tenant Y room/event/ws → 404 |
| CSRF on approve | Lax + custom header + Origin | POST without header → 403; wrong `Origin` → 403 |
| Cross-site WebSocket hijack | Origin check | upgrade with `Origin: https://evil.example` → 403 |
| Stolen old session approves | 15 min step-up | approve with `authenticated_at` 20 min ago → `reauth_required` |
| Offboarded user keeps access | ≤1 h group refresh, admin session purge | removed group → layers empty after refresh |
| Session DB leak | store hashes only | unit test: plaintext cookie value never in any column |
| Dev auth left on | loopback-only guard | boot with `dev` on `0.0.0.0` → exits non-zero |
| Tokens in logs | redaction | log-capture test over a login round trip finds no `code`, `id_token`, cookie |

## 11. Rollout

1. Contracts (leads): `docs/api.md` auth section + `/auth/*` routes; `config.go` fields.
2. A5: `internal/auth` package behind an `Authenticator` interface (`Authenticate(*http.Request) (Principal, error)`);
   `dev` and `oidc` implementations; httpapi middleware injects `Principal{UserKey, Name, TenantID, Layers, AuthTime}`.
   Do dev first so A6 is not blocked.
3. A6: remove `UserSwitcher` from prod build; handle `reauth_required`.
4. Integration test against a local OIDC provider container (e.g. Dex in `dev/`, A1) so CI needs no external IdP.

## 12. Decisions needed from the leads

1. Confirm OIDC-first (SAML via broker later).
2. Confirm one IdP issuer = one tenant for the pilot.
3. Idle/absolute session timeouts (proposed 8 h / 12 h) and step-up window (proposed 15 min).
4. Who owns the IdP-group → layer mapping file: customer admin (self-serve) or us (pilot).
