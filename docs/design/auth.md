# Design: real login (replaces `X-User`)

Task C9, doc 1 of 3 (`webhook-auth.md`, `event-retention.md`). Design only, no code changed. Written against `main`
@ `8e326a3`+; unmerged A5 facts are cited "(a5 @ 935f17d)" from the lead's task file.
**Findings:** F1 (any caller can become any layer owner). Also gives F2/F10 their "who approved" (approver identity).

## 1. Problem

- Identity is a header the client chooses. `authenticate` reads `X-User` (a5 @ 935f17d, `internal/httpapi/server.go:102`); the
  WebSocket reads `?user=` (`internal/ws/gateway.go:115-118`). `docs/api.md:12,30` makes this the contract.
- Approval rights come from `dev/users.yaml` `layers` (`internal/room/users.go`, `service_proposals.go:114,175`, a5 @ 935f17d).
  `dev/owners.yaml` has a second `layers:` map (`internal/owners/owners.go:30,60`). The two can disagree. `docs/api.md:31-33`
  adds a start-up check for it.
- CORS hard-codes `http://localhost:5173` and allows `X-User` (`server.go:125-127`, a5 @ 935f17d); the web app sends `X-User`
  from `web/src/api/client.ts:34`. The dev-only `UserSwitcher` is already gated by `isDev` (`web/src/components/UserSwitcher.tsx`).
- `approval.given` / `approval.rejected` events carry only `Actor`; the `approvals` table has no approver column (a5 @ 935f17d).
  With a spoofable actor, the audit trail proves nothing.
- The default listener is now loopback (`config.go:27`), which only hides the problem.

## 2. Options

### 2a. Login flow

| | A. OIDC Authorization Code + PKCE, server-side (recommended) | B. Reverse proxy sets `X-User` (oauth2-proxy style) | C. Own passwords |
|---|---|---|---|
| Security | server verifies the ID token itself (OIDC Core 1.0 §3.1, §3.1.3.7); no trusted header | safe only if the app is unreachable except via the proxy; one mis-route = full bypass | we own hashing, reset, MFA |
| Effort | medium: one new module, ~500 lines | low code, high deploy coupling | highest |
| Dev-mode impact | dev flag keeps `X-User` | dev flag keeps `X-User` | needs seed users |
| Ops burden | needs an IdP client registration | needs a proxy per deployment | password/MFA support |

### 2b. Session cookie

| | A. Server-side session in Postgres (recommended) | B. Signed stateless cookie |
|---|---|---|
| Security | opaque random id; revoke by deleting the row; new id at login | cannot be revoked before expiry; key rotation logs everyone out |
| Effort | one table `sessions` | no table, but key management |
| Ops burden | a row per login (small) | a signing key to protect |

### 2c. CSRF defence

| | A. `net/http.CrossOriginProtection` + same origin in production (recommended) | B. Signed double-submit token |
|---|---|---|
| Security | rejects unsafe cross-origin requests by `Sec-Fetch-Site`, else `Origin` vs `Host`; allows requests with neither header (non-browser or same-origin) | works on any browser, but every client must send the token |
| Effort | a few lines (`go doc net/http.CrossOriginProtection`; `AddTrustedOrigin` for the dev web origin) | token issue + check + web change |
| Gap | safe methods are never checked, so the WebSocket `GET` needs its own `Origin` check (see 2d) | none |

### 2d. WebSocket auth

| | A. Session cookie on the handshake + `OriginPatterns` (recommended) | B. One-time ticket in the URL | C. Token in the first message |
|---|---|---|---|
| Security | no secret in the URL; Origin allow-list blocks cross-site hijack (OWASP WebSocket cheat sheet) | URL ticket is short-lived but is logged by proxies | server accepts an unauthenticated socket until the message |
| Effort | same cookie code as REST | ticket store | handshake state machine |
| Needs | production web and API on one origin | | |

### 2e. User → layer owner

| | A. `owners.yaml` `layers:` is the source, mapped to IdP identity (recommended) | B. IdP groups |
|---|---|---|
| Security | one reviewed file in git; no dependence on provider claims | `groups` is not a standard OIDC claim; its name and shape differ per provider |
| Effort | small: drop `layers` from users file, derive it | per-provider config, and group → layer mapping still needed |
| Ops burden | edit a file to change an owner | change in the IdP, but outside git review |

## 3. Recommendation

1. **OIDC Authorization Code with PKCE**, handled by the Go server. Redirect to the IdP with `state`, `nonce` and a PKCE
   challenge; on callback exchange the code, verify the ID token (signature, `iss`, `aud`, `exp`; **nonce checked by us**), then create a session.
   Go: `golang.org/x/oauth2` `GenerateVerifier`, `S256ChallengeOption` (auth URL), `VerifierOption` + `Config.Exchange` (callback);
   `github.com/coreos/go-oidc/v3/oidc` `NewProvider`, `Provider.Verifier(&oidc.Config{ClientID: ...})`, `IDTokenVerifier.Verify`,
   `IDToken.Claims`, `IDToken.Nonce`. (`go doc` confirms `Verify` does **not** check the nonce.) go-oidc is a **new module**; oauth2 comes with it.
2. **User id = `iss` + `#` + `sub`.** OIDC Core §5.7 says email MUST NOT be the identifier. `users.yaml` maps `oidc: {iss, sub}` to the internal user id.
3. **Server-side session** in a new `sessions` table: `id` = 32 random bytes (store only SHA-256), `user_id`, `created_at`, `last_seen_at`.
   Cookie `__Host-piperoom_session`: `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, no `Domain` (OWASP Session Management).
   Idle timeout 8 h, absolute 12 h (code constants, adjustable in review). **A new session id is issued at login.** Logout deletes the row.
4. **Pre-login cookie** `__Host-piperoom_login` (an opaque id; state, nonce, PKCE verifier and return path sit in a `login_states` row, 10 min) must be **`SameSite=Lax`, not `Strict`**:
   the IdP returns the user with a cross-site top-level redirect, and `Strict` cookies are not sent on it.
5. **CSRF:** `http.CrossOriginProtection` around every route except `/auth/callback` and the alert webhook. In production the web build is served from
   the **same origin** as the API (reverse proxy or Go `embed`), so no CORS is needed. If they ever stay cross-origin: `fetch(..., {credentials:'include'})`,
   `Access-Control-Allow-Credentials: true`, one explicit `Access-Control-Allow-Origin` (never `*`), and `AddTrustedOrigin` for it.
6. **WebSocket:** cookie on the handshake; `websocket.AcceptOptions.OriginPatterns` = host of `PIPEROOM_PUBLIC_URL` only; `?user=` is read in dev mode only.
   Close the socket when the session row is gone (checked on the 20 s presence tick).
7. **Layers:** the approver's layers = `{ l : owners.yaml layers[l] == user.id }`. `users.yaml` `layers` becomes deprecated. The `approve` check uses this set.
8. **Approver identity in the log:** the actor of `approval.given` / `approval.rejected` is the session user, and both payloads gain `subject` (`iss#sub`).
9. **Dev mode:** `X-User` / `?user=` work only when `PIPEROOM_DEV_AUTH=1` **and** the listener host is loopback; otherwise the server exits at start-up.
   `devctl` sets the flag. Production builds omit `UserSwitcher` (Vite replaces `import.meta.env.DEV` at build time; `isDev` is already used).
10. **Dev IdP:** no container image is pinned here because registry tags could not be checked from this sandbox. A1 builds a **fake IdP with
    `httptest`** (discovery, JWKS, authorize, token endpoints) in `internal/testenv`, used by A5's tests.

## 4. Contract changes

`docs/api.md` (Auth section, WebSocket line):
```diff
--- a/docs/api.md
+++ b/docs/api.md
@@ -7,9 +7,15 @@
 real login yet). All bodies are JSON. Times are RFC 3339 **UTC** (`Z`), also when read back from Postgres.
 Arrays in responses are always JSON arrays (`[]`), never `null`.
 
-## Auth (MVP)
+## Auth
 
-No SSO yet. Every request sends `X-User: <user id>`. Users come from `dev/users.yaml`:
+Production: OIDC login (`docs/design/auth.md`). `GET /auth/login` starts it, `GET /auth/callback` finishes it and sets
+the session cookie `__Host-piperoom_session`, `POST /auth/logout` ends the session. The server derives the user from
+the cookie; the client sends no identity. No valid session → `401 unauthenticated` and the web app goes to `/auth/login`.
+
+### Dev mode (`PIPEROOM_DEV_AUTH=1`, loopback listener only)
+
+Every request sends `X-User: <user id>`. Users come from `dev/users.yaml`:
 
 ```yaml
 users:
@@ -27,7 +33,8 @@
     layers: [consumer]
 ```
 
-Unknown or missing `X-User` → `401`. The WebSocket takes `?user=<id>` instead (browsers cannot set headers on WS).
+In dev mode, unknown or missing `X-User` → `401`. The WebSocket takes `?user=<id>` instead (browsers cannot set headers on WS).
+Outside dev mode both are ignored.
 `dev/users.yaml` (who may approve a layer) and the `layers:` map in `dev/owners.yaml` (owner shown on the pipeline
 map) must agree; the server checks this at start-up and refuses to start on a mismatch. Real mode requires
 `USERS_FILE`; the built-in users are for `--demo` only. Real login is designed in `docs/design/auth.md` (C9).
@@ -127,7 +134,7 @@
 
 ## WebSocket
 
-`GET /api/rooms/{id}/ws?user=<id>&after=<seq>`
+`GET /api/rooms/{id}/ws?after=<seq>` (session cookie; the `Origin` header must be the web app's origin; dev mode adds `&user=<id>`)
 
 Server → client messages:
 ```json
```
`internal/events/events.go` (who approved):
```diff
--- a/internal/events/events.go
+++ b/internal/events/events.go
@@ -209,6 +209,7 @@
 type ApprovalGivenPayload struct {
 	ProposalID string `json:"proposal_id"`
 	Layer      string `json:"layer"`
+	Subject    string `json:"subject,omitempty"` // OIDC iss + "#" + sub of the approver; "" in dev mode
 }
 
 // ApprovalRejectedPayload is one layer owner's rejection.
@@ -216,6 +217,7 @@
 	ProposalID string `json:"proposal_id"`
 	Layer      string `json:"layer"`
 	Reason     string `json:"reason"`
+	Subject    string `json:"subject,omitempty"` // as ApprovalGivenPayload.Subject
 }
 
 // ActionExecutedPayload is the result of running one approved step.
```
`internal/config/config.go` (empty = off; the client secret is read from a file):
```diff
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -16,6 +16,11 @@
 	SchemaRegistryURL string // SCHEMA_REGISTRY_URL
 	OwnersFile        string // OWNERS_FILE
 	UsersFile         string // USERS_FILE
+	DevAuth           string // PIPEROOM_DEV_AUTH ("1" = accept X-User / ?user=, loopback listener only; "" = off)
+	PublicURL         string // PIPEROOM_PUBLIC_URL (https origin users open; used for cookies, redirect URI, Origin check)
+	OIDCIssuer        string // PIPEROOM_OIDC_ISSUER ("" = login off)
+	OIDCClientID      string // PIPEROOM_OIDC_CLIENT_ID
+	OIDCClientSecret  string // PIPEROOM_OIDC_CLIENT_SECRET_FILE (path to a file; never the secret itself)
 	Model             string // PIPEROOM_MODEL
 	Effort            string // PIPEROOM_EFFORT
 	// The Anthropic SDK reads ANTHROPIC_API_KEY itself; it is never stored here.
@@ -33,6 +38,11 @@
 		SchemaRegistryURL: get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
 		OwnersFile:        get("OWNERS_FILE", "dev/owners.yaml"),
 		UsersFile:         get("USERS_FILE", "dev/users.yaml"),
+		DevAuth:           get("PIPEROOM_DEV_AUTH", ""),
+		PublicURL:         get("PIPEROOM_PUBLIC_URL", ""),
+		OIDCIssuer:        get("PIPEROOM_OIDC_ISSUER", ""),
+		OIDCClientID:      get("PIPEROOM_OIDC_CLIENT_ID", ""),
+		OIDCClientSecret:  get("PIPEROOM_OIDC_CLIENT_SECRET_FILE", ""),
 		Model:             get("PIPEROOM_MODEL", "claude-opus-5-5"),
 		Effort:            get("PIPEROOM_EFFORT", "high"),
 	}
```
`dev/users.yaml` (additive): each user may gain `oidc: {iss: <issuer-url>, sub: <subject>}`. Existing keys stay.
The `webhook-auth.md` and `event-retention.md` config diffs each apply to `main` on their own; the three edit the same struct, so the lead merges them by hand.

## 5. Migration from today's dev mode

1. Ship the code with `PIPEROOM_DEV_AUTH` unset: the dev stack breaks unless `devctl` sets it. A1 updates `devctl` in the same week.
2. While `users.yaml` still has `layers`, keep the start-up agreement check from `docs/api.md:31-33`. When all users carry `oidc:`, drop `layers` and the check.
3. `--demo` keeps its built-in users and implies dev mode.
4. Existing events keep their `actor`; `subject` is empty for them. No data migration; add the `sessions` table in a new migration.
5. First production deploy: set `PIPEROOM_PUBLIC_URL`, `PIPEROOM_OIDC_*`; leave `PIPEROOM_DEV_AUTH` empty.

## 6. Tasks per owner

- **A5 (1)** `internal/auth`: OIDC login, callback, logout, `sessions` + `login_states` tables in one migration, session cookie, pre-login cookie.
- **A5 (2)** Replace `authenticate` with the session lookup; dev mode behind `PIPEROOM_DEV_AUTH` + loopback check at start-up; fail closed otherwise.
- **A5 (3)** Wrap routes in `CrossOriginProtection`; same-origin static serving; WS `OriginPatterns`; close WS on session end; remove `X-User` from CORS in production.
- **A5 (4)** Derive layers from `owners.yaml`; set `subject` on approval events; tests: forged `X-User` → 401, wrong layer → 403, foreign `Origin` on WS → rejected, expired session → 401.
- **A6 (1)** Handle `401`: navigate to `/auth/login`; drop the user id argument from `request()` outside dev; `/api/me` is the only identity source.
- **A6 (2)** Same-origin base URL in production (`VITE_API_BASE` empty); `credentials: 'include'` only if cross-origin; show the approver name from the event.
- **A1 (1)** `devctl` sets `PIPEROOM_DEV_AUTH=1`; add `oidc:` placeholders to `dev/users.yaml` (additive).
- **A1 (2)** Fake IdP (`httptest`) in `internal/testenv` for A5's login tests.

## 7. Open questions

1. Which IdP does the pilot customer use, and does it return a stable `sub`? (Needed to fill `users.yaml`.)
2. Timeouts: 8 h idle / 12 h absolute — confirm.
3. Re-authentication before an approval (OIDC `max_age` / `auth_time`): not specified here because OIDC Core §3.1.2.1 and §2 could not be fetched. Decide whether the pilot needs it, then verify against the spec.
4. Who may close or take over a session in an emergency (admin role)? Out of scope here.
5. Dev `users.yaml` still lists `schema-registry` under `arjun` while `docs/api.md:18-21` shows it under `priya`; A1 owns that file.
6. Multi-tenant (F7): one issuer = one tenant is assumed and not designed here.

## 8. References

Verified in this sandbox with `go doc`: `golang.org/x/oauth2` v0.37.0, `github.com/coreos/go-oidc/v3` v3.21.0, `net/http.CrossOriginProtection` (Go 1.27).
The pages below could not be fetched from here (network blocked); claims about them come from the lead's task file.
- OpenID Connect Core 1.0: https://openid.net/specs/openid-connect-core-1_0.html (§3.1, §3.1.3.7, §5.7)
- RFC 7636 (PKCE): https://www.rfc-editor.org/rfc/rfc7636
- OWASP Session Management: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html
- OWASP CSRF Prevention: https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html
- OWASP WebSocket Security: https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html
