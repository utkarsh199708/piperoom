# Design: OIDC login, identity-bound approvals, params hash

C9, 2026-10-13, against `main` @ 2a7bc34. Replaces PR #10's `auth.md`. Covers F1, F2 and feedback T1, T5, T6.

**Today:** `authenticate` trusts `X-User` (`internal/httpapi/server.go:145-161`), the WebSocket trusts `?user=`
(`internal/ws/gateway.go:156`), rights are `users.yaml` `layers` (`service_proposals.go:165`), and `approval.given`
records only `Actor{id,name}` (`:180`). Steps are frozen at creation (`:112-126`), but nothing proves what an approver saw.

## 1. Login

**OIDC authorization code + PKCE**, done by the Go server (works with Okta, Azure AD/Entra, Google, Keycloak).

- Libraries: `golang.org/x/oauth2` (in `go.mod`, indirect v0.30.0; `go doc` verified `GenerateVerifier`,
  `S256ChallengeOption`, `VerifierOption`, `Config.Exchange`) and `github.com/coreos/go-oidc/v3/oidc` v3.21.0
  (**new module, not in the local module cache**: `go doc` fails; `go list -m -versions` finds it).
  CSRF: `net/http.CrossOriginProtection` (Go 1.27, verified).
- `GET /auth/login?return=/rooms/r_x`: create a `login_states` row (state, nonce, PKCE verifier, return path;
  10 min; deleted on first use) and set `__Host-piperoom_login` (opaque id, `SameSite=Lax`, because the IdP comes
  back with a cross-site redirect). `return` must start with `/` and not `//` (no open redirect).
- `GET /auth/callback`: match state, `Exchange` with `VerifierOption`, verify the ID token (`IDTokenVerifier.Verify`
  checks signature, `iss`, `aud`, `exp`), then **check `nonce` ourselves** (Verify does not). Map the identity (§2).
  Discard all IdP tokens; we keep none.
- **Session:** server-side `sessions` table: `id` (public, `ses_`+ULID), `token_hash` (SHA-256 of 32 random
  bytes), `user_id`, `iss`, `sub`, `groups`, `auth_time`, `created_at`, `last_seen_at`, `expires_at`,
  `revoked_at`. Cookie `__Host-piperoom_session`: `HttpOnly`, `Secure`, `SameSite=Lax`, `Path=/`, no `Domain`.
  A new token at every login. `last_seen_at` written at most once a minute.
- **Expiry:** idle 8 h, absolute 12 h. Expired → `401 session_expired`; the web app goes to `/auth/login?return=…`.
- **Logout:** `POST /auth/logout` sets `revoked_at`, clears the cookie, closes that session's WebSockets. The IdP
  session stays (RP-initiated logout is later).
- **CSRF:** `CrossOriginProtection` on every route; POSTs must also be `Content-Type: application/json`. Production
  is same-origin (`PIPEROOM_WEB_DIR` serves `web/dist`), so the `cors` middleware (`server.go:169-180`) is deleted.
  In dev, Vite proxies `/api` and `/auth` to `:8090`, so dev is same-origin too.
- **WebSocket:** the session cookie is checked at upgrade; **`?user=` is removed**. `OriginPatterns` = host of
  `PIPEROOM_PUBLIC_URL` (dev: `localhost:5173`). The 20 s presence tick re-checks the session and closes with
  `session_expired`.

## 2. Identity mapping

- **Stable key is `iss` + `sub`**, never email alone.
- `users.yaml` (additive): each user may add `email:` and/or `sub:`. `sub` wins. Email matches only when
  `email_verified` is true. The first email match binds `iss`+`sub` in an `identities` table; a later login with
  the same email and a different `sub` is refused (recycled account).
- Groups come from the claim named by `PIPEROOM_OIDC_GROUPS_CLAIM`. `groups` is not a standard OIDC claim: Okta and
  Keycloak need a mapper, Azure sends object ids (and drops the claim on overage; we then grant no group rights),
  Google sends none (map Google users by `users.yaml`).
- A user with no `users.yaml` entry gets id `u_` + first 12 hex of SHA-256(`iss`|`sub`), name from `name`.
- **Login gate (fail closed):** login works only if the user is in `users.yaml`, in any group named in
  `owners.yaml`, or in `PIPEROOM_AUTH_VIEWER_GROUPS`. Else `403 not_provisioned`.
- **Approval rights move to `owners.yaml` (one source of truth).** A layer value may be a string (today) or a
  list of principals `user:<id>` / `group:<name>`. Per pipeline, an optional `owners:` block overrides it:

```yaml
pipelines:
  - id: orders
    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:priya] }
layers:
  postgres: priya                              # today's form still valid
  kafka: [user:arjun, group:dataeng-oncall]
```

  Rights are derived per request from the session's user and groups. `users.yaml` `layers` is deprecated; while
  present, `ValidateUsers` keeps checking agreement.
- **Backup approver:** may approve at once; the event records `grant: "backup:…"`. Escalation timers are T6 later work.

## 3. Approval integrity (F2+)

- At `CreateProposal`, compute `params_hash` = hex SHA-256 of the **RFC 8785 (JCS)** form of
  `{"v":1,"room_id","proposal_id","steps":[{"action","layer","params","description"}]}`. Store it in the proposal
  row; publish it in `proposal.created` and `open_proposals[]`. Library: `github.com/gowebpki/jcs` v1.0.2 (new
  module, not in cache); else a small in-house canonicalizer with the same test vectors.
- `POST /api/proposals/{pid}/approve` body becomes `{layer, params_hash}` (reject: `{layer, reason, params_hash}`).
  Missing → `400 validation`; different from the stored hash → **`409 params_mismatch`**. The web app sends the hash
  it rendered next to the params.
- The executor recomputes the hash from the row before `Execute` and refuses on mismatch (catches DB edits).
- `ApprovalGivenPayload` / `ApprovalRejectedPayload` gain `params_hash` and `identity`:
  `{iss, sub, session_id, method: "oidc"|"dev", grant: "user:…"|"group:…"|"backup:…"|"solo", auth_time}`.
  `session_id` is the public id, never the token. The `approvals` table gains the same columns, so "who approved"
  no longer lives only in the event actor. `HumanInput` (`internal/agent/contract.go:18`) is unchanged.
- **Export (`a5/retention-audit`; today that branch has only config commits):** one row per decision with
  `room_id, proposal_id, layer, decision, at, actor.id, iss, sub, session_id, method, grant, params_hash` plus the
  proposal's JCS text, so an auditor re-hashes it and matches. Retention must never delete `proposal.created`,
  `approval.*` or `action.executed` before they are exported.

## 4. Machine routes

- `POST /api/alerts/{source}` (`a5/signed-alerts` @ b238daa) stays signature/token-based, outside session auth;
  cookies are ignored there.
- `POST /api/alerts/webhook` (unsigned) and the `X-User` header work **only in dev mode** and only on a loopback
  listener. In OIDC mode the webhook route is not mounted (404) and `X-User` is ignored. Use
  `/api/alerts/generic` (HMAC, `GENERIC_ALERT_SECRET`).

## 5. Dev, demo, solo, migration

- `PIPEROOM_AUTH_MODE=dev` (default): the server **refuses to start** unless `PIPEROOM_HTTP_ADDR` is loopback,
  and refuses any request carrying `Forwarded` or `X-Forwarded-For` (a local proxy would expose it). `--demo` forces
  dev. `--allow-remote` is removed: non-loopback needs `oidc`.
- Dev login: `POST /auth/dev-login {user}` issues a **real session** (`method: "dev"`) for a `users.yaml` user.
  `UserSwitcher` calls it, so dev and prod share the cookie path. `X-User` stays for curl only.
- `PIPEROOM_AUTH_MODE=oidc`: needs issuer, client id and an `https` `PIPEROOM_PUBLIC_URL`, else refuse to start.
- **Solo mode** (`PIPEROOM_SOLO_USER=<id>`): that user may approve every layer. Each approval must also send
  `confirm: "<pipeline_id>/<layer>"`, typed by hand; the UI shows a solo banner; events record `grant: "solo"`.
  Startup logs a warning.
- **Migration:** old events keep their actor (new fields empty). One migration adds `sessions`, `login_states`,
  `identities` and the new `approvals` / `proposals` columns. `devctl` needs no new env var.

## 6. Config fields (contract, `internal/config/config.go`)

| Env var | Default | Meaning |
|---|---|---|
| `PIPEROOM_AUTH_MODE` | `dev` | `dev` \| `oidc` |
| `PIPEROOM_PUBLIC_URL` | `""` (exists) | `https` origin; redirect URI = this + `/auth/callback` |
| `PIPEROOM_OIDC_ISSUER` | `""` | issuer URL |
| `PIPEROOM_OIDC_CLIENT_ID` | `""` | |
| `PIPEROOM_OIDC_CLIENT_SECRET` | `""` | secret, never logged; empty = public client (PKCE only) |
| `PIPEROOM_OIDC_SCOPES` | `openid email profile` | space separated |
| `PIPEROOM_OIDC_GROUPS_CLAIM` | `groups` | `""` = no group rights |
| `PIPEROOM_AUTH_VIEWER_GROUPS` | `""` | comma separated; may log in and watch |
| `PIPEROOM_SESSION_IDLE` | `8h` | Go duration |
| `PIPEROOM_SESSION_MAX` | `12h` | Go duration |
| `PIPEROOM_SOLO_USER` | `""` | off |

## 7. Threats and mitigations

| Threat | Mitigation |
|---|---|
| Session fixation | New token at login; separate pre-login cookie; no id in URLs; `__Host-` prefix |
| CSRF | `SameSite=Lax`, `CrossOriginProtection`, JSON content type, same origin |
| WebSocket hijack | Cookie + `OriginPatterns` at upgrade |
| Token leakage in logs | Only the token's SHA-256 is stored; IdP tokens discarded; `requestLog` logs path, not query (`server.go:193`); never log cookies; a leaked `code` is useless without the PKCE verifier |
| Replayed callback | `login_states` row deleted on first use; state + nonce checked |
| IdP outage | Live sessions keep working; new logins get `503 idp_unavailable`; discovery retried, not fatal at start |
| Clock skew | Session expiry uses server time; ID token `exp` checked once at login; NTP required |
| Replayed approvals | Live session + CSRF pass; one decision per layer (`already_decided`); `params_hash` mismatch → `409` |
| Stale groups / recycled email | Groups refresh at login (worst case 12 h); `iss`+`sub` binding in `identities` |

## 8. PR split

| PR | Owner | Scope | Done when |
|---|---|---|---|
| 1 | Lead (contract) | `config.go` fields (§6); `events.go` `ParamsHash` on `ProposalCreatedPayload`, `ParamsHash` + `Identity` on approval payloads; `docs/api.md` auth, `/auth/*`, approve body, `409 params_mismatch`, WS without `?user=` | `go build ./...` passes; api.md matches this doc |
| 2 | A5 + A6 | Sessions, `/auth/dev-login`, `/auth/logout`, cookie WS, `?user=` removed, `CrossOriginProtection`, `cors` deleted, dev start guards, webhook dev-only. A6: Vite proxy, cookie client, `UserSwitcher` via dev-login, 401 redirect | `X-User` in oidc mode → 401; dev on `0.0.0.0` refuses start; WS without cookie → 401; cross-origin POST → 403; logout closes WS |
| 3 | A5 + A3 + A1 | OIDC login, `identities`, login gate, rights from `owners.yaml` principals, groups, backup, solo. A3: `internal/owners` lists + per-pipeline `owners:`. A1: fake IdP (`httptest`) in `internal/testenv` | With the fake IdP: bad nonce/state → 401; replayed callback → 401; unmapped user → 403; group owner approves; missing groups claim → no rights |
| 4 | A5 + A6 | `params_hash` compute/store/check, executor re-check, approval identity columns, export fields (`a5/retention-audit`). A6: show params + hash, send hash, solo confirm box | Changed params → `409`; DB-edited row → executor refuses; export row re-hashes to `params_hash` |

## Decisions needed (founder)

1. Default `PIPEROOM_AUTH_MODE=dev` (safe because loopback-only), or no default (must be set)?
2. Session lengths 8 h idle / 12 h absolute?
3. Backup approver: approve at once, or only after N minutes pending?
4. Step-up re-login (`prompt=login`) before approvals for regulated pilots: now or later?
5. Add the two new modules (`go-oidc`, `jcs`)?
