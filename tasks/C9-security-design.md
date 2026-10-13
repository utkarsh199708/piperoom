# C9 — Security design docs: login, webhook auth, event retention (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-11 23:50 IST):** PR #10 round 2. **Corrections to the facts below:** on `main`
> `PIPEROOM_HTTP_ADDR` defaults to `127.0.0.1:8090` (f65c3ba, not `:8090`); `a3/row-scope` is merged (a1ceb36);
> A6's `UserSwitcher` dev-only gate is merged (ec6d9d8); `a5/room-service` has moved past 935f17d (round 3 in
> progress) — cite main where possible. **Webhook design must use C5's names** (`Incoming.DeliveryID`,
> `Incoming.DedupKey`, `Verify`, `DefaultAuth` in `internal/alertin`); Standard Webhooks only as an option.
> Propose the secret config in `config.go` style, e.g. `PIPEROOM_WEBHOOK_SECRETS_<SOURCE>` (comma-separated,
> current secret first, "" = webhook source off). `webhook-auth.md` is the single `docs/api.md` proposal.


> **Status (C9, 2026-10-13):** `docs/design/auth.md` rewritten against `main` @ 2a7bc34 on branch `c9/auth-design`
> (OIDC + PKCE, server-side session, owners.yaml principals/groups/backup, solo mode, `params_hash` on approvals,
> 4-PR split). It replaces PR #10's `auth.md`; `webhook-auth.md` and `event-retention.md` stay in PR #10.
>
> **Revision 2 (C9, 2026-10-13, PR #44):** rebased on `main` @ 9a97cb2 and revised for every item of
> `reviews/auth-design-critique.md`: dev non-loopback opt-in `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` (keeps the Docker
> image working), Host allow-list against DNS rebinding, login CSRF (row by cookie, then `state`), mode-specific
> cookie names, `params_hash` on `action.executed` with title/risk/rollback hashed and a JCS number rule, export
> limited to owners/driver/auditors, solo start check, `ValidateUsers` group rule, `return` parsing, client secret
> rule, groups-overage message. The "Changes in revision 2" table at the top of `auth.md` maps item → section.
> `jcs` is already a direct dependency on `main`; only `go-oidc` is new.
>
> **Revision 3 (C9, 2026-10-13, PR #44):** answers the two critic reviews of revision 2 (security; fit with `main`).
> Viewer role (`403 viewer` on every room and proposal POST, so a viewer can never take the driver role), driver
> dropped from the export list, `/auth/login` limit raised to 300/min with `PIPEROOM_TRUSTED_PROXIES`, loopback
> hosts on any port plus `PIPEROOM_ALLOWED_HOSTS`, the dev-mode `X-Forwarded-For` 403 deleted, a reverse proxy
> needs `oidc` (proxy pilots stay on the pre-PR-2 image until PR 3), `--allow-remote` kept one release as an alias,
> `/api/me.layers` stays, Vite proxy is a PR 2 prerequisite before `cors` goes, line refs fixed. The "Changes in
> revision 3" table at the top of `auth.md` maps item → section; §10 lists what was narrowed or rejected and why.

Read these first: `AGENTS.md`, `docs/collab.md`, `docs/security/findings.md` (F1–F10), `docs/security/threat-model.md`, `docs/api.md`,
`internal/events/events.go`, `internal/config/config.go`, `internal/owners/owners.go`, `dev/users.yaml`, `dev/owners.yaml`,
`web/src/api/client.ts` and C5's task file (`tasks/C5-*.md`). You own a **new** folder, `docs/design/**`. Branch: `c9/security-design`.
You **do not change any code**, contract file or `docs/security/**`. You write design docs, and the leads make the decisions.

You do not need Docker or an API key. A5's room service is **not on GitHub**: it is only on the local branch `a5/room-service` @ `935f17d`.
The facts below are everything you need from that branch. Cite them as "(a5 @ 935f17d)" and do not guess past them. If you cannot
fetch a URL, use only the facts in this file and list anything else under Open questions.

## Facts from unmerged branches (checked by the lead, 2026-10-11)
- `internal/httpapi/server.go`: `authenticate` reads `X-User` (l.102). `POST /api/alerts/webhook` sits in the same `X-User` group (l.59).
  The WS route is outside that group (l.50-51). CORS hard-codes `Access-Control-Allow-Origin: http://localhost:5173` and allows the `X-User` header (l.125-127).
  The body is capped at 1 MB with `http.MaxBytesReader` (l.24, l.150).
- `internal/ws/gateway.go`: identity comes from `?user=` (l.115-118). It calls `websocket.Accept` with `OriginPatterns: []string{"localhost:5173", "127.0.0.1:5173"}` (l.138-139).
- `internal/room/users.go` loads users and their `layers` from `USERS_FILE` (`dev/users.yaml`). Approve and reject check `user.OwnsLayer(layer)`
  (`service_proposals.go:114,175`). They do **not** use `owners.yaml`'s `layers:` map (`owners.LayerOwner`).
- `cmd/server/main.go:104` sets `http.Server{Addr: cfg.HTTPAddr}`. The default is `:8090`, which listens on all interfaces (`config.go:27`).
- `migrations/00001_init.sql` defines `room_events(room_id, seq, type, actor jsonb, payload jsonb, at, PK(room_id, seq))` with an FK to `rooms` and
  `ON DELETE CASCADE`. It also defines `approvals(proposal_id, layer, decision, reason, decided_at)`, which has **no approver column**. The actor of the
  `approval.given` / `approval.rejected` event is the only record of who decided.
- `internal/room/service.go:199` (`rebuild`) builds each room's snapshot by folding the whole log from seq 0. The hand-off also reads every event
  (`service_handoff.go:149`, `allEvents`).
- `a3/row-scope` @ `bd6e5e9` (approved, merge held) cuts the `Data` of `pg_row` / `warehouse_row` down to the key and timestamp (F3 at the source).
  Other payloads can still hold sensitive text:
  - `Query` and `Username` in `pg_long_transactions` / `pg_recent_ddl` results (`clients.Transaction`, `clients.DDLChange`)
  - key values in tool-call `Input`
  - `RoomCreatedPayload.Alert` (the raw webhook body)
  - comment and steer `Text`
  - `ActionExecutedPayload.Output`
  - `TracePinnedPayload.Key`
  - `Actor.Name`
- A1 is adding `topic_prefix` to `dev/owners.yaml` pipelines right now. Any owners.yaml change you propose must be additive.

## Goal
Write three decision docs the leads can approve as written. Each doc turns findings into one chosen design, gives the exact contract diffs, and splits the work into tasks per owner.

## Work
Each doc is 150–300 lines (aim for about 220). Use these sections, in this order:
1. **Problem**: finding ids, plus today's behaviour with file:line.
2. **Options**: 2–3 options in one table. Compare security, effort, dev-mode impact and ops burden.
3. **Recommendation**: one clear choice.
4. **Contract changes**: fenced ```diff blocks against the current text on `main` of `docs/api.md`, `internal/events/events.go` or
   `internal/config/config.go`. Write "none" if there are none.
5. **Migration from today's dev mode**.
6. **Tasks per owner**: A5 server, A6 web and A1 dev stack. Add others (A3, A4, C5) only if needed. Use one PR-sized bullet per task.
7. **Open questions**.
8. **References**: official URLs only.

1. `docs/design/auth.md` (F1; also "who approved" for F2/F10):
   - **Login flow.** OIDC Authorization Code flow with PKCE (RFC 7636). The server alone derives identity.
     - Cite OIDC Core 1.0 §3.1 for the flow and §3.1.3.7 for ID token validation.
     - The stable id is `iss` + `sub` (§5.7: email MUST NOT be used as the identifier).
     - If you name Go APIs, use only these (verified):
       - `golang.org/x/oauth2`: `GenerateVerifier`, `S256ChallengeOption`, `VerifierOption`, `Config.Exchange`
       - `github.com/coreos/go-oidc/v3/oidc`: `NewProvider`, `Provider.Verifier(*oidc.Config)` (field `ClientID`), `IDTokenVerifier.Verify`,
         `IDToken.Claims`, `IDToken.Nonce`. Say that go-oidc is a new module.
   - **Session cookie.** Compare a server-side session with a signed stateless cookie.
     - Follow the OWASP Session Management cheat sheet: `__Host-` prefix, `Secure`, `HttpOnly`, `SameSite`, idle and absolute timeouts, and a new session id after login.
     - Explain why the pre-login cookie (state, nonce, PKCE verifier) must be `SameSite=Lax`: the IdP sends the user back with a cross-site redirect.
   - **CSRF.** Follow the OWASP CSRF cheat sheet. Compare two defences:
     - `net/http.CrossOriginProtection` (in this repo's Go 1.27; see `go doc net/http.CrossOriginProtection`). It checks `Sec-Fetch-Site`, or else
       `Origin` against `Host`. Trusted origins are added with `AddTrustedOrigin`. It allows requests that have neither header.
     - A signed double-submit token.
     In dev, the web app (`:5173`) calls the API (`:8090`) cross-origin today. Say how production avoids this (same origin). Also say what
     `fetch` needs (`credentials`) and what CORS needs (`Access-Control-Allow-Credentials`) if the two stay cross-origin.
   - **WebSocket auth with no identity in the URL.** The OWASP WebSocket cheat sheet says: no tokens in query strings, and check `Origin` against an
     allow-list on every handshake. Compare three options: the session cookie on the handshake plus `AcceptOptions.OriginPatterns`, a one-time
     ticket, and a token in the first message.
   - **User → layer owner.** Today there are two sources of a different kind: `dev/users.yaml` `users[].layers` (used by A5) and the
     `dev/owners.yaml` `layers:` map (used by `internal/owners`). Compare owners.yaml mapped to IdP identities with IdP groups. Note that `groups`
     is not a standard OIDC claim; it is provider-specific. Pick one source of truth.
   - **Dev mode.** Allow `X-User` / `?user=` only when an explicit dev flag is set **and** the listener is loopback. The server refuses to start otherwise.
     The production web build leaves out `UserSwitcher` (`import.meta.env.DEV` is replaced at build time by Vite).
   - **Dev IdP for A1.** If you propose a dev IdP container, name its official image with a pinned tag that you checked on the registry.
     If you cannot check it, propose a fake IdP in Go tests (`httptest`) instead.
2. `docs/design/webhook-auth.md` (F9).
   - **Depends on C5.** C5 builds per-source alert parsing and signature verification helpers in `internal/alertin`. This doc must use exactly
     C5's header names, secret config shape, error values and function names. Read C5's task file, and its PR if it is open. Never redefine them.
     Put anything you disagree with under Open questions. State this dependency at the top of the doc.
   - **Formats (verified):**
     - **Prometheus Alertmanager `webhook_config`:** **no signing**. Auth goes through `http_config`: `authorization` (`type`, default `Bearer`;
       `credentials` / `credentials_file`), `basic_auth`, `oauth2` or `tls_config`. Payload fields: `version` `"4"`, `groupKey`, `status`,
       `alerts[].fingerprint`, `alerts[].startsAt`.
     - **PagerDuty V3:** `X-PagerDuty-Signature` holds comma-separated `v1=<hex HMAC-SHA256 of the raw body>` values, one per secret, which allows
       rotation. There is no timestamp. Body fields: `event.id`, `event.occurred_at`.
     - **Grafana webhook contact point:** hex HMAC-SHA256 in `X-Grafana-Alerting-Signature` (the header name can be changed). It has an optional
       timestamp header; when that is set, Grafana signs `timestamp + ":" + body`.
     - **Standard Webhooks:** headers `webhook-id`, `webhook-timestamp` (unix seconds) and `webhook-signature: v1,<base64>`, signed over
       `msg_id.timestamp.payload`. Several signatures are space-separated for rotation. The secret is `whsec_` + base64.
   - **Options to compare:** a bearer token per source, an HMAC per source, and network controls (mTLS or an allow-list) as an extra layer.
   - **The doc must cover:**
     - Verify over the raw bytes before JSON decode.
     - Compare in constant time (`crypto/hmac.Equal`, `crypto/subtle.ConstantTimeCompare`).
     - Replay protection: use a time tolerance when the source sends a timestamp. When it does not, dedupe by `webhook-id`, `event.id`, or
       `groupKey` + fingerprints, so a repeat maps to the same open room instead of a new one.
     - Rotation with two live secrets.
     - Secrets come only from env or a file, never from the repo or logs.
     - The webhook route moves out of the `X-User` group.
     - A rate limit or a cap on room creation.
     - `title`, `severity` and `pipeline_id` from the body stay untrusted, and `pipeline_id` must exist in `owners.yaml`.
3. `docs/design/event-retention.md` (F3, F6; audit needs for F1/F2/F10):
   - **Inventory.** For each `events.Type`, list the payload fields that can hold PII or secrets. Start from the facts list above.
   - **Redaction at write time.** Decide where it happens: the tool layer, the A4 `Sink`, or A5 `Store.Append`. Give a field policy per type and
     size caps on `Data`.
   - **Retention without breaking replay.** These rules must keep working:
     - `Seq` starts at 1 and has no gaps (events.go:66-67).
     - `Subscribe` never skips or repeats an event (:84-86).
     - WS `after=N` (docs/api.md:112,124).
     - The snapshot rebuild folds the whole log.
     Compare three options: keep everything forever with write-time redaction; an in-place tombstone that keeps `room_id, seq, type, actor, at`
     and replaces the payload; and archiving closed rooms after N days, then deleting them and returning a 410-style error on replay.
     Say exactly what a client gets when its `after` is below the first kept seq.
   - **Audit.** Approvals, rejections, executed actions, control changes and hand-offs must outlive redaction, because the event actor is the
     only record of who approved. Cover tamper evidence (OWASP Logging cheat sheet: "Protection" and "Data to exclude"). Give GDPR Art. 5(1)(c),
     Art. 5(1)(e) and Art. 17 as the reasons for limits.
4. The PR description follows the AGENTS.md §6 template and adds `## Decisions for the leads`: one line per decision, grouped by doc.

## Checks (docs only, so no unit tests)
- `go build ./...` still passes, and `git diff --stat main` shows only `docs/design/*.md`.
- `wc -l docs/design/*.md` shows 150–300 lines for each doc.
- Every repo claim has a file:line that matches `main`, or comes from the facts list above and is marked "(a5 @ 935f17d)".
- Every header, payload field, claim, cookie attribute or Go symbol you name is either in this file or verified by you (`go doc`, or an
  official URL listed in References). Do not invent endpoints for third-party services.
- Every diff block applies to the current file text on `main`. Example secrets are placeholders such as `<secret>`, never values that look real.

## Done when
- All three docs exist and have every section.
- Each recommendation is one clear choice.
- Every contract change is an exact diff.
- Every owner task is one PR-sized bullet.
- webhook-auth.md states its dependency on C5.
- No other file changed.
