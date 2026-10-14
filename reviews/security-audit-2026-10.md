# Security audit 2026-10: login, roles, approvals, export

Agent O4, 2026-10-13, against `main` @ 8283a58 (auth PR 4 merged). Branch `o4/security-audit`.

Scope: `internal/auth`, `internal/httpapi` (`auth.go`, `auth_oidc.go`, `ratelimit.go`), `internal/room`
(`access.go`, `params_hash.go`, `service_executor.go`, `service_export.go`), `internal/owners`, checked against
`docs/design/auth.md` revision 4. Method: each attack is a unit test named `TestAttack_<Thing>` (no Docker, no
network: `httptest`, `memstore`, `testenv.FakeIdP`). A test for a defended attack passes and stays as a regression
test. A test marked "documented" pins a design limit: it passes today and fails if the behaviour changes, so the
change is noticed.

Test files: `internal/httpapi/attack_test.go`, `internal/room/attack_test.go`, `internal/owners/attack_test.go`,
`internal/auth/attack_test.go`.

## Summary

| Id | Area | Outcome | Severity | Test |
|---|---|---|---|---|
| A-PH-1 | params_hash: steps + hash edited between two approvals | **bug, fixed** | Medium | `TestAttack_ParamsHashEditBetweenTwoApprovals` |
| A-PH-3 | params_hash: steps edited to `null`/`[]` marked the proposal `executed` | **bug, fixed** | Low | `TestAttack_ParamsHashBypassBetweenApproveAndExecute/steps_null` |
| A-OWN-1 | owners.yaml `owner:` typo fell back to top-level owners (fail open) | **bug, fixed** | Medium | `TestAttack_OwnersYAMLEdgeCases` |
| A-USR-1 | users.yaml repeated id: sub pin bypass through the other entry's email | **bug, fixed** | Medium | `TestAttack_UsersYAMLEdgeCases` |
| A-USR-2 | users.yaml unknown key (`subject:`) silently dropped a sub pin | **bug, fixed** | Low | `TestAttack_UsersYAMLEdgeCases` |
| A-ID-3 | OIDC session outlives a new users.yaml sub pin | **bug, fixed** | Medium | `TestAttack_SessionOutlivesSubPin` |
| A-ID-4 | bidi / zero-width characters in the name claim | **bug, fixed** | Low | `TestAttack_NameClaimBidiOverride`, `TestAttack_StripFormat` |
| A-ID-2 | display-name impersonation by an IdP user | design issue | Medium | `TestAttack_IdentitySpoofOIDC` |
| A-AP-3 | users.yaml layer over a group-owned layer is not validated | design issue | Medium | `TestAttack_UsersYAMLLayerOverGroupLayer` |
| A-ID-5 | identity unbind leaves the old account's sessions alive | design issue | Low | `TestAttack_UnbindLeavesSessionsAlive` |
| A-RL-4 | login rate limit: shared proxy key; trusted peer picks its key | design issue | Low | `TestAttack_LoginLimitDesignLimits` |
| A-SF-3 | dev cookie has no `__Host-`: other localhost apps can toss it | design issue | Low (dev only) | `TestAttack_SessionFixationDevLogin` (server side) |
| A-EX-2 | viewers read every event the export holds | design issue | Info | `TestAttack_ViewerReadsWhatExportWouldShow` |
| A-SF-1, A-SF-2 | session fixation (dev, oidc) | defended | — | `TestAttack_SessionFixationDevLogin`, `TestAttack_SessionFixationOIDCCallback` |
| A-CSRF-1 | login CSRF (attacker's code in victim's browser) | defended | — | `TestAttack_LoginCSRFAttackerCodeInVictimBrowser` |
| A-CSRF-2 | CSRF on every state-changing route, incl. same-site other port | defended | — | `TestAttack_CSRFStateChangingRoutes` |
| A-CSRF-3 | cross-site WebSocket hijacking | defended | — | `TestAttack_CrossSiteWebSocketHijack` |
| A-DNS-1 | DNS rebinding, 18 Host spellings, webhook path tricks | defended | — | `TestAttack_DNSRebindingHostSpellings` |
| A-VW-1 | viewer write paths (method override, HEAD/OPTIONS, path tricks, WS messages, forged group claims) | defended | — | `TestAttack_ViewerWritePaths` |
| A-AP-1, A-AP-2 | approval forgery (other layer, unneeded layer, other proposal's hash, look-alike groups, double approve, agent-chosen layers/hash, closed room) | defended | — | `TestAttack_ApprovalForgeryOverHTTP`, `TestAttack_ApprovalForgeryAtService` |
| A-PH-2 | every hashed field edited between approve and execute | defended | — | `TestAttack_ParamsHashBypassBetweenApproveAndExecute` |
| A-ID-1 | sub equal to a users.yaml id, unverified email, look-alike email | defended | — | `TestAttack_IdentitySpoofOIDC` |
| A-OWN-2 | principal matching (case, spaces, look-alikes, `group:user:x`, unknown pipeline) | defended | — | `TestAttack_PrincipalMatchingIsExact` |
| A-EX-1 | export access (driver, viewer, auditor) and content leaks | defended | — | `TestAttack_ExportAccessAndContent` |
| A-RL-1..3, A-RL-5 | rate-limit lockout of a named victim; key flood; login-state flood | defended | — | `TestAttack_LoginLimit*`, `TestAttack_LoginStateFloodDoesNotEvictVictim` |
| A-RED-1 | open redirect after login (23 spellings) | defended | — | `TestAttack_ReturnOpenRedirect` |

## Bugs fixed in this PR

### A-PH-1 (Medium): edited steps ran on an earlier approver's approval

Attack: a proposal needs postgres (priya) and kafka (arjun). priya approves with hash H1. Someone who can write the
`proposals` row (a scripted edit, a partial DB foothold; not necessarily the full DB admin of design §3.3) changes
`steps` and recomputes `params_hash` (the formula is public) to H2. arjun (or the attacker, if they own the second
layer) approves with H2: the approve check compares the echo with the row, so it passes. The executor compared the
row with the queued row and the last approval only, so the edited steps ran, with priya's approval of H1 in the
audit trail.

Fix (`service_executor.go`, `recheckStep`): before each step, every `approved` row in `approvals` must carry the hash
that runs; otherwise the step fails with `params hash mismatch`. Side effect: a proposal that was part-approved before
auth PR 4 (approval rows with an empty hash) and completed after it now fails instead of running; it can be proposed
again. Design §3.3 still holds: an admin who edits `approvals` too is not caught.

### A-PH-3 (Low): steps edited to `null` marked the proposal executed

Attack: edit `proposals.steps` to `null` or `[]` after the approve check. The executor decoded zero steps, ran no
re-check, and set status `executed`: a false record that the approved change was applied.

Fix: `executeProposal` fails a proposal whose row decodes to zero steps with `params hash mismatch` (`CreateProposal`
already refuses an empty proposal, so only an edit produces one).

### A-OWN-1 (Medium): `owner:` typo widened who may approve

Attack / mistake: a pipeline block written `owner:` (or a top-level `auditor:`) was ignored by `yaml.Unmarshal`. The
layer fell back to the top-level `layers:` entry, so the person the pipeline block meant to exclude could approve.

Fix (`owners.Load`): decode with `yaml.Decoder.KnownFields(true)`; unknown keys are a start error. The shipped files
(`dev/owners.yaml`, `deploy/config/owners.example.yaml`, the discover golden draft) still load
(`TestAttack_ShippedOwnersFilesStillLoad`).

### A-USR-1 (Medium) and A-USR-2 (Low): users.yaml repeated ids and unknown keys

Attack: `users.yaml` lists `priya` twice, `{id: priya, email: priya@x}` and `{id: priya, sub: real-sub}` (a merge
gone wrong). `Get` returned the last entry, `ByEmail` the first. An IdP account with the verified email but another
sub found the first entry (no pin), bound itself, and became priya: the pin on the second entry was bypassed. Ids that
differ only in case (`Priya`/`priya`, or the Kelvin sign) made two users that read the same. An unknown key such as
`subject:` for `sub:` silently dropped the pin.

Fix (`room.ParseUsers`): refuse an id seen before under Unicode case folding (`foldKey`), and decode with
`KnownFields(true)`. The shipped users files still load (`TestAttack_ShippedUsersFilesStillLoad`).

### A-ID-3 (Medium): a session outlived a new sub pin

Attack: priya's mailbox is recycled; the new holder logs in by verified email and gets a session. The admin adds
`sub: <priya's real sub>` to users.yaml and restarts. `mapIdentity` now refuses the recycled account at login, but its
existing session (sessions live in Postgres and survive the restart) kept acting as priya for up to 12 h; the
per-request gate checked only that the user still exists.

Fix (`httpapi/auth.go`, `pinMismatch`): an `oidc` session whose user has a users.yaml `sub:` other than the session's
`sub` is `401 session_expired`, on every request and on the WebSocket presence tick.

### A-ID-4 (Low): bidi and invisible characters in the name claim

Attack: an IdP user sets the display name to `U+202E` + `ayirP`, which renders as "Priya" in the room and in
approval events. `clean` dropped C0 controls only.

Fix (`auth.claimsFrom`): the name claim drops Unicode format characters (category Cf: bidi embeddings, overrides,
isolates, zero-width characters, soft hyphen, BOM). Only the name: removing characters from the email could turn one
address into another before the users.yaml match.

## Design-level issues (not fixed here)

### A-ID-2 (Medium): display-name impersonation

Attack: a member who is only in an owners.yaml group (id `u_<12 hex>`) sets the IdP name claim to "Priya", or "Priya"
with a Cyrillic а. Comments, steers, hand-offs and approval events show `name: Priya`. The id differs, but the web UI
shows names. Test: `TestAttack_IdentitySpoofOIDC` (case `idp-x4`: the id is a derived `u_` id while the name is
"Priya").

Proposed fix: the server marks a person who is not in users.yaml (`actor.id` starts with `u_`) and the UI shows a
badge or the email next to the name; or the login refuses (or suffixes) an IdP name that equals a users.yaml name
under case folding and confusable mapping. A6 + A5 work; needs a design line in §2.1.

### A-AP-3 (Medium): users.yaml layer over a group-owned layer

Attack: owners.yaml says `consumer: [user:kabir, group:consumers]`. users.yaml gives `mallory` `layers: [consumer]`.
`ValidateUsers` skips every layer that lists a group (design §2.3), so the server starts, and `authorizeLayer` still
honours users.yaml layers in every mode, so mallory approves consumer steps that owners.yaml never gave her. Test:
`TestAttack_UsersYAMLLayerOverGroupLayer`.

Proposed fix: in the users → owners direction, a users.yaml layer must name the user as a `user:` principal even when
the layer also lists groups (the server cannot vouch for group membership at start-up, which is exactly why a users.yaml
claim must not stand in for it). Or stop reading users.yaml `layers` in `oidc` mode, as §2.3 already calls them
deprecated. Either changes design §2.3, so it is listed here, not changed.

### A-ID-5 (Low): unbind leaves sessions alive

Attack: as A-ID-3, but the admin uses the documented remedy, deleting the `identities` row (`devctl identities
unbind`). The row goes; the recycled account's session keeps acting as priya until idle (8 h) or absolute (12 h)
expiry. Test: `TestAttack_UnbindLeavesSessionsAlive`.

Proposed fix: `Unbind` also revokes every live session with `user_id = <user> AND iss = <iss> AND sub = <sub>` (one
`UPDATE sessions SET revoked_at = now()`), and `devctl` prints how many it revoked. Or the per-request gate checks
that a session made by an email binding still has its `identities` row (one indexed lookup per request).

### A-RL-4 (Low): login rate-limit keys an attacker can share or choose

Attack (a): oidc behind a TLS proxy with `PIPEROOM_TRUSTED_PROXIES` unset: every user has the proxy's address as key,
so one client sending 300 `GET /auth/login` a minute stops all new logins. Attack (b): `PIPEROOM_TRUSTED_PROXIES` set to
a wide range (`10.0.0.0/8`): any host in it that can reach the server directly sends `X-Forwarded-For: <victim>` and
spends the victim's budget. Both stop only new logins, for at most a minute after the flood. Test:
`TestAttack_LoginLimitDesignLimits`.

Proposed fix: log one Warn at start (and once per hour at run time) when oidc mode sees `X-Forwarded-For` from a peer
that is not trusted; document in `deploy/README.md` that the trusted list should be the proxy's own addresses (/32),
not the subnet.

### A-SF-3 (Low, dev mode only): dev cookies can be tossed by other localhost apps

Attack: cookies are not isolated by port. Any page served from another `localhost` port (a malicious npm dev server)
can set `piperoom_session` for `localhost`: it can sign the developer out, or plant the attacker's own session so the
developer's comments and approvals are recorded as the attacker (login fixation). The server side is correct (a login
always issues a fresh token, `TestAttack_SessionFixationDevLogin`); the browser side cannot be unit-tested.

Proposed fix: when the request Host is `localhost`, set the dev cookies as `__Host-` + `Secure` (Chrome and Firefox
accept Secure cookies on `http://localhost`); keep the plain name for LAN demos. Or accept it as a dev-only limit and
say so in design §1.3.

### A-EX-2 (Info): the export rule does not keep content from viewers

Viewers may not export (§3.4), but `GET /api/rooms/{id}/events` returns the same events, so a viewer can rebuild
everything except the integrity hash. Test: `TestAttack_ViewerReadsWhatExportWouldShow`. Proposed: say in §3.4 that
the export rule controls the signed evidence file, not confidentiality; no code change.

## Attacks tried and defended

- **Session fixation.** A planted (valid or made-up) token is never adopted; dev login and the OIDC callback always
  issue a fresh token and revoke the cookie's old session; OIDC cookies are `__Host-`, `Secure`, `HttpOnly`,
  `SameSite=Lax`, `Path=/`, no `Domain`.
- **Login CSRF.** The callback finds the row through the login cookie; an attacker's code+state in a browser with no
  cookie is `login_expired`, with the victim's cookie a state mismatch that also spends the victim's row.
- **CSRF.** Cross-site, same-site (another localhost port), `Origin` only and `Origin: null` POSTs to comment, steer,
  close, approve, logout and dev-login are 403; simple content types without fetch metadata are 400; logout is not a
  GET. Cross-site WebSocket handshakes are 403.
- **DNS rebinding.** 18 Host spellings (suffix/prefix tricks, `localhost.`, `127.1`, decimal IP, mapped IPv6, empty
  and long ports, a tab) are 421 even with `Sec-Fetch-Site: same-origin`; webhook path spellings that might look like a
  signed alert route (`/webhook/`, `%77ebhook`, `//webhook`, `./webhook`, `webhook%2F`) never reach the unsigned
  webhook; no room is opened.
- **Viewer writes.** Method override headers, HEAD, OPTIONS, PROPFIND, path spellings (`//`, upper case, trailing
  slash), `X-User` in oidc mode, a groups claim containing `user:priya` or the approver group with a trailing space,
  and steer/approve/comment messages on the WebSocket: nothing is written, nothing runs.
- **Approval forgery.** Approving another owner's layer (403), a layer the proposal does not need (400), with another
  room's proposal hash (409; the hash binds room and proposal ids), with look-alike group claims (case, spaces,
  U+2011, U+200B, `group:` prefix) (403), twice (409); an agent that sets `required_layers` or `params_hash` itself
  is overwritten; a closed room refuses.
- **params_hash between approve and execute.** Edits to params, description, title, risk, rollback, required layers,
  room id, an appended step, a re-cased key, broken JSON: the executor refuses and nothing runs.
- **Identity spoofing.** An IdP `sub` equal to a users.yaml id, the right email unverified, a Cyrillic look-alike
  email, a trailing zero-width space: all get a derived `u_` id, never priya.
- **owners.yaml / users.yaml edge cases.** Duplicate YAML keys, duplicate pipeline ids, bad or mixed-case prefixes,
  empty principals, null items, commas, whitespace in user ids, unknown layers, owners blocks with no approvers,
  repeated or empty ids, colons, no-break spaces, the `u_` prefix, bad or duplicate emails and subs: refused at load.
  Matching is exact; an unknown pipeline fails closed; `auditors:` never makes a member.
- **Export.** A plain member who is the driver and a viewer get 403; an auditor gets the file; the file holds no
  session token, email, IdP group or client secret.
- **Rate-limit lockout.** A spoofed `X-Forwarded-For` from an untrusted peer, a prepended victim address or fake proxy
  hop behind a trusted proxy, and an IPv6 attacker in another /64 all spend only their own budget; a flood of new
  keys evicts old buckets instead of refusing new clients; a flood of login rows does not evict the victim's row.
- **Open redirect.** 23 `return` spellings (`//`, `/\`, encoded slashes and backslashes, `/.//`, schemes, userinfo,
  CR/LF, tabs, overlong) all end on the same site after `SafeReturn` + `http.Redirect`.

## Contract change request

None. No contract file is touched.
