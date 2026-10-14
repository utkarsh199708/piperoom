# Design: alert webhook authentication — deltas on main

Task C9, doc 2 of 3. `auth.md` is already on main, and `event-retention.md` is the third. This is a design only;
no code is changed. **Findings:** F9 (webhook auth). Related: F6 (an alert flood grows the log) and F10 (pipeline
scoping).

Main already ships signed alert routes. §1 describes what main does. §2–§6 propose only the changes on top of it.
Each delta stands alone and fits in one PR.

## 1. Current state on main

| What | Where |
|---|---|
| `POST /api/alerts/{source}` for `alertmanager`, `grafana`, `pagerduty`, `datadog`, `generic`; outside `X-User` / session auth; unknown source → `404 not_found` | `internal/httpapi/server.go:144`, `internal/httpapi/alerts.go:30-35` |
| A source with an empty secret is disabled: `401` before the body is read | `internal/httpapi/alerts.go:37-41`, `internal/alertin/verify.go:90` |
| Body read through `http.MaxBytesReader(alertin.MaxBodyBytes+1)`; over 1 MiB → `413 too_large` | `internal/httpapi/alerts.go:44-53` |
| `alertin.Config.Ingest(src, header, body)` verifies, then parses: bearer token (Alertmanager), Grafana HMAC + timestamp, PagerDuty V3 `v1=` HMAC, Datadog header token, generic `t=…,v1=…` HMAC; constant-time compares | `internal/alertin/verify.go:109-289`, `internal/alertin/README.md` |
| `ErrUnauthenticated` / `ErrStale` → `401 unauthenticated`; `ErrTooLarge` → `413 too_large`; `ErrIgnored` → `202 {"ignored":true}`; anything else → `400 validation` | `internal/httpapi/alerts.go:56-78` |
| Refused requests are logged (source, remote address, reason) at Warn at most once per source per minute, the rest at Debug | `internal/httpapi/alerts.go:107-135` (`refuseWarner`) |
| One secret per source: `ALERTMANAGER_TOKEN`, `GRAFANA_ALERT_SECRET`, `PAGERDUTY_ALERT_SECRET`, `DATADOG_ALERT_TOKEN`, `GENERIC_ALERT_SECRET`; plus `PAGERDUTY_SERVICES` and `DEFAULT_ALERT_PIPELINE`, both checked against `owners.yaml` at start | `internal/config/config.go:64-72`, `cmd/server/alerts.go:17-41` |
| Reply: `RoomSnapshot` (`201` new, `200` joined). With a `tenants:` block in `owners.yaml` the reply is `{"room_id","created"}` only | `internal/httpapi/alerts.go:84-104`, `internal/httpapi/tenancy.go:49-59`, `docs/api.md` (`/api/alerts/{source}` row) |
| Dedup: pipeline id + the source's `DedupKey` form an index key; a repeat while that room is open joins it; a resolved alert with no open room → `202` | `internal/room/service_room.go:164-246`, `alertIndexKey` at `:296` |
| Cross-replica dedup: advisory lock on the key plus a store lookup (`LastRoomByDedupKey`) | `docs/design/replicas.md` §6.3; `migrations/00005_room_dedup_key_idx.sql` (a non-unique partial index on `payload->>'dedup_key'`) |
| Pipeline resolution order: label → PagerDuty service map → owners match → default | `internal/alertin/parse.go:56-70` (`resolvePipeline`) |
| A pipeline id that is not in `owners.yaml` opens a room with proposals disabled | `internal/room/service_room.go:197`, `:252` |

What main does **not** have:
- secret rotation (each source has one secret);
- a database guarantee of one open room per dedup key;
- a cap on rooms or requests per source;
- alert metrics;
- a check that a label-supplied pipeline id exists in `owners.yaml`.

## 2. Delta 1 — secret rotation (two live secrets)

**Problem.** To rotate a source's secret today, the sender and the server must switch at the same moment. Every
alert sent in between is refused with `401`.

| | A. Comma-separated list in the existing variable (recommended) | B. A `<VAR>_PREVIOUS` per source | C. A secrets file reloaded on SIGHUP |
|---|---|---|---|
| Security | same secrets; two live only during the switch | same | same; file permissions become a new thing to get right |
| Effort | `cmd/server` splits the value; alertin tries each secret | five new variables | file watcher and reload path |
| Ops | edit one variable | edit two | distribute a file |

**Proposal (A).**
- Each existing secret variable may hold `current,previous`. Secrets contain no commas: generate them as 32 random
  bytes, hex-encoded. An empty value still disables the source.
- In `alertin.Config` each secret becomes a list: `GrafanaSecret string` → `GrafanaSecrets []string`, and the same
  for the other four. `Ingest` tries the secrets in order. PagerDuty already accepts several `v1=` values in one
  header, so at most 2×2 HMACs are computed.
- `Ingest` reports which secret matched in a new field `Alert.SecretIndex int` (the index, never the value). The
  handler logs it at Debug and counts it in the Delta 4 metric, so the operator can see when the old secret is no
  longer used.
- Procedure: put the new secret first → update the sender → wait until `secret_index=1` drops to zero → remove the
  old secret.

Contract change in `internal/config/config.go`. Only the comment changes; the fields stay strings. `git apply --check`
passes on main:

````diff
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -63,6 +63,8 @@ type Config struct {
 
 	// Signed alert sources (internal/alertin). A source with an empty secret is
 	// disabled: its route answers 401. All secrets; never log them.
+	// Each secret value may list two secrets "current,previous" during a
+	// rotation; both are accepted until the previous one is removed.
 	AlertmanagerToken    string // ALERTMANAGER_TOKEN
 	GrafanaAlertSecret   string // GRAFANA_ALERT_SECRET
 	PagerDutyAlertSecret string // PAGERDUTY_ALERT_SECRET
````

## 3. Delta 2 — one open room per dedup key, enforced by the database

**Problem.** The in-memory index plus the advisory lock (`replicas.md` §6.3) is correct as long as every writer takes
the lock. Nothing stops a writer that skips it: a new route, a script, a later refactor. The index from migration
00005 is not unique.

**Proposal.**
- A5 adds a table `open_alert_keys(index_key text primary key, room_id text not null references rooms(id) on delete cascade)`.
- The row is inserted in the same transaction as `CreateRoom` and `room.created`. It is deleted with `room.closed`
  and in the retention purge.
- A conflicting insert means another writer won. The service loads that room and joins it (`200`), as it does for an
  index hit today.
- The advisory lock stays, because it serialises the join path. The table is a backstop: a skipped lock can no longer
  produce two rooms.
- Test: two services on one Postgres, the lock bypassed, 50 concurrent identical deliveries → one room.

No contract file changes. The API is unchanged.

## 4. Delta 3 — per-source room cap and request rate limit

**Problem.** Anyone who has a source's secret can open rooms without limit. So can anyone who replays a validly
signed request with a new dedup key. Each room starts a billed agent run against production read tools. Main has no
limit on the alert routes: the only rate limit is on `/auth/login` (`internal/httpapi/auth_oidc.go:44`), and
`replicas.md` §9 item 4 notes that rate limits are per replica.

| | A. Room cap in the DB + per-replica request limiter (recommended) | B. Request limiter only | C. Ingress only |
|---|---|---|---|
| Holds across replicas | room cap yes; request rate per replica | no | yes |
| Stops billed agent runs | yes | partly | depends on the proxy |
| Effort | count query + `newRateLimiter` (`internal/httpapi/ratelimit.go`) | small | per deployment |

**Proposal (A).**
- **Room cap.** `PIPEROOM_ALERT_ROOMS_PER_HOUR` (default `20`, `0` = off).
  - The room-creation path counts the `room.created` events of the last hour whose `alert_source` is this source.
  - A join and a resolved alert never count against the cap.
  - Over the cap → `429 rate_limited` with `Retry-After: 300`, and one Warn log per source per minute (reuse
    `refuseWarner`).
- **Request rate.** Reuse `newRateLimiter`. Over either limit → `429 rate_limited`. This is a backstop: production
  should also limit at the ingress.
  - 120 requests per minute per source on each replica.
  - 30 refused requests per minute per remote address.

Contract changes. The diff applies to main (`git apply --check`):

````diff
--- a/docs/api.md
+++ b/docs/api.md
@@ -110,7 +110,7 @@ PRs 2 and 3 are live (dev sessions, Host allow-list, proxy guard, OIDC login, id
 | 413 | `too_large` (signed alert body over 1 MB) |
 | 409 | `already_decided`, `room_closed`, `lease_held`, `stale_handoff`; with several server replicas: `conflict` (another replica changed the room between the check and the write three times in a row; retry); after auth PR 4: `params_mismatch`; shadow mode: `room_open` (shadow route on an open room, or `?include=shadow` on an open room), `not_labelled` (shadow data read before the blind label), `not_revealed` (grade before `shadow.revealed`: the run is still going during the grace period); room limits: `already_extended` (a second `extend`), `log_full` (the room log is over its cap and its reserve; see Room limits) |
 | 421 | `bad_host` (after auth PR 2: `Host` not on the allow-list) |
-| 429 | after auth PR 3: `rate_limited` (`/auth/login` over 300 per minute per client) |
+| 429 | after auth PR 3: `rate_limited` (`/auth/login` over 300 per minute per client); signed alerts: `rate_limited` with `Retry-After` (source over `PIPEROOM_ALERT_ROOMS_PER_HOUR` new rooms in the last hour, or over its request rate on this replica) |
 | 500 | `internal_error`; `archive_failed` (`DELETE /api/rooms/{id}?archive=true` could not write the archive, so nothing was deleted) |
 | 503 | after auth PR 3: `idp_unavailable` (new logins while the IdP is down; live sessions keep working) |
 
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -70,6 +70,8 @@ type Config struct {
 	DatadogAlertToken    string // DATADOG_ALERT_TOKEN
 	GenericAlertSecret   string // GENERIC_ALERT_SECRET (HMAC for /api/alerts/generic)
 	DefaultAlertPipeline string // DEFAULT_ALERT_PIPELINE: used when an alert names no pipeline
+	// AlertRoomsPerHour caps new rooms per signed alert source per hour; "0" = off.
+	AlertRoomsPerHour string // PIPEROOM_ALERT_ROOMS_PER_HOUR (default 20)
 
 	// Agent cost guard: at most this many agent runs working at once on THIS
 	// replica, across all its rooms; further rooms wait in a queue (round-robin
@@ -225,6 +227,7 @@ func Load() Config {
 		DatadogAlertToken:    get("DATADOG_ALERT_TOKEN", ""),
 		GenericAlertSecret:   get("GENERIC_ALERT_SECRET", ""),
 		DefaultAlertPipeline: get("DEFAULT_ALERT_PIPELINE", ""),
+		AlertRoomsPerHour:    get("PIPEROOM_ALERT_ROOMS_PER_HOUR", "20"),
 
 		MaxAgents:        get("PIPEROOM_MAX_AGENTS", ""),
 		MaxAgentsGlobal:  get("PIPEROOM_MAX_AGENTS_GLOBAL", ""),
````

## 5. Delta 4 — metrics

**Problem.** A failed rotation or an attack shows up only in Warn logs, and those are capped at one per source per
minute.

**Proposal.**
- A counter `piperoom_alerts_total{source,outcome}`. `outcome` is one of `created`, `joined`, `ignored`,
  `unauthenticated`, `stale`, `too_large`, `bad_payload`, `rate_limited`.
- A histogram of handler latency.
- The server has no metrics endpoint yet. Until it has one, log one Info line per source per minute with the counts.

The endpoint form is a lead decision (Open question 2).

## 6. Delta 5 — check label-supplied pipeline ids against `owners.yaml`

**Problem.** `resolvePipeline` (`internal/alertin/parse.go:56-70`) accepts a label-supplied id as long as it passes
`ValidPipelineID`, and never checks that the pipeline exists. The room service then opens a room for an unknown
pipeline with proposals disabled (`internal/room/service_room.go:197`, `:252`). So a sender, or a typo in a label,
opens rooms under pipeline ids that do not exist. Meanwhile the later steps of the chain (service map, owners match,
default) are never tried, and one of them might have found the real pipeline.

**Proposal (C5 owns `internal/alertin`).**
- When `Config.Owners` is set, a `ViaLabel` id is accepted only if `owners.yaml` defines that pipeline. Otherwise
  resolution falls through to the next step.
- The dropped label is kept in `Alert.UnknownPipelineLabel string`. The room's system message can then say "alert
  named pipeline X, which owners.yaml does not define".
- With `Owners == nil` (tests, `--demo`) the behaviour does not change.
- Table test cases:
  - a known label;
  - an unknown label with a service-map hit;
  - an unknown label with an owners match;
  - an unknown label and nothing else (→ `DefaultPipeline`, else empty).

No contract file changes.

## 7. Tasks per owner

- **C5 (1)** Delta 1 in `alertin`: secret lists, `Alert.SecretIndex`, tests that both secrets are accepted and the right index is reported.
- **C5 (2)** Delta 5: the owners check for `ViaLabel`, `Alert.UnknownPipelineLabel`, the table test.
- **A5 (1)** Delta 1 wiring in `cmd/server/alerts.go`: split on commas, trim, drop empty entries.
- **A5 (2)** Delta 2: the `open_alert_keys` migration (next free number), the insert in the create transaction, the delete on close and on purge, the concurrency test.
- **A5 (3)** Delta 3: the room-cap count, `PIPEROOM_ALERT_ROOMS_PER_HOUR`, the request limiters, `429` with `Retry-After`, tests.
- **A5 (4)** Delta 4: counters and the histogram, behind the lead's choice of endpoint.
- **A1** `devctl` signs with the first secret in `GENERIC_ALERT_SECRET`. **C14** adds rotation to `docs/pilot/setup.md`.
- **A6** none.

## 8. Open questions

1. Are the defaults right (20 rooms per source per hour, 120 requests per minute)? Check against the pilot's alert volume.
2. Metrics: `expvar`, or a hand-written Prometheus text endpoint (no new dependency)?
3. Multi-tenant (F7): there is one secret set per deployment. Per-tenant secrets would need an `integrations` table
   and routes like `/api/alerts/{source}/{integration_id}`. Decide when a second tenant is real.
4. A replayed PagerDuty body can open one new room after its room closes, within `MaxEventAge`. Add a re-open
   cooldown per dedup key? The watcher already has one (`LastRoomByDedupKey`).

## 9. References

Header names, signing strings and payload fields come from C5 (`internal/alertin/README.md`, `verify.go`). The design
sandbox could not fetch the vendor pages below, so the reviewer should confirm them:
- Prometheus Alertmanager `webhook_config` / `http_config`: https://prometheus.io/docs/alerting/latest/configuration/
- PagerDuty V3 webhook signatures: https://developer.pagerduty.com/docs/verifying-webhook-signatures
- Grafana webhook contact point: https://grafana.com/docs/grafana/latest/alerting/configure-notifications/manage-contact-points/integrations/webhook-notifier/
- Datadog webhooks: https://docs.datadoghq.com/integrations/webhooks/
