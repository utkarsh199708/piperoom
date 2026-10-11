# Design: alert webhook authentication

Task C9, doc 2 of 3 (`auth.md`, `event-retention.md`). Design only, no code changed. Written against `main` @ `f417abf`.
**Findings:** F9 (webhook auth undefined). Related: F4/F10 (pipeline scoping), F6 (a flood grows the log).

**Depends on C5.** C5 builds the verification and parsing helpers in `internal/alertin` (PR #11, branch
`claude/dazzling-volta-8xn3f3`, head `9903240`). This doc uses **C5's real names from that branch**:
`alertin.Config` (`AlertmanagerToken`, `GrafanaSecret`, `PagerDutySecret`, `DatadogToken`, `GenericSecret`, `Owners`,
`PagerDutyServices`, `DefaultPipeline`), `Config.Ingest(src Source, h http.Header, body []byte) (Alert, error)`, `Config.Enabled`,
`Alert{Source, Status, Title, Severity, PipelineID, PipelineVia, DedupKey, URL, OccurredAt, Raw}`, `Alert.RoomCreated()`,
`Alert.Agent(roomID, pipelineMap)`, `SignGeneric`, and the errors `ErrUnauthenticated`, `ErrStale`, `ErrTooLarge`, `ErrIgnored`.
Header names and checks are C5's (`internal/alertin/README.md`). Nothing is redefined here. `tasks/C5-alert-ingest.md` still names an older
API (`Incoming`, `DefaultAuth`, `Verify`); see Open question 1.

## 1. Problem

- `POST /api/alerts/webhook` sits in the same `X-User` group as human routes (a5 @ 935f17d, `internal/httpapi/server.go:59`). A real alert
  sender cannot send `X-User`, so today either the endpoint is unusable by Alertmanager/Grafana/PagerDuty, or an operator hands out a user id.
  `docs/api.md:103` says nothing about credentials.
- Anyone who reaches the endpoint can open rooms, which start billed agent runs against production read tools, and can post a believable fake sev1 (F9).
- The body is stored verbatim in `room.created.alert` (`events.go:105`) and capped at 1 MB (`server.go:24,150`, a5 @ 935f17d). `alertin.MaxBodyBytes`
  is the same 1 MiB.
- `title`, `severity` and `pipeline_id` come from the sender. C5 cleans title and severity, but accepts a label-supplied pipeline id if it only
  matches `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$` (`validPipelineID`, `alertin.go`); it is **not** checked against `owners.yaml` for `ViaLabel`.

## 2. Options

| | A. Bearer token per source | B. HMAC signature per source | C. Network controls only (mTLS / IP allow-list) |
|---|---|---|---|
| Security | the token is the whole secret; stolen from a log or proxy = full access until rotated; replayable | body is bound to the secret; with a signed timestamp (Grafana, generic) replay is bounded; no secret on the wire | strong against outsiders; useless against anyone inside the allowed range; Alertmanager/Grafana/PagerDuty cloud IPs change |
| Effort | trivial | needs a sender that can sign | proxy config, per customer |
| Sender support | Alertmanager (no signing, `http_config.authorization`), Datadog (custom header) | Grafana, PagerDuty V3, our generic form | any |
| Dev-mode impact | `curl -H 'Authorization: Bearer ...'` | `alertin.SignGeneric` helper | none |
| Ops burden | one secret per source | one secret per source | certificates / lists to maintain |

## 3. Recommendation

**Use the strongest method each source supports (A or B per source, exactly as C5 implements it), require TLS, and treat network controls as an
optional extra layer at the reverse proxy, not as the control.**

| Source | Check (C5) | Replay bound |
|---|---|---|
| Alertmanager | `Authorization: Bearer <token>` | none from the credential; `DedupKey` (group key) |
| Grafana | hex HMAC-SHA256 of `<ts>:<body>` in `X-Grafana-Alerting-Signature`; `X-Grafana-Alerting-Timestamp` required | ±5 min (`DefaultMaxSkew`) |
| PagerDuty V3 | `X-PagerDuty-Signature: v1=<hex>[,v1=<hex>]` over the body | no signed timestamp; `occurred_at` ≤ 1 h (`DefaultMaxEventAge`) → `ErrStale`; `DedupKey` |
| Datadog | token in `X-Piperoom-Token` | `date` ≤ 1 h; `DedupKey` |
| Generic | `X-Piperoom-Signature: t=<unix>,v1=<hex HMAC("<t>.<body>")>` | ±5 min |

Rules the room service (A5) follows:
1. **Route and auth.** `POST /api/alerts/{source}` (and `/api/alerts/webhook` as an alias of `generic`) is **outside** the `X-User`/session middleware and does not
   read cookies. The path chooses the `alertin.Source`; an unknown source → `404`.
2. **Raw bytes first.** Read the body with `http.MaxBytesReader(w, r.Body, alertin.MaxBodyBytes)`; pass those exact bytes to `Config.Ingest` (verify happens before
   JSON decode inside alertin). Comparisons are constant time in alertin (`hmac.Equal`, `subtle.ConstantTimeCompare`); A5 must not add its own.
3. **Errors → HTTP:** `ErrUnauthenticated`, `ErrStale` → `401` (same body for both); `ErrTooLarge` → `413`; `ErrIgnored` → `202`; anything else → `400`.
   A source with no secret is disabled and answers `401`. Log source, outcome, body size and a request id. Never log the body, headers or any secret.
4. **Secrets** come only from the environment or a file (§4). Not in the repo, `dev/`, logs, events or the UI.
5. **Rotation with two live secrets.** `alertin.Config` holds one secret per source. A5 builds one `Config` per secret in the list and calls `Ingest` on each in order until one does
   not return `ErrUnauthenticated`. Order: new secret first, old second. PagerDuty already accepts several `v1=` values in one header.
   Procedure: add new in front → update the sender → watch the log for the old secret's use (log the list index that matched, never the value) → drop the old.
6. **Dedupe and replay, race-free.** A5 keeps `alert_dedup(source, dedup_key, room_id, created_at)` with a **unique index on `(source, dedup_key)`** restricted to open rooms.
   Room creation and the dedup insert run in **one transaction**: `INSERT ... ON CONFLICT (source, dedup_key) DO NOTHING RETURNING room_id`; no row returned → read the existing
   `room_id` and answer `200 {room_id, created:false}`; a row returned → create the room and answer `201 {room_id, created:true}`. Two concurrent deliveries of the same alert
   therefore cannot open two rooms, and a replayed, validly signed request is harmless while the room is open. A resolved alert (`Alert.Status != firing`) → `202`, no room change.
   A replay after the room closed, inside `MaxEventAge`, can open one new room (accepted; bounded by rule 7).
7. **Respond fast, start the agent later.** Senders retry on timeouts (PagerDuty and Alertmanager both resend after a few seconds), so the handler does only verify → parse →
   dedupe → insert room and `room.created`, then answers. The agent start (first model call, tool wiring) is queued to the room service's existing worker, exactly as `POST /api/rooms`
   starts its agent. Target: answer within 1 s at p99; never hold the sender's connection on the model.
8. **Caps that hold across replicas.** New rooms per source per hour: a `COUNT(*)` over `rooms` (`created_by = 'webhook:<source>'`, last hour) inside the same transaction as rule 6,
   over → `429 rate_limited` with `Retry-After`. Request rate per source and failed-auth rate per remote address (30/min) belong at the ingress (any reverse proxy or gateway),
   because an in-process limiter is per replica and resets on deploy; A5 keeps a per-replica token bucket (60/min per source) only as a backstop. Numbers are proposals (Open question 3).
9. **Metrics.** Counters `piperoom_alerts_total{source,outcome}` (`created`, `reused`, `ignored`, `unauthenticated`, `stale`, `too_large`, `bad_payload`, `rate_limited`) and a
   histogram of handler latency; alert on a spike of `unauthenticated` per source (a secret rotation gone wrong or an attack).
10. **Untrusted fields.** `Title` and `Severity` are already cleaned by alertin; A5 treats them as data and shows them as text. `PipelineID` is looked up with `owners.Pipeline(id)`;
   an id that is not in `owners.yaml` is dropped and the room opens with a system message and proposals disabled (the existing `docs/api.md` behaviour). `Raw` is never put in
   a prompt without the `<untrusted>` wrapper (A4 `firstUserMessage` ignores `agent.Alert.Raw` today; keep a test for it).
11. **Actor.** The `room.created` event's actor is `{kind:"system", id:"webhook:<source>", name:"<source>"}`. Response: `{room_id, created}` only (no snapshot).

Standard Webhooks (`webhook-id`, `webhook-timestamp`, `webhook-signature: v1,<base64>`) is **not** chosen: no listed sender uses it, and C5 does not implement it. It stays an
option if the generic form is revised (Open question 4).

## 4. Contract changes

`docs/api.md` (this is the single `docs/api.md` proposal for the webhook; it replaces the `/api/alerts/webhook` row and adds error rows):
````diff
--- a/docs/api.md
+++ b/docs/api.md
@@ -43,7 +43,9 @@
 | 400 | `bad_request`, `validation` |
 | 401 | `unauthenticated` |
 | 403 | `not_driver`, `not_layer_owner`, `not_recipient` (hand-off accept by someone other than `to`) |
-| 404 | `not_found` |
+| 404 | `not_found` (also an unknown alert `source`) |
+| 413 | `payload_too_large` |
+| 429 | `rate_limited` |
 | 409 | `already_decided`, `room_closed`, `lease_held`, `stale_handoff` |
 
 `lease_held`: `control/grant` called by a non-driver while another user's lease is still live. All other
@@ -100,7 +102,7 @@
 | GET | `/api/me` | – | `{id,name,layers}` | |
 | GET | `/api/rooms` | – | `[RoomSnapshot]` | open rooms first |
 | POST | `/api/rooms` | `{title, severity, pipeline_id}` | `RoomSnapshot` | creator becomes driver; agent starts; unknown `pipeline_id` → `400 validation` |
-| POST | `/api/alerts/webhook` | any JSON | `RoomSnapshot` | generic alert; maps `title`/`severity`/`pipeline_id` if the body is an object; the raw body is stored verbatim in `room.created.alert`; unknown pipeline → room opens with a system message and proposals are disabled; driver = none until someone takes control. Webhook auth: see `docs/design/webhook-auth.md` (C9) |
+| POST | `/api/alerts/{source}` | the sender's own JSON | `{room_id,created}` | `source` = `alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic`; `/api/alerts/webhook` is an alias of `generic`. **Not** under `X-User`: authenticated per source (`docs/design/webhook-auth.md`); a source with no secret → `401`. Only a firing alert opens a room; a repeat of an open incident returns that room with `created:false`; a resolved or ignored event → `202`. The raw body is stored in `room.created.alert`; unknown pipeline → room opens with a system message and proposals are disabled; driver = none |
 | GET | `/api/rooms/{id}` | – | `RoomSnapshot` | |
 | GET | `/api/rooms/{id}/events?after=0&limit=500` | – | `{events:[Event], last_seq}` | limit max 1000; `events` is `[]` when nothing is after `after` |
 | POST | `/api/rooms/{id}/comments` | `{step_id?, text}` | `Event` | `@agent` in text sets `mentions_agent` |
````
`internal/config/config.go` (secrets; current first; empty = source off):
````diff
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -3,7 +3,10 @@
 // CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
 package config
 
-import "os"
+import (
+	"os"
+	"strings"
+)
 
 // Config holds every setting. Defaults match dev/docker-compose.yml.
 type Config struct {
@@ -18,6 +21,10 @@
 	UsersFile         string // USERS_FILE
 	Model             string // PIPEROOM_MODEL
 	Effort            string // PIPEROOM_EFFORT
+	// WebhookSecrets holds the alert-webhook secrets per alertin source (alertmanager, grafana, pagerduty, datadog,
+	// generic), read from PIPEROOM_WEBHOOK_SECRETS_<SOURCE> (upper case). Each value is comma-separated, current
+	// secret first; "" = that source is off. A value "file:<path>" is read from that file by the server (one per line).
+	WebhookSecrets map[string]string
 	// The Anthropic SDK reads ANTHROPIC_API_KEY itself; it is never stored here.
 
 	// Kafka connection security (internal/clients/netsec). Empty = PLAINTEXT, no login (dev stack).
@@ -60,6 +67,7 @@
 		UsersFile:         get("USERS_FILE", "dev/users.yaml"),
 		Model:             get("PIPEROOM_MODEL", "claude-opus-5-5"),
 		Effort:            get("PIPEROOM_EFFORT", "high"),
+		WebhookSecrets:    webhookSecrets(),
 
 		KafkaSecurityProtocol: get("KAFKA_SECURITY_PROTOCOL", ""),
 		KafkaSASLMechanism:    get("KAFKA_SASL_MECHANISM", ""),
@@ -84,6 +92,17 @@
 	}
 }
 
+// webhookSecrets reads PIPEROOM_WEBHOOK_SECRETS_<SOURCE> for every alertin source. Only sources that are set appear.
+func webhookSecrets() map[string]string {
+	m := map[string]string{}
+	for _, src := range []string{"alertmanager", "grafana", "pagerduty", "datadog", "generic"} {
+		if v := get("PIPEROOM_WEBHOOK_SECRETS_"+strings.ToUpper(src), ""); v != "" {
+			m[src] = v
+		}
+	}
+	return m
+}
+
 func get(key, def string) string {
 	if v, ok := os.LookupEnv(key); ok && v != "" {
 		return v
````
`internal/events/events.go`: none. Setting names: `PIPEROOM_WEBHOOK_SECRETS_ALERTMANAGER`, `_GRAFANA`, `_PAGERDUTY`, `_DATADOG`, `_GENERIC`.
Values contain no commas (generate with 32 random bytes, hex). `file:<path>` is resolved by A5, one secret per line.
`PagerDutyServices` and `DefaultPipeline` (alertin config, not secrets) are read from `owners.yaml`; their shape is an Open question.

## 5. Migration from today's dev mode

1. Today's callers use `X-User` on `/api/alerts/webhook` (a5 @ 935f17d). After the change that route needs a secret: `devctl` sets `PIPEROOM_WEBHOOK_SECRETS_GENERIC=<dev-secret>`
   in its environment and `devctl alert` (A1) signs with `alertin.SignGeneric`.
2. `/api/alerts/webhook` keeps working as the `generic` alias; its old `X-User` form is dropped (a deliberate breaking change, dev only).
3. No secret set → every source off → `401`. The server logs once at start-up which sources are on.
4. Existing rooms are untouched; add the `alert_dedup` table in a new migration.

## 6. Tasks per owner

- **A5 (1)** Build `[]alertin.Config` from `cfg.WebhookSecrets` (`file:` support, comma split, current first), plus `Owners`; fail start-up on an unreadable `file:`.
- **A5 (2)** Route `/api/alerts/{source}` outside the user middleware; `MaxBytesReader`; map errors; uniform `401` body; response `{room_id, created}`.
- **A5 (3)** `alert_dedup` migration with the unique index; dedupe + room creation in one transaction; hourly room cap as a count query; per-replica backstop limiter; log which list index matched.
- **A5 (3b)** Async agent start for webhook rooms (reuse the `POST /api/rooms` path); handler latency histogram and `piperoom_alerts_total` counter; test: 50 concurrent identical deliveries → one room.
- **A5 (4)** `owners.Pipeline(id)` check after `Ingest`; system actor `webhook:<source>`; tests: no/wrong secret, rotated secret both accepted, stale event, oversize body, replay → same room, resolved → 202.
- **C5 (1)** Validate a `ViaLabel` pipeline id against `Config.Owners` when it is set (drop it if absent). Optional if A5 does it; decide once.
- **C5 (2)** Export the matched-secret check or a `VerifyAny(secrets...)` if the lead prefers it to A5's loop (Open question 2).
- **A1** `devctl alert --source generic|...` that signs and posts; dev secret in the compose/`devctl` environment.
- **A6** none (the webhook has no UI). The room list shows the system actor as it shows any actor.

## 7. Open questions

1. `tasks/C5-alert-ingest.md` names `Incoming`, `DeliveryID`, `Verify(a Auth, ...)`, `DefaultAuth(source, secrets...)`; the branch implements `Config.Ingest` / `Alert`. This doc follows the branch. If C5 changes the API again, update §3 and §6 only.
2. Rotation by looping over `[]Config` costs one HMAC per secret and gives A5 the matched index. Prefer that, or a `secrets ...string` API in alertin?
3. Rate and cap numbers (60/min, 20 rooms/hour, 30 failed/min/address): confirm with the pilot's alert volume.
4. Do we want Standard Webhooks for the generic form, so senders can use an existing library?
5. Where do `PagerDutyServices` and `DefaultPipeline` live? Proposal: an additive `alerts:` block in `owners.yaml`; A1 owns that file.
6. Should a resolved alert write an event (e.g. `alert.resolved`) so the room shows it? Not proposed here (`events.go` has no such type).
7. A replayed PagerDuty body after the room closes can open one new room. A 15-minute re-open cooldown per `DedupKey` would close that; is it worth it?
8. Multi-tenant (F7): `PIPEROOM_WEBHOOK_SECRETS_<SOURCE>` is one secret set per deployment. A tenant-aware version is an `integrations` table (`tenant_id, source, secrets, enabled`) addressed as
   `/api/alerts/{source}/{integration_id}`; the env form stays as the single-tenant bootstrap. Decide when the second tenant is real, not before.
9. Ingress rate limiting assumes a reverse proxy in front of the Go server in production (also needed for TLS). Confirm that this is the deployment shape.

## 8. References

Header names, signing strings and field names are taken from C5's README and code (read from its branch) and from the lead's task file. The vendor pages below could not be fetched
from this sandbox (network blocked), so they are **not independently verified here**; the reviewer should check them.
- Prometheus Alertmanager configuration (`webhook_config`, `http_config`): https://prometheus.io/docs/alerting/latest/configuration/
- PagerDuty webhook signatures (V3): https://developer.pagerduty.com/docs/verifying-webhook-signatures
- Grafana webhook contact point: https://grafana.com/docs/grafana/latest/alerting/configure-notifications/manage-contact-points/integrations/webhook-notifier/
- Datadog webhooks integration: https://docs.datadoghq.com/integrations/webhooks/
- Standard Webhooks: https://www.standardwebhooks.com/
- OWASP Logging cheat sheet (what not to log): https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
