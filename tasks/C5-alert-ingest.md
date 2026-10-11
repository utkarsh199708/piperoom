# C5 — Alert ingestion adapters (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-12 00:00 IST):** implemented as **PR #11** (`claude/dazzling-volta-8xn3f3`) before
> this file landed. Its API **supersedes the names below**: `Config.Ingest(src Source, h http.Header, body []byte)
> (Alert, error)` (verify → parse → staleness), `Alert{Source, Status, Title, Severity, PipelineID, PipelineVia,
> DedupKey, URL, OccurredAt, Raw}`, `Alert.Agent(roomID, pipelineMap) agent.Alert`, `Alert.RoomCreated()`,
> `SignGeneric`, `ErrUnauthenticated`; a source with no secret is disabled. C9 and A5 use these names.


Read these first: `AGENTS.md`, `docs/collab.md`, `docs/api.md` (the `/api/alerts/webhook` row), `docs/security/findings.md` (F9),
`internal/agent/contract.go` (`Alert`), `internal/events/events.go` (`RoomCreatedPayload`) and `internal/owners/owners.go`.
You own a **new** package, `internal/alertin/**`. You may import `agent` and `owners`, but do not edit them. Do not edit
`internal/httpapi/**` (A5) or any contract file.

You run in the cloud. That means no Docker and no network in tests. Add no new modules: use the standard library only.
Build no HTTP server, no handler and no wiring. A5 adds the endpoint later and calls your functions. Work on branch
`c5/alert-ingest`, open a GitHub PR, and never touch `main`.

## Goal
Turn alert webhooks into `agent.Alert` values that the room service can open rooms from, with the pipeline already chosen.
The sources are Prometheus Alertmanager, Grafana, PagerDuty V3, Datadog and a generic JSON form. Also give A5
webhook authentication that fails closed (F9).

## Work
1. **API.** Use exactly these names.
   ```go
   const (
       SourceAlertmanager = "alertmanager"; SourceGrafana = "grafana"; SourcePagerDuty = "pagerduty"
       SourceDatadog = "datadog"; SourceGeneric = "generic"
       StatusFiring = "firing"; StatusResolved = "resolved"
       MaxBodyBytes = 1 << 20 // the same 1 MB body limit A5 uses
   )
   // Sentinel errors (errors.New): ErrUnknownSource, ErrTooLarge, ErrBadPayload, ErrIgnored, ErrUnauthorized, ErrBadAuthConfig

   // Incoming is one alert, ready to open (or resolve) a room.
   type Incoming struct {
       Alert      agent.Alert // Title, PipelineID and Raw are set; RoomID and PipelineMap are left for the room service
       Source     string
       Severity   string    // "sev1".."sev4" -> events.RoomCreatedPayload.Severity
       Status     string    // StatusFiring | StatusResolved
       DedupKey   string    // the incident's stable id at the source; the same value comes back on resolve
       DeliveryID string    // per-delivery id if the source sends one (PagerDuty X-Webhook-Id), else ""
       StartsAt   time.Time // zero if unknown
       Match      string    // how PipelineID was chosen (formats in item 7)
   }
   func NewParser(o *owners.Owners) (*Parser, error) // returns an error if o == nil
   func (p *Parser) Parse(source string, body []byte, h http.Header) ([]Incoming, error)
   func DefaultAuth(source string, secrets ...string) (Auth, error)
   func Verify(a Auth, h http.Header, body []byte, now time.Time) error
   ```
   Why this is not `Parse(...) (agent.Alert, error)`:
   - `agent.Alert` has no severity field, and rooms need one.
   - Alertmanager and Grafana send many alerts in one batch, and one batch can cover several pipelines.
   - The owners lookup must be passed in, because AGENTS.md rule 5 bans global state.

   How `Parse` behaves:
   - It returns 1 or more `Incoming` values, or an error. The slice is sorted by `PipelineID`, with unmatched (`""`) last.
   - `Raw` is a **copy** of the whole body.
   - JSON decoding ignores unknown fields, because sources add new ones (for example, Alertmanager's `notification_reason`).
     Do not use `DisallowUnknownFields`.
2. **Common rules.**
   - Error order: an unknown source gives `ErrUnknownSource`. Then a body longer than `MaxBodyBytes` gives `ErrTooLarge`.
     Then an empty body or invalid JSON gives `ErrBadPayload` (wrap the JSON error).
   - Title cleanup:
     - Replace every `unicode.IsControl` rune with a space, then collapse runs of whitespace.
     - Cut the title to 200 runes and add `…` if you cut it.
     - If the title is empty, use `Alert from <source>`.
     - Why: the title goes into the model prompt (A4's `firstUserMessage`), so treat it as untrusted text.
   - Severity table (match case-insensitively):

     | Input | Severity |
     |---|---|
     | `sev1`..`sev4` | unchanged |
     | `critical`, `p1` | `sev1` |
     | `error`, `high`, `p2` | `sev2` |
     | `warning`, `warn`, `medium`, `p3` | `sev3` |
     | `info`, `low`, `success`, `p4`, `p5` | `sev4` |
     | empty or anything else | `sev3` |
3. **Alertmanager and Grafana** (`prom.go`, one shared decode struct).
   - Top-level fields: `version`, `groupKey`, `truncatedAlerts`, `status`, `receiver`, `groupLabels`, `commonLabels`,
     `commonAnnotations`, `externalURL` and `alerts[]`.
   - Fields of each item in `alerts[]`: `status`, `labels`, `annotations`, `startsAt`, `endsAt`, `generatorURL` and `fingerprint`.
   - Grafana adds `orgId`, `title`, `state` and `message`, plus `values`, `silenceURL`, `dashboardURL`, `panelURL` and
     `imageURL` on each alert. Ignore all of these.
   - `version` must be `"4"` for Alertmanager and `"1"` for Grafana. Anything else gives `ErrBadPayload`. This also rejects
     payloads from Grafana's legacy alerting.
   - Keep only the alerts whose `status` equals the top-level `status`. If none are left, return `ErrIgnored`.
   - Find each alert's pipeline from its `labels` (item 7). Fall back to the text in `annotations["summary"]`. Group the
     alerts by pipeline and return one `Incoming` per group.
   - **Title:** take the group's first alert, ordered by (`startsAt`, `fingerprint`). Use its `annotations["summary"]`, or
     `labels["alertname"]` if there is no summary. If the group has n > 1 alerts, add ` (+<n-1> more)`.
   - **Severity:** the highest severity among the group's `labels["severity"]` values.
   - **DedupKey:** `groupKey + "/" + pipelineID`. Use `"/unmatched"` when the pipeline ID is empty.
   - **StartsAt:** the earliest `startsAt` in the group.
4. **PagerDuty V3** (`pagerduty.go`).
   - The payload is an envelope: `event.{id, event_type, resource_type, occurred_at, agent, client, data}`.
   - For incidents, `data` has these fields: `id`, `type`, `self`, `html_url`, `number`, `status`, `incident_key`,
     `created_at`, `title`, `service{summary,…}`, `assignees`, `escalation_policy`, `teams`, `priority{summary,…}`
     (can be null), `urgency`, `conference_bridge` and `resolve_reason`.
   - `resource_type` must be `incident`.
   - `incident.triggered` means firing and `incident.resolved` means resolved. Any other `event_type` gives `ErrIgnored`.
   - Field mapping:
     - Title comes from `data.title`.
     - Severity comes from `data.priority.summary` (P1 to P5). If `priority` is null, use `data.urgency`.
     - `DedupKey` is `data.id`.
     - `DeliveryID` is the `X-Webhook-Id` header. PagerDuty repeats it on every retry.
     - `StartsAt` is `data.created_at`.
   - PagerDuty has no labels, so the pipeline comes only from text (item 7c): `data.title` and `data.service.summary`.
5. **Datadog** (`datadog.go`).
   - Datadog payloads are defined by the user, so you define this template for the Webhooks tile. Document it in the
     package doc. It uses only documented variables, and every value is quoted:
     ```json
     {"piperoom_template":"1","alert_id":"$ALERT_ID","alert_cycle_key":"$ALERT_CYCLE_KEY","title":"$EVENT_TITLE",
      "transition":"$ALERT_TRANSITION","priority":"$ALERT_PRIORITY","alert_type":"$ALERT_TYPE",
      "pipeline":"$TAGS[pipeline]","tags":"$TAGS","scope":"$ALERT_SCOPE","date_posix":"$DATE_POSIX",
      "link":"$LINK","org_id":"$ORG_ID"}
     ```
   - If `piperoom_template` is not `"1"`, return `ErrBadPayload`. This catches the tile's default payload.
   - `transition` equal to `Recovered` (case-insensitive) means resolved. Every other value means firing.
   - Labels:
     - Parse `tags` first, then `scope`. Both are comma-separated `key:value` pairs: split each pair on the first `:` and
       trim spaces. A `scope` value wins over the same key in `tags`.
     - Last, set the `pipeline` label if the `pipeline` field is not empty.
   - Other fields:
     - Severity comes from `priority`, or from `alert_type` if `priority` is empty.
     - `DedupKey` is `alert_cycle_key`, or `alert_id` if it is empty.
     - `StartsAt` comes from `date_posix` (Unix seconds). If it is empty or not a number, leave `StartsAt` zero.
6. **Generic** (`generic.go`). Keep the behaviour in `docs/api.md` ("maps title/severity/pipeline_id if present").
   Document this form in the package doc:
   `{"title","severity","pipeline_id","status","dedup_key","starts_at" (RFC 3339),"labels":{...}}`
   - Every field is optional.
   - `status` defaults to `firing`. Any value other than `firing` or `resolved` gives `ErrBadPayload`.
   - Treat `pipeline_id` as the explicit `pipeline` label.
7. **Pipeline resolution** (`resolve.go`). Try the rules in order (a, b, c, d); the first rule that gives an answer wins.
   Use only the fields that `owners.Pipeline` has on `main`: `ID`, `Connectors`, `ConsumerGroups`, `SourceTables` and
   `SinkTables`. Do not parse `dev/owners.yaml` yourself. Topic matching waits until owners has a topic field.

   a. **Explicit label.** Use the label `pipeline`, or else `pipeline_id`.
      - If `Owners.Pipeline(id)` knows the ID, use it. `Match` = `label pipeline=<id>`.
      - If the ID is unknown, the alert is unmatched. `Match` = `unknown pipeline "<id>"`. Never guess past an explicit value.

   b. **Name labels.** Compare values with `strings.EqualFold`.

      | Label keys | Compared with |
      |---|---|
      | `connector`, `connector_name` | `Connectors` |
      | `consumergroup` (kafka_exporter's label), `consumer_group`, `group` | `ConsumerGroups` |
      | `table`, `source_table`, `sink_table` | `SourceTables ∪ SinkTables` |

      - A table value without a dot also matches the part after the last dot of an owners table name.
      - If exactly one pipeline matches, use it. `Match` = `<key>=<value>`.
      - If several pipelines match, the alert is unmatched. `Match` = `ambiguous: <id>, <id>` (IDs sorted).

   c. **Text.**
      - Split the text on every rune that is not a letter, a digit, or one of `_ . - : =`.
      - Trim trailing `.` and `:` from each token.
      - A token `pipeline:<id>` or `pipeline=<id>` works like rule a, with `Match` = `text pipeline=<id>`.
      - Otherwise, a token matches if it equals a connector name, a consumer group name or a schema-qualified table name.
        Then `Match` = `text <name>`.
      - A bare pipeline ID in free text does **not** match. For example, "orders API slow" must not pick `orders`.
      - One pipeline gives a match. Several pipelines give ambiguous, as in rule b.

   d. **Nothing matched.** `PipelineID` = `""` and `Match` = `unmatched`. `Parse` does not fail; A5 decides what to do.
8. **Auth** (`verify.go`). It fails closed, and nothing it returns or logs contains secrets, signatures or the body.
   ```go
   const (AuthPagerDuty = "pagerduty"; AuthHMAC = "hmac"; AuthBearer = "bearer")
   type Auth struct {
       Mode            string
       Secrets         []string      // at least 1; a match with any of them passes (supports secret rotation)
       SignatureHeader string        // AuthHMAC only
       TimestampHeader string        // AuthHMAC only; "" means the body alone is signed (no replay check)
       MaxSkew         time.Duration // AuthHMAC with a timestamp; default 5m
   }
   ```
   - **Config errors:** a zero `Auth`, an unknown mode or no secrets gives `ErrBadAuthConfig`. HMAC and Bearer secrets
     shorter than 16 bytes also give `ErrBadAuthConfig`. Every rejected request wraps `ErrUnauthorized`.
   - **`AuthPagerDuty`:**
     - Header: `X-PagerDuty-Signature`. It holds 1 or more comma-separated `v1=<hex>` entries. Keep only the `v1=` entries.
     - Expected value: hex of HMAC-SHA256(secret, raw body). The request passes if any entry matches any secret.
     - PagerDuty sends no timestamp and retries for up to 48 h, so there is no freshness window. Replay defence is A5
       deduplicating on `DeliveryID`.
   - **`AuthHMAC`:**
     - This is Grafana's built-in signing. Use it also for scripts and any other sender that can sign.
     - The header holds the hex of HMAC-SHA256(secret, `ts + ":" + body`) when `TimestampHeader` is set, and of the body
       alone when it is not.
     - `ts` is in Unix seconds. Reject the request if `ts` is missing, is not an integer, or `|now − ts| > MaxSkew`.
   - **`AuthBearer`:**
     - Use it for Alertmanager `http_config.authorization`, Grafana's "Authentication Header", and Datadog Custom Headers
       that use a hidden custom variable.
     - The header is `Authorization: Bearer <token>`. The scheme is case-insensitive.
     - Compare `sha256.Sum256` of the token and of the secret with `subtle.ConstantTimeCompare`. This hides the length.
   - **MAC checks:** decode the signature with `hex.DecodeString`, then compare with `hmac.Equal`. Bad hex means reject.
   - **`DefaultAuth`:**
     - `alertmanager` and `datadog` use Bearer.
     - `pagerduty` uses PagerDuty.
     - `grafana` uses HMAC with `X-Grafana-Alerting-Signature` and `X-Piperoom-Timestamp`. Grafana has no default
       timestamp header, so the operator must type this name into Grafana's "Timestamp Header" field.
     - `generic` uses HMAC with `X-Piperoom-Signature` and `X-Piperoom-Timestamp`.
9. **`doc.go`.** Write the package doc. For each source, describe the receiver config, the Datadog template, the generic
   form and the resolution rules, and cite the URLs below. Add one rule for callers: `Verify` the **raw, unaltered** body
   before calling `Parse`.
10. **PR.** Add a `## Contract change request` with the exact `docs/api.md` text you propose for A5. It should cover:
    - a route per source, for example `POST /api/alerts/webhook/{source}`
    - these auth headers in place of `X-User`
    - `ErrIgnored` leading to a `202` with no room

    Do not edit `docs/api.md` yourself.

## Sources
Before coding, check every field name again with WebFetch.
- Alertmanager: https://prometheus.io/docs/alerting/latest/configuration/#webhook_config. The docs give the schema only,
  with no concrete example, so build the fixture from the schema and say so.
- Grafana: https://grafana.com/docs/grafana/latest/alerting/configure-notifications/manage-contact-points/integrations/webhook-notifier/.
  Copy the "Default JSON payload" verbatim, and read the HMAC section.
- PagerDuty:
  - https://docs.pagerduty.com/developer/webhooks-overview (its only incident example is `incident.priority_updated`)
  - https://docs.pagerduty.com/developer/verifying-webhook-signatures
  - https://docs.pagerduty.com/developer/webhook-behavior (`X-Webhook-Id`, 48 h retries, 256 KB size limit)
- Datadog: https://docs.datadoghq.com/integrations/webhooks/ (variables, custom headers, custom variables)
- kafka_exporter's `consumergroup` label: https://github.com/danielqsj/kafka_exporter#metrics
- HMAC-SHA256 test vector: https://www.rfc-editor.org/rfc/rfc4231 (test case 2)

## Tests
Write table-driven unit tests with no network. Build the owners config as an `owners.Owners` struct literal that mirrors
`dev/owners.yaml`. For the ambiguous case, add a second pipeline that also sinks to a table named `orders`.

Put fixtures in `internal/alertin/testdata/`. List each one in `testdata/SOURCES.md` with its URL and whether it is
**verbatim** or **derived**.

**Grafana**
- `grafana_doc.json` (verbatim) gives 1 `Incoming` with:
  - PipelineID `""`, Match `unmatched`
  - Status firing, Severity sev3
  - Title `This alert was triggered for zone us-1 (+1 more)`
  - DedupKey `{}:{}/unmatched`
- `grafana_pipeline.json` is the same payload with `"pipeline":"orders"` added to both alerts' labels. It gives
  PipelineID `orders` and Match `label pipeline=orders`.

**Alertmanager**
- `alertmanager_firing.json` (version `"4"`) has two alerts:
  - one with `consumergroup=order-sync` and `severity=critical`
  - one with `connector=other-connector`

  It gives two `Incoming` values: first `orders` (sev1, Match `consumergroup=order-sync`), then `""`.
- `alertmanager_resolved.json` gives Status resolved.
- Version `"3"` gives `ErrBadPayload`.

**PagerDuty**
- `pagerduty_priority_updated.json` (verbatim) gives `ErrIgnored`.
- `pagerduty_triggered.json` is the same payload with only `event_type` changed. It gives:
  - firing, sev1, DedupKey `PGR0VU2`
  - Title `A little bump in the road`, unmatched
  - DeliveryID taken from the `X-Webhook-Id` header
- The same body with the title `Lag on pg-orders-connector.` gives `orders` with Match `text pg-orders-connector`.

**Datadog**
- `datadog_triggered.json` is the template filled with the docs' example values, so it is derived:
  - `ALERT_ID` 1234, `ALERT_PRIORITY` P1, `DATE_POSIX` 1406662672
  - `TAGS` `monitor, name:myService, role:computing-node, pipeline:orders`

  It gives `orders`, sev1 and StartsAt `2014-07-29T19:37:52Z`.
- `Recovered` gives resolved.
- A body with no `piperoom_template` gives `ErrBadPayload`.

**Generic**
- The full form parses.
- `{}` gives the title `Alert from generic`, sev3 and unmatched.
- A bad `status` gives `ErrBadPayload`.

**Error paths**
- An unknown source gives `ErrUnknownSource`.
- A body of `MaxBodyBytes+1` bytes gives `ErrTooLarge`.
- Invalid JSON and an empty body both give `ErrBadPayload`.
- `Raw` is a copy: change the input slice after `Parse` and check that `Raw` did not change.

**Title cleanup and severity**
- Test titles containing ESC, U+009B and newlines, and a 500-rune title.
- Test every row of the severity table.

**Resolver**
- Test every rule a to d.
- Also test: an unknown explicit pipeline, a bare pipeline ID in text (must not match), and a token with a trailing dot.

**Verify**
- PagerDuty:
  - RFC 4231 test case 2: secret `Jefe`, body `what do ya want for nothing?`,
    header `v1=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843`. It passes.
  - The same value as the second of two signatures also passes.
  - `v0=` only, a wrong secret, or a missing header gives `ErrUnauthorized`.
  - Two secrets (rotation) work.
- HMAC with timestamp:
  - Secret `piperoom-test-secret-0001`, `X-Piperoom-Timestamp: 1760000000`, body `{"title":"x"}`,
    `X-Piperoom-Signature: e40e1307b63e1ba6f726ef186d5d0c8bfa7d39ed96c6e7dc4c25c37e146499cc`.
  - `now` = ts+60 s passes. ts+301 s and ts−301 s are rejected.
  - A missing timestamp, a non-numeric timestamp, or bad hex is rejected.
  - Also test body-only mode.
- Bearer: the right token passes. A wrong token, a `Basic` scheme or a missing header is rejected.
- Config: a zero `Auth` and a short secret give `ErrBadAuthConfig`.
- Check every error with `errors.Is`.

## Done when
- `go build ./...`, `go vet ./...` and `go test ./internal/alertin/...` all pass.
- Only `internal/alertin/**` changed, and there are no new modules.
- Every fixture is listed in `testdata/SOURCES.md`.
- The Go code plus tests come to about 800 lines. The JSON copied from the docs does not count.
- The PR includes the `docs/api.md` contract change request.
- The PR lists these gaps:
  - There is no topic matching yet.
  - Bearer tokens (Alertmanager, Datadog) cannot stop replays.
  - PagerDuty replay defence depends on A5 deduplicating on `DeliveryID`.
