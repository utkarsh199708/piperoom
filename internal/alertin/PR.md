# [C5] Alert ingestion: verify and parse Alertmanager, Grafana, PagerDuty, Datadog and generic webhooks

PR text for a lead to open: base `main`, head `claude/dazzling-volta-8xn3f3`. The branch name is forced by the sandbox: this session may push only to its assigned branch, and it does not open PRs itself. Delete this file when the PR is opened or merged.

[C5] READY FOR REVIEW

## What
New package `internal/alertin` (only files under it changed):
- `Config.Ingest(src, header, body)`: verify → parse → staleness check → one normalized `Alert`
  (`Source`, `Status`, single-line `Title`, `Severity` sev1..sev4, `PipelineID` + `PipelineVia`, `DedupKey`, `URL`, `OccurredAt`, verbatim `Raw`).
- `Alert.Agent(roomID, pipelineMap) agent.Alert` and `Alert.RoomCreated() events.RoomCreatedPayload`. These map onto the contract types without changing them.
- Sources and how each is checked:
  | Source | Check |
  |---|---|
  | Alertmanager v4 | `Authorization: Bearer` token (Alertmanager can't sign bodies) |
  | Grafana unified alerting | `X-Grafana-Alerting-Signature` = hex HMAC-SHA256(`ts:body`); timestamp header **required**, ±5 min |
  | PagerDuty V3 | `X-PagerDuty-Signature` v1 list (rotation); `occurred_at` ≤ 1 h |
  | Datadog | shared token header (Datadog can't sign bodies); documented payload template; `date` ≤ 1 h |
  | Generic JSON | `X-Piperoom-Signature: t=<unix>,v1=<hex HMAC-SHA256("t.body")>`, ±5 min |
- Pipeline mapping order: explicit `pipeline`/`pipeline_id` label, tag or field → PagerDuty service map → exactly one `owners.yaml` pipeline whose connector, consumer group or table the alert names → `DefaultPipeline`. If more than one pipeline matches, it does not guess.
- `README.md` in the package: how A5 calls it, and setup for each sender.
- `origin/main` merged in at 8e326a3 (normal merge, no rebase or force-push).

## Why
So that real alerts can open rooms (C5). This also settles part of security F9 (webhook authentication).

## How I tested
```
gofmt -l .
(no output)
go vet ./... && go vet -tags integration ./... && go vet -tags claude ./internal/agent/...
(no output)
go test -count=1 -race ./internal/alertin
ok  	piperoom/internal/alertin	1.043s
go test -count=1 ./...
ok  	piperoom/cmd/devconsumer
ok  	piperoom/cmd/devctl
ok  	piperoom/cmd/investigate
ok  	piperoom/internal/actions
ok  	piperoom/internal/agent
ok  	piperoom/internal/alertin
ok  	piperoom/internal/clients/connect
ok  	piperoom/internal/clients/kafka
ok  	piperoom/internal/clients/pgconv
ok  	piperoom/internal/clients/postgres
ok  	piperoom/internal/clients/schemareg
ok  	piperoom/internal/clients/snowflake
ok  	piperoom/internal/clients/warehouse
ok  	piperoom/internal/owners
ok  	piperoom/internal/stacklock
ok  	piperoom/internal/testenv
ok  	piperoom/internal/tools/dbtools
ok  	piperoom/internal/tools/kafkatools
ok  	piperoom/internal/topology
```
- Table tests for each verifier: good, wrong secret, body changed, timestamp changed, stale and future, missing header, not hex, rotation, disabled source fails closed. Error text is checked to never contain a secret.
- Mutation check: disabling each check (Grafana MAC, PagerDuty MAC, generic MAC, bearer, header token, skew, event age) makes 2–5 tests fail.
- Fixtures: `grafana.json` is copied as is from https://grafana.com/docs/grafana/latest/alerting/configure-notifications/manage-contact-points/integrations/webhook-notifier/ ("Default JSON payload").
  `pagerduty_priority_updated.json` is copied as is from https://developer.pagerduty.com/docs/webhooks-overview (Webhook Payload example). It is tested as ignored.
  `pagerduty_triggered.json` is the same file with only `event_type` changed to `incident.triggered`.
  `alertmanager.json` is built from the documented v4 schema at https://prometheus.io/docs/alerting/latest/configuration/#webhook_config, because that page has no concrete example.
  `datadog.json` follows our template. The variables are from https://docs.datadoghq.com/integrations/webhooks/#variables.
  Signature schemes: https://docs.pagerduty.com/developer/verifying-webhook-signatures and the Grafana page above (HMAC Signature).

## Not done / known gaps
- No HTTP endpoint and no wiring. A5 owns `internal/httpapi` (the snippet is in the README).
- Dedup lives in the room service: it must look up an open room by `DedupKey`. This is also the replay defense, because PagerDuty signs no timestamp.
- No rate limiting per source (A5, HTTP layer).
- Grafana's timestamp unit isn't documented. Seconds are assumed, and values > 1e12 are treated as ms.
- Lead's message asked for "HMAC for sources without signing". Alertmanager and Datadog cannot compute a body HMAC, so their native header token is used. The generic form gets the HMAC.
- About 900 non-test lines, above the 600 aim. Most of it is doc comments and per-source structs.
- `docs/design/webhook-auth.md` (C9) was not on main when this was written. If C9 picks a different generic signature format, `verifyGeneric` is the only place that changes.

## New dependencies
None (standard library only).

## Contract change request
1. **`internal/config/config.go`**: add the following. Secrets are never logged.
   ```go
   AlertmanagerToken string // ALERTMANAGER_WEBHOOK_TOKEN
   GrafanaWebhookSecret string // GRAFANA_WEBHOOK_SECRET
   PagerDutyWebhookSecret string // PAGERDUTY_WEBHOOK_SECRET
   DatadogWebhookToken string // DATADOG_WEBHOOK_TOKEN
   GenericWebhookSecret string // ALERT_WEBHOOK_SECRET
   PagerDutyServicePipelines string // PAGERDUTY_SERVICE_PIPELINES, "PF9KMXH=orders,PAB123=payments"
   AlertDefaultPipeline string // ALERT_DEFAULT_PIPELINE
   ```
   All default to empty, so every source stays disabled until it is configured.
2. **`docs/api.md`**:
   - Add `POST /api/alerts/{alertmanager|grafana|pagerduty|datadog}`, with the same response as `/api/alerts/webhook`.
   - `/api/alerts/webhook` becomes the `generic` source and requires `X-Piperoom-Signature`.
   - Status codes: `202 {"status":"ignored"}` for resolved and non-alert events, `401` for a bad signature or a stale event, and `413` above 1 MiB.
3. **`internal/events/events.go`**: add `Source string \`json:"source,omitempty"\`` and `DedupKey string \`json:"dedup_key,omitempty"\`` to `RoomCreatedPayload`. With these, dedup survives restarts and the UI can show where the alert came from.
4. Finding for **A4** (not a contract change): `firstUserMessage` (`internal/agent/prompt.go`) puts `alert.Title` into the prompt without wrapping it. With real alerts the title is external text, so it should go through `wrapUntrusted`. alertin already makes it one line and caps it at 200 runes.
