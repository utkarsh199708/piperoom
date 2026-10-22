# Alert ingestion: how it is used and sender setup

`internal/alertin` checks that a webhook really comes from the configured sender, then turns it into one
`alertin.Alert`. It has no HTTP server. The room service (A5) reads the body (capped at `MaxBodyBytes`),
then:

```go
a, err := cfg.Ingest(alertin.PagerDuty, r.Header, body)
switch {
case errors.Is(err, alertin.ErrIgnored):                                   // 202, not an alert
case errors.Is(err, alertin.ErrUnauthenticated), errors.Is(err, alertin.ErrStale): // 401
case errors.Is(err, alertin.ErrTooLarge):                                  // 413
case err != nil:                                                           // 400
case a.Status != alertin.StatusFiring:                                     // 202, resolved
default: // open (or reuse, by a.DedupKey) a room with a.RoomCreated(); start the agent with a.Agent(roomID, pipelineMap)
}
```

| Source | How the request is checked | Opens a room on |
|---|---|---|
| Alertmanager | `Authorization: Bearer <token>` | group `status: firing` |
| Grafana | hex HMAC-SHA256 of `<unix ts>:<body>` in `X-Grafana-Alerting-Signature`; ts in `X-Grafana-Alerting-Timestamp`, ±5 min | group `status: firing` |
| PagerDuty | `X-PagerDuty-Signature: v1=<hex HMAC-SHA256(body)>` (any of a comma list); `occurred_at` ≤ 1 h old | `incident.triggered`, `incident.reopened` |
| Datadog | shared token in `X-Piperoom-Token`; `date` ≤ 1 h old | any transition except `Recovered` |
| Generic | `X-Piperoom-Signature: t=<unix ts>,v1=<hex HMAC-SHA256("<ts>.<body>")>`, ±5 min | `status` other than `resolved` |

A source with no secret is disabled, and every request for it fails `ErrUnauthenticated`.

**Pipeline**, in this order:
1. the alert's own `pipeline` or `pipeline_id` label, tag or field;
2. the `dag_id` label (Alertmanager, Grafana) or field (generic): the single batch pipeline whose `dags`
   lists it. The id is compared exactly (trimmed, case-sensitive). In an Alertmanager or Grafana group the
   `dag_id` of every firing alert counts (plus the common and group labels): an id no pipeline lists is
   skipped, and the pipeline must list every id that is left. Datadog and PagerDuty `dag_id` tags are not
   read here;
3. for PagerDuty, the service ID looked up in `Config.PagerDutyServices`;
4. the single `owners.yaml` pipeline whose connector, consumer group or table the alert mentions (label
   values exactly, or whole names in the title or summary). Only the first `MaxMatchTextRunes` (2000) runes
   of each title or description are scanned, and only the first `MaxLabelValues` (256) label values are
   compared, each cut to `MaxLabelRunes` (256). A name past those limits is not matched. Explicit
   `pipeline` / `pipeline_id` and `dag_id` labels are read directly and are not affected by these limits;
5. `Config.DefaultPipeline`.

If more than one pipeline matches in step 2 or step 4, no guess is made: a DAG listed by two pipelines, or a
group whose alerts name DAGs of two pipelines, skips step 2. A `dag_id` that no pipeline lists changes
nothing: the alert resolves as it would without one. `Alert.PipelineVia` records which step was used
(`label`, `dag`, `service`, `owners`, `default`).

**Repeats:** `DedupKey` is the Alertmanager/Grafana group key, the PagerDuty incident ID, the Datadog
alert cycle key, or the generic `dedup_key` (otherwise a hash of the body). The room service should
return the room that is already open for that key. This stops Alertmanager `repeat_interval` re-sends,
and validly signed replays, from opening new rooms.

## Alertmanager

```yaml
receivers:
  - name: piperoom
    webhook_configs:
      - url: https://piperoom.example.com/api/alerts/alertmanager
        send_resolved: false
        http_config:
          authorization:
            type: Bearer
            credentials_file: /etc/alertmanager/piperoom-token
```

Add the label `pipeline: orders` to the alert rules (or add it with `alert_relabel_configs`). For a batch
pipeline, an Airflow metric's `dag_id` label is enough when `owners.yaml` lists that DAG under `dags`.

## Grafana (unified alerting)

Contact point → **Webhook**. Under **Optional settings → HMAC Signature**, set **Secret**. Leave
**Header** as `X-Grafana-Alerting-Signature` and set **Timestamp Header** to
`X-Grafana-Alerting-Timestamp`. Grafana has no default timestamp header, and requests without one are
refused. Add a `pipeline` label to the alert rule.

## PagerDuty

Integrations → Generic Webhooks (v3). Subscribe to `incident.triggered` and `incident.reopened`. Put the
signing secret that PagerDuty shows once in `PagerDutySecret`. Map each service ID (`PXXXXXX`) to a
pipeline in `PagerDutyServices`.

## Datadog

Integrations → Webhooks → New. Custom headers: `{"X-Piperoom-Token": "<token>"}`. Payload:
`alertin.DatadogPayloadTemplate`, copied as is. Tag the monitors `pipeline:orders` and add
`@webhook-<name>` to the monitor message.

## Generic

Any JSON body. If the body is an object, these keys are read when present: `title`, `severity`,
`pipeline_id`, `dag_id`, `status`, `dedup_key`, `url`. An Airflow failure callback can post
`{"title": "...", "dag_id": "revenue_daily"}`. Sign the body with `alertin.SignGeneric(secret, time.Now(), body)`
and send the result in `X-Piperoom-Signature`.

Alertmanager and Datadog cannot compute a signature over the body. Their token is a bearer secret:
send it only over HTTPS and rotate it like a password.

# Change webhook: deploys, migrations and vendor incidents (X4)

`POST /api/changes/webhook` records one external change in a pipeline's change log (`docs/api.md`, External
changes). It opens no room and wakes no agent: the agent finds the row with the "what changed" tool when a room of
that pipeline opens. The room service mounts `alertin.NewChangeWebhook` on `alertin.ChangeWebhookPath`.

- **Signature:** as the generic alert form, `X-Piperoom-Signature: t=<unix ts>,v1=<hex HMAC-SHA256("<ts>.<body>")>`,
  timestamp within ±5 min, keyed with `CHANGES_WEBHOOK_SECRET`. During a rotation set both secrets, comma
  separated (`new,old`); a request signed with either passes. Empty = every request `401`.
- **Body:** one JSON object, at most 4 KB, no other fields: `pipeline_id`, `kind` (`deploy`, `migration`,
  `vendor_incident`), `at` (RFC 3339, at most 7 days old and 5 min ahead), `title` (≤ 300 characters after
  control characters are removed), `service` (≤ 128 bytes, the name `owners.yaml` `teams[].owns` uses, or the
  vendor component); optional `url` (`http`/`https`, ≤ 1024 bytes), `actor` (≤ 128 bytes), `status`
  (`vendor_incident` only: `investigating`, `identified`, `monitoring`, `resolved`) and `id` (your own id,
  ≤ 128 bytes).
- **Repeats:** with `id`, a second delivery of the same `kind`, `service` and `id` is not stored again
  (`202 {"stored": false}`); without it, the same `kind`, `service`, `at` and `title`. Send a new `id` per
  vendor status (`INC-123-monitoring`) when every update should be a row.
- **Replies:** `202 {"stored": true|false}`; `401 unauthenticated`, `413 too_large`, `400 validation`,
  `404 not_found` (a pipeline `owners.yaml` does not define), `429 rate_limited` (over 60 a minute per pipeline).
  Retry only `429` and `5xx`.

The title, URL, actor and service are stored and shown as text, and reach the model only inside its
untrusted-data wrapper. The server logs the pipeline, the kind and whether a row was stored, never the text.

## Generic JSON (curl)

<!-- change-webhook-curl: TestChangeWebhookREADMECurl sends this body through the handler -->
```sh
body='{"pipeline_id":"orders","kind":"deploy","at":"2026-10-20T14:03:00Z","title":"checkout-api v1.42.0","service":"checkout-api","url":"https://github.com/acme/checkout/actions/runs/123","actor":"octocat","id":"gha-123-1"}'
ts=$(date +%s)
sig=$(printf '%s.%s' "$ts" "$body" | openssl dgst -sha256 -hmac "$CHANGES_WEBHOOK_SECRET" -hex | sed 's/^.* //')
curl -fsS -X POST "https://piperoom.example.com/api/changes/webhook" \
  -H 'Content-Type: application/json' \
  -H "X-Piperoom-Signature: t=$ts,v1=$sig" \
  --data-binary "$body"
```

Send exactly the bytes you signed (`--data-binary`; `-d` strips newlines).

## GitHub Actions deploy step

```yaml
- name: Tell Piperoom about the deploy
  if: success()
  env:
    CHANGES_WEBHOOK_SECRET: ${{ secrets.PIPEROOM_CHANGES_WEBHOOK_SECRET }}
  run: |
    body=$(jq -nc \
      --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      --arg title "${GITHUB_REPOSITORY#*/} ${GITHUB_REF_NAME} (${GITHUB_SHA::7})" \
      --arg url "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}" \
      --arg actor "${GITHUB_ACTOR}" \
      --arg id "gha-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}" \
      '{pipeline_id:"orders", kind:"deploy", at:$at, title:$title, service:"checkout-api", url:$url, actor:$actor, id:$id}')
    ts=$(date +%s)
    sig=$(printf '%s.%s' "$ts" "$body" | openssl dgst -sha256 -hmac "$CHANGES_WEBHOOK_SECRET" -hex | sed 's/^.* //')
    curl -fsS --retry 3 --retry-all-errors -X POST "https://piperoom.example.com/api/changes/webhook" \
      -H 'Content-Type: application/json' -H "X-Piperoom-Signature: t=$ts,v1=$sig" --data-binary "$body"
```

A retry resends the same body and signature, so one later than 5 minutes is refused `401`; the `id` makes a
retry of a delivery that did arrive harmless. Use `kind:"migration"` in the step that runs your migrations.

## Argo CD

Argo CD Notifications sends fixed headers only and cannot sign a body, so post from a `PostSync` hook Job, which
runs once per successful sync:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  generateName: piperoom-deploy-
  annotations:
    argocd.argoproj.io/hook: PostSync
    argocd.argoproj.io/hook-delete-policy: HookSucceeded
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: notify
          image: alpine:3.20
          env:
            - name: CHANGES_WEBHOOK_SECRET
              valueFrom: {secretKeyRef: {name: piperoom-changes, key: secret}}
            - name: VERSION
              value: "1.42.0" # your image tag, set by Helm or Kustomize
          command: ["/bin/sh", "-c"]
          args:
            - |
              apk add -q curl openssl jq
              body=$(jq -nc --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg v "$VERSION" \
                '{pipeline_id:"orders", kind:"deploy", at:$at, title:("checkout-api " + $v), service:"checkout-api", id:("argocd-" + $v)}')
              ts=$(date +%s)
              sig=$(printf '%s.%s' "$ts" "$body" | openssl dgst -sha256 -hmac "$CHANGES_WEBHOOK_SECRET" -hex | sed 's/^.* //')
              curl -fsS -X POST "http://piperoom.piperoom.svc:8080/api/changes/webhook" \
                -H 'Content-Type: application/json' -H "X-Piperoom-Signature: t=$ts,v1=$sig" --data-binary "$body"
```

Like the signed alert routes, the change webhook skips the Host allow-list, so an in-cluster name works.

## Vendor incidents

From your own incident tooling, post `kind:"vendor_incident"` with the vendor component as `service`
(`confluent-cloud`, `msk`, `aiven-kafka`) and a `status`. Or let the server poll the vendor's public status feed:
`alertin.StatusPoller` reads a statuspage.io RSS or Atom feed (Confluent Cloud, Aiven) every 5 minutes (10 s
deadline, 4 MB cap). Each incident whose newest update is `investigating`, `identified` or `monitoring` becomes one
`vendor_incident` row with `source: statuspage`, one per incident and status. Resolved incidents, maintenances and
items older than 7 days are skipped. An unreachable feed writes nothing and logs one warning an hour. The poller
connects only to public addresses (a feed host or redirect target that resolves to a loopback, private, link-local or
cloud-metadata address is refused), follows at most 5 redirects and only to `https`, and uses no HTTP proxy.
