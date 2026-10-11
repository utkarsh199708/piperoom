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
2. for PagerDuty, the service ID looked up in `Config.PagerDutyServices`;
3. the single `owners.yaml` pipeline whose connector, consumer group or table the alert mentions (label
   values exactly, or whole names in the title or summary);
4. `Config.DefaultPipeline`.

If more than one pipeline matches in step 3, no guess is made. `Alert.PipelineVia` records which step
was used.

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

Add the label `pipeline: orders` to the alert rules (or add it with `alert_relabel_configs`).

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
`pipeline_id`, `status`, `dedup_key`, `url`. Sign the body with `alertin.SignGeneric(secret, time.Now(), body)`
and send the result in `X-Piperoom-Signature`.

Alertmanager and Datadog cannot compute a signature over the body. Their token is a bearer secret:
send it only over HTTPS and rotate it like a password.
