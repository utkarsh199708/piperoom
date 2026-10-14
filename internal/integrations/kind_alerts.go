package integrations

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/alertin"
	"piperoom/internal/integrations/spec"
)

// AlertSources are the signed alert sources, in card order.
var AlertSources = []string{"alertmanager", "grafana", "pagerduty", "datadog", "generic"}

// alertSecretField holds an alert source's secret in a row's sealed blob.
const alertSecretField = "secret"

// AlertKindID is the kind id of an alert source ("alerts-grafana").
func AlertKindID(source string) string { return "alerts-" + source }

// alertSource returns the source of an alert kind id, or "".
func alertSource(kind string) string {
	s, ok := strings.CutPrefix(kind, "alerts-")
	if !ok || !validAlertSource(s) {
		return ""
	}
	return s
}

func validAlertSource(s string) bool {
	for _, a := range AlertSources {
		if a == s {
			return true
		}
	}
	return false
}

// pagerDutyOwnsSecret: PagerDuty generates the signing secret itself and
// shows it once, so Piperoom stores the pasted value instead of making one.
func pagerDutyOwnsSecret(source string) bool { return source == "pagerduty" }

var alertTitles = map[string]string{
	"alertmanager": "Prometheus Alertmanager", "grafana": "Grafana alerting", "pagerduty": "PagerDuty",
	"datadog": "Datadog", "generic": "Generic signed webhook",
}

func alertKinds() []spec.Kind {
	out := make([]spec.Kind, 0, len(AlertSources))
	for _, src := range AlertSources {
		k := spec.Kind{
			ID: AlertKindID(src), Category: spec.CategoryAlerts, Title: alertTitles[src],
			Summary: "Open a room when " + alertTitles[src] + " fires", Logo: src,
			Reads: []string{"the alerts it sends to Piperoom"},
			Never: []string{"calls " + alertTitles[src] + " back"},
			// Nothing to dial: Piperoom receives alerts.
			Test:  func(context.Context, spec.Values) []spec.Check { return []spec.Check{} },
			Build: func(context.Context, spec.Values) (any, func(), error) { return nil, func() {}, nil },
		}
		if pagerDutyOwnsSecret(src) {
			k.Fields = []spec.Field{{Name: alertSecretField, Label: "Signing secret", Help: "PagerDuty shows it once when you create the Generic Webhook (v3)", Type: spec.FieldSecret, Required: true}}
		}
		out = append(out, k)
	}
	return out
}

// alertSnippet is the copy-paste config for source, with <secret> in place
// of the secret. The forms follow internal/alertin/README.md.
func alertSnippet(source, webhookURL string) (snippet, lang string) {
	switch source {
	case "alertmanager":
		return fmt.Sprintf(`receivers:
  - name: piperoom
    webhook_configs:
      - url: %s
        send_resolved: false
        http_config:
          authorization:
            type: Bearer
            credentials: <secret>   # or credentials_file: /etc/alertmanager/piperoom-token
# Add the label pipeline: <pipeline id> to the alert rules.
`, webhookURL), "yaml"
	case "grafana":
		return fmt.Sprintf(`Contact point → Webhook
  URL:                %s
  HTTP method:        POST
  Optional settings → HMAC Signature:
    Secret:           <secret>
    Header:           %s
    Timestamp Header: %s   (required: requests without it are refused)
Add a pipeline label to the alert rule.
`, webhookURL, alertin.DefaultGrafanaSignatureHeader, alertin.DefaultGrafanaTimestampHeader), "text"
	case "pagerduty":
		return fmt.Sprintf(`Integrations → Generic Webhooks (v3) → New webhook
  Webhook URL: %s
  Events:      incident.triggered, incident.reopened
Paste the signing secret PagerDuty shows once into this card.
Map each service ID (PXXXXXX) to a pipeline with PAGERDUTY_SERVICES.
`, webhookURL), "text"
	case "datadog":
		return fmt.Sprintf(`Integrations → Webhooks → New
  URL: %s
  Custom headers: {"%s": "<secret>"}
  Payload:
%s
Tag monitors pipeline:<pipeline id> and add @webhook-piperoom to the monitor message.
`, webhookURL, alertin.DefaultDatadogTokenHeader, indent(alertin.DatadogPayloadTemplate, "    ")), "text"
	default:
		return fmt.Sprintf(`POST %s
Content-Type: application/json
%s: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<unix seconds>.<body>" with <secret>>

{"title": "...", "severity": "sev2", "pipeline_id": "orders", "status": "firing", "dedup_key": "..."}
`, webhookURL, alertin.GenericSignatureHeader), "text"
	}
}

func indent(s, prefix string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n")
}
