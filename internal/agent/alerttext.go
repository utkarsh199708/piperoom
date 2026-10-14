package agent

import "piperoom/internal/redact"

// Alert text is written by whoever configured the alert rule and reaches the
// room from an external webhook, so it can carry more than the operator meant
// to share: a DSN in a title, a token in a pipeline label. Every alert field
// the model reads is masked with the same redaction rules as read-tool output,
// then wrapped as untrusted data. The agent reads only the Alert fields, never
// the webhook body (Alert.Raw): selecting the alert in a body is
// internal/alertin's job.

// maskAlertText masks secrets in one piece of alert text.
func maskAlertText(s string) string {
	out, _ := redact.Text(s)
	return out
}
