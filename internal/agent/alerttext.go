package agent

import (
	"encoding/json"
	"sort"
	"strings"

	"piperoom/internal/redact"
)

// Alert text is written by whoever configured the alert rule, and a webhook
// body often carries more than the operator meant to share: a DSN in a
// description, a token in a label. Every alert field the model reads is
// masked with the same redaction rules as read-tool output, then wrapped as
// untrusted data. Masking runs before truncation, so a cut-off secret is never
// left half visible.

const (
	// alertDescriptionBytes caps the alert description in the prompt. The
	// description says what fired; a long runbook pasted into it adds tokens
	// on every turn without adding evidence.
	alertDescriptionBytes = 1024
	// alertLabelCount caps how many labels the prompt lists.
	alertLabelCount = 20
	// alertLabelBytes caps one rendered "key=value" label.
	alertLabelBytes = 200
)

// maskAlertText masks secrets in one piece of alert text.
func maskAlertText(s string) string {
	out, _ := redact.Text(s)
	return out
}

// alertDetails pulls the description and labels out of an alert's raw body.
// It knows the generic webhook shape ({description, labels}) and the
// Alertmanager / Grafana shape (commonAnnotations, commonLabels, alerts[0]);
// anything else yields nothing, never an error, since the room opens either
// way. Returned text is masked and capped but not yet escaped.
func alertDetails(raw json.RawMessage) (description string, labels []string) {
	if len(raw) == 0 {
		return "", nil
	}
	var body struct {
		Description       string            `json:"description"`
		Summary           string            `json:"summary"`
		Message           string            `json:"message"`
		Labels            map[string]any    `json:"labels"`
		Annotations       map[string]any    `json:"annotations"`
		CommonLabels      map[string]any    `json:"commonLabels"`
		CommonAnnotations map[string]any    `json:"commonAnnotations"`
		Alerts            []json.RawMessage `json:"alerts"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", nil
	}
	var first struct {
		Labels      map[string]any `json:"labels"`
		Annotations map[string]any `json:"annotations"`
	}
	if len(body.Alerts) > 0 {
		_ = json.Unmarshal(body.Alerts[0], &first) // a malformed alert adds nothing
	}

	description = firstNonEmpty(
		body.Description,
		annotation(body.Annotations),
		annotation(body.CommonAnnotations),
		annotation(first.Annotations),
		body.Summary,
		body.Message,
	)
	description = truncatePromptField(maskAlertText(description), alertDescriptionBytes)

	merged := map[string]string{}
	for _, m := range []map[string]any{first.Labels, body.CommonLabels, body.Labels} {
		for k, v := range m {
			if s, ok := v.(string); ok {
				merged[k] = s
			}
		}
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys) // stable order: the first turn is cached as part of history
	if len(keys) > alertLabelCount {
		keys = keys[:alertLabelCount]
	}
	for _, k := range keys {
		// Mask the pair as one line so the key=value rule sees the key name
		// (password=..., token=...), not just the value.
		labels = append(labels, truncatePromptField(maskAlertText(k+"="+merged[k]), alertLabelBytes))
	}
	return description, labels
}

// annotation returns the description or summary annotation, if any.
func annotation(m map[string]any) string {
	for _, k := range []string{"description", "summary", "message"} {
		if s, ok := m[k].(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
