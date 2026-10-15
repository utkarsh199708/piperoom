package agent

import (
	"sort"
	"strings"

	"piperoom/internal/redact"
)

// Alert text is written by whoever configured the alert rule and reaches the
// room from an external webhook, so it can carry more than the operator meant
// to share: a DSN in a description, a token in a label. Every alert field the
// model reads is masked with the same redaction rules as read-tool output,
// then capped and wrapped as untrusted data. Masking runs before the cap, so a
// cut-off secret is never left half visible. The agent reads the Alert fields
// internal/alertin selected; it never parses an external webhook body
// (Alert.Raw) itself.

const (
	// alertDescriptionBytes caps the description. It is resent with the
	// history on every turn; a runbook pasted into it adds tokens, not
	// evidence.
	alertDescriptionBytes = 1024
	// alertLabelCount caps how many labels are listed.
	alertLabelCount = 20
	// alertLabelBytes caps one rendered "key=value" label.
	alertLabelBytes = 200
)

// preferredLabels are listed first, in this order, so the cap never drops the
// labels that say what fired and where.
var preferredLabels = []string{"alertname", "severity", "pipeline", "pipeline_id", "team", "service", "connector", "topic", "table"}

// maskAlertText masks secrets in one piece of alert text.
func maskAlertText(s string) string {
	out, _ := redact.Text(s)
	return out
}

// alertDescription renders Alert.Description for the prompt: masked and
// capped, not yet wrapped. Empty when there is none.
func alertDescription(a Alert) string {
	if strings.TrimSpace(a.Description) == "" {
		return ""
	}
	return truncatePromptField(maskAlertText(a.Description), alertDescriptionBytes)
}

// alertLabels renders Alert.Labels for the prompt as "k=v, ..." lines: the
// preferred keys first, then the rest by key, so the text is byte-stable
// across runs (the first turn is cached as history). Each pair is masked as
// one line, so the key=value rule sees the key name (password=..., token=...).
func alertLabels(a Alert) string {
	if len(a.Labels) == 0 {
		return ""
	}
	rank := make(map[string]int, len(preferredLabels))
	for i, k := range preferredLabels {
		rank[k] = i + 1
	}
	keys := make([]string, 0, len(a.Labels))
	for k := range a.Labels {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ri, rj := rank[keys[i]], rank[keys[j]]
		switch {
		case ri != 0 && rj != 0:
			return ri < rj
		case ri != 0 || rj != 0:
			return ri != 0
		}
		return keys[i] < keys[j]
	})
	if len(keys) > alertLabelCount {
		keys = keys[:alertLabelCount]
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, truncatePromptField(maskAlertText(k+"="+a.Labels[k]), alertLabelBytes))
	}
	return strings.Join(parts, ", ")
}
