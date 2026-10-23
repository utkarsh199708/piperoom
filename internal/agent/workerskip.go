package agent

import (
	"encoding/json"
	"strings"
)

// Q3e (KAN-276): a connector whose error handling skips failed records keeps
// its tasks RUNNING while records go missing, so connect_status reads healthy
// and the model blames another stage. The worker's own metrics count the
// skipped records and its logs say why (Q3b connect_worker_metrics, Q3c
// connect_worker_logs). The CDC system prompt is at its word ceiling and
// every sentence of it is pinned by a guard test, so the rule sits beside the
// evidence that makes it apply, as the pause rule (connectorpause.go) and the
// upgrade rule (versions.go) do: a room whose connectors fail loudly reads
// nothing new.

// skipRuleNote is fixed agent text appended after a connect_config read whose
// error handling lets a task skip records. It carries no tool data, so no
// config value can forge or break it.
const skipRuleNote = "[agent rule] This connector's error handling lets a RUNNING task skip records without failing. If records are missing, read connect_worker_metrics and connect_worker_logs for it before blaming another stage, and cite them."

// skipRuleTools are the tools skipRuleNote tells the model to read; the note
// is added only when the room offers both.
var skipRuleTools = []string{"connect_worker_metrics", "connect_worker_logs"}

// skipsRecords reports whether a connect_config read's Data shows error
// handling that drops records instead of failing the task: Kafka Connect's
// errors.tolerance=all, or Debezium's event.processing.failure.handling.mode
// skip or warn. Data is round-tripped through JSON so the tool's map and a
// fake's are read the same way; data that cannot be decoded adds no rule.
func skipsRecords(tool string, data any) bool {
	if tool != "connect_config" || data == nil {
		return false
	}
	b, err := json.Marshal(data)
	if err != nil {
		return false
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return false
	}
	value := func(k string) string {
		s, _ := cfg[k].(string)
		return strings.ToLower(strings.TrimSpace(s))
	}
	if value("errors.tolerance") == "all" {
		return true
	}
	switch value("event.processing.failure.handling.mode") {
	case "skip", "warn":
		return true
	}
	return false
}
