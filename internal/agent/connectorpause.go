package agent

import (
	"encoding/json"
	"strings"
)

// pauseRuleNote is fixed agent text appended after a connector-state tool
// output that shows a PAUSED or STOPPED connector. The prompt states the same
// rule, but the evidence is what the model has just read, so the rule is
// repeated beside it: the real-model eval had the model read a PAUSED
// connector and conclude "source quiet" instead of reporting the pause as the
// incident. The note carries no tool data, so no connector name or summary
// text can forge or break it; it sits outside the <tool_output> wrapper, as
// the system prompt's own instruction, never as untrusted text.
const pauseRuleNote = "[agent rule] A PAUSED or STOPPED connector is the incident, not a deliberate pause, unless a record shows a person paused it: a maintenance window covering now, a connect_pause_connector this room executed, or a change record naming who and when. If no record explains it, cite this step, name the connector as the incident in the kafka-connect layer, ask who paused it and when, propose connect_resume_connector after the resume checks, and check the source's newest write before saying \"source quiet\"."

// connectorStateTool reports whether a read tool's Data lists Kafka Connect
// connectors with their state. The check is by tool name because other reads
// carry a "state" of their own (a replication slot's active flag, a schema
// version, a task) where PAUSED does not mean a paused connector.
func connectorStateTool(name string) bool {
	return name == "connect_status" || name == "connect_list_connectors"
}

// pausedConnector reports whether a connector-state read's Data holds a
// connector in PAUSED or STOPPED. Data is round-tripped through JSON so both
// the tool's structs and a fake's maps are read the same way; data that
// cannot be encoded is not a pause (the tool summary is still the model's
// evidence, and a broken payload must not add a rule on its own).
func pausedConnector(tool string, data any) bool {
	if !connectorStateTool(tool) || data == nil {
		return false
	}
	b, err := json.Marshal(data)
	if err != nil {
		return false
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return false
	}
	return findPausedConnector(v)
}

// findPausedConnector walks decoded tool Data for a connector row: an object
// with both a name and a state. A task's state has no name, so a paused task
// alone does not fire; the rule is about the connector.
func findPausedConnector(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		name, _ := x["name"].(string)
		state, _ := x["state"].(string)
		if name != "" && pausedState(state) {
			return true
		}
		for _, child := range x {
			if findPausedConnector(child) {
				return true
			}
		}
	case []any:
		for _, child := range x {
			if findPausedConnector(child) {
				return true
			}
		}
	}
	return false
}

// pausedState matches the two Connect states that stop data flow without a
// failure to diagnose. Case and surrounding space are ignored so a fake or a
// future client format cannot hide a pause behind formatting.
func pausedState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "PAUSED", "STOPPED":
		return true
	}
	return false
}

// readToolContent is the text of a successful read's tool_result: the wrapped
// summary, plus the pause rule when the read shows a paused connector, the
// upgrade rule when a known issue affects a running version (Q1d) and the
// skip rule when a connector's error handling drops records (Q3e, only when
// the room offers the worker tools it names).
func (r *runner) readToolContent(tool, stepID, summary string, data any) string {
	content := r.wrapReadToolOutput(tool, stepID, summary)
	if pausedConnector(tool, data) {
		content += "\n" + pauseRuleNote
	}
	if upgradeAdvised(tool, data) {
		content += "\n" + upgradeRuleNote
	}
	if skipsRecords(tool, data) && r.offers(skipRuleTools...) {
		content += "\n" + skipRuleNote
	}
	return content
}

// offers reports whether the room's tool list has every named tool, so a
// rule never tells the model to call a tool it cannot.
func (r *runner) offers(names ...string) bool {
	for _, n := range names {
		if _, ok := r.toolList.byName[n]; !ok {
			return false
		}
	}
	return true
}
