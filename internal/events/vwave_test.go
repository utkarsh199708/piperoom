package events_test

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
)

// TestV0FieldsOmittedWhenEmpty pins that the V0 fields leave events written
// before V0 byte-identical, so their export hashes do not change.
func TestV0FieldsOmittedWhenEmpty(t *testing.T) {
	u, err := json.Marshal(events.AgentUsagePayload{Model: "claude-opus-5-5", Role: events.UsageRoleMain, InputTokens: 1})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"model":"claude-opus-5-5","role":"main","input_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":0,"latency_ms":0}`; string(u) != want {
		t.Errorf("usage = %s\nwant    %s", u, want)
	}
	r, err := json.Marshal(events.AgentToolResultPayload{StepID: "s1", ToolUseID: "t1", Tool: "x", Summary: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"step_id":"s1","tool_use_id":"t1","tool":"x","summary":"ok","is_error":false,"took_ms":0}`; string(r) != want {
		t.Errorf("tool_result = %s\nwant          %s", r, want)
	}
}

func TestV0FieldsJSONNames(t *testing.T) {
	u, _ := json.Marshal(events.AgentUsagePayload{Provider: "openai", Estimated: true})
	var m map[string]any
	if err := json.Unmarshal(u, &m); err != nil || m["provider"] != "openai" || m["estimated"] != true {
		t.Errorf("usage = %s, %v", u, err)
	}
	for _, v := range []string{events.ToolRejectedUnknownTool, events.ToolRejectedInvalidJSON, events.ToolRejectedInvalidInput} {
		r, _ := json.Marshal(events.AgentToolResultPayload{IsError: true, Rejected: v})
		m = nil
		if err := json.Unmarshal(r, &m); err != nil || m["rejected"] != v {
			t.Errorf("tool_result = %s, %v", r, err)
		}
	}
	if events.ToolRejectedUnknownTool != "unknown_tool" || events.ToolRejectedInvalidJSON != "invalid_json" || events.ToolRejectedInvalidInput != "invalid_input" {
		t.Error("Rejected values changed")
	}
}
