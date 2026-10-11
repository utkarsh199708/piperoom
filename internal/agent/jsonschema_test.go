package agent

import (
	"encoding/json"
	"testing"
)

func TestValidateInput(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string"},
			"task":      map[string]any{"type": "integer"},
			"level":     map[string]any{"type": "string", "enum": []any{"NONE", "BACKWARD"}},
		},
		"required":             []any{"connector"},
		"additionalProperties": false,
	}

	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"valid", `{"connector":"pg-orders-connector","task":0,"level":"NONE"}`, true},
		{"missing required", `{"task":0}`, false},
		{"unknown field", `{"connector":"x","bogus":1}`, false},
		{"wrong type", `{"connector":1}`, false},
		{"non-integer", `{"connector":"x","task":1.5}`, false},
		{"bad enum", `{"connector":"x","level":"SIDEWAYS"}`, false},
		{"not json", `{`, false},
		{"not object", `"hello"`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateInput(schema, json.RawMessage(c.input))
			if (err == nil) != c.want {
				t.Fatalf("validateInput(%s) err=%v, want valid=%v", c.input, err, c.want)
			}
		})
	}
}

func TestValidateInputArrayItems(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"steps": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":       "object",
					"properties": map[string]any{"action": map[string]any{"type": "string"}},
					"required":   []any{"action"},
				},
			},
		},
		"required": []any{"steps"},
	}
	if err := validateInput(schema, json.RawMessage(`{"steps":[{"action":"a"}]}`)); err != nil {
		t.Fatalf("valid array rejected: %v", err)
	}
	if err := validateInput(schema, json.RawMessage(`{"steps":[{}]}`)); err == nil {
		t.Fatal("expected item missing required to fail")
	}
}

func TestValidateInputEnumStringSlice(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"confidence": map[string]any{"type": "string", "enum": []string{"low", "medium", "high"}},
		},
		"required": []string{"confidence"},
	}
	if err := validateInput(schema, json.RawMessage(`{"confidence":"high"}`)); err != nil {
		t.Fatalf("valid enum rejected: %v", err)
	}
	if err := validateInput(schema, json.RawMessage(`{"confidence":"sure"}`)); err == nil {
		t.Fatal("expected invalid enum to fail")
	}
}

func TestStripWhy(t *testing.T) {
	raw := json.RawMessage(`{"why":"check connect","connector":"pg-orders-connector"}`)
	cleaned, why, err := stripWhy(raw)
	if err != nil {
		t.Fatalf("stripWhy: %v", err)
	}
	if why != "check connect" {
		t.Fatalf("why = %q", why)
	}
	var m map[string]any
	if err := json.Unmarshal(cleaned, &m); err != nil {
		t.Fatalf("unmarshal cleaned: %v", err)
	}
	if _, ok := m["why"]; ok {
		t.Fatal("why still present after strip")
	}
	if m["connector"] != "pg-orders-connector" {
		t.Fatalf("connector lost: %v", m)
	}
}
