package gemini

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/agent/llm"
)

func TestOpenAPISchema(t *testing.T) {
	for _, tc := range []struct {
		name, in, want, err string
	}{
		{"object", `{"type":"object","properties":{"a":{"type":"string","description":"d"}},"required":["a"],"additionalProperties":false,"$schema":"x"}`,
			`{"properties":{"a":{"description":"d","type":"STRING"}},"required":["a"],"type":"OBJECT"}`, ""},
		{"nullable", `{"type":["integer","null"],"minimum":1}`, `{"minimum":1,"nullable":true,"type":"INTEGER"}`, ""},
		{"array", `{"type":"array","items":{"type":"string","enum":["x","y"]},"maxItems":3}`, `{"items":{"enum":["x","y"],"type":"STRING"},"maxItems":3,"type":"ARRAY"}`, ""},
		{"int enum dropped", `{"type":"integer","enum":[1,2]}`, `{"type":"INTEGER"}`, ""},
		{"const string", `{"type":"string","const":"v"}`, `{"enum":["v"],"type":"STRING"}`, ""},
		{"oneOf scalars", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`, `{"anyOf":[{"type":"STRING"},{"type":"INTEGER"}]}`, ""},
		{"inferred object", `{"properties":{"a":{"type":"boolean"}}}`, `{"properties":{"a":{"type":"BOOLEAN"}},"type":"OBJECT"}`, ""},
		{"oneOf objects", `{"type":"object","properties":{"s":{"oneOf":[{"type":"object","properties":{"a":{"type":"string"}}},{"type":"object"}]}}}`, "", "properties.s: oneOf over objects"},
		{"ref", `{"type":"object","properties":{"s":{"$ref":"#/$defs/x"}}}`, "", "properties.s: $ref"},
		{"allOf", `{"allOf":[{"type":"object"}]}`, "", "the schema root: allOf"},
		{"union", `{"type":["string","integer"]}`, "", "union of types"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var in map[string]any
			if err := json.Unmarshal([]byte(tc.in), &in); err != nil {
				t.Fatal(err)
			}
			out, err := openAPISchema(in, "")
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.Marshal(out)
			if string(got) != tc.want {
				t.Fatalf("got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// TestCheckToolsNamesTheTool: a schema the converter cannot express, or a
// name Gemini refuses, fails the start check with the tool named.
func TestCheckToolsNamesTheTool(t *testing.T) {
	bad := llm.ToolDef{Name: "propose_fix", Schema: map[string]any{
		"type": "object",
		"properties": map[string]any{"step": map[string]any{"oneOf": []any{
			map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}}},
			map[string]any{"type": "object", "properties": map[string]any{"b": map[string]any{"type": "string"}}},
		}}},
	}}
	err := CheckTools("gemini-2.0-flash", []llm.ToolDef{statusTool, bad})
	if err == nil || !strings.Contains(err.Error(), `"propose_fix"`) || !strings.Contains(err.Error(), "oneOf over objects") {
		t.Fatalf("err = %v", err)
	}
	// Gemini 2.5 takes JSON Schema as is.
	if err := CheckTools("gemini-2.5-pro", []llm.ToolDef{statusTool, bad}); err != nil {
		t.Fatalf("2.5: %v", err)
	}
	if err := CheckTools("gemini-2.5-pro", []llm.ToolDef{{Name: "9bad"}}); err == nil || !strings.Contains(err.Error(), `"9bad"`) {
		t.Fatalf("name: %v", err)
	}
	many := make([]llm.ToolDef, maxTools+1)
	for i := range many {
		many[i] = llm.ToolDef{Name: "t" + strings.Repeat("x", i%10)}
	}
	if err := CheckTools("gemini-2.5-pro", many); err == nil {
		t.Fatal("over the tool cap")
	}
	// Turn refuses the same request unsent.
	m := newTestModel(t, "http://127.0.0.1:1", nil)
	req := baseReq("gemini-2.0-flash")
	req.Tools = append(req.Tools, bad)
	if _, err := m.Turn(t.Context(), req); err == nil || !strings.Contains(err.Error(), "bad_request") {
		t.Fatalf("Turn = %v", err)
	}
}

func TestCapabilities(t *testing.T) {
	m := newTestModel(t, "http://127.0.0.1:1", nil)
	c := m.Capabilities("gemini-2.5-pro")
	if !c.StructuredOutput || !c.Effort || !c.PromptCache || c.ContextTokens != contextTokens || c.StrictTools || c.Compaction || c.ToolNameMax != 64 || c.MaxTools != maxTools {
		t.Fatalf("2.5 caps = %+v", c)
	}
	if c := m.Capabilities("gemini-2.0-flash"); c.StructuredOutput || c.Effort || c.ContextTokens != legacyContextTokens {
		t.Fatalf("2.0 caps = %+v", c)
	}
	m2 := newTestModel(t, "http://127.0.0.1:1", func(o *Options) { o.ContextTokens = 200000 })
	if c := m2.Capabilities("gemini-3-pro-preview"); c.ContextTokens != 200000 {
		t.Fatalf("override = %+v", c)
	}
}
