package kafkatools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

const diffV1 = `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"}]}`
const diffV2 = `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"},{"name":"risk_score","type":"double"}]}`

func diffReg() *fakeRegistry {
	return &fakeRegistry{schemaFn: func(_ context.Context, _ string, v int) (clients.SchemaVersion, error) {
		return clients.SchemaVersion{Subject: "s", Version: 1, Schema: diffV1}, nil
	}}
}

func runDiff(t *testing.T, in string) (string, map[string]any) {
	t.Helper()
	r, err := (&registryDiff{d: Deps{Registry: diffReg()}}).Run(tools.WithUnscoped(context.Background()), json.RawMessage(in))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(r.Data)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return r.Summary, m
}

// A raw candidate schema is the "to" side: a field it adds is "added".
func TestDiffRawSchemaDirection(t *testing.T) {
	esc, _ := json.Marshal(diffV2)
	sum, data := runDiff(t, `{"subject":"s","schema":`+string(esc)+`}`)
	if sum != "+ Value.risk_score: double" {
		t.Errorf("summary = %q", sum)
	}
	if data["from_version"] != float64(1) || data["to_version"] != float64(0) {
		t.Errorf("versions = %v → %v, want 1 → 0 (raw)", data["from_version"], data["to_version"])
	}
	// Candidate drops a field: removed.
	esc, _ = json.Marshal(`{"type":"record","name":"Value","fields":[{"name":"id","type":"long"}]}`)
	sum, _ = runDiff(t, `{"subject":"s","schema":`+string(esc)+`}`)
	if !strings.Contains(sum, "- Value.amount: int") {
		t.Errorf("summary = %q", sum)
	}
}

// Explicit to_version with a raw schema: the registered version is the from side.
func TestDiffRawSchemaWithToVersion(t *testing.T) {
	esc, _ := json.Marshal(diffV2)
	sum, data := runDiff(t, `{"subject":"s","to_version":1,"schema":`+string(esc)+`}`)
	if sum != "+ Value.risk_score: double" || data["from_version"] != float64(1) {
		t.Errorf("summary=%q data=%v", sum, data)
	}
}

func TestDiffFieldsNestedTypes(t *testing.T) {
	rec := func(typ string) string {
		return `{"type":"record","name":"R","fields":[{"name":"f","type":` + typ + `}]}`
	}
	tests := []struct {
		name, from, to, want string
	}{
		{"enum symbol removed", `{"type":"enum","name":"E","symbols":["A","B"]}`, `{"type":"enum","name":"E","symbols":["A"]}`, "R.f"},
		{"enum symbol added", `{"type":"enum","name":"E","symbols":["A"]}`, `{"type":"enum","name":"E","symbols":["A","B"]}`, "R.f"},
		{"array items", `{"type":"array","items":"string"}`, `{"type":"array","items":"long"}`, "R.f"},
		{"map values", `{"type":"map","values":"string"}`, `{"type":"map","values":"long"}`, "R.f"},
		{"union with array", `["null",{"type":"array","items":"int"}]`, `["null",{"type":"array","items":"long"}]`, "R.f"},
		{"nested record in array",
			`{"type":"array","items":{"type":"record","name":"N","fields":[{"name":"x","type":"int"}]}}`,
			`{"type":"array","items":{"type":"record","name":"N","fields":[{"name":"x","type":"long"}]}}`, "N.x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch, err := diffFields(rec(tc.from), rec(tc.to))
			if err != nil {
				t.Fatal(err)
			}
			if len(ch) == 0 || ch[0].Field != tc.want {
				t.Errorf("changes = %+v, want a change on %s", ch, tc.want)
			}
		})
	}
	same := `{"type":"array","items":{"type":"enum","name":"E","symbols":["A","B"]}}`
	if ch, _ := diffFields(rec(same), rec(same)); len(ch) != 0 {
		t.Errorf("identical schemas changed: %+v", ch)
	}
}
