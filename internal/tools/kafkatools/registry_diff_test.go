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
		{"enum symbol removed", `{"type":"enum","name":"E","symbols":["A","B"]}`, `{"type":"enum","name":"E","symbols":["A"]}`, "enum E"},
		{"enum symbol added", `{"type":"enum","name":"E","symbols":["A"]}`, `{"type":"enum","name":"E","symbols":["A","B"]}`, "enum E"},
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

func TestDiffRawSchemaWithFromVersion(t *testing.T) {
	reg := &fakeRegistry{schemaFn: func(_ context.Context, _ string, v int) (clients.SchemaVersion, error) {
		if v == 1 {
			return clients.SchemaVersion{Version: 1, Schema: diffV1}, nil
		}
		return clients.SchemaVersion{Version: 2, Schema: diffV2}, nil // latest
	}}
	esc, _ := json.Marshal(diffV2)
	r, err := (&registryDiff{d: Deps{Registry: reg}}).Run(tools.WithUnscoped(context.Background()),
		json.RawMessage(`{"subject":"s","from_version":1,"schema":`+string(esc)+`}`))
	if err != nil {
		t.Fatal(err)
	}
	if r.Summary != "+ Value.risk_score: double" {
		t.Errorf("summary = %q, want v1 → raw", r.Summary)
	}
	if d := r.Data.(map[string]any); d["from_version"] != 1 || d["to_version"] != 0 {
		t.Errorf("versions = %v → %v", d["from_version"], d["to_version"])
	}
}

func TestInputSchemaDescribesDirection(t *testing.T) {
	d := (&registryDiff{}).InputSchema()["properties"].(map[string]any)["schema"].(map[string]any)["description"].(string)
	if !strings.Contains(d, "to side") {
		t.Errorf("schema description = %q", d)
	}
}

// Named types move between an inline definition and a by-name reference
// without any change: main answered these correctly.
func TestDiffNamedTypeDefinitionVsReference(t *testing.T) {
	enum := `{"type":"enum","name":"S","symbols":["A","B"]}`
	rec := func(fields ...string) string {
		return `{"type":"record","name":"R","fields":[` + strings.Join(fields, ",") + `]}`
	}
	arr := func(items string) string { return `{"type":"array","items":` + items + `}` }
	fa := `{"name":"a","type":` + arr(enum) + `}`
	fb := `{"name":"b","type":` + arr(`"S"`) + `}`
	reorder, err := diffFields(rec(fa, fb), rec(fb, `{"name":"a","type":`+arr(`"S"`)+`}`))
	if err != nil {
		t.Fatal(err)
	}
	// v2 defines nothing; the reference alone matches the old definition.
	if len(reorder) != 0 {
		t.Errorf("reorder: %+v, want no changes", reorder)
	}
	v1 := rec(`{"name":"status","type":`+enum+`}`, `{"name":"hist","type":`+arr(`"S"`)+`}`)
	v2 := rec(`{"name":"hist","type":` + arr(enum) + `}`)
	ch, _ := diffFields(v1, v2)
	if len(ch) != 1 || ch[0].Field != "R.status" || ch[0].Kind != "removed" {
		t.Errorf("dropped definition: %+v, want only - R.status", ch)
	}
	m, _ := diffFields(rec(`{"name":"m","type":{"type":"map","values":`+enum+`}}`), rec(`{"name":"m","type":{"type":"map","values":"S"}}`))
	if len(m) != 0 {
		t.Errorf("map def vs ref: %+v", m)
	}
}

// Known gap, same as main: outside array/map a definition and a by-name
// reference to the same type render differently. Pinned so a fix is noticed.
func TestDiffGapPlainFieldDefinitionVsReference(t *testing.T) {
	enum := `{"type":"enum","name":"S","symbols":["A"]}`
	v1 := `{"type":"record","name":"R","fields":[{"name":"a","type":` + enum + `},{"name":"b","type":"S"}]}`
	v2 := `{"type":"record","name":"R","fields":[{"name":"b","type":` + enum + `},{"name":"a","type":"S"}]}`
	ch, _ := diffFields(v1, v2)
	if len(ch) != 2 {
		t.Errorf("gap changed: %+v (was 2 false changes on main)", ch)
	}
}

func TestEnumDeltaIsCompact(t *testing.T) {
	var syms []string
	for i := 0; i < 600; i++ {
		syms = append(syms, `"S`+strings.Repeat("x", 3)+string(rune('a'+i%26))+`_`+strings.Repeat("y", i/26)+`"`)
	}
	mk := func(s []string) string {
		return `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"enum","name":"E","symbols":[` + strings.Join(s, ",") + `]}}]}`
	}
	ch, _ := diffFields(mk(syms), mk(syms[:599]))
	if len(ch) != 1 || len(formatChanges(ch)) > 100 {
		t.Errorf("changes %+v, text %q", ch, formatChanges(ch))
	}
}
