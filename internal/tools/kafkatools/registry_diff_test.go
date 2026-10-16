package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
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

// Known gap (follow-up #493), same as main: outside array/map a definition and a by-name
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

// TestDiffCorpus runs the schema-pair corpus. A row may differ from main only
// where #425 names the case, and must say why.
func TestDiffCorpus(t *testing.T) {
	for _, p := range diffPairs {
		v, ok := pairVerdicts[p.name]
		if !ok {
			t.Fatalf("pair %q has no verdict", p.name)
		}
		t.Run(p.name, func(t *testing.T) {
			if v.want != v.main && v.why == "" {
				t.Fatalf("want differs from main without a reason")
			}
			ch, err := diffFields(p.from, p.to)
			if err != nil {
				t.Fatal(err)
			}
			if got := formatChanges(ch); got != v.want {
				t.Errorf("got %q, want %q (main: %q)", got, v.want, v.main)
			}
		})
	}
}

// A long enum that drops a symbol prints a short delta, so the real cause in
// the same diff still fits a capped summary (connect_status caps each task).
func TestEnumDeltaIsCompact(t *testing.T) {
	syms := make([]string, 600)
	for i := range syms {
		syms[i] = fmt.Sprintf("S%03d", i)
	}
	enum := func(s []string) string {
		return `{"type":"enum","name":"E","symbols":["` + strings.Join(s, `","`) + `"]}`
	}
	v1 := `{"type":"record","name":"Value","fields":[{"name":"st","type":` + enum(syms) + `},{"name":"amount","type":"int"}]}`
	v2 := `{"type":"record","name":"Value","fields":[{"name":"st","type":` + enum(syms[1:]) +
		`},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}}]}`
	ch, err := diffFields(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	got := formatChanges(ch)
	want := "Value.amount: int → bytes(decimal 12,2)\nenum E: symbols → -S000"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// Many symbols on one side are capped.
	ch, _ = diffFields(v1, `{"type":"record","name":"Value","fields":[{"name":"st","type":`+enum(syms[:500])+`},{"name":"amount","type":"int"}]}`)
	if got := formatChanges(ch); len(got) > 200 || !strings.Contains(got, "(90 more -)") {
		t.Errorf("not capped: %d bytes: %.200q", len(got), got)
	}
}
