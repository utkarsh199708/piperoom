package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

const diffV3 = `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"risk_score","type":"double"}]}`

// versionedReg serves a subject with the given versions; the schema of a
// version comes from schemas (version -1 = the highest). calls records every
// version asked for.
func versionedReg(versions []int, schemas map[int]string, calls *[]int) *fakeRegistry {
	return &fakeRegistry{
		versionsFn: func(context.Context, string) ([]int, error) { return versions, nil },
		schemaFn: func(_ context.Context, s string, v int) (clients.SchemaVersion, error) {
			if calls != nil {
				*calls = append(*calls, v)
			}
			if v == -1 {
				v = versions[len(versions)-1]
			}
			sc, ok := schemas[v]
			if !ok {
				return clients.SchemaVersion{}, fmt.Errorf("schemareg: version %d: %w", v, clients.ErrNotFound)
			}
			return clients.SchemaVersion{Subject: s, Version: v, Schema: sc}, nil
		},
	}
}

func runDiffWith(reg *fakeRegistry, in string) (tools.Result, error) {
	return (&registryDiff{d: Deps{Registry: reg}}).Run(tools.WithUnscoped(context.Background()), json.RawMessage(in))
}

// A subject-only call used to fail with "from_version or schema is required"
// and the model repeated it until its tool budget ran out. It now diffs the
// latest version against the one before it and names both.
func TestDiffDefaultsToLatestVsPrevious(t *testing.T) {
	tests := []struct {
		name     string
		versions []int
		schemas  map[int]string
		input    string
		wantHead string
		wantBody string
		wantFrom int
		wantTo   int
	}{
		{"two versions", []int{1, 2}, map[int]string{1: diffV1, 2: diffV2}, `{"subject":"s"}`,
			"s: v1 → v2 (latest vs previous", "+ Value.risk_score: double", 1, 2},
		{"three versions picks the last pair", []int{1, 2, 3}, map[int]string{1: diffV1, 2: diffV2, 3: diffV3}, `{"subject":"s"}`,
			"s: v2 → v3 (latest vs previous", "- Value.amount: int", 2, 3},
		{"unsorted registry listing", []int{3, 1, 2}, map[int]string{1: diffV1, 2: diffV2, 3: diffV3}, `{"subject":"s"}`,
			"s: v2 → v3 (latest vs previous", "- Value.amount: int", 2, 3},
		{"gap in version numbers", []int{1, 3}, map[int]string{1: diffV1, 3: diffV3}, `{"subject":"s"}`,
			"s: v1 → v3 (latest vs previous", "- Value.amount: int", 1, 3},
		{"to_version only diffs against the one before it", []int{1, 2, 3}, map[int]string{1: diffV1, 2: diffV2, 3: diffV3}, `{"subject":"s","to_version":2}`,
			"s: v1 → v2 (to_version vs the version before it", "+ Value.risk_score: double", 1, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := runDiffWith(versionedReg(tt.versions, tt.schemas, nil), tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(r.Summary, tt.wantHead) || !strings.Contains(r.Summary, tt.wantBody) {
				t.Errorf("summary = %q, want head %q and body %q", r.Summary, tt.wantHead, tt.wantBody)
			}
			if len(r.Summary) > tools.MaxSummaryBytes {
				t.Errorf("summary is %d bytes, over MaxSummaryBytes", len(r.Summary))
			}
			d := r.Data.(map[string]any)
			if d["from_version"] != tt.wantFrom || d["to_version"] != tt.wantTo {
				t.Errorf("versions = %v → %v, want %d → %d", d["from_version"], d["to_version"], tt.wantFrom, tt.wantTo)
			}
		})
	}
}

// Nothing to compare: say so plainly, as a finding and not as an error to retry.
func TestDiffSubjectOnlyWithOneVersion(t *testing.T) {
	var calls []int
	r, err := runDiffWith(versionedReg([]int{1}, map[int]string{1: diffV1}, &calls), `{"subject":"s"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"s version 1 has no earlier version", "[1]", "nothing to diff"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary = %q, missing %q", r.Summary, want)
		}
	}
	d := r.Data.(map[string]any)
	if d["single_version"] != true || len(d["changes"].([]fieldChange)) != 0 {
		t.Errorf("data = %v", d)
	}
	if len(calls) != 0 {
		t.Errorf("schema fetched %v, want no schema reads for a single version", calls)
	}
}

func TestDiffSubjectOnlyNoVersions(t *testing.T) {
	r, err := runDiffWith(versionedReg(nil, nil, nil), `{"subject":"s"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "no registered versions") {
		t.Errorf("summary = %q", r.Summary)
	}
}

// A to_version the registry does not hold is refused with the versions it
// does hold, so the model can pick one instead of guessing again.
func TestDiffDefaultUnknownToVersionNamesAvailable(t *testing.T) {
	_, err := runDiffWith(versionedReg([]int{1, 2}, map[int]string{1: diffV1, 2: diffV2}, nil), `{"subject":"s","to_version":7}`)
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v, want ErrBadInput", err)
	}
	if !strings.Contains(err.Error(), "available versions: [1 2]") {
		t.Errorf("err = %q, want the available versions named", err)
	}
}

// Error paths: the registry being down must stay an error (never read as "one
// version"), and a missing subject keeps its not-found cause.
func TestDiffDefaultRegistryErrors(t *testing.T) {
	down := errors.New("schemareg: GET /subjects/s/versions: dial tcp 10.0.0.1:8081: connection refused")
	tests := []struct {
		name string
		reg  *fakeRegistry
		want error
	}{
		{"registry down on version listing", &fakeRegistry{versionsFn: func(context.Context, string) ([]int, error) { return nil, down }}, down},
		{"subject not found", &fakeRegistry{versionsFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound }}, clients.ErrNotFound},
		{"registry down on schema read", &fakeRegistry{
			versionsFn: func(context.Context, string) ([]int, error) { return []int{1, 2}, nil },
			schemaFn: func(context.Context, string, int) (clients.SchemaVersion, error) {
				return clients.SchemaVersion{}, down
			},
		}, down},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runDiffWith(tt.reg, `{"subject":"s"}`)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want it to wrap %v", err, tt.want)
			}
			if errors.Is(err, tools.ErrBadInput) {
				t.Errorf("err = %v: an outage must not read as bad input", err)
			}
		})
	}
}

// A long change list cannot push the compared versions out of the Summary.
func TestDiffDefaultSummaryStaysWithinCap(t *testing.T) {
	var fields []string
	for i := 0; i < 600; i++ {
		fields = append(fields, fmt.Sprintf(`{"name":"column_with_a_long_name_%d","type":"string"}`, i))
	}
	big := `{"type":"record","name":"Value","fields":[` + strings.Join(fields, ",") + `]}`
	r, err := runDiffWith(versionedReg([]int{1, 2}, map[int]string{1: diffV1, 2: big}, nil), `{"subject":"s"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Summary) > tools.MaxSummaryBytes || !r.Truncated {
		t.Errorf("summary %d bytes, truncated = %v", len(r.Summary), r.Truncated)
	}
	if !strings.HasPrefix(r.Summary, "s: v1 → v2") {
		t.Errorf("summary head = %q", r.Summary[:40])
	}
}

// The explicit paths do not change: with from_version given, versions are not
// listed and the Summary is the bare change list as before.
func TestDiffExplicitPathsUnchanged(t *testing.T) {
	reg := versionedReg([]int{1, 2}, map[int]string{1: diffV1, 2: diffV2}, nil)
	reg.versionsFn = func(context.Context, string) ([]int, error) {
		t.Error("Versions called on an explicit from_version call")
		return nil, nil
	}
	r, err := runDiffWith(reg, `{"subject":"s","from_version":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Summary != "+ Value.risk_score: double" {
		t.Errorf("from_version summary = %q", r.Summary)
	}
	d := r.Data.(map[string]any)
	if d["from_version"] != 1 || d["to_version"] != 2 {
		t.Errorf("versions = %v → %v", d["from_version"], d["to_version"])
	}
	if _, ok := d["versions"]; ok {
		t.Errorf("explicit path data gained a versions key: %v", d)
	}
	r, err = runDiffWith(reg, `{"subject":"s","from_version":1,"to_version":2}`)
	if err != nil || r.Summary != "+ Value.risk_score: double" {
		t.Errorf("from/to summary = %q err = %v", r.Summary, err)
	}
}

func TestDiffInputSchemaDocumentsDefault(t *testing.T) {
	tool := &registryDiff{}
	schema := tool.InputSchema()
	if req, _ := schema["required"].([]string); len(req) != 1 || req[0] != "subject" {
		t.Errorf("required = %v, want only subject", schema["required"])
	}
	from := schema["properties"].(map[string]any)["from_version"].(map[string]any)["description"].(string)
	for _, want := range []string{"optional", "latest vs previous"} {
		if !strings.Contains(from, want) {
			t.Errorf("from_version description = %q, missing %q", from, want)
		}
	}
	if d := tool.Description(); !strings.Contains(d, "Only subject is required") || !strings.Contains(d, "latest version against the one before it") {
		t.Errorf("description = %q", d)
	}
}
