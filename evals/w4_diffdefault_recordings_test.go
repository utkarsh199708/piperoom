package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
	"piperoom/internal/tools/kafkatools"
)

// W4-C7 (KAN-489): schema_registry_diff with only a subject diffs the latest
// version against the one before it, so the scenarios no longer record the old
// "from_version or schema is required" error. Replay serves the recorded
// answer by input, so each subject-only recording (and each to_version-only
// one) is the real tool's output over a registry holding the scenario's
// versions. The tests below rebuild a handful of them from the real tool and
// compare, and keep the old error out of every file.

// w4ddOldError is the answer a subject-only call got before the tool defaulted.
const w4ddOldError = "from_version or schema is required"

// w4ddRegistry is a Schema Registry client over fixed versions and schemas.
type w4ddRegistry struct {
	versions []int
	schemas  map[int]string
}

func (r w4ddRegistry) Subjects(context.Context) ([]string, error) { return nil, nil }
func (r w4ddRegistry) Versions(context.Context, string) ([]int, error) {
	return append([]int(nil), r.versions...), nil
}
func (r w4ddRegistry) Schema(_ context.Context, subject string, v int) (clients.SchemaVersion, error) {
	if v == -1 {
		v = r.versions[len(r.versions)-1]
	}
	s, ok := r.schemas[v]
	if !ok {
		return clients.SchemaVersion{}, fmt.Errorf("schemareg: GET /subjects/%s/versions/%d: %w", subject, v, clients.ErrNotFound)
	}
	return clients.SchemaVersion{Subject: subject, Version: v, Schema: s}, nil
}
func (r w4ddRegistry) Compatibility(context.Context, string) (string, bool, error) {
	return "BACKWARD", false, nil
}
func (w4ddRegistry) SetCompatibility(context.Context, string, string) error { return nil }
func (w4ddRegistry) Register(context.Context, string, string) (int, error)  { return 0, nil }

// w4ddRecord builds the value schema of an orders-like record from its fields.
func w4ddRecord(fields ...string) string {
	return `{"type":"record","name":"Value","fields":[` + strings.Join(fields, ",") + `]}`
}

const (
	w4ddID       = `{"name":"id","type":"long"}`
	w4ddCustomer = `{"name":"customer_id","type":"long"}`
	w4ddAmount   = `{"name":"amount","type":"int"}`
	w4ddStatus   = `{"name":"status","type":"string"}`
	w4ddReason   = `{"name":"status_reason","type":["null","string"]}`
	w4ddRisk     = `{"name":"risk_score","type":["null","double"]}`
	w4ddTotal    = `{"name":"total_amount","type":["null",{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}]}`
)

func w4ddBase(extra ...string) string {
	return w4ddRecord(append([]string{w4ddID, w4ddCustomer, w4ddAmount, w4ddStatus}, extra...)...)
}

// TestW4DiffDefaultRecordingsMatchTheRealTool rebuilds the subject-only and
// to_version-only answers of several scenarios from the real tool. The schemas
// are the ones the scenario's other recordings imply: every recorded
// from_version diff of the scenario is reproduced by them first.
func TestW4DiffDefaultRecordingsMatchTheRealTool(t *testing.T) {
	const orders = "pg.public.orders-value"
	tests := []struct {
		scenario string
		subject  string
		reg      w4ddRegistry
	}{
		// One version: the tool's single-version message.
		{"ambiguous-lag-spike", orders, w4ddRegistry{[]int{1}, map[int]string{1: w4ddBase()}}},
		{"missing-grant", orders, w4ddRegistry{[]int{1}, map[int]string{1: w4ddBase()}}},
		{"renamed-property-after-upgrade", orders, w4ddRegistry{[]int{1}, map[int]string{1: w4ddBase()}}},
		{"tolerance-all-skips", orders, w4ddRegistry{[]int{1}, map[int]string{1: w4ddBase()}}},
		// to_version 2 is refused when the registry holds only version 1.
		{"schema-change", orders, w4ddRegistry{[]int{1}, map[int]string{1: w4ddBase()}}},
		{"injection-ddl-comment", orders, w4ddRegistry{[]int{1}, map[int]string{1: w4ddBase()}}},
		// Version 1 was deleted: only version 2 is left.
		{"recurring-schema-deleted", orders, w4ddRegistry{[]int{2}, map[int]string{2: w4ddBase()}}},
		// Two versions, and a to_version the registry does not hold.
		{"avro-schema-id-deleted-sink-failed", orders, w4ddRegistry{[]int{1, 2}, map[int]string{1: w4ddBase(), 2: w4ddBase(w4ddReason)}}},
		{"sasl-credential-rotated", "pay.public.payments-value", w4ddRegistry{[]int{1, 2}, map[int]string{1: w4ddBase(), 2: w4ddBase(w4ddReason)}}},
		// Four versions: only the last pair is diffed. The story says version 4
		// added risk_score; versions 2 and 3 are not recorded and are derived as
		// equal to version 1.
		{"clickhouse-mv-schema-change", orders, w4ddRegistry{[]int{1, 2, 3, 4}, map[int]string{1: w4ddBase(), 2: w4ddBase(), 3: w4ddBase(), 4: w4ddBase(w4ddRisk)}}},
		{"snowflake-sink-column-missing", orders, w4ddRegistry{[]int{1, 2, 3, 4}, map[int]string{1: w4ddBase(), 2: w4ddBase(), 3: w4ddBase(), 4: w4ddBase(w4ddRisk)}}},
		// Three versions: version 2 added total_amount, version 3 dropped amount.
		{"source-column-dropped", orders, w4ddRegistry{[]int{1, 2, 3}, map[int]string{
			1: w4ddBase(),
			2: w4ddBase(w4ddTotal),
			3: w4ddRecord(w4ddID, w4ddCustomer, w4ddStatus, w4ddTotal),
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + tt.scenario + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			treg := tools.NewRegistry()
			if err := kafkatools.Register(treg, kafkatools.Deps{Registry: tt.reg}); err != nil {
				t.Fatal(err)
			}
			diff, ok := treg.Get("schema_registry_diff")
			if !ok {
				t.Fatal("schema_registry_diff is not registered")
			}
			var subjectOnly int
			for _, r := range sc.Recordings {
				if r.Tool != "schema_registry_diff" || r.Input["subject"] != tt.subject {
					continue
				}
				_, hasFrom := r.Input["from_version"]
				_, hasSchema := r.Input["schema"]
				if hasSchema {
					continue
				}
				raw, err := json.Marshal(r.Input)
				if err != nil {
					t.Fatal(err)
				}
				res, runErr := diff.Run(tools.WithUnscoped(context.Background()), raw)
				if hasFrom {
					// A recorded from_version answer pins the schemas; skip the
					// ones that are registry errors, whose wording is the client's.
					if r.Error != "" {
						continue
					}
					if runErr != nil || res.Summary != r.Summary {
						t.Errorf("input %s: the scenario's schemas give %q (err %v), recorded %q", raw, res.Summary, runErr, r.Summary)
					}
					continue
				}
				subjectOnly++
				switch {
				case runErr != nil:
					if r.Error != runErr.Error() {
						t.Errorf("input %s: recorded error %q, real tool %q", raw, r.Error, runErr.Error())
					}
				default:
					if r.Error != "" || r.Summary != res.Summary {
						t.Errorf("input %s: recorded summary %q (error %q), real tool %q", raw, r.Summary, r.Error, res.Summary)
					}
				}
			}
			if subjectOnly == 0 {
				t.Errorf("%s records no subject-only or to_version-only schema_registry_diff", tt.scenario)
			}
		})
	}
}

// A subject-only recording names the versions it compared, or says there is
// nothing earlier to compare; neither is an error.
func TestW4SubjectOnlyDiffRecordingsAreNotErrors(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for _, sc := range scs {
		for _, r := range sc.Recordings {
			if r.Tool != "schema_registry_diff" || len(r.Input) != 1 || r.Input["subject"] == nil {
				continue
			}
			n++
			if r.Error != "" {
				t.Errorf("%s: subject-only schema_registry_diff records the error %q", sc.Name, r.Error)
			}
			if !strings.Contains(r.Summary, "(latest vs previous; pass from_version to pick another pair)") &&
				!strings.Contains(r.Summary, "has no earlier version") &&
				!strings.Contains(r.Summary, "has no registered versions") {
				t.Errorf("%s: subject-only schema_registry_diff summary %q is not the tool's current answer", sc.Name, r.Summary)
			}
		}
	}
	if n == 0 {
		t.Fatal("no subject-only schema_registry_diff recordings found")
	}
}

// No scenario file keeps the answer the tool no longer gives.
func TestW4NoScenarioKeepsTheOldDiffError(t *testing.T) {
	var files int
	err := filepath.WalkDir("scenarios", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		files++
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), w4ddOldError) {
			t.Errorf("%s still records %q", path, w4ddOldError)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("no scenario files found")
	}
}
