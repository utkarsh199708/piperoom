package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// ageSource records the age threshold the tool asked the client for.
type ageSource struct {
	*fakeSource
	got   time.Duration
	calls int
}

func (s *ageSource) LongTransactions(_ context.Context, d time.Duration) ([]clients.Transaction, error) {
	s.got = d
	s.calls++
	return nil, nil
}

func TestLongTransactionsOlderThanBounds(t *testing.T) {
	maxAge := time.Duration(longTxnMaxMinutes) * time.Minute
	tests := []struct {
		name   string
		input  string
		want   time.Duration
		refuse bool
	}{
		{"missing", `{}`, 5 * time.Minute, false},
		{"null", `{"older_than_minutes":null}`, 5 * time.Minute, false},
		{"in range", `{"older_than_minutes":30}`, 30 * time.Minute, false},
		{"smallest", `{"older_than_minutes":1}`, time.Minute, false},
		{"at max", `{"older_than_minutes":43200}`, maxAge, false},
		{"above max", `{"older_than_minutes":43201}`, maxAge, false},
		// 153722868 minutes is the first count whose Duration wraps int64.
		{"would wrap a Duration", `{"older_than_minutes":153722868}`, maxAge, false},
		{"huge", `{"older_than_minutes":9223372036854775807}`, maxAge, false},
		{"zero", `{"older_than_minutes":0}`, 0, true},
		{"negative", `{"older_than_minutes":-1}`, 0, true},
		{"most negative", `{"older_than_minutes":-9223372036854775808}`, 0, true},
		{"beyond int64", `{"older_than_minutes":99999999999999999999}`, 0, true},
		{"exponent", `{"older_than_minutes":1e30}`, 0, true},
		{"fraction", `{"older_than_minutes":1.5}`, 0, true},
		{"string", `{"older_than_minutes":"5"}`, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := &ageSource{fakeSource: &fakeSource{}}
			d := testDeps()
			d.Src = src
			_, err := runTool(t, pgLongTransactionsTool(d), tc.input)
			if tc.refuse {
				if !errors.Is(err, tools.ErrBadInput) {
					t.Fatalf("err = %v, want ErrBadInput", err)
				}
				if src.calls != 0 {
					t.Fatalf("refused input reached the client %d times", src.calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if src.got != tc.want {
				t.Errorf("client asked for %v, want %v", src.got, tc.want)
			}
		})
	}
}

// The range is told to the model in the description, not in the schema: strict
// tool use rejects minimum and maximum (tasks/C11-runbook-tool.md).
func TestLongTransactionsSchemaStatesRange(t *testing.T) {
	prop := pgLongTransactionsTool(testDeps()).InputSchema()["properties"].(map[string]any)["older_than_minutes"].(map[string]any)
	if desc, _ := prop["description"].(string); !strings.Contains(desc, "1-"+strconv.Itoa(longTxnMaxMinutes)) {
		t.Errorf("description %q does not state the 1-%d range", desc, longTxnMaxMinutes)
	}
	b, err := json.Marshal(prop)
	if err != nil {
		t.Fatal(err)
	}
	for _, kw := range []string{"minimum", "maximum"} {
		if strings.Contains(string(b), kw) {
			t.Errorf("schema uses %s", kw)
		}
	}
}
