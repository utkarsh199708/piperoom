package runbooks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/tools"
)

// max_results is the only number the model sends. A value that does not fit
// an int never reaches Run's arithmetic: decoding refuses it.
func TestMaxResultsBounds(t *testing.T) {
	const q = `"query":"slot wal connector consumer schema retention"`
	tests := []struct {
		name   string
		input  string
		want   int
		refuse bool
	}{
		{"missing", `{` + q + `}`, defaultResults, false},
		{"null", `{` + q + `,"max_results":null}`, defaultResults, false},
		{"in range", `{` + q + `,"max_results":2}`, 2, false},
		{"smallest", `{` + q + `,"max_results":1}`, 1, false},
		{"at max", `{` + q + `,"max_results":5}`, maxResults, false},
		{"above max", `{` + q + `,"max_results":6}`, maxResults, false},
		{"huge", `{` + q + `,"max_results":9223372036854775807}`, maxResults, false},
		// 0 is the documented spelling of "default" (tasks/C11-runbook-tool.md).
		{"zero", `{` + q + `,"max_results":0}`, defaultResults, false},
		{"negative", `{` + q + `,"max_results":-1}`, 0, true},
		{"most negative", `{` + q + `,"max_results":-9223372036854775808}`, 0, true},
		{"beyond int64", `{` + q + `,"max_results":99999999999999999999}`, 0, true},
		{"exponent", `{` + q + `,"max_results":1e30}`, 0, true},
		{"fraction", `{` + q + `,"max_results":1.5}`, 0, true},
	}
	tl := New(embedded(t))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tl.Run(context.Background(), json.RawMessage(tc.input))
			if tc.refuse {
				if !errors.Is(err, tools.ErrBadInput) {
					t.Fatalf("err = %v, want ErrBadInput", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := len(res.Data.(map[string]any)["hits"].([]hit)); got != tc.want {
				t.Errorf("%d hits, want %d", got, tc.want)
			}
		})
	}
}
