package similarincidents_test

import (
	"errors"
	"fmt"
	"testing"

	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

// limit is the only number the model sends. A value that does not fit an int
// never reaches rank: decoding refuses it.
func TestLimitBounds(t *testing.T) {
	var rooms []fake.Room
	for i := 1; i <= 8; i++ {
		rooms = append(rooms, fake.Room{ID: fmt.Sprintf("r-%d", i), Title: "kafka consumer lag", ClosedAt: day(9, i), Resolution: "raised consumers"})
	}
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(seeded(t, rooms...))})
	const q = `"query":"kafka consumer lag"`
	tests := []struct {
		name   string
		input  string
		want   int
		refuse bool
	}{
		{"missing", `{` + q + `}`, 3, false},
		{"null", `{` + q + `,"limit":null}`, 3, false},
		{"in range", `{` + q + `,"limit":2}`, 2, false},
		{"smallest", `{` + q + `,"limit":1}`, 1, false},
		{"at max", `{` + q + `,"limit":5}`, 5, false},
		{"above max", `{` + q + `,"limit":6}`, 5, false},
		{"huge", `{` + q + `,"limit":9223372036854775807}`, 5, false},
		// 0 reads as "not given", the same as the runbook tool's max_results.
		{"zero", `{` + q + `,"limit":0}`, 3, false},
		{"negative", `{` + q + `,"limit":-1}`, 0, true},
		{"most negative", `{` + q + `,"limit":-9223372036854775808}`, 0, true},
		{"beyond int64", `{` + q + `,"limit":99999999999999999999}`, 0, true},
		{"exponent", `{` + q + `,"limit":1e30}`, 0, true},
		{"fraction", `{` + q + `,"limit":1.5}`, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := run(t, tl, scoped("orders"), tc.input)
			if tc.refuse {
				if !errors.Is(err, tools.ErrBadInput) {
					t.Fatalf("err = %v, want ErrBadInput", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := len(hitIDs(t, res)); got != tc.want {
				t.Errorf("%d hits, want %d", got, tc.want)
			}
		})
	}
}
