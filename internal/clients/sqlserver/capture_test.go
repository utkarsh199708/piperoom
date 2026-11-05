package sqlserver

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestCaptureInstances(t *testing.T) {
	long := strings.Repeat("c", 95)
	atLimit := strings.Repeat("c", MaxCaptureInstanceChars-len("_dbo_orders"))
	tests := []struct {
		name      string
		inst      string
		tables    []string
		want      []string
		wantErr   bool
		wantLimit bool
	}{
		{"one table keeps the name", "piperoom", []string{"dbo.orders"}, []string{"piperoom"}, false, false},
		{"several get name_schema_table", "piperoom", []string{"dbo.orders", "dbo.customers"}, []string{"piperoom_dbo_orders", "piperoom_dbo_customers"}, false, false},
		{"a duplicate counts once", "piperoom", []string{"dbo.orders", "dbo.orders"}, []string{"piperoom"}, false, false},
		{"duplicates keep first-seen order", "piperoom", []string{"dbo.a", "dbo.b", "dbo.a"}, []string{"piperoom_dbo_a", "piperoom_dbo_b"}, false, false},
		{"no tables", "piperoom", nil, []string{}, false, false},
		{"long name, one table", long, []string{"dbo.orders"}, []string{long}, false, false},
		{"long name, a duplicate", long, []string{"dbo.orders", "dbo.orders"}, []string{long}, false, false},
		{"exactly the limit", atLimit, []string{"dbo.orders", "dbo.events"}, []string{atLimit + "_dbo_orders", atLimit + "_dbo_events"}, false, false},
		{"one over the limit", atLimit + "c", []string{"dbo.orders", "dbo.events"}, nil, true, true},
		{"long name, two tables", long, []string{"dbo.orders", "dbo.customers"}, nil, true, true},
		{"bad name", "x]; DROP", []string{"dbo.orders"}, nil, true, false},
		{"bad table", "piperoom", []string{"orders"}, nil, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CaptureInstances(tc.inst, tc.tables)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %v, want an error", got)
				}
				if errors.Is(err, ErrCaptureInstance) != tc.wantLimit {
					t.Fatalf("err = %v, want ErrCaptureInstance=%v", err, tc.wantLimit)
				}
				return
			}
			if err != nil {
				t.Fatalf("CaptureInstances: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCreatePublicationRefusesLongCaptureInstanceBeforeIO: a derived name over
// the limit is refused before sp_cdc_enable_db or any other statement runs.
func TestCreatePublicationRefusesLongCaptureInstanceBeforeIO(t *testing.T) {
	f := &fakeDB{}
	w := testWriter(f)
	err := w.CreatePublication(unscopedCtx(), strings.Repeat("c", 95), []string{"dbo.orders", "dbo.customers"})
	if !errors.Is(err, ErrCaptureInstance) {
		t.Fatalf("err = %v, want ErrCaptureInstance", err)
	}
	if len(f.sqls) != 0 {
		t.Fatalf("statements ran before the refusal: %q", f.sqls)
	}
}
