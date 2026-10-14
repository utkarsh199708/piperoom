package dbtools

import (
	"strings"
	"testing"
	"time"
)

// TestWarehouseFreshnessSkewAllowance: a newest value up to a minute ahead
// of this server's clock is ordinary clock skew and reads as fresh, the same
// allowance topology health uses; beyond it the value is flagged.
func TestWarehouseFreshnessSkewAllowance(t *testing.T) {
	tests := []struct {
		name       string
		ahead      time.Duration
		wantFuture bool
	}{
		{"30s ahead is skew", 30 * time.Second, false},
		{"2m ahead is flagged", 2 * time.Minute, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := testDeps()
			d.Wh = &fakeWarehouse{fresh: time.Now().Add(tt.ahead)}
			res, err := runTool(t, toolOf(t, d, "warehouse_freshness"), `{"table":"analytics.orders","ts_column":"updated_at"}`)
			if err != nil {
				t.Fatal(err)
			}
			data := res.Data.(map[string]any)
			future := strings.Contains(res.Summary, "IN THE FUTURE") && data["in_future"] == true
			if future != tt.wantFuture {
				t.Fatalf("in future = %v, want %v: %q %v", future, tt.wantFuture, res.Summary, data)
			}
			if !tt.wantFuture && data["stale_for"] != "0s" {
				t.Fatalf("within skew: stale_for = %v, want 0s (%q)", data["stale_for"], res.Summary)
			}
		})
	}
}
