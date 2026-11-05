package owners

import (
	"strings"
	"testing"
)

func TestRecoveryBlock(t *testing.T) {
	cases := []struct {
		name    string
		block   string
		want    int64
		wantErr string
	}{
		{name: "absent", block: "", want: DefaultRecoveryMaxRows},
		{name: "empty block", block: "    recovery: {}\n", want: DefaultRecoveryMaxRows},
		{name: "override", block: "    recovery:\n      max_rows: 250000\n", want: 250000},
		{name: "upper bound", block: "    recovery: {max_rows: 10000000}\n", want: MaxRecoveryMaxRows},
		{name: "zero", block: "    recovery: {max_rows: 0}\n", wantErr: "recovery.max_rows 0: want 1 to 10000000"},
		{name: "negative", block: "    recovery: {max_rows: -5}\n", wantErr: "recovery.max_rows -5"},
		{name: "too big", block: "    recovery: {max_rows: 10000001}\n", wantErr: "want 1 to"},
		{name: "fraction", block: "    recovery: {max_rows: 1.5}\n", wantErr: "parse"},
		{name: "text", block: "    recovery: {max_rows: lots}\n", wantErr: "parse"},
		{name: "unknown key", block: "    recovery:\n      max_row: 5\n", wantErr: `recovery: unknown field "max_row"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, withWatch(tc.block)))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p, _ := o.Pipeline("orders")
			if got := p.RecoveryMaxRows(); got != tc.want {
				t.Fatalf("RecoveryMaxRows() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRecoveryBlockRefusedOnBatch(t *testing.T) {
	_, err := loadBatch(t, "    dags: [revenue_daily]\n    recovery: {max_rows: 5}\n")
	if err == nil || !strings.Contains(err.Error(), "recovery is a cdc field") {
		t.Fatalf("Load = %v, want the cdc-field refusal", err)
	}
}

func TestRecoveryMaxRowsZeroValue(t *testing.T) {
	if got := (Pipeline{ID: "x"}).RecoveryMaxRows(); got != DefaultRecoveryMaxRows {
		t.Fatalf("a pipeline built in code: %d, want the default", got)
	}
}
