package wiring

import (
	"context"
	"testing"

	"piperoom/internal/tools"
)

// TestOpenKeepsConnSettings: a set opened from cfg hands net_probe that cfg
// (ConnSettings), the read-only Connect value (ReadOnlyConnect), and calls
// the ProbeTools hook with itself for a CDC set only.
func TestOpenKeepsConnSettings(t *testing.T) {
	rec := &openRecorder{}
	cfg := devA2Config()
	cfg.SourceDBURL = "postgres://u@db.example:5433/orders"
	w, err := openClientsWith(context.Background(), cfg, nil, ownersOf(cdcPipe("a")), false, rec.openers(t))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := w.ConnSettings(); got.SourceDBURL != cfg.SourceDBURL || got.ConnectURL != cfg.ConnectURL {
		t.Fatalf("ConnSettings = %q, %q; want the set's own settings", got.SourceDBURL, got.ConnectURL)
	}
	if _, ok := w.ReadOnlyConnect().(readOnlyConnect); ok {
		t.Fatal("ReadOnlyConnect lost the config validate of the real client")
	}
	if w.ReadOnlyConnect() == w.Connect {
		t.Fatal("ReadOnlyConnect returned the writing client")
	}
	if (&Wired{}).ReadOnlyConnect() != nil {
		t.Fatal("no Connect client must stay nil")
	}

	for _, tt := range []struct {
		name string
		w    *Wired
		want int
	}{
		{"cdc", &Wired{Owners: ownersOf(cdcPipe("a"))}, 1},
		{"batch only", &Wired{Owners: ownersOf(batchPipe("b"))}, 0},
	} {
		calls := 0
		tt.w.ProbeTools = func(_ *tools.Registry, got *Wired) error {
			calls++
			if got != tt.w {
				t.Errorf("%s: hook got another set", tt.name)
			}
			return nil
		}
		registered(t, tt.w)
		if calls != tt.want {
			t.Errorf("%s: ProbeTools calls = %d, want %d", tt.name, calls, tt.want)
		}
	}
}
