package events_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

// decodeStrict decodes like a careful reader of watcher evidence: an unknown
// field is an error, so a renamed JSON tag cannot pass silently.
func decodeStrict(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func TestDriftSignalRoundTrip(t *testing.T) {
	from := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	tests := []struct {
		name string
		in   events.DriftSignal
	}{
		{"zero value", events.DriftSignal{}},
		{"count_drift with zero missing", events.DriftSignal{
			Kind: events.SignalCountDrift, Severity: "sev2", Table: "public.orders", Window: events.DriftWindow{From: from, To: to},
			Source: 1000, Sink: 1000, Missing: 0,
		}},
		{"count_drift", events.DriftSignal{
			Kind: events.SignalCountDrift, Severity: "sev2", Table: "public.orders", Topic: "pg.public.orders",
			Window: events.DriftWindow{From: from, To: to}, Source: 1000, Sink: 990, Missing: 10,
			Detail: "1.0 % rows missing on the sink over 1 h",
		}},
		{"row_drift", events.DriftSignal{
			Kind: events.SignalRowDrift, Severity: "sev2", Table: "public.orders", Window: events.DriftWindow{From: from, To: to},
			Missing: 1, Mismatched: 2, Sample: 50,
		}},
		{"dup_keys", events.DriftSignal{
			Kind: events.SignalDupKeys, Severity: "sev3", Topic: "pg.public.orders", Window: events.DriftWindow{From: from, To: to},
			Source: 41200, Missing: 3, Sample: 2,
		}},
		{"order_skew", events.DriftSignal{
			Kind: events.SignalOrderSkew, Severity: "sev3", Topic: "pg.public.orders", Window: events.DriftWindow{From: from, To: to},
			Source: 500, Mismatched: 1, Sample: 1,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			// The numbers are evidence even when 0: they are always written.
			for _, k := range []string{"signal", "severity", "window", "source", "sink", "missing", "mismatched", "sample"} {
				if !strings.Contains(string(raw), `"`+k+`":`) {
					t.Errorf("%s has no %q", raw, k)
				}
			}
			var out events.DriftSignal
			if err := decodeStrict(raw, &out); err != nil {
				t.Fatalf("decode %s: %v", raw, err)
			}
			if !reflect.DeepEqual(out, tt.in) {
				t.Errorf("round trip changed the signal:\n got %+v\nwant %+v", out, tt.in)
			}
		})
	}
}

// The web (X1f) and the evidence renderer read these exact keys: a drift
// signal is one flat entry of the evidence's signals list, "signal" and
// "severity" first, like every other watcher signal.
func TestDriftSignalWireShape(t *testing.T) {
	from := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	raw, err := json.Marshal(events.DriftSignal{
		Kind: events.SignalCountDrift, Severity: events.SignalSeverity(events.SignalCountDrift), Table: "public.orders",
		Window: events.DriftWindow{From: from, To: from.Add(time.Hour)}, Source: 1000, Sink: 990, Missing: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"signal":"count_drift","severity":"sev2","table":"public.orders","window":{"from":"2026-10-22T09:00:00Z","to":"2026-10-22T10:00:00Z"},` +
		`"source":1000,"sink":990,"missing":10,"mismatched":0,"sample":0}`
	if string(raw) != want {
		t.Errorf("wire shape\n got %s\nwant %s", raw, want)
	}
}

func TestDriftSignalRejectsUnknownFields(t *testing.T) {
	for _, raw := range []string{
		`{"signal":"count_drift","keys":["42"]}`,
		`{"signal":"row_drift","window":{"from":"2026-10-22T09:00:00Z","until":"2026-10-22T10:00:00Z"}}`,
		`{"kind":"count_drift"}`, // the pre-review nested shape's key: refused, not silently empty
	} {
		var out events.DriftSignal
		if err := decodeStrict([]byte(raw), &out); err == nil {
			t.Errorf("strict decode of %s: want an unknown-field error", raw)
		}
	}
}

// TC4 (X1-0) and the X3 defaults: one severity table for the six X-wave
// signals, the one docs/api.md shows.
func TestSignalSeverity(t *testing.T) {
	tests := []struct{ kind, want string }{
		{events.SignalCountDrift, "sev2"},
		{events.SignalRowDrift, "sev2"},
		{events.SignalDupKeys, "sev3"},
		{events.SignalOrderSkew, "sev3"},
		{events.SignalConfigDrift, "sev3"},
		{events.SignalForecast, "sev4"},
		{"slot_wal", ""},
		{"", ""},
		{"COUNT_DRIFT", ""}, // kinds are exact
	}
	for _, tt := range tests {
		if got := events.SignalSeverity(tt.kind); got != tt.want {
			t.Errorf("SignalSeverity(%q) = %q, want %q", tt.kind, got, tt.want)
		}
	}
}
