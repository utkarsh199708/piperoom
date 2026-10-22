package events_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"piperoom/internal/events"
)

// roundTrip marshals v, decodes it into a new value of the same type and
// fails unless every field survived.
func roundTrip[T any](t *testing.T, name string, v T) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("%s: marshal: %v", name, err)
	}
	var got T
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%s: unmarshal %s: %v", name, raw, err)
	}
	if !reflect.DeepEqual(got, v) {
		t.Errorf("%s: round trip lost data:\n got %+v\nwant %+v\njson %s", name, got, v, raw)
	}
}

func TestDriftSignalRoundTrip(t *testing.T) {
	from := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	w := events.DriftWindow{From: from, To: from.Add(5 * time.Minute)}
	tests := []struct {
		name string
		v    events.DriftSignal
	}{
		{"count drift, zero missing", events.DriftSignal{Kind: events.SignalCountDrift, Table: "public.orders", Topic: "pg.public.orders", Window: w, Source: 1200, Sink: 1188, Detail: "12 rows short"}},
		{"row drift", events.DriftSignal{Kind: events.SignalRowDrift, Table: "public.orders", Window: w, Missing: 3, Mismatched: 2, Sample: 200}},
		{"dup keys", events.DriftSignal{Kind: events.SignalDupKeys, Table: "public.orders", Topic: "pg.public.orders", Window: w, Mismatched: 4, Sample: 200}},
		{"order skew", events.DriftSignal{Kind: events.SignalOrderSkew, Table: "public.orders", Topic: "pg.public.orders", Window: w, Mismatched: 1, Sample: 50}},
		{"empty", events.DriftSignal{}},
	}
	for _, tt := range tests {
		roundTrip(t, tt.name, tt.v)
	}
}

// Zero counts are facts ("0 missing"), not absent data: they stay on the wire.
func TestDriftSignalKeepsZeroCounts(t *testing.T) {
	raw, err := json.Marshal(events.DriftSignal{Kind: events.SignalCountDrift})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"source", "sink", "missing", "mismatched", "sample", "window"} {
		if _, ok := m[k]; !ok {
			t.Errorf("%s has no %q", raw, k)
		}
	}
}

func TestDefaultSignalSeverity(t *testing.T) {
	tests := map[string]string{
		events.SignalCountDrift: "sev2",
		events.SignalRowDrift:   "sev2",
		events.SignalDupKeys:    "sev3",
		events.SignalOrderSkew:  "sev3",
		"connector_down":        "", // an owners.Signal* kind: the watcher keeps its own rule
		"":                      "",
	}
	for kind, want := range tests {
		if got := events.DefaultSignalSeverity(kind); got != want {
			t.Errorf("DefaultSignalSeverity(%q) = %q, want %q", kind, got, want)
		}
	}
}
