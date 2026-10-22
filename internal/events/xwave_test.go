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

func TestHypothesisScopeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		v    events.AgentHypothesisPayload
	}{
		{"pipeline", events.AgentHypothesisPayload{Text: "slot inactive", BasedOnSteps: []string{"s_1"}, Confidence: "high", Scope: events.ScopePipeline}},
		{"upstream app with team", events.AgentHypothesisPayload{Text: "orders-api deploy dropped a column", BasedOnSteps: []string{"s_2"}, Confidence: "medium", Scope: events.ScopeUpstreamApp, Team: "checkout"}},
		{"vendor", events.AgentHypothesisPayload{Text: "MSK incident", BasedOnSteps: []string{}, Confidence: "low", Scope: events.ScopeVendor}},
		{"unknown", events.AgentHypothesisPayload{Text: "unclear", BasedOnSteps: []string{}, Confidence: "low", Scope: events.ScopeUnknown}},
	}
	for _, tt := range tests {
		roundTrip(t, tt.name, tt.v)
	}
}

func TestHypothesisScopeDecoding(t *testing.T) {
	tests := []struct {
		name, raw string
		want      events.HypothesisScope
	}{
		{"known", `{"text":"x","based_on_steps":[],"confidence":"low","confirmed":false,"scope":"vendor"}`, events.ScopeVendor},
		{"value from a newer writer", `{"text":"x","based_on_steps":[],"confidence":"low","confirmed":false,"scope":"cosmic_ray"}`, events.ScopeUnknown},
		{"wrong case", `{"text":"x","based_on_steps":[],"confidence":"low","confirmed":false,"scope":"Pipeline"}`, events.ScopeUnknown},
		{"null", `{"text":"x","based_on_steps":[],"confidence":"low","confirmed":false,"scope":null}`, events.ScopeUnknown},
		{"before X4: no scope", `{"text":"x","based_on_steps":["s_1"],"confidence":"high","confirmed":false}`, events.ScopeUnknown},
	}
	for _, tt := range tests {
		e := events.Event{Type: events.AgentHypothesis, Payload: json.RawMessage(tt.raw)}
		p, err := events.Decode[events.AgentHypothesisPayload](e)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if p.Scope != tt.want {
			t.Errorf("%s: scope = %q, want %q", tt.name, p.Scope, tt.want)
		}
	}
	if _, err := events.Decode[events.AgentHypothesisPayload](events.Event{Type: events.AgentHypothesis, Payload: json.RawMessage(`{"scope":7}`)}); err == nil {
		t.Error("a non-string scope decoded without error")
	}
}

// Replay compatibility: an agent.hypothesis written before X4 decodes and
// marshals back to the same bytes, so snapshots and export hashes of old
// rooms do not move.
func TestHypothesisBeforeX4MarshalsUnchanged(t *testing.T) {
	old := `{"text":"connector task failed on a schema change","based_on_steps":["s_1","s_3"],"confidence":"high","confirmed":false,"unbacked_terms":["orders_v2"]}`
	p, err := events.Decode[events.AgentHypothesisPayload](events.Event{Type: events.AgentHypothesis, Payload: json.RawMessage(old)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != old {
		t.Errorf("re-marshalled\n got %s\nwant %s", got, old)
	}
}

func TestParseHypothesisScope(t *testing.T) {
	for in, want := range map[string]events.HypothesisScope{
		"pipeline": events.ScopePipeline, "upstream_app": events.ScopeUpstreamApp, "vendor": events.ScopeVendor,
		"unknown": events.ScopeUnknown, "": events.ScopeUnknown, "team": events.ScopeUnknown,
	} {
		if got := events.ParseHypothesisScope(in); got != want {
			t.Errorf("ParseHypothesisScope(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPreventiveSignalsRoundTrip(t *testing.T) {
	eta := time.Date(2026, 10, 23, 4, 30, 0, 0, time.UTC)
	forecasts := []struct {
		name string
		v    events.ForecastSignal
	}{
		{"slot WAL", events.ForecastSignal{Kind: events.SignalForecast, Metric: "slot_wal_bytes", Current: 7.5e9, Limit: 1e10, ETA: eta, Slope: 1.2e8}},
		{"falling slope, zero current", events.ForecastSignal{Kind: events.SignalForecast, Metric: "consumer_lag", Current: 0, Limit: 50000, ETA: eta, Slope: -3.5}},
		{"empty", events.ForecastSignal{}},
	}
	for _, tt := range forecasts {
		roundTrip(t, tt.name, tt.v)
	}
	drifts := []struct {
		name string
		v    events.ConfigDriftSignal
	}{
		{"plain keys", events.ConfigDriftSignal{Kind: events.SignalConfigDrift, Connector: "pg-orders", OldHash: "ab12", NewHash: "cd34", ChangedKeys: []string{"snapshot.mode", "table.include.list"}}},
		{"a secret key", events.ConfigDriftSignal{Kind: events.SignalConfigDrift, Connector: "pg-orders", OldHash: "ab12", NewHash: "ef56", ChangedKeys: []string{events.SecretKeyChanged, "tasks.max"}}},
		{"no keys", events.ConfigDriftSignal{Kind: events.SignalConfigDrift, Connector: "pg-orders", OldHash: "ab12", NewHash: "ab13", ChangedKeys: []string{}}},
	}
	for _, tt := range drifts {
		roundTrip(t, tt.name, tt.v)
	}
}

// The placeholder is part of the contract: the watcher writes it, the web app
// and the agent recognise it.
func TestSecretKeyChangedPlaceholder(t *testing.T) {
	if events.SecretKeyChanged != "<secret key changed>" {
		t.Fatalf("SecretKeyChanged = %q", events.SecretKeyChanged)
	}
}

// X3 adds kinds; it changes no severity of the X1 kinds.
func TestPreventiveSignalsKeepSeverities(t *testing.T) {
	for _, k := range []string{events.SignalForecast, events.SignalConfigDrift} {
		if s := events.DefaultSignalSeverity(k); s != "" {
			t.Errorf("DefaultSignalSeverity(%q) = %q; X3-0 sets no default (owners.yaml or the watcher decide)", k, s)
		}
	}
}
