package events_test

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

// strictDecode decodes raw into a new value of v's type, refusing unknown
// fields and trailing data, as a contract-shape check does.
func strictDecode(t *testing.T, raw []byte, v any) any {
	t.Helper()
	ptr := reflect.New(reflect.TypeOf(v))
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(ptr.Interface()); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return ptr.Elem().Interface()
}

// TC1, E1: both X3 signal payloads, the escalation payload and room.created
// with an origin survive a JSON round trip, zero values included.
func TestX3PayloadsRoundTrip(t *testing.T) {
	eta := time.Date(2026, 10, 24, 3, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		v    any
	}{
		{"forecast", events.ForecastSignal{Signal: events.SignalForecast, Severity: events.ForecastSeverity,
			Metric: "slot_wal_bytes", Object: "orders_slot", Current: 64 << 30, Limit: 100 << 30, ETA: eta,
			Slope: 1 << 30, R2: 0.97, Samples: 96}},
		{"forecast zero", events.ForecastSignal{}},
		{"forecast fractional", events.ForecastSignal{Metric: "topic_oldest_unread_age", Current: 518400.5,
			Limit: 604800, ETA: eta, Slope: 3600.25, R2: 0.6, Samples: 8}},
		{"config_drift", events.ConfigDriftSignal{Signal: events.SignalConfigDrift, Severity: events.ConfigDriftSeverity,
			Connector: "orders-src", OldHash: strings.Repeat("a", 64), NewHash: strings.Repeat("b", 64),
			ChangedKeys: []string{events.SecretKeyChanged, "decimal.handling.mode"}, KeysOmitted: 3}},
		{"config_drift empty keys", events.ConfigDriftSignal{Connector: "c", ChangedKeys: []string{}}},
		{"config_drift zero", events.ConfigDriftSignal{}},
		{"room.escalated", events.RoomEscalatedPayload{Severity: "sev2", From: "sev4", Signal: "slot_wal"}},
		{"room.escalated zero", events.RoomEscalatedPayload{}},
		{"room.created forecast", events.RoomCreatedPayload{Title: "WAL fills in 40 h", Severity: "sev4",
			PipelineID: "orders", AlertSource: "watcher", DedupKey: "watcher:orders", Origin: events.RoomOriginForecast}},
	}
	for _, tt := range tests {
		raw, err := json.Marshal(tt.v)
		if err != nil {
			t.Fatalf("%s: marshal: %v", tt.name, err)
		}
		if got := strictDecode(t, raw, tt.v); !reflect.DeepEqual(got, tt.v) {
			t.Errorf("%s: round trip\n got %#v\nwant %#v", tt.name, got, tt.v)
		}
	}
}

// E1: the strict decoder refuses a key the contract does not name, so a
// typo in the watcher or a client fails a contract test, not silently.
func TestX3PayloadsRejectUnknownFields(t *testing.T) {
	for _, tt := range []struct {
		raw string
		v   any
	}{
		{`{"signal":"forecast","metric":"db_size_bytes","slope":1}`, events.ForecastSignal{}},
		{`{"signal":"config_drift","connector":"c","changed_keys":[],"old_value":"x"}`, events.ConfigDriftSignal{}},
		{`{"severity":"sev2","from":"sev4","signal":"slot_wal","agent":true}`, events.RoomEscalatedPayload{}},
	} {
		dec := json.NewDecoder(strings.NewReader(tt.raw))
		dec.DisallowUnknownFields()
		ptr := reflect.New(reflect.TypeOf(tt.v)).Interface()
		if err := dec.Decode(ptr); err == nil {
			t.Errorf("%s: decoded with an unknown field", tt.raw)
		}
	}
}

// The wire keys are the contract docs/api.md shows; pin them.
func TestX3PayloadKeys(t *testing.T) {
	tests := []struct {
		v    any
		keys []string
	}{
		{events.ForecastSignal{Object: "x"}, []string{"signal", "severity", "metric", "object", "current", "limit", "eta", "slope_per_hour", "r2", "samples"}},
		{events.ConfigDriftSignal{KeysOmitted: 1}, []string{"signal", "severity", "connector", "old_hash", "new_hash", "changed_keys", "keys_omitted"}},
		{events.RoomEscalatedPayload{}, []string{"severity", "from", "signal"}},
	}
	for _, tt := range tests {
		raw, _ := json.Marshal(tt.v)
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if len(m) != len(tt.keys) {
			t.Errorf("%T: %d keys %s; want %v", tt.v, len(m), raw, tt.keys)
		}
		for _, k := range tt.keys {
			if _, ok := m[k]; !ok {
				t.Errorf("%T: key %q missing from %s", tt.v, k, raw)
			}
		}
	}
}

// TC3, E6: the new fields are omitempty, so a room.created from before X3
// marshals byte for byte as before (export hashes), and an absent object
// stays absent.
func TestX3FieldsAreOmittedWhenEmpty(t *testing.T) {
	old := events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders", AlertSource: "watcher"}
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"title":"t","severity":"sev2","pipeline_id":"orders","alert_source":"watcher"}`; string(raw) != want {
		t.Errorf("room.created = %s; want %s", raw, want)
	}
	raw, _ = json.Marshal(events.ForecastSignal{})
	if strings.Contains(string(raw), `"object"`) {
		t.Errorf("forecast without object: %s", raw)
	}
	raw, _ = json.Marshal(events.ConfigDriftSignal{})
	if strings.Contains(string(raw), `"keys_omitted"`) {
		t.Errorf("config_drift without omitted keys: %s", raw)
	}
}

// TC2: the ChangedKeys doc comment is the rule implementers read; it must
// say values are never stored and name the secret placeholder.
func TestConfigDriftChangedKeysDocStatesTheRule(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "events.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var doc string
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "ConfigDriftSignal" {
			return true
		}
		for _, fld := range ts.Type.(*ast.StructType).Fields.List {
			if len(fld.Names) == 1 && fld.Names[0].Name == "ChangedKeys" {
				doc = fld.Doc.Text()
			}
		}
		return false
	})
	doc = strings.Join(strings.Fields(doc), " ")
	for _, want := range []string{"NAMES", "never stored", "SecretKeyChanged", "actions.LooksSecret"} {
		if !strings.Contains(doc, want) {
			t.Errorf("ConfigDriftSignal.ChangedKeys doc %q does not say %q", doc, want)
		}
	}
	if events.SecretKeyChanged != "<secret key changed>" {
		t.Errorf("SecretKeyChanged = %q", events.SecretKeyChanged)
	}
}
