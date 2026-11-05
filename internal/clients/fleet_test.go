package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeFleetStore proves the X5-0 interfaces can be implemented together, as
// the room store will (X5a, X5c), with no adapter.
type fakeFleetStore struct{}

func (fakeFleetStore) ListFleetChanges(context.Context, []string, time.Time, []string, ChangeCursor, int) ([]PipelineChange, error) {
	return []PipelineChange{}, nil
}
func (fakeFleetStore) PutWatchStatus(context.Context, WatchStatus) error { return nil }
func (fakeFleetStore) WatchStatuses(context.Context, []string) ([]WatchStatus, error) {
	return nil, nil
}
func (fakeFleetStore) RecordConfigVersion(context.Context, ConnectorConfigVersion) (bool, error) {
	return true, nil
}
func (fakeFleetStore) ConfigVersions(context.Context, string, string, int) ([]ConnectorConfigVersion, error) {
	return nil, nil
}
func (fakeFleetStore) ConfigVersion(context.Context, string, string, string) (ConnectorConfigVersion, error) {
	return ConnectorConfigVersion{}, ErrNotFound
}

var (
	_ FleetChangeReader   = fakeFleetStore{}
	_ WatchStatusWriter   = fakeFleetStore{}
	_ WatchStatusReader   = fakeFleetStore{}
	_ ConfigVersionWriter = fakeFleetStore{}
	_ ConfigVersionReader = fakeFleetStore{}
)

// strictRoundTrip encodes v, decodes it into a fresh value of the same type
// with unknown fields refused, and checks the two are equal.
func strictRoundTrip[T any](t *testing.T, v T) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var got T
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("strict decode of %s: %v", b, err)
	}
	if !reflect.DeepEqual(got, v) {
		t.Fatalf("round trip changed the value:\n got %#v\nwant %#v", got, v)
	}
	return b
}

func TestFleetShapesRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 22, 10, 1, 0, 0, time.UTC)
	eta := at.Add(18 * time.Hour)
	t.Run("ChangeCursor", func(t *testing.T) {
		strictRoundTrip(t, ChangeCursor{At: at, ID: 42})
		strictRoundTrip(t, ChangeCursor{})
	})
	t.Run("WatchStatus", func(t *testing.T) {
		b := strictRoundTrip(t, WatchStatus{PipelineID: "orders", CheckedAt: at, Signals: []WatchSignal{
			{Signal: "connector_down", State: WatchFiring, Severity: "sev2", Node: "connect:pg-orders", Since: at},
			{Signal: "forecast", State: WatchFiring, Severity: "sev4", Since: at, Metric: "slot_wal_bytes", ETA: &eta},
		}})
		if !strings.Contains(string(b), `"eta":"2026-10-23T04:01:00Z"`) {
			t.Fatalf("eta not encoded as RFC 3339: %s", b)
		}
		// Zero values: empty signals stay an empty list, not null; unset
		// optional fields are omitted.
		b = strictRoundTrip(t, WatchStatus{PipelineID: "orders", Signals: []WatchSignal{}})
		if !strings.Contains(string(b), `"signals":[]`) {
			t.Fatalf("empty signals: %s", b)
		}
		b = strictRoundTrip(t, WatchSignal{Signal: "slot_wal", State: WatchAcked, Severity: "sev3", Since: at})
		for _, f := range []string{"node", "metric", "eta"} {
			if strings.Contains(string(b), `"`+f+`"`) {
				t.Fatalf("zero %s not omitted: %s", f, b)
			}
		}
	})
	t.Run("ConnectorConfigVersion", func(t *testing.T) {
		strictRoundTrip(t, ConnectorConfigVersion{PipelineID: "orders", Connector: "pg-orders", Hash: strings.Repeat("a", 64),
			At: at, By: "proposal:p_01", ChangedKeys: []string{"<secret key changed>", "decimal.handling.mode"},
			Config: map[string]string{"decimal.handling.mode": "double", "database.password": "<redacted>"}})
		// A list answer: Config empty is omitted; By and ChangedKeys stay.
		b := strictRoundTrip(t, ConnectorConfigVersion{PipelineID: "orders", Connector: "pg-orders", Hash: "h", At: at, ChangedKeys: []string{}})
		if strings.Contains(string(b), `"config"`) || !strings.Contains(string(b), `"by":""`) || !strings.Contains(string(b), `"changed_keys":[]`) {
			t.Fatalf("list form: %s", b)
		}
	})
}

func TestFleetShapesRejectUnknownFields(t *testing.T) {
	for name, tc := range map[string]struct {
		in  string
		dst any
	}{
		"WatchStatus":            {`{"pipeline_id":"orders","checked_at":"2026-10-22T10:01:00Z","signals":[],"evidence":{}}`, &WatchStatus{}},
		"WatchSignal":            {`{"signal":"slot_wal","state":"firing","severity":"sev3","since":"2026-10-22T10:01:00Z","value":"x"}`, &WatchSignal{}},
		"ConnectorConfigVersion": {`{"pipeline_id":"o","connector":"c","hash":"h","at":"2026-10-22T10:01:00Z","by":"","changed_keys":[],"raw":{}}`, &ConnectorConfigVersion{}},
		"ChangeCursor":           {`{"at":"2026-10-22T10:01:00Z","id":1,"offset":3}`, &ChangeCursor{}},
	} {
		t.Run(name, func(t *testing.T) {
			dec := json.NewDecoder(strings.NewReader(tc.in))
			dec.DisallowUnknownFields()
			if err := dec.Decode(tc.dst); err == nil {
				t.Fatalf("unknown field accepted")
			}
		})
	}
}

// TestFleetCaps pins the caps docs/api.md ("Fleet view") documents, so a
// change to one is a visible contract change.
func TestFleetCaps(t *testing.T) {
	for _, tc := range []struct {
		name      string
		got, want int
	}{
		{"MaxFleetChangesPage", MaxFleetChangesPage, 200},
		{"MaxWatchSignals", MaxWatchSignals, 32},
		{"MaxConfigKeys", MaxConfigKeys, 1000},
		{"MaxConfigVersionList", MaxConfigVersionList, 200},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, docs/api.md says %d", tc.name, tc.got, tc.want)
		}
	}
}
