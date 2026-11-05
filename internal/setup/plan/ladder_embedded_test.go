package plan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestEmbeddedLadder(t *testing.T) {
	rungs, err := EmbeddedLadder("postgres")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{RungEngineStarted, RungEngineEvents, RungCanaryEvent, RungPersonChange}
	if len(rungs) != len(want) {
		t.Fatalf("%d rungs, want %d", len(rungs), len(want))
	}
	for i, r := range rungs {
		if r.Name != want[i] || r.Index != i+1 || len(r.Reads) != 1 || r.Reads[0] != ReadEmbeddedLog || r.Predicate == nil {
			t.Errorf("rung %d = %+v", i, r)
		}
	}
	if _, err := EmbeddedLadder("db2"); !errors.Is(err, ErrLadder) {
		t.Fatalf("unknown source: %v", err)
	}
	// Ladder still refuses the embedded target: the room cannot fill its reads yet.
	if _, err := Ladder("postgres", "embedded", SinkNone); !errors.Is(err, ErrLadder) {
		t.Fatalf("Ladder(embedded) = %v, want ErrLadder", err)
	}
}

func TestEmbeddedPredicates(t *testing.T) {
	cases := []struct {
		name string
		pred Predicate
		data string
		ok   bool
		note string
	}{
		{"started", engineStarted, `{"started":true,"events":0}`, true, "running"},
		{"not started", engineStarted, `{"started":false}`, false, "no \"piperoom-reference started\""},
		{"stopped", engineStarted, `{"started":true,"stopped_with_error":true,"error":"Connection refused"}`, false, "Connection refused"},
		{"not a log", engineStarted, `{"state":"RUNNING"}`, false, "did not return"},
		{"summary prose never counts", engineStarted, `"started: yes"`, false, "did not return"},
		{"events", engineEvents, `{"started":true,"events":3}`, true, "3 change events"},
		{"no events", engineEvents, `{"started":true,"events":0}`, false, "no change event"},
		{"canary", embeddedCanary, `{"started":true,"nonce_seen":true}`, true, "canary"},
		{"no canary", embeddedCanary, `{"started":true,"nonce_seen":false}`, false, "not printed"},
		{"person change", embeddedPersonChange, `{"started":true,"nonce_seen":true,"events_after_nonce":1}`, true, "after the canary"},
		{"no change yet", embeddedPersonChange, `{"started":true,"nonce_seen":true,"events_after_nonce":0}`, false, "waiting"},
	}
	for _, tc := range cases {
		ok, note := tc.pred(json.RawMessage(tc.data))
		if ok != tc.ok || !strings.Contains(note, tc.note) {
			t.Errorf("%s: (%v, %q), want (%v, ...%q...)", tc.name, ok, note, tc.ok, tc.note)
		}
	}
}
