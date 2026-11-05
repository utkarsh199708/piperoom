package plan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestLadder is the done-when: Ladder(postgres, compose, none) is the eight
// rungs of design §3.4, in order, each with a predicate on structured Data.
func TestLadder(t *testing.T) {
	rungs, err := Ladder("postgres", "compose", SinkNone)
	if err != nil {
		t.Fatalf("Ladder: %v", err)
	}
	want := []string{
		RungConnector, RungTasks, RungCanaryTopic, RungSnapshot,
		RungCanaryEvent, RungCanarySubject, RungSinkRow, RungPersonChange,
	}
	if len(rungs) != len(want) {
		t.Fatalf("got %d rungs, want %d", len(rungs), len(want))
	}
	for i, rung := range rungs {
		if rung.Index != i+1 {
			t.Errorf("rung %d has Index %d", i, rung.Index)
		}
		if rung.Name != want[i] {
			t.Errorf("rung %d is %q, want %q", i, rung.Name, want[i])
		}
		if rung.Predicate == nil {
			t.Errorf("rung %q has no predicate", rung.Name)
		}
		if i != 6 && (len(rung.Reads) == 0 || rung.Reads[0] == "") {
			t.Errorf("rung %q has no read", rung.Name)
		}
	}
	if rungs[6].NotApplicable != "no sink was set up" {
		t.Errorf("rung 7 with no sink: NotApplicable = %q", rungs[6].NotApplicable)
	}
	for _, i := range []int{0, 1, 2, 3, 4, 5, 7} {
		if rungs[i].NotApplicable != "" {
			t.Errorf("rung %q unexpectedly not applicable: %q", rungs[i].Name, rungs[i].NotApplicable)
		}
	}
	if len(rungs[3].Reads) != 2 || rungs[3].Reads[0] != "debezium_metrics" || rungs[3].Reads[1] != "kafka_topic_offsets" {
		t.Errorf("rung 4 reads = %v, want debezium_metrics then kafka_topic_offsets", rungs[3].Reads)
	}
}

func TestLadderSink(t *testing.T) {
	rungs, err := Ladder("postgres", "compose", "clickhouse")
	if err != nil {
		t.Fatalf("Ladder with a sink: %v", err)
	}
	if rungs[6].NotApplicable != "" {
		t.Errorf("rung 7 with a sink is not applicable: %q", rungs[6].NotApplicable)
	}
	if _, err := Ladder("postgres", "compose", ""); err != nil {
		t.Errorf("an empty sink means none: %v", err)
	}
	if _, err := Ladder("mysql", "strimzi", SinkNone); err != nil {
		t.Errorf("a Strimzi target reaches the Connect REST status: %v", err)
	}
}

func TestLadderRefusesUnknown(t *testing.T) {
	tests := []struct{ name, source, target, sink string }{
		{name: "unknown source", source: "db2", target: "compose", sink: SinkNone},
		{name: "empty source", source: "", target: "compose", sink: SinkNone},
		{name: "unknown target", source: "postgres", target: "nomad", sink: SinkNone},
		{name: "embedded has its own ladder", source: "postgres", target: "embedded", sink: SinkNone},
		{name: "dbzserver has its own ladder", source: "postgres", target: "dbzserver", sink: SinkNone},
		{name: "bad sink kind", source: "postgres", target: "compose", sink: "ClickHouse"},
		{name: "sink with a separator", source: "postgres", target: "compose", sink: "postgres/eu"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Ladder(tt.source, tt.target, tt.sink); !errors.Is(err, ErrLadder) {
				t.Errorf("Ladder(%q, %q, %q) error = %v, want ErrLadder", tt.source, tt.target, tt.sink, err)
			}
		})
	}
}

// TestLadderPredicates feeds each rung's predicate the Data shape its read
// returns (internal/tools) and checks the verdict.
func TestLadderPredicates(t *testing.T) {
	tests := []struct {
		rung string
		data string
		ok   bool
	}{
		// 1: connect_status Data.
		{rung: RungConnector, data: `{"name":"orders","state":"RUNNING","tasks":[{"id":0,"state":"RUNNING"}]}`, ok: true},
		{rung: RungConnector, data: `{"name":"orders","state":"FAILED","tasks":[{"id":0,"state":"FAILED"}]}`, ok: false},
		{rung: RungConnector, data: `{"name":"orders","state":"PAUSED"}`, ok: false},
		{rung: RungConnector, data: `{"name":"orders"}`, ok: false},
		{rung: RungConnector, data: `not json`, ok: false},
		{rung: RungConnector, data: `null`, ok: false},
		// 2: every task RUNNING, at least one task.
		{rung: RungTasks, data: `{"state":"RUNNING","tasks":[{"id":0,"state":"RUNNING"},{"id":1,"state":"RUNNING"}]}`, ok: true},
		{rung: RungTasks, data: `{"state":"RUNNING","tasks":[{"id":0,"state":"RUNNING"},{"id":1,"state":"FAILED"}]}`, ok: false},
		{rung: RungTasks, data: `{"state":"RUNNING","tasks":[]}`, ok: false},
		{rung: RungTasks, data: `{"state":"RUNNING"}`, ok: false},
		{rung: RungTasks, data: `not json`, ok: false},
		// 3: kafka_topic_offsets Data.
		{rung: RungCanaryTopic, data: `{"topic":"pg.public.orders","partitions":[{"partition":0,"start":0,"end":12}],"total_records":12}`, ok: true},
		{rung: RungCanaryTopic, data: `{"topic":"pg.public.orders","partitions":[],"total_records":0}`, ok: false},
		{rung: RungCanaryTopic, data: `{"partitions":[{"partition":0}]}`, ok: false},
		{rung: RungCanaryTopic, data: `[]`, ok: false},
		// 4: debezium_metrics first, topic offsets as the fallback.
		{rung: RungSnapshot, data: `{"connector":"orders","metrics_found":8,"snapshot":{"SnapshotCompleted":1,"SnapshotRunning":0}}`, ok: true},
		{rung: RungSnapshot, data: `{"connector":"orders","metrics_found":8,"snapshot":{"SnapshotCompleted":0,"SnapshotRunning":1}}`, ok: false},
		{rung: RungSnapshot, data: `{"connector":"orders","metrics_found":8,"snapshot":{"SnapshotCompleted":0,"SnapshotRunning":0}}`, ok: false},
		{rung: RungSnapshot, data: `{"topic":"pg.public.orders","partitions":[{"partition":0}],"total_records":3}`, ok: true},
		{rung: RungSnapshot, data: `{"topic":"pg.public.orders","partitions":[],"total_records":0}`, ok: false},
		{rung: RungSnapshot, data: `{"configured":false}`, ok: false},
		// 5 and 8: kafka_find_by_key Data.
		{rung: RungCanaryEvent, data: `{"topic":"pg.public.orders","key":"ab12","found":true,"records":[{"partition":0,"offset":3}]}`, ok: true},
		{rung: RungCanaryEvent, data: `{"topic":"pg.public.orders","key":"ab12","found":false,"records":[]}`, ok: false},
		{rung: RungCanaryEvent, data: `{"topic":"pg.public.orders","key":"ab12"}`, ok: false},
		{rung: RungPersonChange, data: `{"found":true}`, ok: true},
		{rung: RungPersonChange, data: `{"found":false}`, ok: false},
		{rung: RungPersonChange, data: `not json`, ok: false},
		// 6: schema_registry_subjects Data: a bare list, or the object with
		// include_deleted, or not configured.
		{rung: RungCanarySubject, data: `["pg.public.orders-value"]`, ok: true},
		{rung: RungCanarySubject, data: `[]`, ok: false},
		{rung: RungCanarySubject, data: `{"subjects":["pg.public.orders-value"],"deleted_subjects":[]}`, ok: true},
		{rung: RungCanarySubject, data: `{"subjects":[],"deleted_subjects":[]}`, ok: false},
		{rung: RungCanarySubject, data: `{"configured":false}`, ok: true},
		{rung: RungCanarySubject, data: `{"configured":true}`, ok: false},
		{rung: RungCanarySubject, data: `not json`, ok: false},
		// 7: warehouse_row Data.
		{rung: RungSinkRow, data: `{"found":true,"updated_at":"2026-10-20T10:00:00Z"}`, ok: true},
		{rung: RungSinkRow, data: `{"found":false}`, ok: false},
		{rung: RungSinkRow, data: `{}`, ok: false},
	}
	byName := map[string]Predicate{}
	rungs, err := Ladder("postgres", "compose", "clickhouse")
	if err != nil {
		t.Fatalf("Ladder: %v", err)
	}
	for _, rung := range rungs {
		byName[rung.Name] = rung.Predicate
	}
	for _, tt := range tests {
		predicate, ok := byName[tt.rung]
		if !ok {
			t.Fatalf("no rung %q", tt.rung)
		}
		got, note := predicate(json.RawMessage(tt.data))
		if got != tt.ok {
			t.Errorf("%s(%s) = %v (%q), want %v", tt.rung, tt.data, got, note, tt.ok)
		}
		if note == "" {
			t.Errorf("%s(%s) has no note", tt.rung, tt.data)
		}
	}
}

// TestLadderIgnoresSummary is the done-when clause: a Summary beside the Data
// cannot turn a rung green. A rung's Predicate only ever sees Data; the
// fixture below carries the model-facing summary the tool would write beside
// a FAILED connector.
func TestLadderIgnoresSummary(t *testing.T) {
	data := json.RawMessage(`{"summary":"orders (source): RUNNING, 0 failed tasks","name":"orders","state":"FAILED","tasks":[{"id":0,"state":"FAILED"}]}`)
	rungs, err := Ladder("postgres", "compose", SinkNone)
	if err != nil {
		t.Fatalf("Ladder: %v", err)
	}
	if ok, note := rungs[0].Predicate(data); ok {
		t.Errorf("connector rung green on a FAILED state because of the summary (%q)", note)
	}
	if ok, note := rungs[1].Predicate(data); ok {
		t.Errorf("task rung green on a FAILED task because of the summary (%q)", note)
	}
}

// TestLadderNotesAreSafe: peer state text reaches a note an operator reads, so
// it is clipped and stripped of control characters.
func TestLadderNotesAreSafe(t *testing.T) {
	long := strings.Repeat("X", 100)
	data := json.RawMessage(`{"state":"` + long + "\x1b[31m" + `"}`)
	ok, note := connectorRunning(data)
	if ok {
		t.Fatal("a connector in a state that is not RUNNING is green")
	}
	if strings.ContainsAny(note, "\x1b") {
		t.Errorf("note carries a control character: %q", note)
	}
	if len([]rune(note)) > 32+len("the connector is ")+1 {
		t.Errorf("note is not clipped: %q", note)
	}
}
