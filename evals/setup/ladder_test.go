package setup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/events"
)

func TestRecordingReaderServesLadderReads(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	rec := recs["setup/flat-plugin-dir"]
	r := NewRecordingReader(rec)
	for _, read := range rec.Ladder.Reads {
		data, err := r.Read(context.Background(), ReadRequest{Tool: read.Tool})
		if err != nil {
			t.Fatalf("%s: %v", read.Tool, err)
		}
		if string(data) == "" {
			t.Fatalf("%s: empty data", read.Tool)
		}
	}
	if _, err := r.Read(context.Background(), ReadRequest{Tool: "connect_status"}); !errors.Is(err, ErrNoRecording) {
		t.Fatalf("err = %v, want ErrNoRecording after the reads are used", err)
	}
}

func TestFakeLadderEmitsRequestsAndItsOwnVerified(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	rec := recs["setup/missing-grant"]
	sink := &MemorySink{}
	ladder := NewFakeLadder(LadderConfig{
		Source:     rec.SourceKind(),
		Target:     rec.Target,
		Sink:       rec.SinkKind(),
		PlanID:     rec.Ladder.PlanID,
		NonceHash:  rec.Ladder.NonceHash,
		Table:      rec.Ladder.Table,
		ExampleSQL: rec.Ladder.ExampleSQL,
	}, NewRecordingReader(rec), sink)
	payload, err := ladder.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// The ladder issued the rungs' reads as requests, in ladder order; it did
	// not start from the recorded verification.
	var tools []string
	for _, req := range ladder.Requests() {
		tools = append(tools, req.Tool)
	}
	want := []string{"connect_status", "connect_status", "kafka_topic_offsets", "debezium_metrics", "kafka_find_by_key", "schema_registry_subjects"}
	if strings.Join(tools, ",") != strings.Join(want, ",") {
		t.Errorf("requests = %v, want %v", tools, want)
	}

	// Every rung 1-7 is green, rung 8 is the person's, and every note matches
	// the recorded verification the fake recomputed.
	recorded := rec.Ladder.Verified.Rungs
	if len(payload.Rungs) != len(recorded) {
		t.Fatalf("rungs = %d, want %d", len(payload.Rungs), len(recorded))
	}
	for i, got := range payload.Rungs {
		if got.Name != recorded[i].Name || got.OK != recorded[i].OK || got.Note != recorded[i].Note {
			t.Errorf("rung %d = %+v, want %+v", i, got, recorded[i])
		}
	}

	// The same reads served twice are a miss: the session ran once, not once
	// per verification request. The second ladder reports every rung red with
	// the read error; it never reuses the first run's answers.
	second, err := ladder.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, rung := range second.Rungs {
		if i == 6 || i == 7 {
			// sink_row is not applicable (no sink) and rung 8 is the
			// person's; neither needs a read.
			continue
		}
		if rung.OK {
			t.Errorf("second verify: rung %s is green with no read left", rung.Name)
		}
	}

	// The sink got the awaiting-change request and the verification, both
	// authored by the server, never by the agent.
	evs := sink.Events()
	if len(evs) != 4 {
		t.Fatalf("events = %d, want two per verification run", len(evs))
	}
	if evs[0].Type != events.SetupAwaitingChange || evs[1].Type != events.SetupVerified ||
		evs[2].Type != events.SetupAwaitingChange || evs[3].Type != events.SetupVerified {
		t.Errorf("event types = %s, %s, %s, %s", evs[0].Type, evs[1].Type, evs[2].Type, evs[3].Type)
	}
	for _, ev := range evs {
		if ev.Actor.Kind != events.ActorSystem {
			t.Errorf("%s actor = %+v, want system", ev.Type, ev.Actor)
		}
	}
	change, ok := evs[0].Payload.(events.SetupAwaitingChangePayload)
	if !ok || change.Table != rec.Ladder.Table || change.ExampleSQL != rec.Ladder.ExampleSQL {
		t.Errorf("awaiting change = %+v", evs[0].Payload)
	}
}

func TestFakeLadderAwaitingVerificationReadsNotVerifications(t *testing.T) {
	// An agent that writes setup.verified itself is refused, and the sink
	// sees no verification from it: only the ladder runner writes one.
	sink := &MemorySink{}
	agent := AgentSink(sink)
	err := agent.Emit(context.Background(), Event{Type: events.SetupVerified, Payload: events.SetupVerifiedPayload{PlanID: "pl_1"}})
	if !errors.Is(err, ErrAgentVerified) {
		t.Fatalf("err = %v, want ErrAgentVerified", err)
	}
	if agent.Refused() != 1 || agent.Forwarded() != 0 || len(sink.Events()) != 0 {
		t.Fatalf("refused=%d forwarded=%d events=%d, want 1, 0, 0", agent.Refused(), agent.Forwarded(), len(sink.Events()))
	}
	if err := agent.Emit(context.Background(), Event{Type: events.SetupPlan}); err != nil {
		t.Fatal(err)
	}
	if len(sink.Events()) != 1 || sink.Events()[0].Actor.Kind != events.ActorAgent {
		t.Fatalf("events = %+v, want one agent-authored setup.plan", sink.Events())
	}
}

func TestFakeLadderFailsRungWithoutRead(t *testing.T) {
	// A ladder whose reader has no answer reports the rung red with the read
	// error, and still writes one verification: no rung can pass on prose.
	sink := &MemorySink{}
	ladder := NewFakeLadder(LadderConfig{Source: "postgres", Target: "compose", Sink: SinkNone, PlanID: "pl_1"},
		ReadFunc(func(context.Context, ReadRequest) (json.RawMessage, error) {
			return nil, errors.New("no data")
		}), sink)
	payload, err := ladder.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, rung := range payload.Rungs[:6] {
		if rung.OK {
			t.Errorf("rung %s is green without a read", rung.Name)
		}
	}
	if payload.Rungs[0].Note == "" {
		t.Error("a red rung must say why")
	}
	if len(sink.Events()) != 2 {
		t.Fatalf("events = %d, want awaiting_change and verified", len(sink.Events()))
	}
}

func TestFakeLadderRejectsUnknownTarget(t *testing.T) {
	ladder := NewFakeLadder(LadderConfig{Source: "postgres", Target: "laptop", PlanID: "pl_1"}, NewRecordingReader(nil), &MemorySink{})
	if _, err := ladder.Verify(context.Background()); err == nil || !strings.Contains(err.Error(), "no verification ladder") {
		t.Fatalf("err = %v, want plan.ErrLadder", err)
	}
}

// TestFakeLadderReadsTheManagedStatusOps: on a managed target (U13e) rungs 1
// and 2 read the provider's status op, and MSK's rung 2 is not applicable.
func TestFakeLadderReadsTheManagedStatusOps(t *testing.T) {
	for target, read := range map[string]string{"msk": "msk_describe_connector", "confluent": "ccloud_connector_status", "aiven": "aiven_connector_status"} {
		reader := ReadFunc(func(_ context.Context, req ReadRequest) (json.RawMessage, error) {
			return json.RawMessage(`{"state":"RUNNING","tasks":[{"id":0,"state":"RUNNING"}]}`), nil
		})
		ladder := NewFakeLadder(LadderConfig{Source: "postgres", Target: target, PlanID: "pl_1"}, reader, &MemorySink{})
		v, err := ladder.Verify(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		reqs := ladder.Requests()
		if len(reqs) == 0 || reqs[0].Tool != read {
			t.Errorf("%s: first read %+v, want %s", target, reqs, read)
		}
		if !v.Rungs[0].OK || !v.Rungs[1].OK {
			t.Errorf("%s: rungs 1 and 2 = %+v %+v", target, v.Rungs[0], v.Rungs[1])
		}
		if target == "msk" && !strings.Contains(v.Rungs[1].Note, "no per-task state") {
			t.Errorf("msk rung 2 note %q", v.Rungs[1].Note)
		}
	}
}

// TestFakeLadderReadsTheStrimziConnectorCR: on a Strimzi target rungs 1 and 2
// read the KafkaConnector CR with k8s_get (plan.StrimziLadder, the room's
// choice), never Connect's REST status; the two recordings replay through
// those predicates, and a CR whose task failed fails rung 2 with its trace.
func TestFakeLadderReadsTheStrimziConnectorCR(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"setup/strimzi-connector-not-ready", "setup/strimzi-build-failed"} {
		rec := recs[name]
		ladder := NewFakeLadder(LadderConfig{Source: rec.SourceKind(), Target: rec.Target, Sink: rec.SinkKind(), PlanID: rec.Ladder.PlanID}, NewRecordingReader(rec), &MemorySink{})
		v, err := ladder.Verify(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		reqs := ladder.Requests()
		if len(reqs) < 2 || reqs[0].Tool != "k8s_get" || reqs[1].Tool != "k8s_get" {
			t.Fatalf("%s: rungs 1 and 2 read %+v, want k8s_get twice", name, reqs)
		}
		if !v.Rungs[0].OK || v.Rungs[0].Note != "the KafkaConnector is Ready and the connector is RUNNING" {
			t.Errorf("%s: rung 1 = %+v, want strimziConnectorRunning's verdict", name, v.Rungs[0])
		}
		if !v.Rungs[1].OK || v.Rungs[1].Note != "all 1 tasks are RUNNING" {
			t.Errorf("%s: rung 2 = %+v, want strimziTasksRunning's verdict", name, v.Rungs[1])
		}
	}
	failed := ReadFunc(func(_ context.Context, req ReadRequest) (json.RawMessage, error) {
		if req.Tool != "k8s_get" {
			return nil, errors.New("not recorded")
		}
		return json.RawMessage(`{"kind":"KafkaConnector","status":{"conditions":[{"type":"Ready","status":"True"}],
			"connectorStatus":{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"FAILED","trace":"org.postgresql.util.PSQLException: FATAL: no pg_hba.conf entry\n\tat x"}]}}}`), nil
	})
	ladder := NewFakeLadder(LadderConfig{Source: "postgres", Target: "strimzi", PlanID: "pl_1"}, failed, &MemorySink{})
	v, err := ladder.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !v.Rungs[0].OK || v.Rungs[1].OK || !strings.Contains(v.Rungs[1].Note, "task 0 is FAILED") || !strings.Contains(v.Rungs[1].Note, "pg_hba.conf") {
		t.Fatalf("a failed task: rungs %+v %+v", v.Rungs[0], v.Rungs[1])
	}
}

// TestFakeLadderStopsOnCancel: a context cancelled between rungs stops the
// ladder; it returns the context's error and writes no setup.verified.
func TestFakeLadderStopsOnCancel(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	rec := recs["setup/missing-grant"]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner := NewRecordingReader(rec)
	reads := 0
	r := ReadFunc(func(ctx context.Context, req ReadRequest) (json.RawMessage, error) {
		reads++
		if reads == 1 {
			cancel()
		}
		return inner.Read(context.Background(), req)
	})
	sink := &MemorySink{}
	ladder := NewFakeLadder(LadderConfig{
		Source: rec.SourceKind(), Target: rec.Target, Sink: rec.SinkKind(), PlanID: rec.Ladder.PlanID,
	}, r, sink)
	if _, err := ladder.Verify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	for _, ev := range sink.Events() {
		if ev.Type == events.SetupVerified {
			t.Fatalf("a cancelled ladder wrote setup.verified: %+v", ev)
		}
	}
	if reads != 1 {
		t.Errorf("reads = %d, want 1: no rung after the cancel", reads)
	}
}

// TestLadderSinksAreSafeForConcurrentUse is for -race: the memory sink and
// the agent's counters take concurrent writers.
func TestLadderSinksAreSafeForConcurrentUse(t *testing.T) {
	mem := &MemorySink{}
	agent := AgentSink(mem)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = agent.Emit(context.Background(), Event{Type: events.SetupAwaitingChange})
			_ = agent.Emit(context.Background(), Event{Type: events.SetupVerified})
			_ = mem.Events()
		}()
	}
	wg.Wait()
	if agent.Forwarded() != 8 || agent.Refused() != 8 || len(mem.Events()) != 8 {
		t.Errorf("forwarded %d refused %d events %d, want 8 each", agent.Forwarded(), agent.Refused(), len(mem.Events()))
	}
}
