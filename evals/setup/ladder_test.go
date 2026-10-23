package setup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
