package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

func verificationTool(t *testing.T, sink Emitter) tools.Tool {
	t.Helper()
	toolsOut, err := RoomTools(RoomDeps{
		Catalogue: fakeCatalogue{"file_write": plan.TierT1},
		Sink:      sink,
		ClassOf:   FixedClass(plan.ClassLocal),
	})
	if err != nil {
		t.Fatalf("RoomTools: %v", err)
	}
	for _, tool := range toolsOut {
		if tool.Name() == "setup_request_verification" {
			return tool
		}
	}
	t.Fatal("setup_request_verification was not built")
	return nil
}

func TestRequestVerificationEmitsOnlyARequest(t *testing.T) {
	sink := &recordingSink{}
	tool := verificationTool(t, sink)
	res, err := tool.Run(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(res.Summary, "wait for it") {
		t.Fatalf("summary = %q, want it to tell the model to wait", res.Summary)
	}
	recorded := sink.recorded()
	if len(recorded) != 1 {
		t.Fatalf("emitted %d events, want exactly one request", len(recorded))
	}
	if recorded[0].typ != TypeVerificationRequested {
		t.Fatalf("type = %q, want %q", recorded[0].typ, TypeVerificationRequested)
	}
	if _, ok := recorded[0].payload.(VerificationRequest); !ok {
		t.Fatalf("payload = %T, want VerificationRequest", recorded[0].payload)
	}
	for _, ev := range recorded {
		if ev.typ == events.SetupVerified {
			t.Fatal("setup_request_verification emitted setup.verified")
		}
	}
}

// refusingSink is the room's Sink as U1-8b must keep it: it refuses
// setup.verified from the agent, so no model output can claim a pipeline
// verified.
type refusingSink struct {
	recordingSink
	attemptedVerified int
}

func (s *refusingSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if typ == events.SetupVerified {
		s.mu.Lock()
		s.attemptedVerified++
		s.mu.Unlock()
		return errors.New("room: the agent sink refuses setup.verified")
	}
	return s.recordingSink.Emit(ctx, typ, payload)
}

func TestRequestVerificationNeverWritesSetupVerified(t *testing.T) {
	sink := &refusingSink{}
	tool := verificationTool(t, sink)
	if _, err := tool.Run(context.Background(), nil); err != nil {
		t.Fatalf("Run through a sink that refuses setup.verified: %v", err)
	}
	recorded := sink.recorded()
	if len(recorded) != 1 || recorded[0].typ != TypeVerificationRequested {
		t.Fatalf("emitted %+v, want one verification request", recorded)
	}
	if sink.attemptedVerified != 0 {
		t.Fatalf("the tool attempted setup.verified %d times", sink.attemptedVerified)
	}
}

func TestRequestVerificationReportsASinkFailure(t *testing.T) {
	sink := &recordingSink{refuseAll: true}
	tool := verificationTool(t, sink)
	_, err := tool.Run(context.Background(), json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "requesting verification") {
		t.Fatalf("Run = %v, want the sink failure", err)
	}
}

func TestRequestVerificationRefusesInput(t *testing.T) {
	sink := &recordingSink{}
	tool := verificationTool(t, sink)
	_, err := tool.Run(context.Background(), json.RawMessage(`{"plan_id":"pl_x"}`))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("Run with params = %v, want ErrBadInput", err)
	}
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("a refused call emitted %d events", got)
	}
}

func TestRequestVerificationTakesNoParameters(t *testing.T) {
	tool := verificationTool(t, &recordingSink{})
	schema := tool.InputSchema()
	if schema["additionalProperties"] != false {
		t.Fatal("the schema is open")
	}
	if req, _ := schema["required"].([]string); len(req) != 0 {
		t.Fatalf("required = %v, want none", req)
	}
}

func TestRoomToolsCannotEnterAReadOnlyRegistry(t *testing.T) {
	sink := &recordingSink{}
	toolsOut, err := RoomTools(RoomDeps{
		Catalogue: fakeCatalogue{"file_write": plan.TierT1},
		Sink:      sink,
		ClassOf:   FixedClass(plan.ClassLocal),
	})
	if err != nil {
		t.Fatalf("RoomTools: %v", err)
	}
	reg := tools.NewReadOnlyRegistry()
	for _, tool := range toolsOut {
		if err := reg.Register(tool); !errors.Is(err, tools.ErrNotReadOnly) {
			t.Fatalf("read-only registry accepted %s: %v", tool.Name(), err)
		}
	}
}
