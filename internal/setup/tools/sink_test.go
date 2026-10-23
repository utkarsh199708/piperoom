package setuptools

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// The room's agent Sink refuses setup.verified (design §3.4): only the
// server's ladder runner writes it, so neither setup_request_verification nor
// any other agent code can claim a pipeline verified. The Sink is the one the
// room service hands a real agent run.
func TestRoomAgentSinkRefusesSetupVerified(t *testing.T) {
	sinks := make(chan agent.Sink, 1)
	run := func(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
		sinks <- deps.Sink
		<-ctx.Done()
		return nil
	}
	svc := room.New(room.Deps{Store: memstore.New(), Users: room.DefaultUsers(), Run: run})
	t.Cleanup(svc.Close)
	user, ok := svc.LookupUser("arjun")
	if !ok {
		t.Fatal("no user arjun")
	}
	if _, err := svc.CreateRoom(context.Background(), user, "setup sink", "sev3", "orders"); err != nil {
		t.Fatal(err)
	}
	var sink agent.Sink
	select {
	case sink = <-sinks:
	case <-time.After(5 * time.Second):
		t.Fatal("the agent run never started")
	}
	ctx := context.Background()
	err := sink.Emit(ctx, events.SetupVerified, events.SetupVerifiedPayload{
		PlanID: "plan_0123456789abcdef", NonceHash: "x", Rungs: []events.SetupRung{{Name: "connector_running", OK: true}},
	})
	if err == nil {
		t.Fatal("the agent sink appended setup.verified")
	}
	// The same sink still takes the agent's own events.
	if err := sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "waiting for the ladder"}); err != nil {
		t.Fatalf("agent.message: %v", err)
	}
}
