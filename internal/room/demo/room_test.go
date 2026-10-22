package demo

// Room-level tests for the scripted fix ceremony: the real room service, the
// real write actions over the demo fakes, and the scripted agent. They drive
// the demo through proposal, approvals and execution, the way --demo --solo
// does, then check the log ends with a verdict instead of "verifying" forever
// (KAN-389).

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// roomTestDelay keeps the scripted story quick; only the pauses are shortened,
// the event order is the presenter's.
const roomTestDelay = time.Millisecond

func newDemoRoom(t *testing.T) (*room.Service, string) {
	t.Helper()
	reg := actions.NewRegistry()
	if err := actions.Register(reg, FakeActionDeps()); err != nil {
		t.Fatalf("register demo actions: %v", err)
	}
	// The orders pipeline and the two layers the proposal writes, as
	// dev/owners.yaml defines them.
	own := &owners.Owners{
		Pipelines: []owners.Pipeline{{
			ID:           "orders",
			Title:        "Orders CDC",
			TopicPrefix:  "pg",
			SourceTables: []string{"public.orders"},
			Connectors:   []string{connectorName},
			SinkTables:   []string{"analytics.orders"},
			SinkTSColumn: "updated_at",
		}},
		Layers: map[string]owners.Principals{
			"schema-registry": {{Kind: owners.KindUser, Name: "priya"}},
			"kafka-connect":   {{Kind: owners.KindUser, Name: "arjun"}},
		},
	}
	svc := room.New(room.Deps{
		Store:    memstore.New(),
		Actions:  reg,
		Topology: Builder{},
		Users:    room.DefaultUsers(),
		Owners:   own,
		Run: func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
			return runStory(ctx, a, d, o, time.Now(), roomTestDelay)
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "priya"), "orders lag", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	return svc, snap.ID
}

func mustUser(t *testing.T, svc *room.Service, id string) room.User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("user %q not found", id)
	}
	return u
}

// roomLogLimit is the explicit page size roomLog asks for; limit 0 is an
// empty page since KAN-395, so relying on a default here would silently read
// nothing. 500 matches the HTTP layer's default page (docs/api.md).
const roomLogLimit = 500

// roomLog returns every event the room has so far. The log is never empty:
// CreateRoom writes room.created before it returns, so an empty page means
// the read itself regressed (e.g. an accidental limit-0 call).
func roomLog(t *testing.T, svc *room.Service, roomID string) []events.Event {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, roomLogLimit)
	if err != nil {
		t.Fatalf("read room log: %v", err)
	}
	if len(evs) == 0 {
		t.Fatalf("room log of %s is empty; it must hold at least room.created", roomID)
	}
	return evs
}

// waitForLog polls the room log until pred holds or timeout ends the test.
func waitForLog(t *testing.T, svc *room.Service, roomID string, timeout time.Duration, what string, pred func([]events.Event) bool) []events.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		evs := roomLog(t, svc, roomID)
		if pred(evs) {
			return evs
		}
		if time.Now().After(deadline) {
			types := make([]string, 0, len(evs))
			for _, e := range evs {
				types = append(types, string(e.Type))
			}
			t.Fatalf("timed out waiting for %s; log: %v", what, types)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// awaitProposal waits for the scripted agent to propose its fix.
func awaitProposal(t *testing.T, svc *room.Service, roomID string) events.ProposalCreatedPayload {
	t.Helper()
	evs := waitForLog(t, svc, roomID, 10*time.Second, "proposal.created", func(evs []events.Event) bool {
		for _, e := range evs {
			if e.Type == events.ProposalCreated {
				return true
			}
		}
		return false
	})
	for _, e := range evs {
		if e.Type != events.ProposalCreated {
			continue
		}
		p, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			t.Fatalf("decode proposal.created: %v", err)
		}
		if p.ProposalID == "" || p.ParamsHash == "" {
			t.Fatalf("proposal = %+v, want an id and a params hash", p)
		}
		return p
	}
	t.Fatal("proposal.created vanished from the log")
	return events.ProposalCreatedPayload{}
}

// TestDemoRoomVerifiesAfterBothStepsRun is TC1/TC5: approve and run both steps
// of the scripted proposal; exactly one recovered agent.verification follows
// the last action.executed, and the post-mortem records it.
func TestDemoRoomVerifiesAfterBothStepsRun(t *testing.T) {
	svc, roomID := newDemoRoom(t)
	ctx := context.Background()
	p := awaitProposal(t, svc, roomID)

	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), p.ProposalID, "schema-registry", p.ParamsHash); err != nil {
		t.Fatalf("approve schema-registry: %v", err)
	}
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "arjun"), p.ProposalID, "kafka-connect", p.ParamsHash); err != nil {
		t.Fatalf("approve kafka-connect: %v", err)
	}

	evs := waitForLog(t, svc, roomID, 10*time.Second, "both action.executed", func(evs []events.Event) bool {
		n := 0
		for _, e := range evs {
			if e.Type == events.ActionExecuted {
				n++
			}
		}
		return n == 2
	})
	var lastExecuted events.Event
	for _, e := range evs {
		if e.Type != events.ActionExecuted {
			continue
		}
		ap, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			t.Fatalf("decode action.executed: %v", err)
		}
		if !ap.OK || ap.ProposalID != p.ProposalID {
			t.Fatalf("action.executed = %+v, want an OK step of %s", ap, p.ProposalID)
		}
		lastExecuted = e
	}

	// TC1: the verdict must land within 5 s of the second executed step, not
	// hang the room at "verifying".
	evs = waitForLog(t, svc, roomID, 5*time.Second, "agent.verification after the last step", func(evs []events.Event) bool {
		for _, e := range evs {
			if e.Type == events.AgentVerification {
				return true
			}
		}
		return false
	})
	var verdict events.Event
	n := 0
	for _, e := range evs {
		if e.Type != events.AgentVerification {
			continue
		}
		n++
		verdict = e
	}
	if n != 1 {
		t.Fatalf("%d agent.verification events, want exactly 1", n)
	}
	if verdict.Seq <= lastExecuted.Seq {
		t.Fatalf("verification seq %d is not after the last action.executed seq %d", verdict.Seq, lastExecuted.Seq)
	}
	vp, err := events.Decode[events.AgentVerificationPayload](verdict)
	if err != nil {
		t.Fatalf("decode agent.verification: %v", err)
	}
	if vp.ProposalID != p.ProposalID || !vp.Recovered || !strings.HasPrefix(vp.Verdict, "recovered:") {
		t.Fatalf("verification = %+v, want a recovered verdict of %s", vp, p.ProposalID)
	}
	wantLayers := map[string]bool{"schema-registry": true, "kafka-connect": true}
	for _, c := range vp.Checks {
		if !c.OK || c.Name == "" || c.Detail == "" {
			t.Fatalf("check = %+v, want a passing check with a name and detail", c)
		}
		delete(wantLayers, c.Layer)
	}
	if len(wantLayers) != 0 {
		t.Fatalf("missing layer checks: %v (checks %+v)", wantLayers, vp.Checks)
	}
	if vp.DataFlow == nil || !vp.DataFlow.Checked || !vp.DataFlow.Fresh {
		t.Fatalf("data flow = %+v, want checked and fresh", vp.DataFlow)
	}

	// TC5: the closed-room post-mortem has a real verification section.
	md, err := svc.RenderPostmortem(ctx, roomID)
	if err != nil {
		t.Fatalf("render post-mortem: %v", err)
	}
	if strings.Contains(string(md), "No verification recorded") {
		t.Fatalf("post-mortem says \"No verification recorded\":\n%s", md)
	}
	if !strings.Contains(string(md), "recovered") {
		t.Fatalf("post-mortem does not mention the recovery:\n%s", md)
	}

	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if snap.Status != "open" {
		t.Fatalf("room status = %q, want open (the agent waits for the hand-off)", snap.Status)
	}
}

// TestDemoRoomRejectedFixWritesNoVerification is TC3: a rejection never runs a
// step, so no verdict may be invented.
func TestDemoRoomRejectedFixWritesNoVerification(t *testing.T) {
	svc, roomID := newDemoRoom(t)
	p := awaitProposal(t, svc, roomID)

	if _, err := svc.RejectProposal(context.Background(), mustUser(t, svc, "priya"), p.ProposalID, "schema-registry", "not during the freeze", p.ParamsHash); err != nil {
		t.Fatalf("reject proposal: %v", err)
	}
	// The scripted run answers the rejection; give it a window in which a
	// verification would show up if one were wrongly written.
	time.Sleep(200 * time.Millisecond)
	for _, e := range roomLog(t, svc, roomID) {
		switch e.Type {
		case events.AgentVerification:
			t.Fatalf("agent.verification written for a rejected fix: %+v", e)
		case events.ActionExecuted:
			t.Fatalf("action.executed written for a rejected fix: %+v", e)
		}
	}
}
