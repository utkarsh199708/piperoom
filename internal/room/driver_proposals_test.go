package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// validateAction is an action whose Validate fails with a fixed error, for the
// driver-proposal param test.
type validateAction struct {
	name, layer string
	err         error
}

func (a *validateAction) Name() string     { return a.name }
func (a *validateAction) Layer() string    { return a.layer }
func (a *validateAction) Describe() string { return a.name }
func (a *validateAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (a *validateAction) Validate(context.Context, json.RawMessage) error { return a.err }
func (a *validateAction) CheckScope(owners.Pipeline, json.RawMessage) error {
	return nil
}
func (a *validateAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "", nil
}

// driverProposal builds the POST /api/rooms/{id}/proposals body as a payload.
func driverProposal(title string) events.ProposalCreatedPayload {
	return events.ProposalCreatedPayload{
		Title: title, Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}
}

// TestDriverProposalNonDriverForbidden pins the driver-only rule: a member who
// is not the driver gets 403 not_driver and nothing is stored.
func TestDriverProposalNonDriverForbidden(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, st, _ := newTestService(t, pg)
	roomID := createRoom(t, svc, "arjun").ID
	before := len(allEvents(t, st, roomID))

	_, err := svc.CreateDriverProposal(context.Background(), mustUser(t, svc, "priya"), roomID, driverProposal("fix"))
	wantCode(t, err, "not_driver")
	var re *room.Error
	if !errors.As(err, &re) || re.Status != http.StatusForbidden {
		t.Fatalf("non-driver proposal: %v, want 403", err)
	}
	if got := len(allEvents(t, st, roomID)); got != before {
		t.Fatalf("a refused proposal appended %d events", got-before)
	}
}

// TestDriverProposalSixthOpenProposalRateLimited pins the cap: at most 5 open
// driver-authored proposals per room; the sixth answers 429 too_many_proposals.
func TestDriverProposalSixthOpenProposalRateLimited(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")

	for i := 0; i < 5; i++ {
		if _, err := svc.CreateDriverProposal(context.Background(), arjun, roomID, driverProposal("fix")); err != nil {
			t.Fatalf("driver proposal %d: %v", i, err)
		}
	}
	_, err := svc.CreateDriverProposal(context.Background(), arjun, roomID, driverProposal("fix"))
	wantCode(t, err, "too_many_proposals")
	var re *room.Error
	if !errors.As(err, &re) || re.Status != http.StatusTooManyRequests {
		t.Fatalf("sixth proposal: %v, want 429", err)
	}
}

// TestDriverProposalBadParamsValidateError pins that a bad param answers 400
// validation naming the action's Validate error, exactly like an agent
// proposal.
func TestDriverProposalBadParamsValidateError(t *testing.T) {
	act := &validateAction{name: "pg_act", layer: "postgres", err: errors.New("missing table")}
	svc, _, _ := newTestService(t, act)
	roomID := createRoom(t, svc, "arjun").ID

	_, err := svc.CreateDriverProposal(context.Background(), mustUser(t, svc, "arjun"), roomID, driverProposal("fix"))
	wantCode(t, err, "validation")
	if err == nil || !strings.Contains(err.Error(), "missing table") {
		t.Fatalf("want the Validate error text, got %v", err)
	}
}

// TestDriverProposalWriteModeOffCreatedAndBlocked pins that write mode off does
// not stop the driver from authoring a proposal: it is created with
// origin "driver", and once fully approved it stays open and blocked
// (write_mode_off), never executed.
func TestDriverProposalWriteModeOffCreatedAndBlocked(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _ := newWriteModeService(t, nil, true, "", pg)
	roomID := createRoom(t, svc, "arjun").ID

	ev, err := svc.CreateDriverProposal(context.Background(), mustUser(t, svc, "arjun"), roomID, driverProposal("fix"))
	if err != nil {
		t.Fatalf("create driver proposal: %v", err)
	}
	if ev.Type != events.ProposalCreated {
		t.Fatalf("want proposal.created, got %s", ev.Type)
	}
	p, err := events.Decode[events.ProposalCreatedPayload](ev)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Origin != events.ProposalOriginDriver {
		t.Fatalf("origin = %q, want %q", p.Origin, events.ProposalOriginDriver)
	}
	if p.ParamsHash == "" {
		t.Fatal("driver proposal has no params_hash")
	}

	approveLayer(t, svc, "priya", p.ProposalID, "postgres")
	_, snap := viewProposal(t, svc, "priya", roomID, p.ProposalID)
	if snap.Blocked != room.BlockedWriteModeOff {
		t.Fatalf("blocked = %q, want %q", snap.Blocked, room.BlockedWriteModeOff)
	}
	if pg.Calls() != 0 {
		t.Fatalf("action ran with write mode off: %d calls", pg.Calls())
	}
}

// TestDriverProposalOriginOnSnapshot pins that open_proposals[].origin is
// "driver" for the driver's proposal and "agent" for the agent's, and that an
// agent proposal does not count towards the driver cap.
func TestDriverProposalOriginOnSnapshot(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	roomID := createRoom(t, svc, "arjun").ID
	ctx := context.Background()

	// Five agent proposals never block the driver's own (the cap counts driver
	// proposals only), and each agent proposal is origin "agent".
	for i := 0; i < 5; i++ {
		if _, err := svc.CreateProposal(ctx, roomID, driverProposal("agent fix"), events.AgentActor); err != nil {
			t.Fatalf("agent proposal %d: %v", i, err)
		}
	}
	ev, err := svc.CreateDriverProposal(ctx, mustUser(t, svc, "arjun"), roomID, driverProposal("driver fix"))
	if err != nil {
		t.Fatalf("driver proposal after five agent ones: %v", err)
	}
	dp, _ := events.Decode[events.ProposalCreatedPayload](ev)

	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if len(snap.OpenProposals) != 6 {
		t.Fatalf("want 6 open proposals, got %d", len(snap.OpenProposals))
	}
	drivers := 0
	for _, op := range snap.OpenProposals {
		if op.ProposalID == dp.ProposalID {
			if op.Origin != events.ProposalOriginDriver {
				t.Fatalf("driver proposal origin = %q, want %q", op.Origin, events.ProposalOriginDriver)
			}
			drivers++
			continue
		}
		if op.Origin != events.ProposalOriginAgent {
			t.Fatalf("agent proposal origin = %q, want %q", op.Origin, events.ProposalOriginAgent)
		}
	}
	if drivers != 1 {
		t.Fatalf("want exactly one driver proposal, got %d", drivers)
	}
}

// TestDriverProposalShadowRoomRefused pins that a shadow room has no proposals
// (docs/api.md): the driver's authoring is a 400 validation.
func TestDriverProposalShadowRoomRefused(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	reg := actions.NewRegistry()
	if err := reg.Register(pg); err != nil {
		t.Fatalf("register: %v", err)
	}
	enabled := true
	own := &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", SourceTables: []string{"public.orders"},
		ShadowSpec: &owners.ShadowSpec{Enabled: &enabled},
	}}}
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Owners: own, Run: fakeRun, Now: c.Now,
	})
	t.Cleanup(svc.Close)

	snap, _, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", json.RawMessage(`{"title":"lag"}`))
	if err != nil {
		t.Fatalf("open shadow room: %v", err)
	}
	if !snap.Shadow {
		t.Fatalf("room is not shadow: %+v", snap)
	}
	// A shadow room opens driverless; take control first so the driver check
	// passes and the shadow refusal is what the call returns.
	if _, err := svc.GrantControl(context.Background(), mustUser(t, svc, "arjun"), snap.ID, "arjun"); err != nil {
		t.Fatalf("take control of the shadow room: %v", err)
	}
	_, err = svc.CreateDriverProposal(context.Background(), mustUser(t, svc, "arjun"), snap.ID, driverProposal("fix"))
	wantCode(t, err, "validation")
	if err == nil || !strings.Contains(err.Error(), "shadow") {
		t.Fatalf("want a shadow-room error, got %v", err)
	}
}
