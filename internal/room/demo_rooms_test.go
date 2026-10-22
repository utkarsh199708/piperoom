package room_test

// KAN-392 end to end, as the ticket describes it: one actions registry built
// once over the demo's in-memory clients (as cmd/server --demo does), the
// scripted fix executed in room A, then room B opened in the same process.
// Room B's proposal.previewed must still report the story (BACKWARD / FAILED).

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/room/demo"
	"piperoom/internal/store/memstore"
)

// demoOwners is the orders pipeline as dev/owners.yaml defines it, so the real
// actions pass their scope checks against it.
func demoOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID:           "orders",
		Title:        "Orders CDC",
		TopicPrefix:  "pg",
		SourceTables: []string{"public.orders"},
		Connectors:   []string{"pg-orders-connector"},
		SinkTables:   []string{"analytics.orders"},
	}}}
}

func demoActionsService(t *testing.T) (*room.Service, *memstore.Store) {
	t.Helper()
	acts := actions.NewRegistry()
	if err := actions.Register(acts, demo.FakeActionDeps()); err != nil {
		t.Fatalf("register demo actions: %v", err)
	}
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Actions: acts, Users: room.DefaultUsers(), Owners: demoOwners(),
		Topology: demo.Builder{}, Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now,
	})
	t.Cleanup(svc.Close)
	return svc, st
}

func demoFixSteps() []events.ProposalStep {
	return []events.ProposalStep{
		{
			Action: "schema_set_compatibility", Layer: "schema-registry",
			Params: json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`),
		},
		{
			Action: "connect_restart_task", Layer: "kafka-connect",
			Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`),
		},
	}
}

func proposeDemoFix(t *testing.T, svc *room.Service, roomID string) string {
	t.Helper()
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title:          "Set subject compatibility to NONE and restart the connector task",
		Steps:          demoFixSteps(),
		Risk:           "low",
		Rollback:       "set compatibility back to BACKWARD",
		RequiredLayers: []string{"schema-registry", "kafka-connect"},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create demo proposal: %v", err)
	}
	return pid
}

func waitDemoPreview(t *testing.T, svc *room.Service, roomID, proposalID string) events.ProposalPreviewedPayload {
	t.Helper()
	var got events.ProposalPreviewedPayload
	waitFor(t, 5*time.Second, func() bool {
		for _, p := range eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed) {
			if p.ProposalID == proposalID {
				got = p
				return true
			}
		}
		return false
	})
	return got
}

// wantStoryPreview asserts both steps preview the scripted story: compatibility
// BACKWARD and connector task 0 FAILED, as demo.Run's summaries say.
func wantStoryPreview(t *testing.T, label string, pv events.ProposalPreviewedPayload) {
	t.Helper()
	if len(pv.Steps) != 2 {
		t.Fatalf("%s: preview has %d steps, want 2", label, len(pv.Steps))
	}
	schema := pv.Steps[0].Preview
	if schema == nil {
		t.Fatalf("%s: schema step has no preview: %+v", label, pv.Steps[0])
	}
	if want := "BACKWARD (GET /config/pg.public.orders-value)"; schema.Current != want {
		t.Fatalf("%s: schema preview current = %q, want %q", label, schema.Current, want)
	}
	task := pv.Steps[1].Preview
	if task == nil {
		t.Fatalf("%s: connector step has no preview: %+v", label, pv.Steps[1])
	}
	if !strings.Contains(task.Current, "FAILED") {
		t.Fatalf("%s: connector preview current = %q, want FAILED", label, task.Current)
	}
}

// executeDemoFix approves both layers and waits for the executor to finish.
func executeDemoFix(t *testing.T, svc *room.Service, st *memstore.Store, roomID, proposalID string) {
	t.Helper()
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), proposalID, "schema-registry", proposalHash(t, svc, proposalID)); err != nil {
		t.Fatalf("approve schema-registry: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "arjun"), proposalID, "kafka-connect", proposalHash(t, svc, proposalID)); err != nil {
		t.Fatalf("approve kafka-connect: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), proposalID)
		return err == nil && p.Status == "executed"
	})
}

func TestDemoRoomsPreviewTheirOwnStoryAfterFix(t *testing.T) {
	svc, st := demoActionsService(t)

	// Room A replays the story, then runs the scripted fix.
	roomA := createRoom(t, svc, "arjun").ID
	pidA := proposeDemoFix(t, svc, roomA)
	wantStoryPreview(t, "room A first preview", waitDemoPreview(t, svc, roomA, pidA))
	executeDemoFix(t, svc, st, roomA, pidA)

	// Room B is opened after A's fix in the same process: its preview must be
	// the original story, not A's post-fix state.
	roomB := createRoom(t, svc, "arjun").ID
	pidB := proposeDemoFix(t, svc, roomB)
	wantStoryPreview(t, "room B preview", waitDemoPreview(t, svc, roomB, pidB))

	// B's fix is isolated too: a room opened afterwards still gets the story.
	executeDemoFix(t, svc, st, roomB, pidB)
	roomC := createRoom(t, svc, "arjun").ID
	pidC := proposeDemoFix(t, svc, roomC)
	wantStoryPreview(t, "room C preview", waitDemoPreview(t, svc, roomC, pidC))
}
