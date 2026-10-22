package room_test

// KAN-392: --demo's fake clients keep one state per room, and they read the
// room id from the action context. This test pins the room service side of that
// contract: Validate, Preview and Execute all receive the room's id.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// roomIDAction records the room id carried by the context of each call.
type roomIDAction struct {
	mu                      sync.Mutex
	validate, preview, exec string
}

func (a *roomIDAction) Name() string                 { return "postgres_action" }
func (a *roomIDAction) Layer() string                { return "postgres" }
func (a *roomIDAction) Describe() string             { return "records the room id of the action context" }
func (a *roomIDAction) ParamsSchema() map[string]any { return map[string]any{"type": "object"} }

func (a *roomIDAction) Validate(ctx context.Context, _ json.RawMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.validate = room.RoomID(ctx)
	return nil
}

func (a *roomIDAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }

func (a *roomIDAction) Preview(ctx context.Context, _ json.RawMessage) (actions.PreviewResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.preview = room.RoomID(ctx)
	return actions.PreviewResult{Target: "target", Current: "current"}, nil
}

func (a *roomIDAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.exec = room.RoomID(ctx)
	return "done", nil
}

func (a *roomIDAction) rooms() (validate, preview, exec string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.validate, a.preview, a.exec
}

func TestActionContextCarriesRoomID(t *testing.T) {
	act := &roomIDAction{}
	svc := previewSvc(t, act)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "undo",
		RequiredLayers: []string{"postgres"},
		Steps: []events.ProposalStep{{
			Action: "postgres_action", Layer: "postgres", Params: json.RawMessage(`{}`),
		}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed)) > 0
	})

	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)) > 0
	})

	validate, preview, exec := act.rooms()
	if validate != roomID || preview != roomID || exec != roomID {
		t.Fatalf("action context carried room ids validate=%q preview=%q execute=%q, want %q in all three",
			validate, preview, exec, roomID)
	}
}
