package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// unstorableAppendCases is content an event write must refuse. The Postgres
// integration test (internal/store) lists the same cases, so memstore and
// Postgres give the same answer.
func unstorableAppendCases() map[string]any {
	return map[string]any{
		"nul in a string":       events.CommentAddedPayload{CommentID: "c", Text: "a\x00b"},
		"nul in a raw message":  events.AgentToolCallPayload{Input: json.RawMessage(`{"q":"a` + "\\u0000" + `b"}`)},
		"lone high surrogate":   events.AgentToolResultPayload{Data: json.RawMessage(`{"q":"` + "\\ud83d" + `"}`)},
		"lone low surrogate":    events.AgentToolResultPayload{Data: json.RawMessage(`{"q":"` + "\\ude00" + `x"}`)},
		"invalid utf-8 in raw":  events.AgentToolResultPayload{Data: json.RawMessage("{\"q\":\"a\xffb\"}")},
		"nul in a nested value": map[string]any{"a": []any{map[string]any{"b": "x\x00"}}},
	}
}

func TestAppendRefusesUnstorableAndKeepsSeq(t *testing.T) {
	ctx := context.Background()
	s := New()
	roomID := newTestRoom(t, s)
	for name, payload := range unstorableAppendCases() {
		_, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), payload)
		if !errors.Is(err, store.ErrUnstorable) {
			t.Errorf("%s: err = %v, want store.ErrUnstorable", name, err)
		}
	}
	// A bad actor name is refused as well.
	bad := events.Actor{Kind: events.ActorHuman, ID: "u", Name: "U\x00"}
	if _, err := s.Append(ctx, roomID, events.CommentAdded, bad, events.CommentAddedPayload{Text: "x"}); !errors.Is(err, store.ErrUnstorable) {
		t.Errorf("bad actor: err = %v, want store.ErrUnstorable", err)
	}
	r, err := s.GetRoom(ctx, roomID)
	if err != nil || r.LastSeq != 0 {
		t.Fatalf("room = %+v, %v; refused appends must leave last_seq at 0", r, err)
	}
	if evs, _ := s.Since(ctx, roomID, 0, 10); len(evs) != 0 {
		t.Fatalf("refused appends stored %d events", len(evs))
	}

	// The next good append takes seq 1: no gap.
	e, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{Text: "fine"})
	if err != nil || e.Seq != 1 {
		t.Fatalf("append after refusals = seq %d, %v; want seq 1", e.Seq, err)
	}
}

func TestSideTableWritesRefuseUnstorable(t *testing.T) {
	ctx := context.Background()
	s := New()
	roomID := newTestRoom(t, s)
	unstorable := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrUnstorable) {
			t.Errorf("%s: err = %v, want store.ErrUnstorable", what, err)
		}
	}

	unstorable("create room", s.CreateRoom(ctx, store.Room{ID: "r2", Title: "t\x00", Severity: "sev2", PipelineID: "p", Status: "open"}))
	if _, err := s.GetRoom(ctx, "r2"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused create room left a row: %v", err)
	}
	unstorable("update driver", s.UpdateDriver(ctx, roomID, "u", "N\xff", time.Time{}))

	okSteps := json.RawMessage(`[{"action":"a"}]`)
	unstorable("proposal title", s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: roomID, Title: "t\x00", Steps: okSteps}))
	unstorable("proposal risk", s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: roomID, Risk: "r\xff", Steps: okSteps}))
	unstorable("proposal layers", s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: roomID, RequiredLayers: []string{"a\x00"}, Steps: okSteps}))
	unstorable("proposal steps", s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: roomID, Steps: json.RawMessage(`[{"p":"` + "\\u0000" + `"}]`)}))
	if _, err := s.GetProposal(ctx, "p1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused proposal left a row: %v", err)
	}

	if err := s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p2", RoomID: roomID, Title: "ok", Steps: okSteps}); err != nil {
		t.Fatalf("create good proposal: %v", err)
	}
	unstorable("approval reason", s.RecordApproval(ctx, "p2", "postgres", "rejected", "no\x00"))
	if err := s.RecordApproval(ctx, "p2", "postgres", "rejected", "no"); err != nil {
		t.Errorf("a refused approval must not consume the layer: %v", err)
	}
	if err := s.StartProposalStep(ctx, "p2", 0); err != nil {
		t.Fatalf("start step: %v", err)
	}
	unstorable("step output", s.FinishProposalStep(ctx, "p2", 0, "done", "out\xff"))
	if err := s.FinishProposalStep(ctx, "p2", 0, "done", "out"); err != nil {
		t.Errorf("a refused finish must leave the step started: %v", err)
	}

	unstorable("suggestion text", s.CreateSuggestion(ctx, store.SuggestionRow{SuggestionID: "g1", RoomID: roomID, FromID: "u", FromName: "U", Text: "t\x00"}))
	unstorable("suggestion name", s.CreateSuggestion(ctx, store.SuggestionRow{SuggestionID: "g1", RoomID: roomID, FromID: "u", FromName: "U\xff", Text: "t"}))
	if _, err := s.GetSuggestion(ctx, "g1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused suggestion left a row: %v", err)
	}

	unstorable("handoff summary", s.CreateHandoff(ctx, store.HandoffRow{HandoffID: "h1", RoomID: roomID, FromID: "a", ToID: "b", Summary: json.RawMessage(`{"x":"` + "\\ud83d" + `"}`)}))
	unstorable("handoff name", s.CreateHandoff(ctx, store.HandoffRow{HandoffID: "h1", RoomID: roomID, FromID: "a", FromName: "A\x00", ToID: "b"}))
	if _, err := s.GetHandoff(ctx, "h1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused handoff left a row: %v", err)
	}
}
