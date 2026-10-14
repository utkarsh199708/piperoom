//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/testenv"
)

// Real Postgres and memstore give the same answer for content jsonb or a text
// column cannot hold: store.ErrUnstorable from the raw store call, before any
// row is written and before a seq is reserved. Run against a throwaway Postgres
// (PIPEROOM_DB_URL); the test skips when it is unreachable.
func TestUnstorableContentRefusedByPostgresAndMemstore(t *testing.T) {
	pg, _ := requireStore(t)
	for name, st := range map[string]store.Store{"postgres": pg, "memstore": memstore.New()} {
		t.Run(name, func(t *testing.T) { checkUnstorable(t, st) })
	}
}

func checkUnstorable(t *testing.T, s store.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	roomID := testenv.Name("room")
	if err := s.CreateRoom(ctx, store.Room{ID: roomID, Title: "t", Severity: "sev2", PipelineID: "p", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("create room: %v", err)
	}
	actor := events.Actor{Kind: events.ActorHuman, ID: "u", Name: "U"}
	lastSeq := func() int64 {
		t.Helper()
		r, err := s.GetRoom(ctx, roomID)
		if err != nil {
			t.Fatalf("get room: %v", err)
		}
		return r.LastSeq
	}
	want := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrUnstorable) {
			t.Errorf("%s: err = %v, want store.ErrUnstorable", what, err)
		}
	}

	// Event payloads. Strings are built from escapes in code, never typed as a
	// literal NUL or surrogate in this file.
	bad := map[string]any{
		"nul in a string":       events.CommentAddedPayload{CommentID: "c", Text: "a\x00b"},
		"nul in a raw message":  events.AgentToolCallPayload{Input: json.RawMessage(`{"q":"a` + "\\u0000" + `b"}`)},
		"lone high surrogate":   events.AgentToolResultPayload{Data: json.RawMessage(`{"q":"` + "\\ud83d" + `"}`)},
		"lone low surrogate":    events.AgentToolResultPayload{Data: json.RawMessage(`{"q":"` + "\\ude00" + `x"}`)},
		"invalid utf-8 in raw":  events.AgentToolResultPayload{Data: json.RawMessage("{\"q\":\"a\xffb\"}")},
		"nul in a nested value": map[string]any{"a": []any{map[string]any{"b": "x\x00"}}},
	}
	for name, payload := range bad {
		_, err := s.Append(ctx, roomID, events.CommentAdded, actor, payload)
		want("append: "+name, err)
	}
	_, err := s.Append(ctx, roomID, events.CommentAdded, events.Actor{Kind: events.ActorHuman, ID: "u", Name: "U\x00"}, events.CommentAddedPayload{Text: "x"})
	want("append: nul in actor", err)
	if got := lastSeq(); got != 0 {
		t.Fatalf("refused appends moved last_seq to %d, want 0", got)
	}
	if evs, err := s.Since(ctx, roomID, 0, 10); err != nil || len(evs) != 0 {
		t.Fatalf("refused appends stored %d events (%v), want 0", len(evs), err)
	}
	// What looks similar is stored, and takes seq 1 (no gap).
	ok := []any{
		events.CommentAddedPayload{CommentID: "c", Text: "café 中 \U0001F600 \\u0000"},
		events.AgentToolResultPayload{Data: json.RawMessage(`{"pair":"` + "\\ud83d\\ude00" + `","text":"` + "\\\\u0000" + `"}`)},
	}
	for i, payload := range ok {
		e, err := s.Append(ctx, roomID, events.CommentAdded, actor, payload)
		if err != nil || e.Seq != int64(i+1) {
			t.Fatalf("storable append %d = seq %d, %v", i, e.Seq, err)
		}
	}

	// Rows. A refused insert leaves no row behind.
	want("room title", s.CreateRoom(ctx, store.Room{ID: testenv.Name("room"), Title: "t\x00", Severity: "sev2", PipelineID: "p", Status: "open", OpenedAt: time.Now().UTC()}))
	want("room title, invalid utf-8", s.CreateRoom(ctx, store.Room{ID: testenv.Name("room"), Title: "t\xff", Severity: "sev2", PipelineID: "p", Status: "open", OpenedAt: time.Now().UTC()}))
	want("driver name", s.UpdateDriver(ctx, roomID, "u", "N\x00", time.Now().UTC()))

	steps := json.RawMessage(`[{"action":"a"}]`)
	pid := testenv.Name("prop")
	want("proposal title", s.CreateProposal(ctx, store.ProposalRow{ProposalID: pid, RoomID: roomID, Title: "t\x00", Status: "open", Steps: steps}))
	want("proposal rollback", s.CreateProposal(ctx, store.ProposalRow{ProposalID: pid, RoomID: roomID, Title: "t", Status: "open", Rollback: "r\xff", Steps: steps}))
	want("proposal steps", s.CreateProposal(ctx, store.ProposalRow{ProposalID: pid, RoomID: roomID, Title: "t", Status: "open", Steps: json.RawMessage(`[{"p":"` + "\\u0000" + `"}]`)}))
	if _, err := s.GetProposal(ctx, pid); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused proposal left a row: %v", err)
	}
	if err := s.CreateProposal(ctx, store.ProposalRow{ProposalID: pid, RoomID: roomID, Title: "t", Status: "open", RequiredLayers: []string{"postgres"}, Steps: steps}); err != nil {
		t.Fatalf("create good proposal: %v", err)
	}
	want("approval reason", s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "postgres", Decision: "rejected", Reason: "no\x00"}))
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "postgres", Decision: "rejected", Reason: "no"}); err != nil {
		t.Errorf("a refused approval must not consume the layer: %v", err)
	}
	if err := s.StartProposalStep(ctx, pid, 0); err != nil {
		t.Fatalf("start step: %v", err)
	}
	want("step output", s.FinishProposalStep(ctx, pid, 0, "done", "out\xff"))
	if err := s.FinishProposalStep(ctx, pid, 0, "done", "out"); err != nil {
		t.Errorf("a refused finish must leave the step started: %v", err)
	}

	gid := testenv.Name("sugg")
	want("suggestion text", s.CreateSuggestion(ctx, store.SuggestionRow{SuggestionID: gid, RoomID: roomID, FromID: "u", FromName: "U", Text: "t\x00"}))
	if _, err := s.GetSuggestion(ctx, gid); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused suggestion left a row: %v", err)
	}

	hid := testenv.Name("hand")
	want("handoff summary", s.CreateHandoff(ctx, store.HandoffRow{HandoffID: hid, RoomID: roomID, FromID: "a", ToID: "b", Summary: json.RawMessage(`{"x":"` + "\\ud83d" + `"}`)}))
	want("handoff name", s.CreateHandoff(ctx, store.HandoffRow{HandoffID: hid, RoomID: roomID, FromID: "a", FromName: "A\x00", ToID: "b", Summary: json.RawMessage(`{}`)}))
	if _, err := s.GetHandoff(ctx, hid); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("refused handoff left a row: %v", err)
	}

	// A lookup by an id that cannot be stored is "not found", not a Postgres error.
	if _, err := s.GetRoom(ctx, "r\x00"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("get room by NUL id: %v, want ErrNotFound", err)
	}
	if _, err := s.GetProposal(ctx, "p\x00"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("get proposal by NUL id: %v, want ErrNotFound", err)
	}
	if _, err := s.GetSuggestion(ctx, "g\xff"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("get suggestion by bad id: %v, want ErrNotFound", err)
	}
	if _, err := s.GetHandoff(ctx, "h\x00"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("get handoff by NUL id: %v, want ErrNotFound", err)
	}
	if _, err := s.LastRoomByDedupKey(ctx, "k\x00"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("last room by NUL key: %v, want ErrNotFound", err)
	}
}

// A value that gets past the pre-check and is still refused by Postgres (a
// number outside jsonb's numeric range, SQLSTATE 22003) also comes back as
// store.ErrUnstorable, with the Postgres error still in the chain.
func TestPostgresBadValueIsErrUnstorable(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	roomID := testenv.Name("room")
	if err := s.CreateRoom(ctx, store.Room{ID: roomID, Title: "t", Severity: "sev2", PipelineID: "p", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("create room: %v", err)
	}
	_, err := s.Append(ctx, roomID, events.CommentAdded, events.Actor{Kind: events.ActorHuman, ID: "u", Name: "U"},
		json.RawMessage(`{"n":1e131072}`))
	if !errors.Is(err, store.ErrUnstorable) {
		t.Fatalf("err = %v, want store.ErrUnstorable", err)
	}
	if r, err := s.GetRoom(ctx, roomID); err != nil || r.LastSeq != 0 {
		t.Fatalf("room = %+v, %v; the refused append must not reserve a seq", r, err)
	}
}
