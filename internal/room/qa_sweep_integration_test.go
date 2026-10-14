//go:build integration

// Regression tests for the multi-replica findings of the 2026-10-14 QA sweep
// (QA5): a suggestion sent twice, and human events landing after room.closed.
package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

func errCode(err error) string {
	var re *Error
	if errors.As(err, &re) {
		return re.Code
	}
	if err == nil {
		return "ok"
	}
	return "internal: " + err.Error()
}

func countSent(t *testing.T, s *Service, roomID, suggestionID string) int {
	t.Helper()
	evs, _, err := s.Events(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs {
		if e.Type == events.SteerSent && strings.Contains(string(e.Payload), suggestionID) {
			n++
		}
	}
	return n
}

// Two replicas send the same pending suggestion at once: the agent gets it
// exactly once, and the other driver request answers already_decided.
func TestReplicasSuggestionSentOnce(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, nil)
	ctx := context.Background()
	for r := 0; r < 15; r++ {
		snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "s", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		if snap.Driver == nil || snap.Driver.ID != "arjun" {
			if _, err := a.GrantControl(ctx, user(t, a, "arjun"), snap.ID, "arjun"); err != nil {
				t.Fatal(err)
			}
		}
		e, err := a.Steer(ctx, user(t, a, "priya"), snap.ID, "try restarting the connector")
		if err != nil || e.Type != events.SteerSuggested {
			t.Fatalf("suggest: %v %s", err, e.Type)
		}
		p, err := events.Decode[events.SteerSuggestedPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var e1, e2 error
		wg.Add(2)
		go func() { defer wg.Done(); _, e1 = a.SendSuggestion(ctx, user(t, a, "arjun"), snap.ID, p.SuggestionID) }()
		go func() { defer wg.Done(); _, e2 = b.SendSuggestion(ctx, user(t, b, "arjun"), snap.ID, p.SuggestionID) }()
		wg.Wait()
		if n := countSent(t, a, snap.ID, p.SuggestionID); n != 1 {
			t.Fatalf("round %d: steer.sent for one suggestion = %d (results %s / %s)", r, n, errCode(e1), errCode(e2))
		}
		codes := []string{errCode(e1), errCode(e2)}
		if !(codes[0] == "ok" && codes[1] != "ok" || codes[1] == "ok" && codes[0] != "ok") {
			t.Fatalf("round %d: want one ok, got %v", r, codes)
		}
		for _, c := range codes {
			if c != "ok" && c != codeAlreadyDecided && c != "conflict" {
				t.Fatalf("round %d: loser answered %q", r, c)
			}
		}
		// The row stays sent: the loser never reverts it to pending.
		if row, err := a.store.GetSuggestion(ctx, p.SuggestionID); err != nil || row.Status != "sent" {
			t.Fatalf("round %d: suggestion row %+v, %v", r, row, err)
		}
	}
}

// One replica closes the room while another posts comments, steers and an
// approval: nothing a person does lands after room.closed.
func TestReplicasNoHumanEventsAfterClose(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, &itAction{})
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, &itAction{})
	ctx := context.Background()
	after := map[events.Type]int{}
	for r := 0; r < 15; r++ {
		snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "c", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		pid, err := a.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
			Title: "fix", Risk: "low", Rollback: "revert the connector config",
			Steps: []events.ProposalStep{{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{}`), Description: "k"}},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		row, err := a.store.GetProposal(ctx, pid)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.GetRoom(ctx, snap.ID); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			// A checked close can lose three times in a row to the burst on
			// r1 (409 conflict); a client retries.
			for i := 0; i < 10; i++ {
				if _, err := a.CloseRoom(ctx, user(t, a, "arjun"), snap.ID, "fixed"); errCode(err) != "conflict" {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_, err := b.AddComment(ctx, user(t, b, "priya"), snap.ID, "", fmt.Sprint("c", i))
				if c := errCode(err); c != "ok" && c != codeRoomClosed && c != "conflict" {
					t.Errorf("comment answered %s", c)
				}
			}
		}()
		go func() {
			defer wg.Done()
			_, _ = b.ApproveProposal(ctx, user(t, b, "arjun"), pid, "kafka", row.ParamsHash)
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				_, _ = b.Steer(ctx, user(t, b, "kabir"), snap.ID, fmt.Sprint("s", i))
			}
		}()
		wg.Wait()
		evs, _, err := a.Events(ctx, snap.ID, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		closed := false
		for _, e := range evs {
			if closed && e.Actor.Kind == events.ActorHuman {
				after[e.Type]++
			}
			if e.Type == events.RoomClosed {
				closed = true
			}
		}
		if !closed {
			t.Fatalf("round %d: room not closed", r)
		}
	}
	if len(after) > 0 {
		t.Fatalf("human events after room.closed: %v", after)
	}
}

// The store refuses a person's event on a closed room even when the caller
// never looked at the room state (a replica with a stale cache).
func TestStoreRefusesHumanEventOnClosedRoom(t *testing.T) {
	schema := pgSchema(t)
	st := pgReplicaStore(t, schema, store.Options{})
	ctx := context.Background()
	id := "r_closed_" + fmt.Sprint(time.Now().UnixNano())
	if err := st.CreateRoom(ctx, store.Room{ID: id, Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	human := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	if _, err := st.Append(ctx, id, events.RoomClosed, human, events.RoomClosedPayload{Resolution: "done"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, id, events.CommentAdded, human, events.CommentAddedPayload{CommentID: "c1", Text: "late"}); !errors.Is(err, store.ErrRoomClosed) {
		t.Fatalf("comment after close: want ErrRoomClosed, got %v", err)
	}
	if _, err := st.AppendIfLast(ctx, id, 1, events.ApprovalGiven, human, events.ApprovalGivenPayload{ProposalID: "p", Layer: "kafka"}); !errors.Is(err, store.ErrRoomClosed) {
		t.Fatalf("approval after close: want ErrRoomClosed, got %v", err)
	}
	// Close-out events still land.
	if _, err := st.Append(ctx, id, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{State: "done"}); err != nil {
		t.Fatalf("agent status after close: %v", err)
	}
	row, err := st.GetRoom(ctx, id)
	if err != nil || row.Status != "closed" || row.ClosedAt == nil || row.LastSeq != 2 {
		t.Fatalf("room row after close = %+v, %v", row, err)
	}
	if err := st.CloseRoom(ctx, "r_missing_"+id, time.Now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("close missing room: want ErrNotFound, got %v", err)
	}
}
