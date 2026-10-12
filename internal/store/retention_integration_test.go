//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/slackbridge"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

// seedRoomWithChildren creates a room that has a row in every child table and a
// few events, and closes it at closedAt (nil = leaves it open). It returns the
// room id and the proposal id.
func seedRoomWithChildren(ctx context.Context, t *testing.T, s *store.PG, closedAt *time.Time) (roomID, proposalID string) {
	t.Helper()
	roomID = createTestRoom(t, s)
	for i := 0; i < 3; i++ {
		appendComment(ctx, t, s, roomID, i)
	}
	proposalID = testenv.Name("p")
	if err := s.CreateProposal(ctx, store.ProposalRow{
		ProposalID: proposalID, RoomID: roomID, Title: "fix", Status: "open",
		RequiredLayers: []string{"postgres"}, Steps: json.RawMessage(`[]`),
	}); err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if err := s.RecordApproval(ctx, proposalID, "postgres", "approved", ""); err != nil {
		t.Fatalf("approval: %v", err)
	}
	if err := s.StartProposalStep(ctx, proposalID, 0); err != nil {
		t.Fatalf("step: %v", err)
	}
	sid := testenv.Name("g")
	if err := s.CreateSuggestion(ctx, store.SuggestionRow{SuggestionID: sid, RoomID: roomID, FromID: "kabir", FromName: "Kabir", Text: "x"}); err != nil {
		t.Fatalf("suggestion: %v", err)
	}
	hid := testenv.Name("h")
	if err := s.CreateHandoff(ctx, store.HandoffRow{HandoffID: hid, RoomID: roomID, FromID: "arjun", FromName: "Arjun", ToID: "priya", ToName: "Priya", Summary: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if err := s.SlackThreads().Put(ctx, roomID, slackbridge.Thread{Channel: "C1", TS: "1.1", LastSeq: 3}); err != nil {
		t.Fatalf("slack thread: %v", err)
	}
	if closedAt != nil {
		if err := s.CloseRoom(ctx, roomID, *closedAt); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	return roomID, proposalID
}

func TestPurgeClosedBeforeDeletesOnlyOldClosedRooms(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	now := time.Now().UTC()

	oldClosed := now.Add(-40 * 24 * time.Hour)
	recentClosed := now.Add(-5 * 24 * time.Hour)
	aID, aPid := seedRoomWithChildren(ctx, t, s, &oldClosed)    // goes
	bID, bPid := seedRoomWithChildren(ctx, t, s, &recentClosed) // stays: closed too recently
	cID, cPid := seedRoomWithChildren(ctx, t, s, nil)           // stays: open
	// An open room that is old by opened_at must stay as well.
	if _, err := s.GetRoom(ctx, cID); err != nil {
		t.Fatal(err)
	}

	res, err := s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(res.RoomIDs) != 1 || res.RoomIDs[0] != aID {
		t.Fatalf("want only %s purged, got %v", aID, res.RoomIDs)
	}
	// 3 comments; counts are the RowsAffected of the real DELETEs.
	if res.Events != 3 || res.Proposals != 1 || res.Approvals != 1 || res.Steps != 1 ||
		res.Suggestions != 1 || res.Handoffs != 1 || res.SlackThreads != 1 {
		t.Fatalf("wrong counts: %+v", res)
	}

	if _, err := s.GetRoom(ctx, aID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("purged room still there: %v", err)
	}
	if evs, err := s.Since(ctx, aID, 0, 100); err != nil || len(evs) != 0 {
		t.Fatalf("purged room's events remain: %d %v", len(evs), err)
	}
	if _, err := s.GetProposal(ctx, aPid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("purged proposal remains: %v", err)
	}
	if _, ok, _ := s.SlackThreads().Get(ctx, aID); ok {
		t.Fatalf("purged room's slack thread remains")
	}

	for _, kept := range []struct{ room, proposal string }{{bID, bPid}, {cID, cPid}} {
		if _, err := s.GetRoom(ctx, kept.room); err != nil {
			t.Fatalf("room %s must be kept: %v", kept.room, err)
		}
		if evs, err := s.Since(ctx, kept.room, 0, 100); err != nil || len(evs) != 3 {
			t.Fatalf("room %s lost events: %d %v", kept.room, len(evs), err)
		}
		if _, err := s.GetProposal(ctx, kept.proposal); err != nil {
			t.Fatalf("proposal of kept room %s lost: %v", kept.room, err)
		}
		if _, ok, _ := s.SlackThreads().Get(ctx, kept.room); !ok {
			t.Fatalf("slack thread of kept room %s lost", kept.room)
		}
	}

	// Idempotent.
	res, err = s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 100)
	if err != nil || len(res.RoomIDs) != 0 {
		t.Fatalf("second purge: %+v %v", res, err)
	}
}

func TestPurgeClosedBeforeRespectsMaxRoomsOldestFirst(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	now := time.Now().UTC()
	t1, t2, t3 := now.Add(-90*24*time.Hour), now.Add(-80*24*time.Hour), now.Add(-70*24*time.Hour)
	r1, _ := seedRoomWithChildren(ctx, t, s, &t1)
	r2, _ := seedRoomWithChildren(ctx, t, s, &t2)
	r3, _ := seedRoomWithChildren(ctx, t, s, &t3)

	res, err := s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.RoomIDs) != 2 || res.RoomIDs[0] != r1 || res.RoomIDs[1] != r2 {
		t.Fatalf("want the two oldest (%s, %s), got %v", r1, r2, res.RoomIDs)
	}
	if _, err := s.GetRoom(ctx, r3); err != nil {
		t.Fatalf("third room must wait for the next batch: %v", err)
	}
	if res, _ := s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 2); len(res.RoomIDs) != 1 {
		t.Fatalf("next batch should take the last room, got %v", res.RoomIDs)
	}
}

// TestPurgeLargeRoomDeletesInBatches crosses the 5000-row statement batch.
func TestPurgeLargeRoomDeletesInBatches(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	roomID := createTestRoom(t, s)
	const n = 5100
	var wg sync.WaitGroup
	jobs := make(chan int)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if _, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: testenv.Name("c"), Text: "x"}); err != nil {
					t.Errorf("append %d: %v", i, err)
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if t.Failed() {
		return
	}
	closed := time.Now().UTC().Add(-48 * time.Hour)
	if err := s.CloseRoom(ctx, roomID, closed); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeClosedBefore(ctx, time.Now().UTC().Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if res.Events != n || len(res.RoomIDs) != 1 {
		t.Fatalf("want %d events deleted in one room, got %+v", n, res)
	}
	if evs, err := s.Since(ctx, roomID, 0, 10); err != nil || len(evs) != 0 {
		t.Fatalf("events remain: %d %v", len(evs), err)
	}
}

// TestAppendToPurgedRoomIsNotFound: a late writer gets not-found, not a stray row.
func TestAppendToPurgedRoomIsNotFound(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	closed := time.Now().UTC().Add(-48 * time.Hour)
	roomID, _ := seedRoomWithChildren(ctx, t, s, &closed)
	if _, err := s.PurgeClosedBefore(ctx, time.Now().UTC().Add(-24*time.Hour), 10); err != nil {
		t.Fatal(err)
	}
	_, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: "c_late", Text: "x"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
