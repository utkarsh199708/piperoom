package memstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

func testActor() events.Actor { return events.Actor{Kind: events.ActorHuman, ID: "u", Name: "U"} }

func newTestRoom(t *testing.T, s *Store) string {
	t.Helper()
	if err := s.CreateRoom(context.Background(), store.Room{ID: "r1", Title: "t", Severity: "sev2", PipelineID: "p", Status: "open"}); err != nil {
		t.Fatalf("create room: %v", err)
	}
	return "r1"
}

func TestAppendConcurrentNoGaps(t *testing.T) {
	s := New()
	roomID := newTestRoom(t, s)
	ctx := context.Background()
	const n = 100
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	evs, err := s.Since(ctx, roomID, 0, 1000)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(evs) != n {
		t.Fatalf("want %d events, got %d", n, len(evs))
	}
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq gap at %d: got %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestSinceLimitZeroReturnsEmpty(t *testing.T) {
	s := New()
	roomID := newTestRoom(t, s)
	ctx := context.Background()
	if _, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	evs, err := s.Since(ctx, roomID, 0, 0)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("want 0 events, got %d", len(evs))
	}
}

func TestStartProposalStepUnique(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: "r", Title: "t", Status: "open"}); err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if err := s.StartProposalStep(ctx, "p1", 0); err != nil {
		t.Fatalf("start step: %v", err)
	}
	err := s.StartProposalStep(ctx, "p1", 0)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
}
