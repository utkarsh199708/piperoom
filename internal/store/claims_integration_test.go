//go:build integration

package store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// AppendIfLast writes only on the expected seq; of two writers checking the
// same seq on two replicas exactly one lands.
func TestAppendIfLastConflict(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{})
	roomID := createTestRoom(t, a)
	ctx := context.Background()
	appendComment(ctx, t, a, roomID, 1)
	for round := 0; round < 20; round++ {
		row, err := a.GetRoom(ctx, roomID)
		if err != nil {
			t.Fatal(err)
		}
		var won, conflicts atomic.Int32
		var wg sync.WaitGroup
		for _, s := range []*store.PG{a, b} {
			wg.Add(1)
			go func(s *store.PG) {
				defer wg.Done()
				_, err := s.AppendIfLast(ctx, roomID, row.LastSeq, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"})
				switch {
				case err == nil:
					won.Add(1)
				case errors.Is(err, store.ErrSeqConflict):
					conflicts.Add(1)
				default:
					t.Errorf("append: %v", err)
				}
			}(s)
		}
		wg.Wait()
		if won.Load() != 1 || conflicts.Load() != 1 {
			t.Fatalf("round %d: %d writes landed, %d conflicts; want 1 and 1", round, won.Load(), conflicts.Load())
		}
	}
	if _, err := a.AppendIfLast(ctx, "r_missing", 0, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing room: %v, want ErrNotFound", err)
	}
	evs, _ := a.Since(ctx, roomID, 0, 1000)
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("gap at %d: seq %d", i, e.Seq)
		}
	}
}

// The seq condition and the log caps share one UPDATE: on a full log the
// expected seq gets ErrLogFull, a stale one ErrSeqConflict, and neither
// writes or uses a seq.
func TestAppendIfLastWithLogCap(t *testing.T) {
	a, _, _ := requireStoreSchema(t)
	roomID := createTestRoom(t, a)
	ctx := context.Background()
	a.SetLimits(store.Limits{MaxEvents: 2})
	t.Cleanup(func() { a.SetLimits(store.Limits{}) })
	agent := events.AgentActor
	msg := events.AgentMessagePayload{Text: "x"}
	if _, err := a.AppendIfLast(ctx, roomID, 0, events.AgentMessage, agent, msg); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AppendIfLast(ctx, roomID, 1, events.AgentMessage, agent, msg); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AppendIfLast(ctx, roomID, 2, events.AgentMessage, agent, msg); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("expected seq on a full log: %v, want ErrLogFull", err)
	}
	if _, err := a.AppendIfLast(ctx, roomID, 1, events.AgentMessage, agent, msg); !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("stale seq: %v, want ErrSeqConflict", err)
	}
	row, err := a.GetRoom(ctx, roomID)
	if err != nil || row.LastSeq != 2 {
		t.Fatalf("last seq = %d (%v), want 2", row.LastSeq, err)
	}
}

// Every executor write holds only while its claim does.
func TestClaimedStepWrites(t *testing.T) {
	s, _, schema := requireStoreSchema(t)
	roomID := createTestRoom(t, s)
	createTestProposal(t, s, roomID, "p_c")
	ctx := context.Background()
	if err := s.StartClaimedStep(ctx, "p_c", 0, "me"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("start without a claim: %v, want ErrClaimLost", err)
	}
	if ok, _ := s.ClaimProposal(ctx, "p_c", "me"); !ok {
		t.Fatal("claim")
	}
	if err := s.StartClaimedStep(ctx, "p_c", 0, "other"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("start by a non-claimer: %v", err)
	}
	if err := s.StartClaimedStep(ctx, "p_c", 0, "me"); err != nil {
		t.Fatal(err)
	}
	if err := s.StartClaimedStep(ctx, "p_c", 0, "me"); !errors.Is(err, store.ErrStepOrder) {
		t.Fatalf("second start: %v, want ErrStepOrder", err)
	}
	if err := s.FinishClaimedStep(ctx, "p_c", 0, "me", "done", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := s.StartClaimedStep(ctx, "p_c", 1, "me"); err != nil {
		t.Fatal(err)
	}
	// The sweep takes the claim: nothing more lands for "me".
	execSQL(t, schema, `UPDATE proposals SET claimed_at = now() - interval '1 hour' WHERE proposal_id = 'p_c'`)
	if ok, _ := s.InterruptProposal(ctx, "p_c", "sweeper", time.Minute, time.Minute); !ok {
		t.Fatal("interrupt")
	}
	if err := s.FinishClaimedStep(ctx, "p_c", 1, "me", "done", "ok"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("finish after the sweep: %v, want ErrClaimLost", err)
	}
	if err := s.StartClaimedStep(ctx, "p_c", 2, "me"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("start after the sweep: %v, want ErrClaimLost", err)
	}
	if err := s.SetClaimedProposalStatus(ctx, "p_c", "me", "executed"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("status after the sweep: %v, want ErrClaimLost", err)
	}
	if p, _ := s.GetProposal(ctx, "p_c"); p.Status != "failed" {
		t.Fatalf("status %q, want failed (the sweep's)", p.Status)
	}
}

// The resume cap holds per room per window, across replicas.
func TestNoteResumeCap(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{})
	roomID := createTestRoom(t, a)
	ctx := context.Background()
	if ok, _ := a.AcquireRoomLease(ctx, roomID, "A", "orders", time.Minute); !ok {
		t.Fatal("acquire")
	}
	for i := 0; i < 3; i++ {
		if ok, err := a.NoteResume(ctx, roomID, "A", 3, time.Hour); err != nil || !ok {
			t.Fatalf("resume %d: %v %v", i, ok, err)
		}
	}
	if ok, _ := a.NoteResume(ctx, roomID, "A", 3, time.Hour); ok {
		t.Fatal("a 4th resume within the hour was allowed")
	}
	// Taken over by B: the count stays with the room.
	execSQL(t, schema, `UPDATE room_agent_leases SET expires_at = now() - interval '1 second'`)
	if ok, _ := b.TakeOverRoomLease(ctx, roomID, "B", time.Minute); !ok {
		t.Fatal("take over")
	}
	if ok, _ := b.NoteResume(ctx, roomID, "B", 3, time.Hour); ok {
		t.Fatal("the cap reset on takeover")
	}
	if ok, _ := a.NoteResume(ctx, roomID, "A", 3, time.Hour); ok {
		t.Fatal("a non-owner counted a resume")
	}
	execSQL(t, schema, `UPDATE room_agent_leases SET resume_window = now() - interval '2 hours'`)
	if ok, _ := b.NoteResume(ctx, roomID, "B", 3, time.Hour); !ok {
		t.Fatal("a new window did not reset the count")
	}
}

// Two Slack mirrors of one room: the compare-and-set lets one advance.
func TestSlackThreadCompareAndSet(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{})
	roomID := createTestRoom(t, a)
	ctx := context.Background()
	ta, tb := a.SlackThreads(), b.SlackThreads()
	if ok, err := ta.PutIfLastSeq(ctx, roomID, 0, "C1", "1.1", 1); err != nil || !ok {
		t.Fatalf("first put: %v %v", ok, err)
	}
	if ok, _ := tb.PutIfLastSeq(ctx, roomID, 0, "C1", "1.1", 1); ok {
		t.Fatal("a second creator won")
	}
	var won atomic.Int32
	var wg sync.WaitGroup
	for i, th := range []*store.SlackThreads{ta, tb} {
		wg.Add(1)
		go func(i int, th *store.SlackThreads) {
			defer wg.Done()
			if ok, err := th.PutIfLastSeq(ctx, roomID, 1, "C1", "1.1", int64(2+i)); err != nil {
				t.Error(err)
			} else if ok {
				won.Add(1)
			}
		}(i, th)
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("%d mirrors advanced from the same seq, want 1", won.Load())
	}
	got, ok, err := ta.Get(ctx, roomID)
	if err != nil || !ok || (got.LastSeq != 2 && got.LastSeq != 3) {
		t.Fatalf("thread: %+v %v %v", got, ok, err)
	}
}
