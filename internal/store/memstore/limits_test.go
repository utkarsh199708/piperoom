package memstore_test

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
	"piperoom/internal/store/memstore"
)

var (
	human  = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	system = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
)

func newRoom(t *testing.T, st *memstore.Store, id, pipeline string) {
	t.Helper()
	if err := st.CreateRoom(context.Background(), store.Room{ID: id, Title: "t", Severity: "sev2", PipelineID: pipeline, Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
}

// TestCapsByClass: with a cap of 100 events, the agent stops at 100, people
// and the system at 102 (2% reserve), and room.closed always goes in. A
// refused event uses no seq.
func TestCapsByClass(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	st.SetLimits(store.Limits{MaxEvents: 100})
	newRoom(t, st, "r1", "orders")
	for i := 0; i < 100; i++ {
		if _, err := st.Append(ctx, "r1", events.AgentToolCall, events.AgentActor, map[string]int{"i": i}); err != nil {
			t.Fatalf("agent event %d: %v", i+1, err)
		}
	}
	if _, err := st.Append(ctx, "r1", events.AgentMessage, events.AgentActor, map[string]string{"text": "one more"}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("agent event 101: err = %v, want ErrLogFull", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := st.Append(ctx, "r1", events.CommentAdded, human, map[string]string{"text": "still here"}); err != nil {
			t.Fatalf("human event in the reserve: %v", err)
		}
	}
	if _, err := st.Append(ctx, "r1", events.AgentStatus, system, map[string]string{"state": "log_full"}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("past the reserve: err = %v, want ErrLogFull", err)
	}
	e, err := st.Append(ctx, "r1", events.RoomClosed, human, events.RoomClosedPayload{Resolution: "done"})
	if err != nil {
		t.Fatalf("room.closed must always be stored: %v", err)
	}
	if e.Seq != 103 {
		t.Fatalf("room.closed seq = %d, want 103 (refused events use no seq)", e.Seq)
	}
	u, err := st.RoomUsage(ctx, "r1")
	if err != nil || u.Events != 103 || u.MaxEvents != 100 {
		t.Fatalf("usage = %+v, %v", u, err)
	}
}

// TestByteCapCountsPayloadBytes: the byte measure is len(Event.Payload), the
// same number the room service adds up.
func TestByteCapCountsPayloadBytes(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	st.SetLimits(store.Limits{MaxBytes: 1000})
	newRoom(t, st, "r1", "orders")
	var sum int64
	for {
		e, err := st.Append(ctx, "r1", events.AgentMessage, events.AgentActor, map[string]string{"text": strings.Repeat("x", 90)})
		if errors.Is(err, store.ErrLogFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sum += int64(len(e.Payload))
	}
	u, _ := st.RoomUsage(ctx, "r1")
	if u.Bytes != sum || sum > 1000 || sum < 900 {
		t.Fatalf("bytes = %d, payload sum = %d, want equal and within 900..1000", u.Bytes, sum)
	}
	// The 2% reserve is 20 bytes: a small human event (10 bytes) fits, a
	// large one does not.
	if _, err := st.Append(ctx, "r1", events.CommentAdded, human, map[string]string{"t": "ok"}); err != nil {
		t.Fatalf("small human event refused at %d bytes: %v", u.Bytes, err)
	}
	if _, err := st.Append(ctx, "r1", events.CommentAdded, human, map[string]string{"t": strings.Repeat("y", 200)}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("human event past the byte reserve: %v", err)
	}
}

// TestCapsHoldUnderConcurrency: agents and people appending at once never
// pass cap (agent) or cap + reserve (everyone).
func TestCapsHoldUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	st.SetLimits(store.Limits{MaxEvents: 200})
	newRoom(t, st, "r1", "orders")
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			actor, typ := events.AgentActor, events.AgentToolCall
			if g%2 == 1 {
				actor, typ = human, events.CommentAdded
			}
			for i := 0; i < 100; i++ {
				_, _ = st.Append(ctx, "r1", typ, actor, map[string]int{"g": g, "i": i})
			}
		}(g)
	}
	wg.Wait()
	evs, err := st.Since(ctx, "r1", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	agent := 0
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("gap at %d: seq %d", i, e.Seq)
		}
		if e.Actor.Kind == events.ActorAgent {
			agent++
		}
	}
	if len(evs) != 204 {
		t.Fatalf("events = %d, want exactly cap + reserve = 204", len(evs))
	}
	if agent > 200 {
		t.Fatalf("agent events = %d, over the cap 200", agent)
	}
}

// TestDeleteRoomWritesAuditRow: everything keyed by the room goes, one audit
// row stays, a second delete finds nothing and the row stays the same.
func TestDeleteRoomWritesAuditRow(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	newRoom(t, st, "r1", "orders")
	newRoom(t, st, "r2", "orders")
	for i := 0; i < 3; i++ {
		if _, err := st.Append(ctx, "r1", events.CommentAdded, human, map[string]string{"text": "secret"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: "r1", Title: "fix", Status: "open", Steps: json.RawMessage(`[]`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordApproval(ctx, store.ApprovalRow{ProposalID: "p1", Layer: "kafka", Decision: "approved", IdentityMethod: "dev"}); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 13, 9, 0, 0, 0, time.FixedZone("x", 3600))
	d, res, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: "r1", DeletedBy: "admin", DeletedAt: at, Reason: "pasted a secret"}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if d.PipelineID != "orders" || d.EventCount != 3 || !d.DeletedAt.Equal(at) || d.DeletedAt.Location() != time.UTC {
		t.Fatalf("audit row = %+v", d)
	}
	if res.Events != 3 || res.Proposals != 1 || res.Approvals != 1 {
		t.Fatalf("counts = %+v", res)
	}
	if _, err := st.GetRoom(ctx, "r1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("room still there: %v", err)
	}
	if _, err := st.GetProposal(ctx, "p1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("proposal still there: %v", err)
	}
	if _, err := st.GetRoom(ctx, "r2"); err != nil {
		t.Fatalf("other room gone: %v", err)
	}
	got, err := st.GetRoomDeletion(ctx, "r1")
	if err != nil || got != d {
		t.Fatalf("GetRoomDeletion = %+v, %v; want %+v", got, err, d)
	}
	if _, _, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: "r1", DeletedBy: "admin2", Reason: "again"}, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete: %v, want ErrNotFound", err)
	}
	if again, _ := st.GetRoomDeletion(ctx, "r1"); again != d {
		t.Fatalf("audit row changed: %+v", again)
	}
	if _, err := st.GetRoomDeletion(ctx, "r2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("no row for a room that was not deleted: %v", err)
	}
}

func TestDeleteRoomWantSeq(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	newRoom(t, st, "r1", "orders")
	if _, err := st.Append(ctx, "r1", events.CommentAdded, human, map[string]string{"text": "a"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: "r1", DeletedBy: "admin", Reason: "x"}, 0); !errors.Is(err, store.ErrRoomChanged) {
		t.Fatalf("stale archive seq: %v, want ErrRoomChanged", err)
	}
	if _, _, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: "r1", DeletedBy: "admin", Reason: "x"}, 1); err != nil {
		t.Fatalf("matching seq: %v", err)
	}
}

// TestPurgeClosedPipelineAndArchive: a pipeline filter keeps other pipelines;
// the archive runs before each delete; an archive error keeps the room; an
// archive that saw fewer events than the room has keeps it too.
func TestPurgeClosedPipelineAndArchive(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	closed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, r := range []struct{ id, pipeline string }{{"a1", "orders"}, {"a2", "orders"}, {"a3", "orders"}, {"b1", "billing"}} {
		newRoom(t, st, r.id, r.pipeline)
		if _, err := st.Append(ctx, r.id, events.CommentAdded, human, map[string]string{"text": "x"}); err != nil {
			t.Fatal(err)
		}
		if err := st.CloseRoom(ctx, r.id, closed); err != nil {
			t.Fatal(err)
		}
	}
	var archived []string
	archive := func(_ context.Context, id string) (int64, error) {
		// The room must still be there when its archive runs.
		if _, err := st.GetRoom(ctx, id); err != nil {
			return 0, fmt.Errorf("archive ran after the delete: %w", err)
		}
		switch id {
		case "a2":
			return 0, errors.New("disk full")
		case "a3":
			return 0, nil // saw no events; the room has 1
		}
		archived = append(archived, id)
		r, _ := st.GetRoom(ctx, id)
		return r.LastSeq, nil
	}
	res, err := st.PurgeClosed(ctx, store.PurgeQuery{Cutoff: closed.Add(time.Hour), PipelineID: "orders", MaxRooms: 10, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.RoomIDs) != 1 || res.RoomIDs[0] != "a1" || len(archived) != 1 {
		t.Fatalf("purged %v archived %v, want only a1", res.RoomIDs, archived)
	}
	if len(res.Failed) != 2 {
		t.Fatalf("failed = %+v, want a2 (archive error) and a3 (changed)", res.Failed)
	}
	for _, f := range res.Failed {
		if f.RoomID == "a3" && !errors.Is(f.Err, store.ErrRoomChanged) {
			t.Fatalf("a3: %v, want ErrRoomChanged", f.Err)
		}
	}
	for _, id := range []string{"a2", "a3", "b1"} {
		if _, err := st.GetRoom(ctx, id); err != nil {
			t.Fatalf("%s must be kept: %v", id, err)
		}
	}
}
