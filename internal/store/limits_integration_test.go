//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

var (
	agentActor  = events.AgentActor
	systemActor = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
)

// TestPGCapsByClass: cap 100 events. The agent stops at 100, people and the
// system at 102 (2% reserve), room.closed always goes in, and a refused event
// uses no seq. RoomUsage reads the same counters.
func TestPGCapsByClass(t *testing.T) {
	s, _ := requireStore(t)
	s.SetLimits(store.Limits{MaxEvents: 100})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	roomID := createTestRoom(t, s)
	for i := 0; i < 100; i++ {
		if _, err := s.Append(ctx, roomID, events.AgentToolCall, agentActor, map[string]int{"i": i}); err != nil {
			t.Fatalf("agent event %d: %v", i+1, err)
		}
	}
	if _, err := s.Append(ctx, roomID, events.AgentMessage, agentActor, map[string]string{"text": "more"}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("agent event 101: %v, want ErrLogFull", err)
	}
	for i := 0; i < 2; i++ {
		appendComment(ctx, t, s, roomID, i)
	}
	if _, err := s.Append(ctx, roomID, events.AgentStatus, systemActor, events.AgentStatusPayload{State: "log_full"}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("past the reserve: %v, want ErrLogFull", err)
	}
	e, err := s.Append(ctx, roomID, events.RoomClosed, commentActor(), events.RoomClosedPayload{Resolution: "done"})
	if err != nil {
		t.Fatalf("room.closed must always be stored: %v", err)
	}
	if e.Seq != 103 {
		t.Fatalf("room.closed seq = %d, want 103", e.Seq)
	}
	u, err := s.RoomUsage(ctx, roomID)
	if err != nil || u.Events != 103 || u.MaxEvents != 100 {
		t.Fatalf("usage = %+v, %v", u, err)
	}
	row, err := s.GetRoom(ctx, roomID)
	if err != nil || row.LastSeq != 103 {
		t.Fatalf("last_seq = %d, %v; want 103", row.LastSeq, err)
	}
}

// TestPGByteCapCountsPayloadBytes: byte_size is the sum of len(Payload) of the
// events Append returned, and the byte cap refuses at the edge.
func TestPGByteCapCountsPayloadBytes(t *testing.T) {
	s, _ := requireStore(t)
	s.SetLimits(store.Limits{MaxBytes: 2000})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	roomID := createTestRoom(t, s)
	var sum int64
	for {
		e, err := s.Append(ctx, roomID, events.AgentMessage, agentActor, map[string]string{"text": strings.Repeat("x", 90)})
		if errors.Is(err, store.ErrLogFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sum += int64(len(e.Payload))
	}
	u, err := s.RoomUsage(ctx, roomID)
	if err != nil || u.Bytes != sum || sum > 2000 || sum < 1800 {
		t.Fatalf("byte_size = %d (%v), payload sum = %d; want equal and within 1800..2000", u.Bytes, err, sum)
	}
	if _, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), map[string]string{"t": strings.Repeat("y", 200)}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("human event past the 40-byte reserve: %v, want ErrLogFull", err)
	}
}

// TestPGAppendUnknownRoomWithCaps: a missing room is not found, not full.
func TestPGAppendUnknownRoomWithCaps(t *testing.T) {
	s, _ := requireStore(t)
	s.SetLimits(store.Limits{MaxEvents: 100, MaxBytes: 1 << 20})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.Append(ctx, testenv.Name("nope"), events.AgentMessage, agentActor, map[string]string{"text": "x"})
	if !errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrLogFull) {
		t.Fatalf("err = %v, want ErrNotFound only", err)
	}
	if _, err := s.RoomUsage(ctx, testenv.Name("nope")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("RoomUsage: %v, want ErrNotFound", err)
	}
}

// TestPGCapsHoldUnderConcurrency: 4 agent and 4 human writers on separate
// connections, cap 200. The log ends at exactly cap + reserve, gap-free, and
// the agent never passes the cap. Without the WHERE on the UPDATE (or with a
// read-then-write check) the writers overshoot.
func TestPGCapsHoldUnderConcurrency(t *testing.T) {
	s, _ := requireStore(t)
	s.SetLimits(store.Limits{MaxEvents: 200})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	roomID := createTestRoom(t, s)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		otherErr error
	)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			actor, typ := agentActor, events.AgentToolCall
			if g%2 == 1 {
				actor, typ = commentActor(), events.CommentAdded
			}
			for i := 0; i < 60; i++ {
				_, err := s.Append(ctx, roomID, typ, actor, map[string]int{"g": g, "i": i})
				if err != nil && !errors.Is(err, store.ErrLogFull) {
					mu.Lock()
					otherErr = err
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if otherErr != nil {
		t.Fatalf("append failed for another reason: %v", otherErr)
	}
	evs, err := s.Since(ctx, roomID, 0, 1000)
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
		t.Fatalf("agent events = %d, over the cap", agent)
	}
	u, _ := s.RoomUsage(ctx, roomID)
	if u.Events != 204 {
		t.Fatalf("event_count = %d, want 204", u.Events)
	}
}

// TestPGDeleteRoom: one transaction removes the room and every child row and
// writes the audit row; a second delete finds nothing; a stale archive seq
// stops the delete.
func TestPGDeleteRoom(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	roomID, proposalID := seedRoomWithChildren(ctx, t, s, nil) // open: deletion is not only for closed rooms
	other := createTestRoom(t, s)

	if _, _, err := s.DeleteRoom(ctx, store.RoomDeletion{RoomID: roomID, DeletedBy: "admin", Reason: "x"}, 2); !errors.Is(err, store.ErrRoomChanged) {
		t.Fatalf("stale seq: %v, want ErrRoomChanged", err)
	}
	if _, err := s.GetRoom(ctx, roomID); err != nil {
		t.Fatalf("room must be kept after a refused delete: %v", err)
	}

	at := time.Date(2026, 10, 13, 9, 30, 0, 0, time.UTC)
	d, res, err := s.DeleteRoom(ctx, store.RoomDeletion{RoomID: roomID, DeletedBy: "admin", DeletedAt: at, Reason: "customer asked for erasure"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if d.PipelineID != "orders" || d.EventCount != 3 || !d.DeletedAt.Equal(at) {
		t.Fatalf("audit row = %+v", d)
	}
	if res.Events != 3 || res.Proposals != 1 || res.Approvals != 1 || res.Steps != 1 || res.Suggestions != 1 || res.Handoffs != 1 || res.SlackThreads != 1 {
		t.Fatalf("counts = %+v", res)
	}
	conn := schemaConn(ctx, t, url, schema)
	for _, q := range []string{
		`SELECT count(*) FROM room_events WHERE room_id = $1`,
		`SELECT count(*) FROM proposals WHERE room_id = $1`,
		`SELECT count(*) FROM suggestions WHERE room_id = $1`,
		`SELECT count(*) FROM handoffs WHERE room_id = $1`,
		`SELECT count(*) FROM slack_threads WHERE room_id = $1`,
		`SELECT count(*) FROM rooms WHERE id = $1`,
	} {
		var n int
		if err := conn.QueryRow(ctx, q, roomID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s: %d rows left (%v)", q, n, err)
		}
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM approvals WHERE proposal_id = $1`, proposalID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("approvals left: %d (%v)", n, err)
	}
	// The audit row holds ids and counts only: no column for content exists.
	var cols []string
	rows, err := conn.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'room_deletions' ORDER BY ordinal_position`, schema)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var c string
		_ = rows.Scan(&c)
		cols = append(cols, c)
	}
	rows.Close()
	if fmt.Sprint(cols) != "[room_id pipeline_id deleted_by deleted_at reason event_count]" {
		t.Fatalf("room_deletions columns = %v", cols)
	}

	got, err := s.GetRoomDeletion(ctx, roomID)
	if err != nil || got != d {
		t.Fatalf("GetRoomDeletion = %+v, %v; want %+v", got, err, d)
	}
	if _, _, err := s.DeleteRoom(ctx, store.RoomDeletion{RoomID: roomID, DeletedBy: "admin", Reason: "again"}, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete: %v, want ErrNotFound", err)
	}
	if _, err := s.GetRoomDeletion(ctx, other); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRoomDeletion of a live room: %v", err)
	}
	if _, err := s.GetRoom(ctx, other); err != nil {
		t.Fatalf("the other room must stay: %v", err)
	}
	// A late append to the deleted room is not found, never written.
	if _, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), map[string]string{"text": "late"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("append after delete: %v", err)
	}
}

// TestPGPurgeClosedPipelineAndArchive: the pipeline filter keeps other
// pipelines, the archive runs before the delete, a failed archive keeps the
// room, and an archive that saw an older log keeps it too.
func TestPGPurgeClosedPipelineAndArchive(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	closed := time.Now().UTC().Add(-48 * time.Hour)
	mk := func(pipeline string) string {
		id := testenv.Name("pur")
		if err := s.CreateRoom(ctx, store.Room{ID: id, Title: "t", Severity: "sev2", PipelineID: pipeline, Status: "open", OpenedAt: closed}); err != nil {
			t.Fatal(err)
		}
		appendComment(ctx, t, s, id, 0)
		if err := s.CloseRoom(ctx, id, closed); err != nil {
			t.Fatal(err)
		}
		return id
	}
	pipeline := testenv.Name("pl")
	ok, failing, changed := mk(pipeline), mk(pipeline), mk(pipeline)
	otherPipeline := mk(testenv.Name("pl"))

	var archived []string
	archive := func(actx context.Context, id string) (int64, error) {
		if _, err := s.GetRoom(actx, id); err != nil {
			return 0, fmt.Errorf("archive ran after the delete: %w", err)
		}
		switch id {
		case failing:
			return 0, errors.New("disk full")
		case changed:
			return 0, nil // the room has 1 event
		}
		archived = append(archived, id)
		return 1, nil
	}
	res, err := s.PurgeClosed(ctx, store.PurgeQuery{Cutoff: time.Now().UTC(), PipelineID: pipeline, MaxRooms: 10, Archive: archive})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.RoomIDs) != 1 || res.RoomIDs[0] != ok || len(archived) != 1 {
		t.Fatalf("purged %v archived %v, want only %s", res.RoomIDs, archived, ok)
	}
	if res.Examined != 3 || len(res.Failed) != 2 {
		t.Fatalf("examined %d failed %+v", res.Examined, res.Failed)
	}
	for _, f := range res.Failed {
		if f.RoomID == changed && !errors.Is(f.Err, store.ErrRoomChanged) {
			t.Fatalf("changed room: %v, want ErrRoomChanged", f.Err)
		}
	}
	for _, id := range []string{failing, changed, otherPipeline} {
		if _, err := s.GetRoom(ctx, id); err != nil {
			t.Fatalf("%s must be kept: %v", id, err)
		}
	}
}
