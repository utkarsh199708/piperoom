//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: proposalID, Layer: "postgres", Decision: "approved"}); err != nil {
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

	res, err := s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 100, nil)
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
	res, err = s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 100, nil)
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

	res, err := s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.RoomIDs) != 2 || res.RoomIDs[0] != r1 || res.RoomIDs[1] != r2 {
		t.Fatalf("want the two oldest (%s, %s), got %v", r1, r2, res.RoomIDs)
	}
	if _, err := s.GetRoom(ctx, r3); err != nil {
		t.Fatalf("third room must wait for the next batch: %v", err)
	}
	if res, _ := s.PurgeClosedBefore(ctx, now.Add(-30*24*time.Hour), 2, nil); len(res.RoomIDs) != 1 {
		t.Fatalf("next batch should take the last room, got %v", res.RoomIDs)
	}
}

// schemaConn opens a connection pinned to the test schema, for statements the
// store API has no method for (bulk seeding, a failure-injecting trigger).
func schemaConn(ctx context.Context, t *testing.T, url, schema string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// TestPurgeLargeRoomDeletesInBatches crosses the 5000-row statement batch. The
// room is seeded with one INSERT ... generate_series, not 5100 appends.
func TestPurgeLargeRoomDeletesInBatches(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	roomID := createTestRoom(t, s)
	const n = 5100
	conn := schemaConn(ctx, t, url, schema)
	if _, err := conn.Exec(ctx,
		`INSERT INTO room_events (room_id, seq, type, actor, payload, at)
		 SELECT $1, g, 'comment.added', '{"kind":"human","id":"arjun","name":"Arjun"}'::jsonb, '{"text":"x"}'::jsonb, now()
		 FROM generate_series(1, $2::int) AS g`, roomID, n); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE rooms SET last_seq = $2 WHERE id = $1`, roomID, n); err != nil {
		t.Fatalf("seed last_seq: %v", err)
	}
	closed := time.Now().UTC().Add(-48 * time.Hour)
	if err := s.CloseRoom(ctx, roomID, closed); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeClosedBefore(ctx, time.Now().UTC().Add(-24*time.Hour), 10, nil)
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
	if _, err := s.PurgeClosedBefore(ctx, time.Now().UTC().Add(-24*time.Hour), 10, nil); err != nil {
		t.Fatal(err)
	}
	_, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: "c_late", Text: "x"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestPurgeFailingRoomDoesNotBlockTheOthers makes the delete of one room fail
// (a trigger raises) and checks that the rooms around it are still purged, the
// failing room is intact and reported, and passing it in exclude skips it.
func TestPurgeFailingRoomDoesNotBlockTheOthers(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	now := time.Now().UTC()
	t1, t2, t3 := now.Add(-90*24*time.Hour), now.Add(-80*24*time.Hour), now.Add(-70*24*time.Hour)
	seedRoomWithChildren(ctx, t, s, &t1) // oldest: handled first
	bad, badPid := seedRoomWithChildren(ctx, t, s, &t2)
	last, _ := seedRoomWithChildren(ctx, t, s, &t3)

	conn := schemaConn(ctx, t, url, schema)
	if _, err := conn.Exec(ctx, `
CREATE FUNCTION fail_one_room() RETURNS trigger AS $$
BEGIN
  IF OLD.id = '`+bad+`' THEN RAISE EXCEPTION 'injected failure'; END IF;
  RETURN OLD;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER fail_one_room BEFORE DELETE ON rooms FOR EACH ROW EXECUTE FUNCTION fail_one_room();`); err != nil {
		t.Fatalf("install trigger: %v", err)
	}

	cutoff := now.Add(-30 * 24 * time.Hour)
	res, err := s.PurgeClosedBefore(ctx, cutoff, 10, nil)
	if err != nil {
		t.Fatalf("a failing room must not fail the call: %v", err)
	}
	if res.Examined != 3 || len(res.RoomIDs) != 2 || len(res.Failed) != 1 || res.Failed[0].RoomID != bad {
		t.Fatalf("want 3 examined, 2 purged, %s failed; got %+v", bad, res)
	}
	if _, err := s.GetRoom(ctx, last); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the room behind the failing one must be purged: %v", err)
	}
	// The failing room's transaction rolled back: nothing of it is gone.
	if evs, err := s.Since(ctx, bad, 0, 100); err != nil || len(evs) != 3 {
		t.Fatalf("failing room lost events: %d %v", len(evs), err)
	}
	if _, err := s.GetProposal(ctx, badPid); err != nil {
		t.Fatalf("failing room lost its proposal: %v", err)
	}

	// Without exclude it is tried (and fails) again; with it, it is skipped.
	res, err = s.PurgeClosedBefore(ctx, cutoff, 10, nil)
	if err != nil || res.Examined != 1 || len(res.Failed) != 1 {
		t.Fatalf("second run without exclude: %+v %v", res, err)
	}
	res, err = s.PurgeClosedBefore(ctx, cutoff, 10, []string{bad})
	if err != nil || res.Examined != 0 || len(res.Failed) != 0 {
		t.Fatalf("exclude must skip the failed room: %+v %v", res, err)
	}
}
