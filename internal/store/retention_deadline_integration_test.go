//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestPurgeRunDeadlineMidRoomBlamesNoOne: when the whole call's deadline fires
// while a room is being purged, the call returns the context error, records no
// failure for that room (it is not at fault, so the service will not exclude it)
// and the room is untouched because its transaction rolled back.
func TestPurgeRunDeadlineMidRoomBlamesNoOne(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	setup, cancelSetup := context.WithTimeout(context.Background(), time.Minute)
	defer cancelSetup()
	closed := time.Now().UTC().Add(-48 * time.Hour)
	slow, slowPid := seedRoomWithChildren(setup, t, s, &closed)

	// The delete of this room takes 5 s, far longer than the run's deadline.
	conn := schemaConn(setup, t, url, schema)
	if _, err := conn.Exec(setup, `
CREATE FUNCTION slow_room_delete() RETURNS trigger AS $$
BEGIN
  IF OLD.id = '`+slow+`' THEN PERFORM pg_sleep(5); END IF;
  RETURN OLD;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER slow_room_delete BEFORE DELETE ON rooms FOR EACH ROW EXECUTE FUNCTION slow_room_delete();`); err != nil {
		t.Fatalf("install trigger: %v", err)
	}

	run, cancelRun := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancelRun()
	res, err := s.PurgeClosedBefore(run, time.Now().UTC().Add(-24*time.Hour), 10, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the run's deadline error, got %v", err)
	}
	if len(res.Failed) != 0 || len(res.RoomIDs) != 0 {
		t.Fatalf("a deadline is nobody's fault: %+v", res)
	}

	check, cancelCheck := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCheck()
	if _, err := s.GetRoom(check, slow); err != nil {
		t.Fatalf("room must be intact after the rollback: %v", err)
	}
	if _, err := s.GetProposal(check, slowPid); err != nil {
		t.Fatalf("proposal must be intact: %v", err)
	}
	if evs, err := s.Since(check, slow, 0, 100); err != nil || len(evs) != 3 {
		t.Fatalf("events must be intact: %d %v", len(evs), err)
	}
}
