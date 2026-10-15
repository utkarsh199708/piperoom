package room_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// raceDeleteStore plays another replica at delete time: before the delete
// runs, that replica's agent appends to the room (archived log is now short),
// or its executor holds a claim in the room (the store refuses).
type raceDeleteStore struct {
	*memstore.Store
	appendFirst bool
	executing   bool
}

func (f *raceDeleteStore) DeleteRoom(ctx context.Context, d store.RoomDeletion, wantSeq int64) (store.RoomDeletion, store.PurgeResult, error) {
	if f.executing {
		return store.RoomDeletion{}, store.PurgeResult{}, fmt.Errorf("fake: delete room %s: %w", d.RoomID, store.ErrRoomExecuting)
	}
	if f.appendFirst {
		f.appendFirst = false
		if _, err := f.Store.Append(ctx, d.RoomID, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "still here"}); err != nil {
			return store.RoomDeletion{}, store.PurgeResult{}, err
		}
	}
	return f.Store.DeleteRoom(ctx, d, wantSeq)
}

func TestDeleteRaceWithOtherReplicaIsConflict(t *testing.T) {
	for _, tc := range []struct {
		name    string
		archive bool
		st      func(*memstore.Store) *raceDeleteStore
	}{
		// The archive was taken, then the agent on another replica appended:
		// a retry archives the longer log, so this is 409, not 500.
		{"append after archive", true, func(m *memstore.Store) *raceDeleteStore { return &raceDeleteStore{Store: m, appendFirst: true} }},
		// A fix runs on another replica: this replica's queue is idle, the
		// store's claim check refuses.
		{"claim on another replica", false, func(m *memstore.Store) *raceDeleteStore { return &raceDeleteStore{Store: m, executing: true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &clock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
			st := tc.st(memstore.NewWithClock(c.Now))
			svc := room.New(room.Deps{
				Store: st, Users: adminUsers(t), Owners: testOwners(),
				Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now,
				Limits: config.Limits{ArchiveDir: t.TempDir()},
			})
			t.Cleanup(svc.Close)
			ctx := context.Background()
			snap := createRoom(t, svc, "arjun")

			_, err := svc.DeleteRoom(ctx, mustUser(t, svc, "ada"), snap.ID, "cleanup", tc.archive)
			var re *room.Error
			if !errors.As(err, &re) || re.Status != 409 || re.Code != "conflict" {
				t.Fatalf("err = %v, want a 409 conflict", err)
			}
			if _, err := st.Store.GetRoom(ctx, snap.ID); err != nil {
				t.Fatalf("room gone after a refused delete: %v", err)
			}
			// Not left in the deleting state.
			if _, err := svc.AddComment(ctx, mustUser(t, svc, "arjun"), snap.ID, "", "after"); err != nil {
				t.Fatalf("comment after a refused delete: %v", err)
			}
		})
	}
}
