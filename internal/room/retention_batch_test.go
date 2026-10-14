package room_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// TestPurgeExpiredLoopsPastOneBatch: the service takes 50 rooms per store call
// and must keep going until none is left.
func TestPurgeExpiredLoopsPastOneBatch(t *testing.T) {
	svc, st, c := newTestService(t)
	ctx := context.Background()
	const n = 120
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := createRoom(t, svc, "arjun").ID
		if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), id, "old"); err != nil {
			t.Fatalf("close: %v", err)
		}
		ids = append(ids, id)
	}
	keep := createRoom(t, svc, "arjun").ID // open
	c.Advance(60 * 24 * time.Hour)

	res, err := svc.PurgeExpired(ctx, c.Now().Add(-30*24*time.Hour))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(res.RoomIDs) != n {
		t.Fatalf("want %d rooms purged across batches, got %d", n, len(res.RoomIDs))
	}
	for _, id := range ids {
		if _, err := st.GetRoom(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("room %s survived: %v", id, err)
		}
	}
	if _, err := st.GetRoom(ctx, keep); err != nil {
		t.Fatalf("open room gone: %v", err)
	}
}

// queuePurger is a Store whose retention queue is a plain list, so the test
// controls exactly which room fails. It records the exclude list of each call.
type queuePurger struct {
	store.Store
	pending  []string
	failID   string
	excludes [][]string
}

func (q *queuePurger) PurgeClosedBefore(_ context.Context, _ time.Time, maxRooms int, exclude []string) (store.PurgeResult, error) {
	q.excludes = append(q.excludes, append([]string(nil), exclude...))
	skip := map[string]bool{}
	for _, id := range exclude {
		skip[id] = true
	}
	var res store.PurgeResult
	var rest []string
	for _, id := range q.pending {
		if skip[id] || res.Examined >= maxRooms {
			rest = append(rest, id)
			continue
		}
		res.Examined++
		if id == q.failID {
			res.Failed = append(res.Failed, store.PurgeFailure{RoomID: id, Err: errors.New("boom")})
			rest = append(rest, id)
			continue
		}
		res.RoomIDs = append(res.RoomIDs, id)
		res.Events++
	}
	q.pending = rest
	return res, nil
}

// TestPurgeExpiredFailingRoomDoesNotBlockTheRest: a room that fails early in the
// queue is reported, left out of later calls, and every other room still goes.
func TestPurgeExpiredFailingRoomDoesNotBlockTheRest(t *testing.T) {
	q := &queuePurger{Store: memstore.New(), failID: "r_003"}
	for i := 0; i < 120; i++ {
		q.pending = append(q.pending, fmt.Sprintf("r_%03d", i))
	}
	svc := room.New(room.Deps{Store: q, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)

	res, err := svc.PurgeExpired(context.Background(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "r_003") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want an error naming the failed room, got %v", err)
	}
	if len(res.RoomIDs) != 119 || len(res.Failed) != 1 {
		t.Fatalf("want 119 purged and 1 failed, got %d and %d", len(res.RoomIDs), len(res.Failed))
	}
	if len(q.pending) != 1 || q.pending[0] != "r_003" {
		t.Fatalf("only the failed room may remain, got %v", q.pending)
	}
	if len(q.excludes) != 3 {
		t.Fatalf("want 3 store calls (50, 50, 20), got %d", len(q.excludes))
	}
	if len(q.excludes[0]) != 0 || len(q.excludes[1]) != 1 || q.excludes[1][0] != "r_003" {
		t.Fatalf("the failed room must be excluded from later calls: %v", q.excludes)
	}
}
