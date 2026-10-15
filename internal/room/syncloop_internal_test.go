package room

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// fullPageStore reports a room row far ahead of the cache and answers every
// Since with a full page, as a busy room on a shared store would, so a sync
// that does not advance would read the same page again and again.
type fullPageStore struct {
	*leaseStore
	mu     sync.Mutex
	on     bool
	stale  bool // pages hold events the cache already has (no progress)
	sinces int
}

// syncMark marks the test's own sync context, so Since counts only the reads
// of the sync under test, not those of the service's background loops.
type syncMark struct{}

func (f *fullPageStore) arm(stale bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.on, f.stale, f.sinces = true, stale, 0
}

func (f *fullPageStore) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sinces
}

func (f *fullPageStore) GetRoom(ctx context.Context, id string) (store.Room, error) {
	r, err := f.leaseStore.GetRoom(ctx, id)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil && f.on {
		r.LastSeq += 1_000_000
	}
	return r, err
}

func (f *fullPageStore) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	f.mu.Lock()
	on, stale := f.on, f.stale
	if on && ctx.Value(syncMark{}) != nil {
		f.sinces++
	}
	f.mu.Unlock()
	if !on {
		return f.leaseStore.Since(ctx, roomID, afterSeq, limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, err // a sync that loops ends with its context, and fails below
	}
	evs := make([]events.Event, limit)
	for i := range evs {
		seq := afterSeq + int64(i) + 1
		if stale {
			seq = afterSeq // nothing the cache does not already have
		}
		evs[i] = events.Event{RoomID: roomID, Seq: seq, Type: events.CommentAdded,
			Actor: events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}, Payload: json.RawMessage(`{"text":"x"}`), At: time.Now()}
	}
	return evs, nil
}

// syncRoom stops after one read when the pass folds nothing: with the
// service closing, or with a page that does not move the room forward. It
// used to re-read the same full page until its context ended.
func TestSyncRoomStopsWhenItCannotAdvance(t *testing.T) {
	for _, tc := range []struct {
		name    string
		closing bool
		stale   bool
	}{
		{"service closing", true, false},
		{"page does not advance", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fullPageStore{leaseStore: newLeaseStore()}
			svc := New(Deps{
				Store: fs, Users: DefaultUsers(), Owners: tenantOwners(), ReplicaID: "replica-a",
				// A silent agent: nothing else appends to the room while the
				// sync runs, so any second read is the sync's own.
				Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
					<-ctx.Done()
					return nil
				},
				ReplicaTimings: ReplicaTimings{LeaseTTL: time.Minute, LeaseRenew: time.Hour, TakeoverScan: time.Hour, ExecSweep: time.Hour},
			})
			t.Cleanup(svc.Close)
			ctx := context.Background()
			if err := svc.Load(ctx); err != nil {
				t.Fatal(err)
			}
			id := createRooms(t, svc, 1)[0]

			fs.arm(tc.stale)
			if tc.closing {
				svc.reg.Lock()
				svc.closed = true
				svc.reg.Unlock()
				t.Cleanup(func() {
					svc.reg.Lock()
					svc.closed = false
					svc.reg.Unlock()
				})
			}
			sctx, cancel := context.WithTimeout(context.WithValue(ctx, syncMark{}, true), 2*time.Second)
			defer cancel()
			start := time.Now()
			svc.syncRoomWith(sctx, id, false)
			if n := fs.calls(); n != 1 {
				t.Fatalf("Since called %d times, want 1", n)
			}
			if el := time.Since(start); el > time.Second {
				t.Fatalf("sync took %v; it waited for its context", el)
			}
		})
	}
}
