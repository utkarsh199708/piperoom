package room_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// raceStore makes the repeat check miss the first `blind` lookups, as when
// two requests both pass it before either appends, and counts suggestion
// rows so the loser's compensation is visible.
type raceStore struct {
	*memstore.Store
	blind   atomic.Int32
	created atomic.Int32
	deleted atomic.Int32
	// fullOnTaken answers a taken key as a full log, the order Postgres
	// checks them in when the winner took the room's last slot.
	fullOnTaken atomic.Bool
}

func (r *raceStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	e, err := r.Store.Append(ctx, roomID, typ, actor, payload)
	if errors.Is(err, store.ErrClientIDTaken) && r.fullOnTaken.Load() {
		return events.Event{}, fmt.Errorf("test: %w", store.ErrLogFull)
	}
	return e, err
}

func (r *raceStore) EventByClientID(ctx context.Context, roomID, actorID, clientID string) (events.Event, error) {
	if r.blind.Add(-1) >= 0 {
		return events.Event{}, store.ErrNotFound
	}
	return r.Store.EventByClientID(ctx, roomID, actorID, clientID)
}

func (r *raceStore) CreateSuggestion(ctx context.Context, row store.SuggestionRow) error {
	r.created.Add(1)
	return r.Store.CreateSuggestion(ctx, row)
}

func (r *raceStore) DeleteSuggestion(ctx context.Context, id string) error {
	r.deleted.Add(1)
	return r.Store.DeleteSuggestion(ctx, id)
}

func newRaceService(t *testing.T) (*room.Service, *raceStore) {
	t.Helper()
	st := &raceStore{Store: memstore.New()}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite})
	t.Cleanup(svc.Close)
	return svc, st
}

func keyedCount(t *testing.T, svc *room.Service, roomID string) int {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs {
		switch e.Type {
		case events.CommentAdded, events.SteerSent, events.SteerSuggested:
			n++
		}
	}
	return n
}

// The race loser of a suggestion: its row is removed and it answers the
// winner's event.
func TestSteerKeyedRaceLoserCompensates(t *testing.T) {
	svc, st := newRaceService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	priya := mustUser(t, svc, "priya")

	first, err := svc.SteerKeyed(ctx, priya, roomID, "look", "k")
	if err != nil || first.Type != events.SteerSuggested {
		t.Fatalf("first: %v %+v", err, first)
	}
	st.blind.Store(1) // the next repeat check misses
	again, err := svc.SteerKeyed(ctx, priya, roomID, "look", "k")
	if err != nil || again.Seq != first.Seq {
		t.Fatalf("loser: %v seq %d, want seq %d", err, again.Seq, first.Seq)
	}
	if c, d := st.created.Load(), st.deleted.Load(); c != 2 || d != 1 {
		t.Fatalf("suggestion rows: %d created, %d deleted; want 2 and 1", c, d)
	}
	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.PendingSuggestions) != 1 {
		t.Fatalf("pending suggestions: %+v", snap.PendingSuggestions)
	}
	// A loser whose body differs answers client_id_reused.
	st.blind.Store(1)
	if _, err := svc.SteerKeyed(ctx, priya, roomID, "other", "k"); !isCode(err, "client_id_reused") {
		t.Fatalf("loser with another body: %v", err)
	}
	if n := keyedCount(t, svc, roomID); n != 1 {
		t.Fatalf("events: %d", n)
	}
}

// The race loser of a driver steer and of a comment answers the winner.
func TestKeyedRaceLoserDriverAndComment(t *testing.T) {
	svc, st := newRaceService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")

	sent, err := svc.SteerKeyed(ctx, arjun, roomID, "go", "d")
	if err != nil || sent.Type != events.SteerSent {
		t.Fatalf("sent: %v %+v", err, sent)
	}
	st.blind.Store(1)
	if e, err := svc.SteerKeyed(ctx, arjun, roomID, "go", "d"); err != nil || e.Seq != sent.Seq {
		t.Fatalf("driver loser: %v %+v", err, e)
	}
	c, err := svc.AddCommentKeyed(ctx, arjun, roomID, "", "hi", "c")
	if err != nil {
		t.Fatal(err)
	}
	st.blind.Store(1)
	if e, err := svc.AddCommentKeyed(ctx, arjun, roomID, "", "hi", "c"); err != nil || e.Seq != c.Seq {
		t.Fatalf("comment loser: %v %+v", err, e)
	}
	if st.created.Load() != 0 {
		t.Fatalf("a driver steer creates no suggestion row")
	}
	if n := keyedCount(t, svc, roomID); n != 2 {
		t.Fatalf("events: %d", n)
	}
}

// A repeat wakes nothing and renews no lease; a steer.sent stays steer.sent
// after the driver changed.
func TestSteerKeyedRepeatAfterDriverChange(t *testing.T) {
	svc, _, c := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")
	sent, err := svc.SteerKeyed(ctx, arjun, roomID, "go", "d")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GrantControl(ctx, arjun, roomID, "priya"); err != nil {
		t.Fatal(err)
	}
	before, _ := svc.GetRoom(ctx, roomID)
	c.Advance(10 * time.Minute)
	e, err := svc.SteerKeyed(ctx, arjun, roomID, "go", "d")
	if err != nil || e.Seq != sent.Seq || e.Type != events.SteerSent {
		t.Fatalf("repeat: %v %+v", err, e)
	}
	after, _ := svc.GetRoom(ctx, roomID)
	if after.LastSeq != before.LastSeq {
		t.Fatalf("repeat appended: last seq %d -> %d", before.LastSeq, after.LastSeq)
	}
}

func TestKeyedInvalidClientID(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")
	if _, err := svc.SteerKeyed(context.Background(), arjun, roomID, "go", "a b"); !isCode(err, "validation") {
		t.Fatalf("steer: %v", err)
	}
	if _, err := svc.AddCommentKeyed(context.Background(), arjun, roomID, "", "go", "x/y"); !isCode(err, "validation") {
		t.Fatalf("comment: %v", err)
	}
}

// Goroutines racing one key, mixed steer and comment: one event, every
// answer is it or client_id_reused.
func TestKeyedConcurrentDuplicates(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	priya := mustUser(t, svc, "priya")
	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	seqs := map[int64]int{}
	reused := 0
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var e events.Event
			var err error
			if i%4 == 0 {
				e, err = svc.AddCommentKeyed(ctx, priya, roomID, "", "same", "k")
			} else {
				e, err = svc.SteerKeyed(ctx, priya, roomID, "same", "k")
			}
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				seqs[e.Seq]++
			case isCode(err, "client_id_reused"):
				reused++
			default:
				t.Errorf("request %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if len(seqs) != 1 || keyedCount(t, svc, roomID) != 1 {
		t.Fatalf("seqs %v, events %d; want one", seqs, keyedCount(t, svc, roomID))
	}
	for _, k := range seqs {
		if k+reused != n {
			t.Fatalf("answers: %d ok + %d reused, want %d", k, reused, n)
		}
	}
}

func isCode(err error, code string) bool {
	var re *room.Error
	return errors.As(err, &re) && re.Code == code
}

// A loser that finds the log full (its winner took the last slot) answers as
// a repeat; a full log with an unused key keeps its own answer.
func TestKeyedRaceLoserOnFullLog(t *testing.T) {
	svc, st := newRaceService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	priya := mustUser(t, svc, "priya")
	first, err := svc.SteerKeyed(ctx, priya, roomID, "look", "k")
	if err != nil {
		t.Fatal(err)
	}
	st.fullOnTaken.Store(true)
	st.blind.Store(1)
	if e, err := svc.SteerKeyed(ctx, priya, roomID, "look", "k"); err != nil || e.Seq != first.Seq {
		t.Fatalf("loser on a full log: %v %+v", err, e)
	}
	if c, d := st.created.Load(), st.deleted.Load(); c != 2 || d != 1 {
		t.Fatalf("suggestion rows: %d created, %d deleted; want 2 and 1", c, d)
	}
	st.blind.Store(1)
	if _, err := svc.AddCommentKeyed(ctx, priya, roomID, "", "look", "k"); !isCode(err, "client_id_reused") {
		t.Fatalf("other route on a full log: %v", err)
	}
}
