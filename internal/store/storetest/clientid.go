package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// KeyedStore is a store with the client_id index.
type KeyedStore interface {
	Store
	store.ClientIDs
}

// RunClientIDUnique checks the client_id rule (docs/api.md, Idempotent
// posts): a second event of the same actor with the same payload client_id in
// a room wraps store.ErrClientIDTaken, writes nothing and uses no seq, across
// event types and under concurrent appends; another actor, another room or
// another key is free; events without the key are never refused; and
// EventByClientID finds the stored event or wraps store.ErrNotFound.
func RunClientIDUnique(t *testing.T, s KeyedStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	priya := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	arjun := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	r1, r2 := newRoom(ctx, t, s), newRoom(ctx, t, s)

	first, err := s.Append(ctx, r1, events.CommentAdded, priya, events.CommentAddedPayload{CommentID: "c1", Text: "x", ClientID: "k"})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	// The key spans event types.
	if _, err := s.Append(ctx, r1, events.SteerSuggested, priya, events.SteerSuggestedPayload{SuggestionID: "g", Text: "x", ClientID: "k"}); !errors.Is(err, store.ErrClientIDTaken) {
		t.Fatalf("same key, other type: %v, want ErrClientIDTaken", err)
	}
	for _, c := range []struct {
		room  string
		actor events.Actor
		key   string
	}{{r1, arjun, "k"}, {r2, priya, "k"}, {r1, priya, "K"}} {
		if _, err := s.Append(ctx, c.room, events.CommentAdded, c.actor, events.CommentAddedPayload{CommentID: "c", Text: "x", ClientID: c.key}); err != nil {
			t.Fatalf("free key %+v: %v", c, err)
		}
	}
	// No key: never refused.
	for range 2 {
		if _, err := s.Append(ctx, r1, events.CommentAdded, priya, events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
			t.Fatalf("append without key: %v", err)
		}
	}
	got, err := s.EventByClientID(ctx, r1, "priya", "k")
	if err != nil || got.Seq != first.Seq || got.Type != events.CommentAdded || got.Actor.ID != "priya" {
		t.Fatalf("lookup: %v %+v, want seq %d", err, got, first.Seq)
	}
	for _, c := range [][3]string{{r1, "priya", "nope"}, {r1, "nobody", "k"}, {"r_missing", "priya", "k"}} {
		if _, err := s.EventByClientID(ctx, c[0], c[1], c[2]); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("lookup %v: %v, want ErrNotFound", c, err)
		}
	}

	// Concurrent appends of one key: one lands, the rest are refused.
	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, taken := 0, 0
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Append(ctx, r2, events.SteerSent, arjun, events.SteerSentPayload{Text: "x", ClientID: "race"})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, store.ErrClientIDTaken):
				taken++
			default:
				t.Errorf("racing append: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok != 1 || taken != n-1 {
		t.Fatalf("racing appends: %d ok, %d taken; want 1 and %d", ok, taken, n-1)
	}
	// No seq was used by a refused append: seqs stay gapless.
	evs, err := s.Since(ctx, r2, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq gap at %d: %d", i, e.Seq)
		}
	}
	if len(evs) != 2 { // the free key of priya, the race winner
		t.Fatalf("r2 events: %d, want 2", len(evs))
	}
}
