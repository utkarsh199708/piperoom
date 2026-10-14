// Package storetest holds behaviour checks every room store must pass, so the
// in-memory store the unit tests run on cannot drift from Postgres: memstore
// runs them in its unit tests, *store.PG under the integration tag.
package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Store is what the checks use: the event log, the room rows and the admin
// delete.
type Store interface {
	store.Store
	store.Deleter
}

// checkTimeout bounds every store call of one check.
const checkTimeout = 30 * time.Second

// RunSince checks Since (events.Store): events with Seq > afterSeq, in order,
// at most limit; a non-nil empty page when nothing is after afterSeq, also
// for a room with no events yet; an empty page for limit <= 0, whatever the
// room; and an error wrapping store.ErrNotFound for a room that does not
// exist, never created or deleted, so a caller (the WebSocket replay, a
// subscription) can tell "nothing new" from "room gone".
func RunSince(t *testing.T, s Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	empty := newRoom(ctx, t, s)
	full := newRoom(ctx, t, s)
	const n = 5
	for range n {
		appendComment(ctx, t, s, full)
	}
	missing := roomID()

	for _, tc := range []struct {
		name  string
		room  string
		after int64
		limit int
		want  []int64
	}{
		{"all", full, 0, 10, []int64{1, 2, 3, 4, 5}},
		{"after", full, 2, 10, []int64{3, 4, 5}},
		{"limit", full, 1, 2, []int64{2, 3}},
		{"negative after", full, -1, 1, []int64{1}},
		{"after the last", full, n, 10, nil},
		{"past the last", full, n + 10, 10, nil},
		{"no events yet", empty, 0, 10, nil},
		{"zero limit", full, 0, 0, nil},
		{"negative limit", full, 0, -1, nil},
		{"zero limit, unknown room", missing, 0, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.Since(ctx, tc.room, tc.after, tc.limit)
			if err != nil {
				t.Fatalf("Since(%d, %d): %v", tc.after, tc.limit, err)
			}
			if got == nil {
				t.Fatalf("Since(%d, %d) = nil, want a non-nil page", tc.after, tc.limit)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("Since(%d, %d) = %d events, want %d", tc.after, tc.limit, len(got), len(tc.want))
			}
			for i, e := range got {
				if e.Seq != tc.want[i] || e.RoomID != tc.room || e.Type != events.CommentAdded {
					t.Fatalf("Since(%d, %d)[%d] = %s/%s/%d, want %s/%s/%d", tc.after, tc.limit, i,
						e.RoomID, e.Type, e.Seq, tc.room, events.CommentAdded, tc.want[i])
				}
			}
		})
	}

	for _, tc := range []struct {
		name  string
		room  string
		after int64
	}{
		{"never created", missing, 0},
		{"never created, after a seq", missing, 3},
		{"id that cannot be stored", "st_bad\x00id", 0},
	} {
		t.Run("unknown room: "+tc.name, func(t *testing.T) {
			if got, err := s.Since(ctx, tc.room, tc.after, 10); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("Since = %v, %v; want ErrNotFound", got, err)
			}
		})
	}

	t.Run("deleted room", func(t *testing.T) {
		deleteRoom(ctx, t, s, full)
		for _, after := range []int64{0, n} {
			if got, err := s.Since(ctx, full, after, 10); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("Since(after %d) = %v, %v; want ErrNotFound", after, got, err)
			}
		}
		// Another room is untouched.
		if got, err := s.Since(ctx, empty, 0, 10); err != nil || len(got) != 0 {
			t.Fatalf("Since(other room) = %v, %v", got, err)
		}
	})
}

// RunSubscribeEndsOnDelete checks that a live subscription's channel closes
// once its room is deleted, within the given time (Postgres notices at its
// room feed's next read: an append notification or a poll).
func RunSubscribeEndsOnDelete(t *testing.T, s Store, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout+within)
	defer cancel()
	id := newRoom(ctx, t, s)
	appendComment(ctx, t, s, id)
	ch, err := s.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case e, ok := <-ch:
		if !ok || e.Seq != 1 {
			t.Fatalf("first event = %+v (open %v), want seq 1", e, ok)
		}
	case <-time.After(within):
		t.Fatal("no replayed event")
	}
	deleteRoom(ctx, t, s, id)
	deadline := time.After(within)
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			t.Fatalf("event after the delete: %+v", e)
		case <-deadline:
			t.Fatalf("the subscription is still open %s after the room was deleted", within)
		}
	}
}

// roomID makes an id no other test uses (st_<random>).
func roomID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b) // never returns an error (crypto/rand docs)
	return "st_" + hex.EncodeToString(b)
}

func newRoom(ctx context.Context, t *testing.T, s Store) string {
	t.Helper()
	id := roomID()
	if err := s.CreateRoom(ctx, store.Room{
		ID: id, Title: "storetest", Severity: "sev3", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create room: %v", err)
	}
	return id
}

func appendComment(ctx context.Context, t *testing.T, s Store, roomID string) {
	t.Helper()
	if _, err := s.Append(ctx, roomID, events.CommentAdded,
		events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"},
		events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func deleteRoom(ctx context.Context, t *testing.T, s Store, roomID string) {
	t.Helper()
	if _, _, err := s.DeleteRoom(ctx, store.RoomDeletion{
		RoomID: roomID, DeletedBy: "storetest", DeletedAt: time.Now().UTC(), Reason: "conformance",
	}, -1); err != nil {
		t.Fatalf("delete room: %v", err)
	}
}
