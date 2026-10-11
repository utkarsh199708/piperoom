//go:build integration

// Package store_test integration tests run against the dev Postgres
// (dev/docker-compose.yml). Apply the migrations first:
//
//	goose -dir migrations postgres "$PIPEROOM_DB_URL" up
//
// They skip (never fail) when the room database is unreachable.
package store_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

// requireStore connects to the room database or skips the test.
func requireStore(t *testing.T) (*store.PG, string) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := store.New(ctx, cfg.RoomDBURL, slog.Default())
	if err != nil {
		t.Skipf("store: room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	t.Cleanup(s.Close)
	return s, cfg.RoomDBURL
}

func createTestRoom(t *testing.T, s *store.PG) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	roomID := testenv.Name("store")
	if err := s.CreateRoom(ctx, store.Room{
		ID: roomID, Title: "integration", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("store: create room: %v", err)
	}
	t.Cleanup(func() { deleteRoom(t, roomID) })
	return roomID
}

// deleteRoom removes the room and (via ON DELETE CASCADE) its events.
func deleteRoom(t *testing.T, roomID string) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, `DELETE FROM rooms WHERE id = $1`, roomID)
}

func commentActor() events.Actor {
	return events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
}

func appendComment(ctx context.Context, t *testing.T, s *store.PG, roomID string, i int) {
	t.Helper()
	_, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{
		CommentID: fmt.Sprintf("c_%d", i), Text: "hello",
	})
	if err != nil {
		t.Fatalf("store: append: %v", err)
	}
}

func TestAppendConcurrentNoGaps(t *testing.T) {
	s, _ := requireStore(t)
	roomID := createTestRoom(t, s)

	const n = 20
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{
				CommentID: fmt.Sprintf("c_%d", i), Text: "hello",
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	evs, err := s.Since(ctx, roomID, 0, 1000)
	if err != nil {
		t.Fatalf("store: since: %v", err)
	}
	if len(evs) != n {
		t.Fatalf("want %d events, got %d", n, len(evs))
	}
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq gap at index %d: got %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestSubscriberSeesEveryEventExactlyOnce(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	roomID := createTestRoom(t, s)

	// Five events exist before the subscriber connects.
	for i := 1; i <= 5; i++ {
		appendComment(ctx, t, s, roomID, i)
	}

	// Connect mid-stream: replay must deliver 3,4,5, then live 6..10.
	ch, err := s.Subscribe(ctx, roomID, 2)
	if err != nil {
		t.Fatalf("store: subscribe: %v", err)
	}

	for i := 6; i <= 10; i++ {
		appendComment(ctx, t, s, roomID, i)
	}

	deadline := time.After(10 * time.Second)
	var got []int64
	for len(got) < 8 {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("store: subscriber channel closed early (got %v)", got)
			}
			got = append(got, e.Seq)
		case <-deadline:
			t.Fatalf("store: timed out waiting for events (got %v)", got)
		}
	}

	for i, seq := range got {
		if want := int64(i + 3); seq != want {
			t.Fatalf("store: subscriber event %d: got seq %d, want %d (no skip/repeat)", i, seq, want)
		}
	}
}
