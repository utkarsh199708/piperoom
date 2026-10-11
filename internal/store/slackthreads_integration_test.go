//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/slackbridge"
	"piperoom/internal/store/storetest"
)

// TestSlackThreadsPG covers get, put, upsert and a room with no thread against
// the real slack_threads table. It only touches rooms named it_<random>.
func TestSlackThreadsPG(t *testing.T) {
	pg, tr := newStore(t)
	th := pg.SlackThreads()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	id := newRoom(t, tr)
	// Registered after newStore, so it runs before the rooms are deleted
	// (slack_threads references rooms).
	t.Cleanup(func() {
		conn := admin(t)
		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := conn.Exec(c, `DELETE FROM slack_threads WHERE room_id = $1`, id); err != nil {
			t.Errorf("cleanup slack_threads: %v", err)
		}
	})

	if _, ok, err := th.Get(ctx, id); err != nil || ok {
		t.Fatalf("Get before Put = ok %v, err %v; want ok=false", ok, err)
	}
	if _, ok, err := th.Get(ctx, storetest.NewRoomID()); err != nil || ok {
		t.Fatalf("Get for an unknown room = ok %v, err %v; want ok=false", ok, err)
	}

	first := slackbridge.Thread{Channel: "C1", TS: "1700000000.000100", LastSeq: 1}
	if err := th.Put(ctx, id, first); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := th.Get(ctx, id)
	if err != nil || !ok || got != first {
		t.Fatalf("Get after Put = %+v, %v, %v; want %+v", got, ok, err, first)
	}

	second := slackbridge.Thread{Channel: "C1", TS: "1700000000.000100", LastSeq: 7}
	if err := th.Put(ctx, id, second); err != nil {
		t.Fatalf("Put (upsert): %v", err)
	}
	got, ok, err = th.Get(ctx, id)
	if err != nil || !ok || got != second {
		t.Fatalf("Get after upsert = %+v, %v, %v; want %+v", got, ok, err, second)
	}

	// The foreign key keeps thread rows from pointing at rooms that do not exist.
	if err := th.Put(ctx, storetest.NewRoomID(), first); err == nil {
		t.Fatal("Put for a room that does not exist succeeded")
	}
}
