//go:build integration

package room

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

func countOn(t *testing.T, schema, q string, args ...any) int {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	c, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	var n int
	if err := c.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// Requests with one client_id racing on two replicas append exactly one
// event (the unique index decides); every loser answers the winner's event,
// and a losing suggestion leaves no row (docs/api.md, Idempotent posts).
func TestReplicasClientIDRace(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, nil)
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	roomID := snap.ID
	eventually(t, 5*time.Second, "room on r1", func() bool { _, err := b.GetRoom(ctx, roomID); return err == nil })

	cases := []struct {
		name, user, key string
		comment         bool
		want            events.Type
	}{
		{"comment", "priya", "c1", true, events.CommentAdded},
		{"suggestion", "priya", "g1", false, events.SteerSuggested},
		{"driver steer", "arjun", "d1", false, events.SteerSent},
	}
	for _, c := range cases {
		const n = 12
		var wg sync.WaitGroup
		got := make([]events.Event, n)
		errs := make([]error, n)
		for i := range n {
			svc := a
			if i%2 == 1 {
				svc = b
			}
			u := user(t, svc, c.user)
			wg.Add(1)
			go func() {
				defer wg.Done()
				if c.comment {
					got[i], errs[i] = svc.AddCommentKeyed(ctx, u, roomID, "", "same text", c.key)
				} else {
					got[i], errs[i] = svc.SteerKeyed(ctx, u, roomID, "same text", c.key)
				}
			}()
		}
		wg.Wait()
		for i := range n {
			if errs[i] != nil {
				t.Fatalf("%s: request %d: %v", c.name, i, errs[i])
			}
			if got[i].Seq != got[0].Seq || got[i].Type != c.want {
				t.Fatalf("%s: request %d answered seq %d %s, want seq %d %s", c.name, i, got[i].Seq, got[i].Type, got[0].Seq, c.want)
			}
		}
		if k := countOn(t, schema, `SELECT count(*) FROM room_events WHERE room_id = $1 AND payload->>'client_id' = $2`, roomID, c.key); k != 1 {
			t.Fatalf("%s: %d events with the key, want 1", c.name, k)
		}
	}
	if k := countOn(t, schema, `SELECT count(*) FROM suggestions WHERE room_id = $1`, roomID); k != 1 {
		t.Fatalf("suggestion rows: %d, want 1 (losers compensated)", k)
	}
	// The same key with another body on the other replica: 409, nothing added.
	var re *Error
	if _, err := b.SteerKeyed(ctx, user(t, b, "priya"), roomID, "other text", "c1"); !errors.As(err, &re) || re.Code != "client_id_reused" {
		t.Fatalf("reuse on r1: %v", err)
	}
	last := countOn(t, schema, `SELECT last_seq::int FROM rooms WHERE id = $1`, roomID)
	if k := countOn(t, schema, `SELECT count(*) FROM room_events WHERE room_id = $1`, roomID); k != last {
		t.Fatalf("gap: %d events, last_seq %d", k, last)
	}
}
