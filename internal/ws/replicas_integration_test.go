//go:build integration

package ws_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// replicaSchema makes a migrated throwaway schema (dropped at cleanup), or
// skips when the room database is unreachable.
func replicaSchema(t *testing.T) string {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("ws_replicas")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		if c, err := pgx.Connect(dctx, cfg.RoomDBURL); err == nil {
			_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
			_ = c.Close(dctx)
		}
	})
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cc)
	defer db.Close()
	if _, err := migrations.Up(ctx, db); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return schema
}

// replica opens one server replica (its own pool and room service) on schema.
func replica(t *testing.T, schema, name string, opts store.Options) (*store.PG, *room.Service) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pc, err := pgxpool.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewPoolOptions(pool, cfg.RoomDBURL, slog.Default(), opts)
	t.Cleanup(st.Close)
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, ReplicaID: name})
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	return st, svc
}

// A WebSocket client connected to replica B receives every event replica A
// writes, in order and without gaps: through NOTIFY, and with B's LISTEN off
// through the 2 s-style poll fallback alone.
func TestWebSocketAcrossReplicas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bOpts  store.Options
		within time.Duration
	}{
		{"notify", store.Options{}, 5 * time.Second},
		{"poll fallback", store.Options{NoListen: true, PollInterval: 300 * time.Millisecond}, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := replicaSchema(t)
			_, a := replica(t, schema, "rA", store.Options{})
			stB, b := replica(t, schema, "rB", tc.bOpts)
			srvB := httptest.NewServer(httpapi.New(b, stB, nil))
			t.Cleanup(srvB.Close)

			ctx := context.Background()
			arjun, _ := a.LookupUser("arjun")
			snap, err := a.CreateRoom(ctx, arjun, "lag", "sev2", "orders")
			if err != nil {
				t.Fatal(err)
			}
			conn := dialWS(t, srvB, snap.ID, "priya", 0)
			// The replay: everything A wrote so far, from seq 1.
			last := int64(0)
			for last < snap.LastSeq {
				e := readEvent(t, conn)
				if e.Seq != last+1 {
					t.Fatalf("replay seq %d after %d", e.Seq, last)
				}
				last = e.Seq
			}
			// Live: A writes, B's client reads.
			const n = 25
			go func() {
				for i := 0; i < n; i++ {
					if _, err := a.AddComment(ctx, arjun, snap.ID, "", fmt.Sprintf("from A %d", i)); err != nil {
						t.Errorf("comment on A: %v", err)
						return
					}
				}
			}()
			deadline := time.Now().Add(tc.within)
			for got := 0; got < n; got++ {
				if time.Now().After(deadline) {
					t.Fatalf("only %d of %d events from A reached B's client in %s", got, n, tc.within)
				}
				e := readEvent(t, conn)
				if e.Seq != last+1 || e.Type != events.CommentAdded {
					t.Fatalf("live event seq %d type %s after %d", e.Seq, e.Type, last)
				}
				last = e.Seq
			}
		})
	}
}
