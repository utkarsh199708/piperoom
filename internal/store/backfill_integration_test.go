//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// writtenStats is what the store would have counted for a payload (its
// json.Marshal size) and the two terms of the backfill bound stated in
// migrations/00012_room_limits.sql.
type writtenStats struct {
	bytes   int64 // len(json.Marshal(payload))
	colons  int64 // ':' and ',' in it: jsonb text adds a space after each structural one
	escaped int64 // <, > and & that Go writes as six-byte unicode escapes: jsonb text writes each as one byte
}

func (w *writtenStats) add(raw []byte) {
	s := string(raw)
	w.bytes += int64(len(raw))
	w.colons += int64(strings.Count(s, ":") + strings.Count(s, ","))
	w.escaped += int64(strings.Count(s, "\\u003c") + strings.Count(s, "\\u003e") + strings.Count(s, "\\u0026"))
}

// TestBackfillByteSizeWithinBound seeds rooms and a shadow log at migration
// 00009 (before the counters existed) and runs the rest of the migrations.
// Their byte_size is NULL then: the event cap (last_seq) holds, the byte cap
// does not, and a second backfiller finds the lock taken. After
// BackfillByteSize, byte_size is within the stated bound of the bytes the
// store would have counted:
// written - 5 x escaped <= byte_size <= written + (':' and ',').
func TestBackfillByteSizeWithinBound(t *testing.T) {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("schema")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(func() { dropSchema(cfg.RoomDBURL, schema) })

	sqlCfg, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	sqlCfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*sqlCfg)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 9); err != nil {
		t.Fatalf("migrate to 00009: %v", err)
	}

	human := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	actor, _ := json.Marshal(human)
	rooms := map[string][]any{
		// No characters Go escapes: the backfill over-counts by the spaces.
		"plain": {
			events.RoomCreatedPayload{Title: "orders stale", Severity: "sev2", PipelineID: "orders", Alert: json.RawMessage(`{"a":[1,2,{"b":"c"}],"d":true}`)},
			events.CommentAddedPayload{CommentID: "c_1", Text: "lag is 1204 msgs; é ünïcode", MentionsAgent: true},
			events.AgentToolResultPayload{StepID: "s_1", Tool: "consumer_lag", Summary: "lag 1204", Data: json.RawMessage(`{"partitions":[{"p":0,"lag":600},{"p":1,"lag":604}]}`), TookMS: 12},
		},
		// <, > and & are written by Go as < and the like, which jsonb
		// prints as one byte: here the backfill can come out under.
		"escaped": {
			events.CommentAddedPayload{CommentID: "c_2", Text: "a<b && c>d <<<>>> &&&&&&&&"},
			events.AgentMessagePayload{Text: "<script>&amp;</script>"},
		},
	}
	want := map[string]*writtenStats{}
	for id, payloads := range rooms {
		if _, err := conn.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "rooms"}.Sanitize()+
			` (id, title, severity, pipeline_id, status, opened_at, last_seq) VALUES ($1, 't', 'sev2', 'orders', 'open', now(), $2)`,
			id, len(payloads)); err != nil {
			t.Fatal(err)
		}
		w := &writtenStats{}
		for i, p := range payloads {
			raw, _ := json.Marshal(p)
			w.add(raw)
			if _, err := conn.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "room_events"}.Sanitize()+
				` (room_id, seq, type, actor, payload, at) VALUES ($1, $2, 'x', $3, $4, now())`,
				id, i+1, actor, raw); err != nil {
				t.Fatal(err)
			}
		}
		want[id] = w
	}
	// A shadow log on the plain room.
	if _, err := conn.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "shadow_runs"}.Sanitize()+` (room_id, status, last_seq) VALUES ('plain', 'running', 2)`); err != nil {
		t.Fatal(err)
	}
	shadow := &writtenStats{}
	agentActor, _ := json.Marshal(events.AgentActor)
	for i, p := range []any{
		events.AgentHypothesisPayload{Text: "schema change on public.orders", BasedOnSteps: []string{"s_1", "s_2"}, Confidence: "high"},
		events.AgentMessagePayload{Text: "checked: connector failed -> registry rejected"},
	} {
		raw, _ := json.Marshal(p)
		shadow.add(raw)
		if _, err := conn.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "shadow_events"}.Sanitize()+
			` (room_id, seq, type, actor, payload, at) VALUES ('plain', $1, 'x', $2, $3, now())`, i+1, agentActor, raw); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("migrate to the end: %v", err)
	}

	// The migration counted nothing: every old row is NULL.
	for _, tbl := range []string{"rooms", "shadow_runs"} {
		var n int64
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{schema, tbl}.Sanitize()+` WHERE byte_size IS NOT NULL`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("%s: %d rows counted by the migration, want 0", tbl, n)
		}
	}

	s := pinnedStore(ctx, t, cfg.RoomDBURL, schema)
	// Uncounted: the event cap holds, the byte cap does not.
	s.SetLimits(store.Limits{MaxEvents: 3, MaxBytes: 1})
	if _, err := s.Append(ctx, "plain", events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "fourth"}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("agent event past the event cap of an uncounted room: %v, want ErrLogFull", err)
	}
	extra := events.AgentMessagePayload{Text: "third, far over a 1-byte cap"}
	if _, err := s.Append(ctx, "escaped", events.AgentMessage, events.AgentActor, extra); err != nil {
		t.Fatalf("uncounted room under its event cap: %v, want stored (no byte cap yet)", err)
	}
	raw, _ := json.Marshal(extra)
	want["escaped"].add(raw)
	rooms["escaped"] = append(rooms["escaped"], extra)
	if u, err := s.RoomUsage(ctx, "escaped"); err != nil || !u.BytesUnknown || u.Events != 3 {
		t.Fatalf("usage of an uncounted room = %+v %v, want 3 events, bytes unknown", u, err)
	}
	s.SetLimits(store.Limits{})

	// Another replica holds the lock: this one skips.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, store.BackfillLockKey); err != nil {
		t.Fatal(err)
	}
	res, err := s.BackfillByteSize(ctx, 1)
	if err != nil || !res.Skipped || res.Rooms != 0 {
		t.Fatalf("backfill while locked = %+v %v, want skipped", res, err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, store.BackfillLockKey); err != nil {
		t.Fatal(err)
	}
	// Batch 1: every row takes its own batch, so the loop is exercised.
	res, err = s.BackfillByteSize(ctx, 1)
	if err != nil || res.Skipped || res.Rooms != 2 || res.ShadowRuns != 1 {
		t.Fatalf("backfill = %+v %v, want 2 rooms and 1 shadow run", res, err)
	}
	if res, err := s.BackfillByteSize(ctx, 0); err != nil || res.Rooms != 0 || res.ShadowRuns != 0 {
		t.Fatalf("second backfill = %+v %v, want nothing left", res, err)
	}

	inBound := func(name string, got int64, w *writtenStats) {
		t.Helper()
		lo, hi := w.bytes-5*w.escaped, w.bytes+w.colons
		if got < lo || got > hi {
			t.Fatalf("%s: byte_size %d outside [%d, %d] (written %d, ':'/',' %d, escaped %d)", name, got, lo, hi, w.bytes, w.colons, w.escaped)
		}
		t.Logf("%s: byte_size %d, written %d, bound [%d, %d]", name, got, w.bytes, lo, hi)
	}
	for id, w := range want {
		var lastSeq, size int64
		if err := conn.QueryRow(ctx, `SELECT last_seq, byte_size FROM `+pgx.Identifier{schema, "rooms"}.Sanitize()+` WHERE id = $1`, id).
			Scan(&lastSeq, &size); err != nil {
			t.Fatal(err)
		}
		if lastSeq != int64(len(rooms[id])) {
			t.Fatalf("%s: last_seq %d, want %d", id, lastSeq, len(rooms[id]))
		}
		inBound(id, size, w)
		switch id {
		case "plain":
			// The over-count is real: jsonb's spaces.
			if size <= w.bytes {
				t.Fatalf("plain: byte_size %d not above the written %d; the bound comment is stale", size, w.bytes)
			}
		case "escaped":
			if size >= w.bytes {
				t.Fatalf("escaped: byte_size %d not below the written %d; the bound comment is stale", size, w.bytes)
			}
		}
	}
	// Counted: the byte cap applies now.
	s.SetLimits(store.Limits{MaxBytes: 1})
	if _, err := s.Append(ctx, "escaped", events.AgentMessage, events.AgentActor, extra); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("counted room past its byte cap: %v, want ErrLogFull", err)
	}
	s.SetLimits(store.Limits{})
	var shadowSize int64
	if err := conn.QueryRow(ctx, `SELECT byte_size FROM `+pgx.Identifier{schema, "shadow_runs"}.Sanitize()+` WHERE room_id = 'plain'`).Scan(&shadowSize); err != nil {
		t.Fatal(err)
	}
	inBound("shadow", shadowSize, shadow)
}

// pinnedStore is a store on schema, which the caller migrated.
func pinnedStore(ctx context.Context, t *testing.T, url, schema string) *store.PG {
	t.Helper()
	poolCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewPool(pool, url, slog.Default())
	t.Cleanup(s.Close)
	return s
}
