//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"piperoom/internal/config"
	"piperoom/internal/events"
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
// 00009 (before the counters existed), runs the rest of the migrations, and
// checks event_count = last_seq and that byte_size is within the stated bound
// of the bytes the store would have counted:
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

	inBound := func(name string, got int64, w *writtenStats) {
		t.Helper()
		lo, hi := w.bytes-5*w.escaped, w.bytes+w.colons
		if got < lo || got > hi {
			t.Fatalf("%s: byte_size %d outside [%d, %d] (written %d, ':'/',' %d, escaped %d)", name, got, lo, hi, w.bytes, w.colons, w.escaped)
		}
		t.Logf("%s: byte_size %d, written %d, bound [%d, %d]", name, got, w.bytes, lo, hi)
	}
	for id, w := range want {
		var count, lastSeq, size int64
		if err := conn.QueryRow(ctx, `SELECT event_count, last_seq, byte_size FROM `+pgx.Identifier{schema, "rooms"}.Sanitize()+` WHERE id = $1`, id).
			Scan(&count, &lastSeq, &size); err != nil {
			t.Fatal(err)
		}
		if count != lastSeq || count != int64(len(rooms[id])) {
			t.Fatalf("%s: event_count %d, last_seq %d, want both %d", id, count, lastSeq, len(rooms[id]))
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
	var shadowSize int64
	if err := conn.QueryRow(ctx, `SELECT byte_size FROM `+pgx.Identifier{schema, "shadow_runs"}.Sanitize()+` WHERE room_id = 'plain'`).Scan(&shadowSize); err != nil {
		t.Fatal(err)
	}
	inBound("shadow", shadowSize, shadow)
}
