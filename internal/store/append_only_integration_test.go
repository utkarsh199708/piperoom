//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Migration 00015: the database itself refuses to rewrite or remove room
// events; only the retention purge (and admin delete) may remove them.
func TestRoomEventsAppendOnlyInDB(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ctx := context.Background()
	roomID := createTestRoom(t, s)
	appendComment(ctx, t, s, roomID, 1)
	c := schemaConn(ctx, t, url, schema)

	for name, q := range map[string]string{
		"update":   `UPDATE room_events SET payload = '{"text":"rewritten"}' WHERE room_id = $1`,
		"delete":   `DELETE FROM room_events WHERE room_id = $1`,
		"cascade":  `DELETE FROM rooms WHERE id = $1`,
		"truncate": `TRUNCATE room_events`,
	} {
		args := []any{roomID}
		if name == "truncate" {
			args = nil
		}
		_, err := c.Exec(ctx, q, args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23001" {
			t.Fatalf("%s: want restrict_violation, got %v", name, err)
		}
	}
	// The purge flag is transaction-local: a later plain DELETE is refused again.
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('piperoom.purge', 'on', true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE room_events SET type = 'x' WHERE room_id = $1`, roomID); err == nil {
		t.Fatal("update with the purge flag: want refused")
	}
	_ = tx.Rollback(ctx)

	// An empty room (no events) can still be dropped by DeleteEmptyRoom.
	empty := createTestRoom(t, s)
	if err := s.DeleteEmptyRoom(ctx, empty); err != nil {
		t.Fatalf("delete empty room: %v", err)
	}

	// Retention purge still removes a closed room's events.
	if _, err := s.Append(ctx, roomID, events.RoomClosed, commentActor(), events.RoomClosedPayload{Resolution: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseRoom(ctx, roomID, time.Now().UTC().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeClosedBefore(ctx, time.Now().UTC().Add(-24*time.Hour), 10, nil)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(res.RoomIDs) != 1 || res.Events != 2 {
		t.Fatalf("purge result = %+v", res)
	}
	var n int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM room_events WHERE room_id = $1`, roomID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("events left after purge = %d, %v", n, err)
	}
}

// The byte cap counts what jsonb stores: 1e308 is 309 digits once stored, so
// a payload of such tokens is counted at that size, as the backfill
// (octet_length(payload::text)) would count it.
func TestByteSizeCountsJSONBRendering(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ctx := context.Background()
	roomID := createTestRoom(t, s)
	raw := json.RawMessage(`{"v":[` + strings.TrimSuffix(strings.Repeat("1e308,", 100), ",") + `]}`)
	if _, err := s.Append(ctx, roomID, events.AgentToolResult, events.AgentActor, raw); err != nil {
		t.Fatal(err)
	}
	u, err := s.RoomUsage(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	var stored int64
	if err := schemaConn(ctx, t, url, schema).QueryRow(ctx, `SELECT octet_length(payload::text) FROM room_events WHERE room_id = $1`, roomID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if u.Bytes < 100*309 || u.Bytes > stored {
		t.Fatalf("byte_size = %d, want between %d and the stored %d", u.Bytes, 100*309, stored)
	}
	// With a byte cap, the expanded size is what is refused.
	s.SetLimits(store.Limits{MaxBytes: 10_000})
	if _, err := s.Append(ctx, roomID, events.AgentToolResult, events.AgentActor, raw); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("over the cap once expanded: want ErrLogFull, got %v", err)
	}
}
