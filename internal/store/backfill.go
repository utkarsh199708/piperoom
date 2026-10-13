package store

import (
	"context"
	"fmt"
	"strconv"
)

// byteText prints a nullable byte_size for an error message.
func byteText(b *int64) string {
	if b == nil {
		return "uncounted"
	}
	return strconv.FormatInt(*b, 10)
}

// BackfillBatch is how many rooms (or shadow runs) one backfill UPDATE counts.
// Each batch is its own short transaction that locks only its own rows, so
// appends to other rooms never wait on the backfill.
const BackfillBatch = 500

// backfillLockKey is the advisory lock that lets only one replica backfill at
// a time ("piperoom byte_size backfill").
const backfillLockKey int64 = 0x7069706572_00012

// BackfillResult counts what one BackfillByteSize call counted.
type BackfillResult struct {
	Rooms      int64
	ShadowRuns int64
	Skipped    bool // another process holds the backfill lock
}

// BackfillByteSize counts byte_size for the rooms and shadow runs that predate
// migration 00012 (byte_size IS NULL), batch rows per transaction, until none
// is left or ctx ends. It runs in the background after start rather than in
// the migration, so the migration holds its lock on rooms for an instant and
// no start waits on a large table (docs/design/retention.md section 5.2).
// Until a room is counted its byte cap is not enforced; its event cap (on
// last_seq) is.
//
// Only one process backfills at a time (a session advisory lock); a second one
// returns Skipped. The count is the jsonb text size of the stored payloads,
// an estimate of the written size within the bound stated in the migration.
// An append that commits while its room's batch runs may be left out of that
// room's count; the error is that event's size.
func (s *PG) BackfillByteSize(ctx context.Context, batch int) (BackfillResult, error) {
	var res BackfillResult
	if batch <= 0 {
		batch = BackfillBatch
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return res, fmt.Errorf("store: backfill byte_size: acquire: %w", err)
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, backfillLockKey).Scan(&got); err != nil {
		return res, fmt.Errorf("store: backfill byte_size: lock: %w", err)
	}
	if !got {
		res.Skipped = true
		return res, nil
	}
	defer func() {
		// A fresh context: ctx may be why the loop stopped.
		uctx, cancel := dbCtx(context.WithoutCancel(ctx))
		defer cancel()
		_, _ = conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, backfillLockKey)
	}()
	steps := []struct {
		sql string
		n   *int64
	}{
		{`UPDATE rooms r SET byte_size = COALESCE((SELECT sum(octet_length(e.payload::text)) FROM room_events e WHERE e.room_id = r.id), 0)
		  WHERE r.id IN (SELECT id FROM rooms WHERE byte_size IS NULL ORDER BY id LIMIT $1 FOR UPDATE)`, &res.Rooms},
		{`UPDATE shadow_runs r SET byte_size = COALESCE((SELECT sum(octet_length(e.payload::text)) FROM shadow_events e WHERE e.room_id = r.room_id), 0)
		  WHERE r.room_id IN (SELECT room_id FROM shadow_runs WHERE byte_size IS NULL ORDER BY room_id LIMIT $1 FOR UPDATE)`, &res.ShadowRuns},
	}
	for _, st := range steps {
		for {
			if err := ctx.Err(); err != nil {
				return res, fmt.Errorf("store: backfill byte_size: %w", err)
			}
			tag, err := conn.Exec(ctx, st.sql, batch)
			if err != nil {
				return res, fmt.Errorf("store: backfill byte_size: %w", err)
			}
			*st.n += tag.RowsAffected()
			if tag.RowsAffected() == 0 {
				break
			}
		}
	}
	return res, nil
}
