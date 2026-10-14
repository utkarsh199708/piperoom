package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// MaxRetentionDays bounds PIPEROOM_RETENTION_DAYS (100 years). It keeps the
// cutoff arithmetic far from overflow; no real policy is longer.
const MaxRetentionDays = 36500

// purgeEventBatch is how many room_events rows one DELETE statement removes.
// The statements all run in the room's one transaction; the batching keeps each
// statement short and lets a shutdown stop between them.
const purgeEventBatch = 5000

// ParseRetentionDays turns PIPEROOM_RETENTION_DAYS into days. Empty or "0"
// means keep everything (0). Anything that is not a plain non-negative integer
// is an error: a typo must stop the server, never silently keep (or delete)
// data. Signs, inner spaces, decimals and units are rejected.
func ParseRetentionDays(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("store: retention days %q: want an integer >= 0", s)
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n > MaxRetentionDays {
		return 0, fmt.Errorf("store: retention days %q: want an integer from 0 to %d", s, MaxRetentionDays)
	}
	return n, nil
}

// PurgeRoomTimeout bounds the transaction that purges one room.
const PurgeRoomTimeout = 2 * time.Minute

var _ Purger = (*PG)(nil)

// PurgeClosedBefore implements Purger.
func (s *PG) PurgeClosedBefore(ctx context.Context, cutoff time.Time, maxRooms int, exclude []string) (PurgeResult, error) {
	var res PurgeResult
	if maxRooms <= 0 {
		return res, nil
	}
	ids, err := s.expiredRoomIDs(ctx, cutoff, maxRooms, exclude)
	if err != nil {
		return res, err
	}
	res.Examined = len(ids)
	for _, id := range ids {
		// Only the whole call being cancelled stops the loop. A failure of one
		// room (including its own timeout) is recorded and the next room goes on:
		// otherwise one bad room at the front of the queue would block retention
		// for every room behind it, on every run.
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("store: purge: %w", err)
		}
		one, ok, err := s.purgeRoom(ctx, id, cutoff)
		if err != nil {
			if ctx.Err() != nil {
				return res, fmt.Errorf("store: purge: %w", ctx.Err())
			}
			res.Failed = append(res.Failed, PurgeFailure{RoomID: id, Err: err})
			continue
		}
		if ok {
			res.Add(one)
		}
	}
	return res, nil
}

func (s *PG) expiredRoomIDs(ctx context.Context, cutoff time.Time, limit int, exclude []string) ([]string, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if exclude == nil {
		// A nil slice reaches Postgres as NULL, and id <> ALL(NULL) is NULL,
		// which would match no row at all.
		exclude = []string{}
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM rooms WHERE status = 'closed' AND closed_at IS NOT NULL AND closed_at < $1
		 AND id <> ALL($3::text[])
		 ORDER BY closed_at ASC, id ASC LIMIT $2`, cutoff, limit, exclude)
	if err != nil {
		return nil, fmt.Errorf("store: purge: list expired rooms: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: purge: scan room id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: purge: list expired rooms: %w", err)
	}
	return ids, nil
}

// purgeRoom deletes one room and everything keyed by it in one transaction.
// ok is false when the room no longer qualifies (already gone, or not closed
// before cutoff), which is not an error.
func (s *PG) purgeRoom(ctx context.Context, id string, cutoff time.Time) (PurgeResult, bool, error) {
	var res PurgeResult
	// Each room gets its own budget under the run's deadline, so one huge or
	// stuck room cannot use up the whole run.
	ctx, cancel := context.WithTimeout(ctx, PurgeRoomTimeout)
	defer cancel()
	found := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock the row and re-check: it also makes a late Append wait, then
		// fail with not-found, instead of writing into a half-deleted room.
		var locked string
		err := tx.QueryRow(ctx,
			`SELECT id FROM rooms WHERE id = $1 AND status = 'closed' AND closed_at IS NOT NULL AND closed_at < $2 FOR UPDATE`,
			id, cutoff).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("store: purge %s: lock: %w", id, err)
		}
		found = true

		for {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("store: purge %s: %w", id, err)
			}
			tag, err := tx.Exec(ctx,
				`DELETE FROM room_events WHERE ctid IN (SELECT ctid FROM room_events WHERE room_id = $1 LIMIT $2)`,
				id, purgeEventBatch)
			if err != nil {
				return fmt.Errorf("store: purge %s: delete events: %w", id, err)
			}
			res.Events += tag.RowsAffected()
			if tag.RowsAffected() < purgeEventBatch {
				break
			}
		}

		// Children are deleted explicitly (and counted); the ON DELETE CASCADE
		// foreign keys are the backstop for any table added later.
		children := []struct {
			sql string
			n   *int64
		}{
			{`DELETE FROM approvals WHERE proposal_id IN (SELECT proposal_id FROM proposals WHERE room_id = $1)`, &res.Approvals},
			{`DELETE FROM proposal_steps WHERE proposal_id IN (SELECT proposal_id FROM proposals WHERE room_id = $1)`, &res.Steps},
			{`DELETE FROM proposals WHERE room_id = $1`, &res.Proposals},
			{`DELETE FROM suggestions WHERE room_id = $1`, &res.Suggestions},
			{`DELETE FROM handoffs WHERE room_id = $1`, &res.Handoffs},
			{`DELETE FROM slack_threads WHERE room_id = $1`, &res.SlackThreads},
			// Shadow events before their run row (the run row is their parent).
			{`DELETE FROM shadow_events WHERE room_id = $1`, &res.ShadowEvents},
			{`DELETE FROM shadow_runs WHERE room_id = $1`, &res.ShadowRuns},
		}
		for _, c := range children {
			tag, err := tx.Exec(ctx, c.sql, id)
			if err != nil {
				return fmt.Errorf("store: purge %s: %w", id, err)
			}
			*c.n += tag.RowsAffected()
		}
		if _, err := tx.Exec(ctx, `DELETE FROM rooms WHERE id = $1`, id); err != nil {
			return fmt.Errorf("store: purge %s: delete room: %w", id, err)
		}
		return nil
	})
	if err != nil {
		return PurgeResult{}, false, err
	}
	if !found {
		return PurgeResult{}, false, nil
	}
	res.RoomIDs = []string{id}
	return res, true, nil
}
