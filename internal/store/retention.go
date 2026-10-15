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

// PurgeRoomTimeout bounds the archive and the transaction that purge one room.
const PurgeRoomTimeout = 2 * time.Minute

// ErrRoomChanged is returned (wrapped) when a room's log grew after it was
// archived: the delete is skipped so the archive never misses an event.
var ErrRoomChanged = errors.New("store: room changed after it was archived")

// ErrRoomExecuting is returned (wrapped) by DeleteRoom while an executor on
// any replica holds the claim of one of the room's open proposals: deleting
// would erase the step rows it writes, and its action would land with no
// action.executed.
var ErrRoomExecuting = errors.New("store: a fix is running in the room")

var _ Purger = (*PG)(nil)

// PurgeClosedBefore purges closed rooms of every pipeline, with no archive. It
// is PurgeClosed with only Cutoff, MaxRooms and Exclude set.
func (s *PG) PurgeClosedBefore(ctx context.Context, cutoff time.Time, maxRooms int, exclude []string) (PurgeResult, error) {
	return s.PurgeClosed(ctx, PurgeQuery{Cutoff: cutoff, MaxRooms: maxRooms, Exclude: exclude})
}

// PurgeClosed implements Purger.
func (s *PG) PurgeClosed(ctx context.Context, q PurgeQuery) (PurgeResult, error) {
	var res PurgeResult
	if q.MaxRooms <= 0 {
		return res, nil
	}
	ids, err := s.expiredRoomIDs(ctx, q)
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
		one, ok, err := s.purgeOne(ctx, id, q)
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

// purgeOne archives (when asked) and purges one room under its own budget,
// so one huge or stuck room cannot use up the whole run.
func (s *PG) purgeOne(ctx context.Context, id string, q PurgeQuery) (PurgeResult, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, PurgeRoomTimeout)
	defer cancel()
	wantSeq := int64(-1)
	if q.Archive != nil {
		seq, err := q.Archive(ctx, id)
		if err != nil {
			return PurgeResult{}, false, fmt.Errorf("store: purge %s: archive: %w", id, err)
		}
		wantSeq = seq
	}
	return s.purgeRoom(ctx, id, q, wantSeq)
}

func (s *PG) expiredRoomIDs(ctx context.Context, q PurgeQuery) ([]string, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	exclude := q.Exclude
	if exclude == nil {
		// A nil slice reaches Postgres as NULL, and id <> ALL(NULL) is NULL,
		// which would match no row at all.
		exclude = []string{}
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM rooms WHERE status = 'closed' AND closed_at IS NOT NULL AND closed_at < $1
		 AND id <> ALL($3::text[]) AND ($4 = '' OR pipeline_id = $4)
		 ORDER BY closed_at ASC, id ASC LIMIT $2`, q.Cutoff, q.MaxRooms, exclude, q.PipelineID)
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

// purgeRoom deletes one room and everything keyed by it, and writes its
// content-free room_deletions row (q.audit), in one transaction. ok is false
// when the room no longer qualifies (already gone, or not closed before the
// cutoff), which is not an error. wantSeq >= 0 is the last seq the archive
// covered: a room whose log has moved on is left (ErrRoomChanged).
func (s *PG) purgeRoom(ctx context.Context, id string, q PurgeQuery, wantSeq int64) (PurgeResult, bool, error) {
	var res PurgeResult
	found := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock the row and re-check: it also makes a late Append wait, then
		// fail with not-found, instead of writing into a half-deleted room.
		var (
			lastSeq  int64
			pipeline string
		)
		err := tx.QueryRow(ctx,
			`SELECT last_seq, pipeline_id FROM rooms WHERE id = $1 AND status = 'closed' AND closed_at IS NOT NULL AND closed_at < $2 FOR UPDATE`,
			id, q.Cutoff).Scan(&lastSeq, &pipeline)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("store: purge %s: lock: %w", id, err)
		}
		if wantSeq >= 0 && lastSeq != wantSeq {
			return fmt.Errorf("store: purge %s: archived %d events, room has %d: %w", id, wantSeq, lastSeq, ErrRoomChanged)
		}
		found = true
		if res, err = deleteRoomTx(ctx, tx, id); err != nil {
			return err
		}
		d := q.Audit(id, pipeline, res.Events, s.now)
		if _, err := tx.Exec(ctx,
			`INSERT INTO room_deletions (room_id, pipeline_id, deleted_by, deleted_at, reason, event_count) VALUES ($1, $2, $3, $4, $5, $6)`,
			d.RoomID, d.PipelineID, d.DeletedBy, d.DeletedAt, d.Reason, d.EventCount); err != nil {
			return fmt.Errorf("store: purge %s: audit row: %w", id, classify(err))
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

// deleteRoomTx deletes a locked room and every row keyed by it, and counts
// them. The caller holds the room row lock in tx.
func deleteRoomTx(ctx context.Context, tx pgx.Tx, id string) (PurgeResult, error) {
	var res PurgeResult
	for {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("store: purge %s: %w", id, err)
		}
		tag, err := tx.Exec(ctx,
			`DELETE FROM room_events WHERE ctid IN (SELECT ctid FROM room_events WHERE room_id = $1 LIMIT $2)`,
			id, purgeEventBatch)
		if err != nil {
			return res, fmt.Errorf("store: purge %s: delete events: %w", id, err)
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
			return res, fmt.Errorf("store: purge %s: %w", id, err)
		}
		*c.n += tag.RowsAffected()
	}
	if _, err := tx.Exec(ctx, `DELETE FROM rooms WHERE id = $1`, id); err != nil {
		return res, fmt.Errorf("store: purge %s: delete room: %w", id, err)
	}
	// Same transaction as the delete, so other replicas end this room's live
	// subscriptions at once instead of at their next safety poll.
	if _, err := tx.Exec(ctx, `SELECT pg_notify('room_events', $1)`, id+deleteNotifySuffix); err != nil {
		return res, fmt.Errorf("store: purge %s: notify: %w", id, err)
	}
	return res, nil
}

var _ Deleter = (*PG)(nil)

// DeleteRoom implements Deleter.
func (s *PG) DeleteRoom(ctx context.Context, d RoomDeletion, wantSeq int64) (RoomDeletion, PurgeResult, error) {
	if err := CheckText(d.RoomID, d.DeletedBy, d.Reason); err != nil {
		return RoomDeletion{}, PurgeResult{}, fmt.Errorf("store: delete room: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, PurgeRoomTimeout)
	defer cancel()
	var res PurgeResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var lastSeq int64
		err := tx.QueryRow(ctx, `SELECT pipeline_id, last_seq FROM rooms WHERE id = $1 FOR UPDATE`, d.RoomID).Scan(&d.PipelineID, &lastSeq)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("store: delete room %s: %w", d.RoomID, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("store: delete room %s: lock: %w", d.RoomID, err)
		}
		if wantSeq >= 0 && lastSeq != wantSeq {
			return fmt.Errorf("store: delete room %s: archived %d events, room has %d: %w", d.RoomID, wantSeq, lastSeq, ErrRoomChanged)
		}
		// The service's busy check sees only its own replica's executor; the
		// claim is the record every replica shares. A claim committed after
		// this read loses its row to the delete below, so its executor's
		// StartClaimedStep fails with ErrClaimLost before any action runs.
		var executing bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM proposals WHERE room_id = $1 AND status = 'open' AND claimed_by IS NOT NULL)`,
			d.RoomID).Scan(&executing); err != nil {
			return fmt.Errorf("store: delete room %s: check claims: %w", d.RoomID, err)
		}
		if executing {
			return fmt.Errorf("store: delete room %s: %w", d.RoomID, ErrRoomExecuting)
		}
		res, err = deleteRoomTx(ctx, tx, d.RoomID)
		if err != nil {
			return err
		}
		d.EventCount = res.Events
		// timestamptz keeps microseconds: the row returned now must equal the
		// row read back by GetRoomDeletion.
		d.DeletedAt = d.DeletedAt.UTC().Truncate(time.Microsecond)
		if _, err := tx.Exec(ctx,
			`INSERT INTO room_deletions (room_id, pipeline_id, deleted_by, deleted_at, reason, event_count) VALUES ($1, $2, $3, $4, $5, $6)`,
			d.RoomID, d.PipelineID, d.DeletedBy, d.DeletedAt, d.Reason, d.EventCount); err != nil {
			return fmt.Errorf("store: delete room %s: audit row: %w", d.RoomID, classify(err))
		}
		return nil
	})
	if err != nil {
		return RoomDeletion{}, PurgeResult{}, err
	}
	res.RoomIDs = []string{d.RoomID}
	res.Examined = 1
	return d, res, nil
}

// GetRoomDeletion implements Deleter.
func (s *PG) GetRoomDeletion(ctx context.Context, roomID string) (RoomDeletion, error) {
	if CheckText(roomID) != nil {
		return RoomDeletion{}, fmt.Errorf("store: room deletion: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var d RoomDeletion
	err := s.pool.QueryRow(ctx,
		`SELECT room_id, pipeline_id, deleted_by, deleted_at, reason, event_count FROM room_deletions WHERE room_id = $1`, roomID).
		Scan(&d.RoomID, &d.PipelineID, &d.DeletedBy, &d.DeletedAt, &d.Reason, &d.EventCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return RoomDeletion{}, fmt.Errorf("store: room deletion %s: %w", roomID, ErrNotFound)
	}
	if err != nil {
		return RoomDeletion{}, fmt.Errorf("store: room deletion %s: %w", roomID, err)
	}
	d.DeletedAt = d.DeletedAt.UTC()
	return d, nil
}
