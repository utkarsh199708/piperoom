package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// byteText prints a nullable byte_size for an error message.
func byteText(b *int64) string {
	if b == nil {
		return "uncounted"
	}
	return strconv.FormatInt(*b, 10)
}

// BackfillBatch is how many uncounted rooms (or shadow runs) one backfill
// listing returns. The listing takes no row lock; each room is then counted
// and written on its own (pgBackfill.countOne).
const BackfillBatch = 500

// backfillLockKey is the advisory lock that lets only one replica backfill at
// a time ("piperoom byte_size backfill").
const backfillLockKey int64 = 0x7069706572_00012

// Each backfill statement runs under its own deadline, so a slow room or a
// stuck lock never holds the backfill (or the rows it touches) open-ended.
const (
	// backfillListTimeout bounds one listing (or re-read) of uncounted rows.
	backfillListTimeout = 10 * time.Second
	// backfillRoomTimeout bounds counting and writing one room. Its sum reads
	// without row locks; the guarded UPDATE is the only statement that locks.
	backfillRoomTimeout = 60 * time.Second
	// backfillLockTimeout is how long the guarded UPDATE waits for a room's
	// row lock before the room is left for later: an Append holding the row
	// wins, and the backfill holds the row only for that one UPDATE.
	backfillLockTimeout = "2s"
	// backfillStatementTimeout ends a long sum on the server before
	// backfillRoomTimeout cancels it from the client, so the connection (and
	// the advisory lock on it) survives and the room is just left for later.
	backfillStatementTimeout = "50s"
	// backfillRetries is how often a room is counted when its last_seq keeps
	// moving between the sum and the write, before it is left for the next
	// start.
	backfillRetries = 3
)

// BackfillResult counts what one BackfillByteSize call counted.
type BackfillResult struct {
	Rooms      int64
	ShadowRuns int64
	// Left counts the rows this call did not count (their lock was not free
	// in time, their sum ran past its deadline, or appends kept moving them);
	// they stay NULL and the next start counts them.
	Left    int64
	Skipped bool // another process holds the backfill lock
}

// backfillTarget is one table the backfill counts: rooms with room_events, or
// shadow_runs with shadow_events. Table and column names are constants here,
// never input.
type backfillTarget struct {
	list   string // id, last_seq of uncounted rows with id > $1, LIMIT $2; no row lock
	seq    string // last_seq of $1 if still uncounted
	sum    string // payload bytes of $1 up to seq $2
	update string // byte_size = $1 for id $2 if still NULL and last_seq = $3
	n      func(*BackfillResult) *int64
}

var backfillTargets = []backfillTarget{
	{
		list:   `SELECT id, last_seq FROM rooms WHERE byte_size IS NULL AND id > $1 ORDER BY id LIMIT $2`,
		seq:    `SELECT last_seq FROM rooms WHERE id = $1 AND byte_size IS NULL`,
		sum:    `SELECT COALESCE(sum(octet_length(payload::text)), 0) FROM room_events WHERE room_id = $1 AND seq <= $2`,
		update: `UPDATE rooms SET byte_size = $1 WHERE id = $2 AND byte_size IS NULL AND last_seq = $3`,
		n:      func(r *BackfillResult) *int64 { return &r.Rooms },
	},
	{
		list:   `SELECT room_id, last_seq FROM shadow_runs WHERE byte_size IS NULL AND room_id > $1 ORDER BY room_id LIMIT $2`,
		seq:    `SELECT last_seq FROM shadow_runs WHERE room_id = $1 AND byte_size IS NULL`,
		sum:    `SELECT COALESCE(sum(octet_length(payload::text)), 0) FROM shadow_events WHERE room_id = $1 AND seq <= $2`,
		update: `UPDATE shadow_runs SET byte_size = $1 WHERE room_id = $2 AND byte_size IS NULL AND last_seq = $3`,
		n:      func(r *BackfillResult) *int64 { return &r.ShadowRuns },
	},
}

// backfillRow is one uncounted row as listed.
type backfillRow struct {
	id      string
	lastSeq int64
}

// countOutcome is what one attempt at counting a row did.
type countOutcome int

const (
	countWritten countOutcome = iota // byte_size written
	countMissed                      // guard did not match: moved, counted elsewhere, or deleted
	countBusy                        // lock or statement timeout; leave it for later
)

// backfillDB is the database side of the backfill, so the loop can be tested
// without Postgres.
type backfillDB interface {
	list(ctx context.Context, t backfillTarget, after string, limit int) ([]backfillRow, error)
	// reseq reads a row's current last_seq; ok is false when the row is gone
	// or already counted.
	reseq(ctx context.Context, t backfillTarget, id string) (lastSeq int64, ok bool, err error)
	countOne(ctx context.Context, t backfillTarget, r backfillRow) (countOutcome, error)
}

// BackfillByteSize counts byte_size for the rooms and shadow runs that predate
// migration 00012 (byte_size IS NULL), until none is left or ctx ends. It runs
// in the background after start rather than in the migration, so the
// migration holds its lock on rooms for an instant and no start waits on a
// large table (docs/design/retention.md section 5.2). Until a room is counted
// its byte cap is not enforced; its event cap (on last_seq) is.
//
// No statement holds a row lock across a sum: each room is summed without
// locks up to the last_seq it was listed with, then written by an UPDATE that
// applies only if byte_size is still NULL and last_seq has not moved. An
// Append that commits in between moves last_seq, so the room is summed again
// (up to backfillRetries times, then left for the next start). The UPDATE
// runs under SET LOCAL lock_timeout, so the backfill never queues behind an
// Append for long and holds a room's row only for that one UPDATE. Every
// statement has its own deadline, whatever ctx carries.
//
// Only one process backfills at a time (a session advisory lock); a second one
// returns Skipped. The count is the jsonb text size of the stored payloads,
// an estimate of the written size within the bound stated in the migration.
func (s *PG) BackfillByteSize(ctx context.Context, batch int) (BackfillResult, error) {
	var res BackfillResult
	if batch <= 0 {
		batch = BackfillBatch
	}
	actx, cancel := dbCtx(ctx)
	conn, err := s.pool.Acquire(actx)
	if err != nil {
		cancel()
		return res, fmt.Errorf("store: backfill byte_size: acquire: %w", err)
	}
	var got bool
	err = conn.QueryRow(actx, `SELECT pg_try_advisory_lock($1)`, backfillLockKey).Scan(&got)
	cancel()
	if err != nil {
		conn.Release()
		return res, fmt.Errorf("store: backfill byte_size: lock: %w", err)
	}
	if !got {
		conn.Release()
		res.Skipped = true
		return res, nil
	}
	defer func() {
		// A fresh context: ctx may be why the loop stopped.
		uctx, cancel := dbCtx(context.WithoutCancel(ctx))
		defer cancel()
		if _, err := conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, backfillLockKey); err != nil {
			// A session lock must not go back to the pool with its
			// connection: closing the connection ends the session, and with
			// it the lock. The pool drops a closed connection on Release.
			s.log.Warn("store: backfill byte_size: advisory unlock failed; closing the connection", "err", err)
			_ = conn.Conn().Close(uctx)
		}
		conn.Release()
	}()
	return runBackfill(ctx, pgBackfill{conn: conn}, batch)
}

// runBackfill counts every target table, batch rows per listing.
func runBackfill(ctx context.Context, db backfillDB, batch int) (BackfillResult, error) {
	var res BackfillResult
	for _, t := range backfillTargets {
		after := ""
		for {
			if err := ctx.Err(); err != nil {
				return res, fmt.Errorf("store: backfill byte_size: %w", err)
			}
			rows, err := db.list(ctx, t, after, batch)
			if err != nil {
				return res, fmt.Errorf("store: backfill byte_size: list: %w", err)
			}
			if len(rows) == 0 {
				break
			}
			for _, r := range rows {
				written, err := backfillOne(ctx, db, t, r)
				if err != nil {
					return res, err
				}
				if written {
					*t.n(&res)++
				} else {
					res.Left++
				}
			}
			// Keyset paging: a row left NULL is not listed again in this call.
			after = rows[len(rows)-1].id
		}
	}
	return res, nil
}

// backfillOne counts one row, summing again while appends move its last_seq.
// written is false when the row was left (busy, still moving) or is gone.
func backfillOne(ctx context.Context, db backfillDB, t backfillTarget, r backfillRow) (bool, error) {
	for attempt := 1; ; attempt++ {
		out, err := db.countOne(ctx, t, r)
		if err != nil {
			return false, fmt.Errorf("store: backfill byte_size: count %s: %w", r.id, err)
		}
		switch out {
		case countWritten:
			return true, nil
		case countBusy:
			return false, nil
		}
		seq, ok, err := db.reseq(ctx, t, r.id)
		if err != nil {
			return false, fmt.Errorf("store: backfill byte_size: re-read %s: %w", r.id, err)
		}
		if !ok || attempt >= backfillRetries {
			// Gone or counted elsewhere; or still moving: the next start
			// counts it.
			return false, nil
		}
		r.lastSeq = seq
	}
}

// pgBackfill is backfillDB on the connection that holds the advisory lock.
type pgBackfill struct {
	conn *pgxpool.Conn
}

func (p pgBackfill) list(ctx context.Context, t backfillTarget, after string, limit int) ([]backfillRow, error) {
	ctx, cancel := context.WithTimeout(ctx, backfillListTimeout)
	defer cancel()
	rows, err := p.conn.Query(ctx, t.list, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (backfillRow, error) {
		var r backfillRow
		err := row.Scan(&r.id, &r.lastSeq)
		return r, err
	})
}

func (p pgBackfill) reseq(ctx context.Context, t backfillTarget, id string) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, backfillListTimeout)
	defer cancel()
	var seq int64
	err := p.conn.QueryRow(ctx, t.seq, id).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func (p pgBackfill) countOne(parent context.Context, t backfillTarget, r backfillRow) (countOutcome, error) {
	ctx, cancel := context.WithTimeout(parent, backfillRoomTimeout)
	defer cancel()
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		rctx, rcancel := dbCtx(context.WithoutCancel(parent))
		defer rcancel()
		_ = tx.Rollback(rctx) // after Commit this is a no-op
	}()
	// SET LOCAL ends with this transaction, so the connection keeps its own
	// settings afterwards.
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '`+backfillLockTimeout+`'`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '`+backfillStatementTimeout+`'`); err != nil {
		return 0, err
	}
	var sum int64
	if err := tx.QueryRow(ctx, t.sum, r.id, r.lastSeq).Scan(&sum); err != nil {
		return busyOr(parent, err)
	}
	tag, err := tx.Exec(ctx, t.update, sum, r.id, r.lastSeq)
	if err != nil {
		return busyOr(parent, err)
	}
	if tag.RowsAffected() == 0 {
		return countMissed, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return busyOr(parent, err)
	}
	return countWritten, nil
}

// busyOr maps a lock timeout, a statement timeout or this room's own deadline
// to countBusy (the row is left for later); anything else, including the
// caller's ctx ending, is an error.
func busyOr(parent context.Context, err error) (countOutcome, error) {
	if parent.Err() != nil {
		return 0, err
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && (pe.Code == "55P03" || pe.Code == "57014") {
		return countBusy, nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return countBusy, nil
	}
	return 0, err
}
