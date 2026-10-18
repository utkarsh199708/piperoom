package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/events"
)

// Coordinator is the multi-replica surface of a store that several server
// processes share (docs/design/replicas.md). *PG implements it. memstore does
// not: it is one process, so the room service treats every lease as its own
// and skips every step below.
type Coordinator interface {
	// AcquireRoomLease makes owner the room's agent owner when the room has
	// no lease, an expired one, or one owner already holds; ttl starts now (in
	// Postgres time). ok reports whether owner holds the lease afterwards. A
	// room that does not exist wraps ErrNotFound.
	AcquireRoomLease(ctx context.Context, roomID, owner, pipelineID string, ttl time.Duration) (ok bool, err error)
	// RenewRoomLeases extends, by ttl from now, every lease among roomIDs that
	// owner holds and that has not expired, and returns the ids it extended.
	// An id left out is lost: expired, taken over or deleted.
	RenewRoomLeases(ctx context.Context, owner string, roomIDs []string, ttl time.Duration) ([]string, error)
	// ReleaseRoomLease deletes the room's lease if owner holds it.
	ReleaseRoomLease(ctx context.Context, roomID, owner string) error
	// ExpireRoomLeases ends every lease owner holds now (graceful shutdown),
	// so another replica takes the rooms over at its next scan.
	ExpireRoomLeases(ctx context.Context, owner string) error
	// ExpiredRoomLeases lists leases past expires_at, oldest first, at most
	// limit.
	ExpiredRoomLeases(ctx context.Context, limit int) ([]RoomLease, error)
	// TakeOverRoomLease gives an expired lease to owner. ok is false when the
	// lease is gone or no longer expired (another replica was faster).
	TakeOverRoomLease(ctx context.Context, roomID, owner string, ttl time.Duration) (ok bool, err error)
	// RoomLeases returns every lease row by room id.
	RoomLeases(ctx context.Context) (map[string]RoomLease, error)
	// TakeAgentSlot marks owner's live lease on roomID as holding an agent
	// slot when fewer than max live leases hold one. Takers are serialized by
	// a transaction advisory lock, so max is never exceeded. A lease owner no
	// longer holds wraps ErrLeaseLost.
	TakeAgentSlot(ctx context.Context, roomID, owner string, max int) (ok bool, err error)
	// ReleaseAgentSlot clears the slot flag of owner's lease on roomID.
	ReleaseAgentSlot(ctx context.Context, roomID, owner string) error

	// ClaimProposal claims an open, unclaimed proposal for owner's executor.
	// ok is false when another executor claimed it first (or it is no longer
	// open): the caller must not run any step.
	ClaimProposal(ctx context.Context, proposalID, owner string) (ok bool, err error)
	// TouchProposalClaim refreshes owner's claim before a step. ok is false
	// when owner no longer holds the claim (the stale sweep took it): the
	// step must not run.
	TouchProposalClaim(ctx context.Context, proposalID, owner string) (ok bool, err error)
	// StaleProposals lists open proposals whose executor stopped: claimed,
	// with claimed_at older than claimStale; or fully approved, unclaimed,
	// with the last approval older than unclaimedStale. At most limit.
	StaleProposals(ctx context.Context, claimStale, unclaimedStale time.Duration, limit int) ([]StaleProposal, error)
	// InterruptProposal sets a stale proposal (same rules as StaleProposals,
	// re-checked in the UPDATE) to "failed" and claims it for owner, so
	// exactly one replica records the interruption. ok is false when it was
	// not stale any more or another replica was faster.
	InterruptProposal(ctx context.Context, proposalID, owner string, claimStale, unclaimedStale time.Duration) (ok bool, err error)

	// StartClaimedStep marks a step started, like StartProposalStep, but only
	// while owner still holds the proposal's claim (claimed_by = owner, status
	// open). A lost claim wraps ErrClaimLost; a step already started wraps
	// ErrStepOrder.
	StartClaimedStep(ctx context.Context, proposalID string, stepIndex int, owner string) error
	// FinishClaimedStep records a started step's result, like
	// FinishProposalStep, only while owner holds the claim, and refreshes the
	// claim in the same transaction. A lost claim wraps ErrClaimLost; a step
	// that is not "started" wraps ErrStepOrder.
	FinishClaimedStep(ctx context.Context, proposalID string, stepIndex int, owner, status, output string) error
	// SetClaimedProposalStatus sets the final status only while owner holds
	// the claim of an open proposal. A lost claim wraps ErrClaimLost.
	SetClaimedProposalStatus(ctx context.Context, proposalID, owner, status string) error
	// NoteResume counts one resume of the room's agent by owner, the current
	// lease holder: at most max per window (docs/design/replicas.md 4.3). ok
	// false means the cap is reached (or owner no longer holds the lease).
	NoteResume(ctx context.Context, roomID, owner string, max int, window time.Duration) (ok bool, err error)
	// AppendIfLast appends only when the room's last seq is expectSeq;
	// another writer in between wraps ErrSeqConflict (docs/design/replicas.md
	// 9.1).
	AppendIfLast(ctx context.Context, roomID string, expectSeq int64, typ events.Type, actor events.Actor, payload any) (events.Event, error)

	// WatchAppends calls w for every append notification until ctx ends
	// (hub.go).
	WatchAppends(ctx context.Context, w AppendWatch)
	// LockKey takes a cluster-wide lock on key (a session advisory lock on a
	// pool connection, scoped to the schema) and returns its unlock func.
	// Waiting is bounded by ctx.
	LockKey(ctx context.Context, key string) (unlock func(), err error)
}

// ErrLeaseLost is returned (wrapped) when a lease the caller relied on is not
// its own any more.
var ErrLeaseLost = errors.New("store: room lease lost")

// ErrClaimLost is returned (wrapped) when an executor's proposal claim is not
// its own any more (the stale sweep took it, or the proposal was decided).
var ErrClaimLost = errors.New("store: proposal claim lost")

// RoomLease is one room_agent_leases row.
type RoomLease struct {
	RoomID     string
	Owner      string
	PipelineID string
	Slot       bool
	ExpiresAt  time.Time
}

// StaleProposal is one proposal StaleProposals returned.
type StaleProposal struct {
	ProposalID string
	RoomID     string
	ClaimedBy  string // "" when it was never claimed
	// WriteBlocked: the status is ProposalStatusBlocked, so the proposal was
	// fully approved while write mode was off and never ran.
	WriteBlocked bool
}

// ProposalStatusBlocked is the status of a proposal fully approved while
// write mode was off (PIPEROOM_WRITE_MODE): never claimed, never run. Once
// its unclaimed window passes, the stale rule lists it like an approved,
// unclaimed open proposal, flagged WriteBlocked, so a replica with write mode
// on records why it did not run instead of running it. Only while its room is
// open: a closed room's blocked row stays blocked.
const ProposalStatusBlocked = "blocked"

var _ Coordinator = (*PG)(nil)

// millis turns a duration into whole milliseconds for "n * interval '1
// millisecond'" (at least 1, so a lease is never born expired).
func millis(d time.Duration) int64 {
	if ms := d.Milliseconds(); ms > 0 {
		return ms
	}
	return 1
}

// AcquireRoomLease implements Coordinator.
func (s *PG) AcquireRoomLease(ctx context.Context, roomID, owner, pipelineID string, ttl time.Duration) (bool, error) {
	if err := CheckText(roomID, owner, pipelineID); err != nil {
		return false, fmt.Errorf("store: acquire lease %s: %w", roomID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var got string
	err := s.pool.QueryRow(ctx, `
INSERT INTO room_agent_leases AS l (room_id, owner, pipeline_id, slot, acquired_at, expires_at)
VALUES ($1, $2, $3, false, now(), now() + $4 * interval '1 millisecond')
ON CONFLICT (room_id) DO UPDATE
SET owner       = EXCLUDED.owner,
    pipeline_id = EXCLUDED.pipeline_id,
    expires_at  = EXCLUDED.expires_at,
    slot        = CASE WHEN l.owner = EXCLUDED.owner THEN l.slot ELSE false END,
    acquired_at = CASE WHEN l.owner = EXCLUDED.owner THEN l.acquired_at ELSE now() END
WHERE l.expires_at < now() OR l.owner = EXCLUDED.owner
RETURNING l.owner`, roomID, owner, pipelineID, millis(ttl)).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		if isFKViolation(err) {
			return false, fmt.Errorf("store: acquire lease %s: %w", roomID, ErrNotFound)
		}
		return false, fmt.Errorf("store: acquire lease %s: %w", roomID, err)
	}
	return got == owner, nil
}

// RenewRoomLeases implements Coordinator.
func (s *PG) RenewRoomLeases(ctx context.Context, owner string, roomIDs []string, ttl time.Duration) ([]string, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
UPDATE room_agent_leases SET expires_at = now() + $3 * interval '1 millisecond'
WHERE owner = $1 AND room_id = ANY($2) AND expires_at > now()
RETURNING room_id`, owner, roomIDs, millis(ttl))
	if err != nil {
		return nil, fmt.Errorf("store: renew leases: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("store: renew leases: %w", err)
	}
	return ids, nil
}

// ReleaseRoomLease implements Coordinator.
func (s *PG) ReleaseRoomLease(ctx context.Context, roomID, owner string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx, `DELETE FROM room_agent_leases WHERE room_id = $1 AND owner = $2`, roomID, owner); err != nil {
		return fmt.Errorf("store: release lease %s: %w", roomID, err)
	}
	return nil
}

// ExpireRoomLeases implements Coordinator. The slot flags go too: the runs
// that held them have stopped.
func (s *PG) ExpireRoomLeases(ctx context.Context, owner string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE room_agent_leases SET expires_at = now() - interval '1 millisecond', slot = false
		 WHERE owner = $1 AND expires_at >= now()`, owner); err != nil {
		return fmt.Errorf("store: expire leases: %w", err)
	}
	return nil
}

// ExpiredRoomLeases implements Coordinator.
func (s *PG) ExpiredRoomLeases(ctx context.Context, limit int) ([]RoomLease, error) {
	if limit <= 0 {
		limit = 100
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT room_id, owner, pipeline_id, slot, expires_at FROM room_agent_leases
		 WHERE expires_at < now() ORDER BY expires_at, room_id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: expired leases: %w", err)
	}
	return collectLeases(rows)
}

// TakeOverRoomLease implements Coordinator.
func (s *PG) TakeOverRoomLease(ctx context.Context, roomID, owner string, ttl time.Duration) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
UPDATE room_agent_leases
SET owner = $2, expires_at = now() + $3 * interval '1 millisecond', slot = false, acquired_at = now()
WHERE room_id = $1 AND expires_at < now()`, roomID, owner, millis(ttl))
	if err != nil {
		return false, fmt.Errorf("store: take over lease %s: %w", roomID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// RoomLeases implements Coordinator.
func (s *PG) RoomLeases(ctx context.Context) (map[string]RoomLease, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT room_id, owner, pipeline_id, slot, expires_at FROM room_agent_leases`)
	if err != nil {
		return nil, fmt.Errorf("store: list leases: %w", err)
	}
	ls, err := collectLeases(rows)
	if err != nil {
		return nil, err
	}
	out := make(map[string]RoomLease, len(ls))
	for _, l := range ls {
		out[l.RoomID] = l
	}
	return out, nil
}

func collectLeases(rows pgx.Rows) ([]RoomLease, error) {
	defer rows.Close()
	var out []RoomLease
	for rows.Next() {
		var l RoomLease
		if err := rows.Scan(&l.RoomID, &l.Owner, &l.PipelineID, &l.Slot, &l.ExpiresAt); err != nil {
			return nil, fmt.Errorf("store: scan lease: %w", err)
		}
		l.ExpiresAt = l.ExpiresAt.UTC()
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list leases: %w", err)
	}
	return out, nil
}

// TakeAgentSlot implements Coordinator.
func (s *PG) TakeAgentSlot(ctx context.Context, roomID, owner string, max int) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	ok := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Serialize the takers, so two replicas counting at the same moment
		// cannot both see room for one more.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_schema()), hashtext('agent-slots'))`); err != nil {
			return fmt.Errorf("store: take agent slot: lock: %w", err)
		}
		var held bool
		err := tx.QueryRow(ctx,
			`SELECT slot FROM room_agent_leases WHERE room_id = $1 AND owner = $2 AND expires_at > now()`,
			roomID, owner).Scan(&held)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("store: take agent slot %s: %w", roomID, ErrLeaseLost)
		}
		if err != nil {
			return fmt.Errorf("store: take agent slot %s: %w", roomID, err)
		}
		if held {
			ok = true
			return nil
		}
		var n int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM room_agent_leases WHERE slot AND expires_at > now()`).Scan(&n); err != nil {
			return fmt.Errorf("store: take agent slot: count: %w", err)
		}
		if n >= max {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`UPDATE room_agent_leases SET slot = true WHERE room_id = $1 AND owner = $2`, roomID, owner); err != nil {
			return fmt.Errorf("store: take agent slot %s: %w", roomID, err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return ok, nil
}

// ReleaseAgentSlot implements Coordinator.
func (s *PG) ReleaseAgentSlot(ctx context.Context, roomID, owner string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE room_agent_leases SET slot = false WHERE room_id = $1 AND owner = $2 AND slot`, roomID, owner); err != nil {
		return fmt.Errorf("store: release agent slot %s: %w", roomID, err)
	}
	return nil
}

// ClaimProposal implements Coordinator.
func (s *PG) ClaimProposal(ctx context.Context, proposalID, owner string) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposals SET claimed_by = $2, claimed_at = now()
		 WHERE proposal_id = $1 AND claimed_by IS NULL AND status = 'open'`, proposalID, owner)
	if err != nil {
		return false, fmt.Errorf("store: claim proposal %s: %w", proposalID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// TouchProposalClaim implements Coordinator.
func (s *PG) TouchProposalClaim(ctx context.Context, proposalID, owner string) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposals SET claimed_at = now()
		 WHERE proposal_id = $1 AND claimed_by = $2 AND status = 'open'`, proposalID, owner)
	if err != nil {
		return false, fmt.Errorf("store: touch claim %s: %w", proposalID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// staleProposalSQL is the stale rule shared by StaleProposals and
// InterruptProposal. $a is the claim staleness, $b the unclaimed staleness,
// both in milliseconds; p is the proposals row. 'blocked' is
// ProposalStatusBlocked: such a row is never claimed, so only the unclaimed
// branch can list it, and only while its room is open. In a closed room
// "blocked" is the accurate last word, and no action.executed should land
// after the room's post-mortem.
const staleProposalSQL = `(
    (p.status = 'open' AND p.claimed_by IS NOT NULL AND p.claimed_at < now() - %[1]s * interval '1 millisecond')
 OR ((p.status = 'open'
      OR (p.status = 'blocked' AND EXISTS (SELECT 1 FROM rooms rm WHERE rm.id = p.room_id AND rm.status = 'open')))
     AND p.claimed_by IS NULL
     AND jsonb_array_length(p.required_layers) > 0
     AND NOT EXISTS (
         SELECT 1 FROM jsonb_array_elements_text(p.required_layers) AS r(layer)
         WHERE NOT EXISTS (SELECT 1 FROM approvals a
                           WHERE a.proposal_id = p.proposal_id AND a.layer = r.layer AND a.decision = 'approved'))
     AND (SELECT max(a.decided_at) FROM approvals a WHERE a.proposal_id = p.proposal_id)
         < now() - %[2]s * interval '1 millisecond'))`

// StaleProposals implements Coordinator.
func (s *PG) StaleProposals(ctx context.Context, claimStale, unclaimedStale time.Duration, limit int) ([]StaleProposal, error) {
	if limit <= 0 {
		limit = 100
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT p.proposal_id, p.room_id, coalesce(p.claimed_by, ''), p.status = 'blocked' FROM proposals p
		 WHERE `+fmt.Sprintf(staleProposalSQL, "$1", "$2")+`
		 ORDER BY p.proposal_id LIMIT $3`, millis(claimStale), millis(unclaimedStale), limit)
	if err != nil {
		return nil, fmt.Errorf("store: stale proposals: %w", err)
	}
	defer rows.Close()
	var out []StaleProposal
	for rows.Next() {
		var sp StaleProposal
		if err := rows.Scan(&sp.ProposalID, &sp.RoomID, &sp.ClaimedBy, &sp.WriteBlocked); err != nil {
			return nil, fmt.Errorf("store: scan stale proposal: %w", err)
		}
		out = append(out, sp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: stale proposals: %w", err)
	}
	return out, nil
}

// InterruptProposal implements Coordinator.
func (s *PG) InterruptProposal(ctx context.Context, proposalID, owner string, claimStale, unclaimedStale time.Duration) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposals p SET status = 'failed', claimed_by = $2, claimed_at = now()
		 WHERE p.proposal_id = $1 AND `+fmt.Sprintf(staleProposalSQL, "$3", "$4"),
		proposalID, owner, millis(claimStale), millis(unclaimedStale))
	if err != nil {
		return false, fmt.Errorf("store: interrupt proposal %s: %w", proposalID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// WatchAppends implements Coordinator.
func (s *PG) WatchAppends(ctx context.Context, w AppendWatch) {
	wp := &w
	s.hub.addWatcher(wp)
	context.AfterFunc(ctx, func() { s.hub.removeWatcher(wp) })
}

// isFKViolation reports a foreign key violation (SQLSTATE 23503): the row
// points at a room that does not exist.
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

// lockKeyTimeout bounds the unlock of a LockKey lock.
const lockKeyTimeout = 5 * time.Second

// LockKey implements Coordinator.
func (s *PG) LockKey(ctx context.Context, key string) (func(), error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: lock key: acquire connection: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_schema()), hashtext($1))`, key); err != nil {
		// The lock state of this connection is unknown (the wait may have
		// been cancelled after it was granted): never hand it back to the
		// pool.
		_ = conn.Hijack().Close(context.Background())
		return nil, fmt.Errorf("store: lock key: %w", err)
	}
	return func() {
		uctx, ucancel := context.WithTimeout(context.Background(), lockKeyTimeout)
		defer ucancel()
		var released bool
		if err := conn.QueryRow(uctx, `SELECT pg_advisory_unlock(hashtext(current_schema()), hashtext($1))`, key).Scan(&released); err != nil || !released {
			// Closing the session frees the lock for sure.
			_ = conn.Hijack().Close(uctx)
			return
		}
		conn.Release()
	}, nil
}
