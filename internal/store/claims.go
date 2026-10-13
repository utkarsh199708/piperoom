package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Executor writes that hold only while the executor still owns the
// proposal's claim (docs/design/replicas.md section 5), and the resume cap.

// StartClaimedStep implements Coordinator.
func (s *PG) StartClaimedStep(ctx context.Context, proposalID string, stepIndex int, owner string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO proposal_steps (proposal_id, step_index, status)
		 SELECT $1, $2, 'started'
		 WHERE EXISTS (SELECT 1 FROM proposals WHERE proposal_id = $1 AND claimed_by = $3 AND status = 'open')
		 ON CONFLICT (proposal_id, step_index) DO NOTHING`, proposalID, stepIndex, owner)
	if err != nil {
		return fmt.Errorf("store: start step %s/%d: %w", proposalID, stepIndex, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	held, err := s.claimHeld(ctx, proposalID, owner)
	if err != nil {
		return fmt.Errorf("store: start step %s/%d: %w", proposalID, stepIndex, err)
	}
	if !held {
		return fmt.Errorf("store: start step %s/%d: %w", proposalID, stepIndex, ErrClaimLost)
	}
	return fmt.Errorf("store: step %s/%d already started: %w", proposalID, stepIndex, ErrStepOrder)
}

// FinishClaimedStep implements Coordinator. The claim refresh and the step
// write are one transaction, and the proposals row lock orders it against the
// stale sweep's InterruptProposal.
func (s *PG) FinishClaimedStep(ctx context.Context, proposalID string, stepIndex int, owner, status, output string) error {
	if err := CheckText(proposalID, status, output); err != nil {
		return fmt.Errorf("store: finish step %s/%d: %w", proposalID, stepIndex, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE proposals SET claimed_at = now() WHERE proposal_id = $1 AND claimed_by = $2 AND status = 'open'`,
			proposalID, owner)
		if err != nil {
			return fmt.Errorf("store: finish step %s/%d: %w", proposalID, stepIndex, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("store: finish step %s/%d: %w", proposalID, stepIndex, ErrClaimLost)
		}
		tag, err = tx.Exec(ctx,
			`UPDATE proposal_steps SET status = $3, output = $4
			 WHERE proposal_id = $1 AND step_index = $2 AND status = 'started'`, proposalID, stepIndex, status, output)
		if err != nil {
			return fmt.Errorf("store: finish step %s/%d: %w", proposalID, stepIndex, classify(err))
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("store: step %s/%d is not started: %w", proposalID, stepIndex, ErrStepOrder)
		}
		return nil
	})
}

// SetClaimedProposalStatus implements Coordinator.
func (s *PG) SetClaimedProposalStatus(ctx context.Context, proposalID, owner, status string) error {
	if err := CheckText(proposalID, status); err != nil {
		return fmt.Errorf("store: set proposal status %s: %w", proposalID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposals SET status = $3 WHERE proposal_id = $1 AND claimed_by = $2 AND status = 'open'`,
		proposalID, owner, status)
	if err != nil {
		return fmt.Errorf("store: set proposal status %s: %w", proposalID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: set proposal status %s: %w", proposalID, ErrClaimLost)
	}
	return nil
}

func (s *PG) claimHeld(ctx context.Context, proposalID, owner string) (bool, error) {
	var held bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM proposals WHERE proposal_id = $1 AND claimed_by = $2 AND status = 'open')`,
		proposalID, owner).Scan(&held)
	return held, err
}

// NoteResume implements Coordinator. The count and its window live on the
// lease row, so the cap holds across replicas and survives takeovers.
func (s *PG) NoteResume(ctx context.Context, roomID, owner string, max int, window time.Duration) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
UPDATE room_agent_leases SET
    resumes       = CASE WHEN resume_window > now() - $4 * interval '1 millisecond' THEN resumes + 1 ELSE 1 END,
    resume_window = CASE WHEN resume_window > now() - $4 * interval '1 millisecond' THEN resume_window ELSE now() END
WHERE room_id = $1 AND owner = $2
  AND (resume_window IS NULL OR resume_window <= now() - $4 * interval '1 millisecond' OR resumes < $3)`,
		roomID, owner, max, millis(window))
	if err != nil {
		return false, fmt.Errorf("store: note resume %s: %w", roomID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// PutIfLastSeq stores th for the room only when the stored last_seq is still
// prev (no row yet counts as 0): a compare-and-set, so two Slack mirrors of
// one room can never both advance it. ok false means another mirror moved on.
func (t *SlackThreads) PutIfLastSeq(ctx context.Context, roomID string, prev int64, channel, ts string, lastSeq int64) (bool, error) {
	if err := CheckText(roomID, channel, ts); err != nil {
		return false, fmt.Errorf("store: slack thread put: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, slackThreadTimeout)
	defer cancel()
	tag, err := t.pool.Exec(ctx, `
INSERT INTO slack_threads AS st (room_id, channel, ts, last_seq, updated_at)
SELECT $1, $3, $4, $5, now() WHERE $2::bigint = 0
ON CONFLICT (room_id) DO UPDATE
SET channel = EXCLUDED.channel, ts = EXCLUDED.ts, last_seq = EXCLUDED.last_seq, updated_at = now()
WHERE st.last_seq = $2`, roomID, prev, channel, ts, lastSeq)
	if err != nil {
		return false, fmt.Errorf("store: slack thread put: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	if prev == 0 {
		return false, nil
	}
	tag, err = t.pool.Exec(ctx, `
UPDATE slack_threads SET channel = $3, ts = $4, last_seq = $5, updated_at = now()
WHERE room_id = $1 AND last_seq = $2`, roomID, prev, channel, ts, lastSeq)
	if err != nil {
		return false, fmt.Errorf("store: slack thread put: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
