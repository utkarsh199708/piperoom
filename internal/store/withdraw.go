package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ProposalWithdrawn is the proposals.status of a withdrawn proposal
// (proposal.withdrawn, QA G8/G9). No CHECK constraint limits the column.
const ProposalWithdrawn = "withdrawn"

// Refusals of WithdrawProposal. Each says why the row was left as it was.
var (
	// ErrProposalWithdrawn: the proposal is already withdrawn.
	ErrProposalWithdrawn = errors.New("store: proposal was withdrawn")
	// ErrProposalDecided: the proposal is rejected, executed or failed.
	ErrProposalDecided = errors.New("store: proposal is no longer open")
	// ErrProposalRunning: an executor claimed the proposal or started a step.
	ErrProposalRunning = errors.New("store: proposal is claimed by the executor")
)

// WithdrawProposal sets an open proposal to "withdrawn" in one conditional
// UPDATE: only while no executor has claimed it (claimed_by) and no step of it
// has started (proposal_steps). The executor's ClaimProposal needs status
// 'open' and claimed_by NULL on the same row, so a withdrawal and a claim
// racing on two replicas cannot both win: the row lock orders them and the
// loser's WHERE no longer matches. A refusal wraps ErrNotFound,
// ErrProposalWithdrawn, ErrProposalDecided or ErrProposalRunning.
func (s *PG) WithdrawProposal(ctx context.Context, proposalID string) error {
	if CheckText(proposalID) != nil {
		return fmt.Errorf("store: withdraw proposal: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposals SET status = $2
		 WHERE proposal_id = $1 AND status = 'open' AND claimed_by IS NULL
		   AND NOT EXISTS (SELECT 1 FROM proposal_steps WHERE proposal_id = $1)`, proposalID, ProposalWithdrawn)
	if err != nil {
		return fmt.Errorf("store: withdraw proposal %s: %w", proposalID, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	var running bool
	err = s.pool.QueryRow(ctx,
		`SELECT status, claimed_by IS NOT NULL OR EXISTS (SELECT 1 FROM proposal_steps WHERE proposal_id = $1)
		 FROM proposals WHERE proposal_id = $1`, proposalID).Scan(&status, &running)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("store: withdraw proposal %s: %w", proposalID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("store: withdraw proposal %s: %w", proposalID, err)
	}
	return WithdrawRefusal(proposalID, status, running)
}

// WithdrawRefusal is the error WithdrawProposal returns for a row it did not
// change, given the row's status and whether an executor claimed it or
// started a step. Both stores use it, so they refuse alike.
func WithdrawRefusal(proposalID, status string, running bool) error {
	switch {
	case status == ProposalWithdrawn:
		return fmt.Errorf("store: withdraw proposal %s: %w", proposalID, ErrProposalWithdrawn)
	case status != "open":
		return fmt.Errorf("store: withdraw proposal %s (status %s): %w", proposalID, status, ErrProposalDecided)
	case running:
		return fmt.Errorf("store: withdraw proposal %s: %w", proposalID, ErrProposalRunning)
	}
	// Open, unclaimed and not started, yet not updated: the row changed
	// between the two statements. The caller may retry; to it, the proposal is
	// busy.
	return fmt.Errorf("store: withdraw proposal %s changed meanwhile: %w", proposalID, ErrProposalRunning)
}
