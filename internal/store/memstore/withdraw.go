package memstore

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/store"
)

// WithdrawProposal sets an open proposal to "withdrawn" while none of its
// steps has started, with the refusals of the Postgres store
// (store.WithdrawRefusal). This store has no executor claims: a started step
// is what makes a proposal running here.
func (s *Store) WithdrawProposal(ctx context.Context, proposalID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.proposals[proposalID]
	if !ok {
		return fmt.Errorf("memstore: withdraw proposal %q: %w", proposalID, store.ErrNotFound)
	}
	running := false
	for key := range s.steps {
		if strings.HasPrefix(key, proposalID+"\x00") {
			running = true
			break
		}
	}
	if p.Status != "open" || running {
		return store.WithdrawRefusal(proposalID, p.Status, running)
	}
	p.Status = store.ProposalWithdrawn
	return nil
}
