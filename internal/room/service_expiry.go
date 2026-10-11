package room

import (
	"time"

	"piperoom/internal/events"
)

// evProposalExpired is recorded when a proposal has waited longer than the
// proposal TTL for its approvals (review item 16). It only documents the fact
// in the log; it adds no new snapshot state and approvals may still arrive.
// CONTRACT CHANGE REQUEST: add this type to internal/events/events.go.
const evProposalExpired events.Type = "proposal.expired"

// proposalExpiredPayload is the payload of evProposalExpired.
type proposalExpiredPayload struct {
	ProposalID string `json:"proposal_id"`
	Title      string `json:"title"`
	AgeSeconds int64  `json:"age_seconds"`
}

// expiryLoop sweeps open proposals until the service context ends. The service
// runs exactly one sweeper (not one per room), stopped by Close.
func (s *Service) expiryLoop(ttl time.Duration) {
	defer s.wg.Done()
	tick := time.NewTicker(expiryInterval(ttl))
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
			s.expireProposals()
		}
	}
}

// expiryInterval is how often the sweeper checks: a quarter of the TTL,
// clamped so tiny test TTLs do not spin and long TTLs are checked at least
// once a minute.
func expiryInterval(ttl time.Duration) time.Duration {
	iv := ttl / 4
	if iv < 10*time.Millisecond {
		iv = 10 * time.Millisecond
	}
	if iv > time.Minute {
		iv = time.Minute
	}
	return iv
}

// expireProposals emits one proposal.expired event for every open proposal
// older than the TTL. The in-memory expired set guarantees exactly one event
// per proposal per service lifetime; the snapshot itself is untouched.
func (s *Service) expireProposals() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	now := s.now()
	for roomID, st := range s.rooms {
		if st.snap.Status != "open" {
			continue
		}
		for _, p := range st.snap.OpenProposals {
			if p.CreatedAt.IsZero() || now.Sub(p.CreatedAt) < s.proposalTTL {
				continue
			}
			if st.expired == nil {
				st.expired = map[string]bool{}
			}
			if st.expired[p.ProposalID] {
				continue
			}
			_, err := s.emitLocked(s.ctx, st, roomID, evProposalExpired, systemActor, proposalExpiredPayload{
				ProposalID: p.ProposalID,
				Title:      p.Title,
				AgeSeconds: int64(now.Sub(p.CreatedAt) / time.Second),
			})
			if err != nil {
				s.log.Warn("room: emit proposal.expired failed", "room", roomID, "proposal", p.ProposalID, "err", err)
				continue
			}
			st.expired[p.ProposalID] = true
		}
	}
}
