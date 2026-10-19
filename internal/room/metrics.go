package room

import (
	"sync"

	"piperoom/internal/events"
)

// AgentSlotStats reports this replica's agent slots in use and the rooms queued
// for one (PIPEROOM_MAX_AGENTS), for the /metrics gauges.
func (s *Service) AgentSlotStats() (inUse, queued int) {
	return s.slots.stats()
}

// OpenRooms is how many rooms in this replica's room cache are open, for the
// /metrics gauge. Load caches every open room at start and rooms are cached as
// they are opened or reached here, so with several replicas each one reports
// the rooms it has seen. It counts the rooms' published snapshots under the
// registry lock only, so it never waits on a room whose command holds its lock
// across a store append, and right after Load it is already exact.
func (s *Service) OpenRooms() int {
	n := 0
	for _, p := range s.publishedRooms() {
		if p.Status == "open" {
			n++
		}
	}
	return n
}

// approvalCounts counts the approval.given events this process appended, by
// grant. The zero value is ready to use.
type approvalCounts struct {
	mu sync.Mutex
	n  map[string]uint64
}

func (c *approvalCounts) inc(grant string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]uint64{}
	}
	c.n[grant]++
}

// ApprovalsByGrant returns how many approval.given events this replica
// appended since it started, per grant, for the piperoom_approvals_total
// counter. Every grant of P0 item 4 is present, zero included, so each series
// exists before its first approval and the label set stays bounded.
func (s *Service) ApprovalsByGrant() map[string]uint64 {
	s.approvals.mu.Lock()
	defer s.approvals.mu.Unlock()
	out := make(map[string]uint64, 4)
	for _, g := range [...]string{events.GrantOwner, events.GrantSolo, events.GrantFallback, events.GrantPolicy} {
		out[g] = s.approvals.n[g]
	}
	return out
}
