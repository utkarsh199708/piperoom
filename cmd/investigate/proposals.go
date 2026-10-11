package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"piperoom/internal/events"
)

var idCounter atomic.Uint64

// rejectProposals auto-rejects every proposal the agent creates, printing it
// so the demo is safe to run against any stack.
type rejectProposals struct{}

func (rejectProposals) Create(_ context.Context, p events.ProposalCreatedPayload) (string, error) {
	id := "p_" + newID()
	b, _ := json.MarshalIndent(p, "", "  ")
	fmt.Printf("PROPOSAL %s (auto-rejected):\n%s\n", id, b)
	return id, nil
}

// newID returns a short random-ish id for the demo. The real room service
// assigns ULIDs; here a timestamped counter is enough.
func newID() string {
	return fmt.Sprintf("%d", idCounter.Add(1))
}
