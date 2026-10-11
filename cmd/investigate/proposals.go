package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"

	"piperoom/internal/events"
)

// rejectProposals auto-rejects every proposal the agent creates, printing it
// so the demo is safe to run against any stack. The id counter lives on the
// struct rather than a package global (rule 5: no global mutable state).
type rejectProposals struct {
	counter atomic.Uint64
}

func (r *rejectProposals) Create(_ context.Context, p events.ProposalCreatedPayload) (string, error) {
	id := "p_" + strconv.FormatUint(r.counter.Add(1), 10)
	b, _ := json.MarshalIndent(p, "", "  ")
	fmt.Printf("PROPOSAL %s (auto-rejected):\n%s\n", id, truncatePrinted(sanitizeTerminal(string(b))))
	return id, nil
}
