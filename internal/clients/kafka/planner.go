package kafka

import (
	"context"
	"errors"

	"piperoom/internal/clients"
)

// Planner is clients.GroupResetPlanner on a read-only Client: the group
// state check and the planned per-partition moves of a group offset reset,
// with nothing that commits or produces. It lets kafka_reset_consumer_offsets
// validate and preview with write mode off, when wiring builds no Writer; it
// is the Writer's own read code (CheckGroupInactive, PlanGroupReset) over
// the reader's connections, so the preview and the reset plan alike.
type Planner struct {
	// w is a Writer over the reader Client. It is unexported and only its
	// two read methods are called, so no write is reachable through a
	// Planner. w.Close is never called: the Client belongs to the caller.
	w *Writer
}

var _ clients.GroupResetPlanner = (*Planner)(nil)

// NewPlanner returns a Planner reading through c. The caller keeps owning c
// (and closes it); a nil c gives a Planner whose calls fail.
func NewPlanner(c *Client) *Planner {
	if c == nil {
		return &Planner{}
	}
	return &Planner{w: &Writer{c: c}}
}

var errNoPlannerClient = errors.New("kafka: planner has no client")

// CheckGroupInactive is Writer.CheckGroupInactive.
func (p *Planner) CheckGroupInactive(ctx context.Context, group string) error {
	if p == nil || p.w == nil {
		return errNoPlannerClient
	}
	return p.w.CheckGroupInactive(ctx, group)
}

// PlanGroupReset is Writer.PlanGroupReset.
func (p *Planner) PlanGroupReset(ctx context.Context, group string, to clients.OffsetTarget) ([]OffsetMove, error) {
	if p == nil || p.w == nil {
		return nil, errNoPlannerClient
	}
	return p.w.PlanGroupReset(ctx, group, to)
}
