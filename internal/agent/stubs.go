package agent

import (
	"context"
	"errors"

	"piperoom/internal/events"
)

// Placeholders so other packages compile. A4 deletes this file and adds the
// real Run (run.go) and WriteHandoff (handoff.go).

func Run(ctx context.Context, alert Alert, deps Deps, opts Options) error {
	return errors.New("agent.Run: not implemented")
}

func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{}, errors.New("agent.WriteHandoff: not implemented")
}

var (
	_ Runner        = Run
	_ HandoffWriter = WriteHandoff
)
