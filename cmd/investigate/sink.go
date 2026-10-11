package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"piperoom/internal/events"
)

// terminalSink prints each event as one colored line.
type terminalSink struct {
	w io.Writer
}

func newTerminalSink(w io.Writer) *terminalSink { return &terminalSink{w: w} }

// colorByType returns an ANSI color for an event type, empty when the type has
// no color or colors are disabled.
func colorByType(t events.Type) string {
	switch t {
	case events.AgentStatus:
		return "\x1b[1;37m" // bold white
	case events.AgentStepStart:
		return "\x1b[1;36m" // bold cyan
	case events.AgentToolCall:
		return "\x1b[36m" // cyan
	case events.AgentToolResult:
		return "\x1b[32m" // green
	case events.AgentMessage:
		return "\x1b[35m" // magenta
	case events.AgentHypothesis:
		return "\x1b[1;33m" // bold yellow
	case events.ProposalCreated:
		return "\x1b[1;31m" // bold red
	default:
		return ""
	}
}

func (s *terminalSink) Emit(_ context.Context, typ events.Type, payload any) error {
	c := colorByType(typ)
	reset := ""
	if c != "" {
		reset = "\x1b[0m"
	}
	payloadJSON, _ := json.Marshal(payload)
	_, err := fmt.Fprintf(s.w, "%s%-20s%s %s\n", c, typ, reset, payloadJSON)
	return err
}
