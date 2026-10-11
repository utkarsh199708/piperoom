package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"piperoom/internal/events"
)

// maxPrintedBytes caps the size of one printed payload so a huge tool result
// (e.g. pg_recent_ddl) cannot flood the terminal. Anything longer is cut and
// marked by truncatePrinted.
const maxPrintedBytes = 4000

// printedTruncateMarker is appended when truncatePrinted cuts a payload.
const printedTruncateMarker = "…(truncated)"

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
	text := truncatePrinted(sanitizeTerminal(string(payloadJSON)))
	_, err := fmt.Fprintf(s.w, "%s%-20s%s %s\n", c, typ, reset, text)
	return err
}

// sanitizeTerminal removes bytes a terminal could interpret as commands, so
// untrusted model/tool/room text cannot inject escape sequences into stdout:
// C0 controls except \n and \t, DEL (0x7F), and the C1 controls U+0080–U+009F.
// encoding/json already escapes the C0 range (including ESC) but not DEL or C1,
// so this closes the gap. Everything else, including normal Unicode, is kept
// unchanged.
func sanitizeTerminal(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\t':
			return r
		case r < 0x20: // remaining C0 controls, including ESC (0x1B)
			return -1
		case r == 0x7f: // DEL
			return -1
		case r >= 0x80 && r <= 0x9f: // C1 controls
			return -1
		default:
			return r
		}
	}, s)
}

// truncatePrinted cuts s to maxPrintedBytes, never splitting a UTF-8 rune, and
// appends printedTruncateMarker when it cuts anything. It mirrors
// tools.Truncate but with the sink's own, smaller limit and marker.
func truncatePrinted(s string) string {
	if len(s) <= maxPrintedBytes {
		return s
	}
	cut := maxPrintedBytes - len(printedTruncateMarker)
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + printedTruncateMarker
}
