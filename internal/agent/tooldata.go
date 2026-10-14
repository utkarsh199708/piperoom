package agent

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/jsonnum"
)

// maxToolDataBytes caps one tool result's Data as the agent emits it. It is the
// room's PIPEROOM_EVENT_MAX_DATA_BYTES default (64 KiB). The room cuts the same
// way at store time, so the agent's own event is already within the limit, and
// sinks that keep events without the room (evals, shadow runs) stay bounded.
const maxToolDataBytes = 64 << 10

// encodeToolData marshals a tool's Data to JSON for a tool_result event. Numbers
// outside the double range are kept as strings (jsonnum.Quote); n counts them.
// A value that cannot be encoded returns a wrapped error, never a panic.
func encodeToolData(v any) (out json.RawMessage, n int, err error) {
	defer func() {
		// A tool's own MarshalJSON may panic; the run must survive it.
		if p := recover(); p != nil {
			err = fmt.Errorf("agent: encode tool data: panic: %v", scrubPanic(p))
		}
	}()
	b, err := json.Marshal(v)
	if err != nil {
		return nil, 0, fmt.Errorf("agent: encode tool data: %w", err)
	}
	q, n := jsonnum.Quote(b)
	return q, n, nil
}

// capToolDataJSON returns raw unchanged when it fits in maxToolDataBytes.
// Otherwise it returns an events.TruncatedData object that holds the original
// size and a preview of the start of raw, cut at a UTF-8 boundary. The preview
// is halved until the encoded object fits, because JSON escaping can grow it.
// The bool reports whether the data was cut.
//
// The preview is cut mid-token on purpose: this is the shape the room service
// and the UI already read (events.TruncatedData). A valid-JSON cut would need a
// new shape, which is a contract change.
func capToolDataJSON(raw json.RawMessage) (json.RawMessage, bool, error) {
	if len(raw) <= maxToolDataBytes {
		return raw, false, nil
	}
	td := events.TruncatedData{Truncated: true, OriginalBytes: int64(len(raw))}
	n := maxToolDataBytes
	for {
		td.Preview = cutUTF8(string(raw), n)
		out, err := json.Marshal(td)
		if err != nil {
			return nil, false, fmt.Errorf("agent: encode truncated tool data: %w", err)
		}
		if len(out) <= maxToolDataBytes || n == 0 {
			return out, true, nil
		}
		n /= 2
	}
}

// cutUTF8 returns at most n bytes of s, never splitting a rune.
func cutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
