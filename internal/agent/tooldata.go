package agent

import (
	"encoding/json"
	"fmt"

	"piperoom/internal/jsonnum"
)

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
