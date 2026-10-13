package agent

import (
	"fmt"
	"log/slog"

	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// Read tools return text from systems the room does not control: connector
// configs, error traces, record values. Secrets in that text (a DSN password
// in a connection error, a SASL JAAS config, a bearer token in a log line)
// must not reach the model, the room log or anything fed from it (WebSocket,
// Slack, exports, shadow runs). These helpers are the single place read-tool
// output is masked, applied right after tool.Run and before the result is
// truncated, emitted or wrapped for the prompt. Masking before truncation
// matters: a cut-off secret is harder to recognize.

// scrubToolResult masks secrets in a read tool's summary and data.
func (r *runner) scrubToolResult(tool string, res tools.Result) tools.Result {
	summary, st := redact.Text(res.Summary)
	data, dst := redact.JSONStats(res.Data)
	res.Summary, res.Data = summary, data
	r.logRedaction(tool, "result", st)
	r.logRedaction(tool, "data", dst)
	return res
}

// scrubToolError masks secrets in a read tool's error text. Driver errors
// often echo the connection string they failed with. The result's Error() is
// the masked text; errors.Is and errors.As still reach the original.
func (r *runner) scrubToolError(tool string, err error) error {
	msg, st := redact.Text(err.Error())
	if st.Total == 0 {
		return err
	}
	r.logRedaction(tool, "error", st)
	return &scrubbedError{msg: msg, err: err}
}

// scrubbedError is a tool error with secrets masked out of its text.
type scrubbedError struct {
	msg string
	err error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.err }

// scrubPanic masks secrets in a recovered panic value. A value with nothing
// to mask is returned as is, so callers that compare it still can; otherwise
// the masked text replaces it.
func scrubPanic(p any) any {
	s, st := redact.Text(fmt.Sprint(p))
	if st.Total == 0 {
		return p
	}
	return s
}

// logRedaction records that masking happened. It logs counts per kind only,
// never the text.
func (r *runner) logRedaction(tool, part string, st redact.Stats) {
	if st.Total == 0 {
		return
	}
	attrs := []any{"tool", tool, "part", part, "count", st.Total}
	for k, n := range st.ByKind {
		attrs = append(attrs, slog.Int("kind_"+string(k), n))
	}
	r.log.Info("agent: masked secrets in tool output", attrs...)
}
