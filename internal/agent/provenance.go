package agent

import (
	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// receipt turns the calls behind one tool result into the provenance of an
// agent.tool_result event: the calls the tool set itself first, then the ones
// its clients recorded under rec. It applies the caps (tools.CapProvenance)
// and masks secrets in every string, because a driver error can put a
// connection string in a target and a parameter can be a secret. The model
// never sees it: it only goes to the room log. A tool that made no call gives
// nil.
func (r *runner) receipt(tool string, set []tools.Call, rec *tools.Recorder) []events.ToolCall {
	calls := append(append([]tools.Call(nil), set...), rec.Calls()...)
	if len(calls) == 0 {
		return nil
	}
	calls, _ = tools.CapProvenance(calls)
	out := make([]events.ToolCall, len(calls))
	for i, c := range calls {
		target, st := redact.Text(c.Target)
		r.logRedaction(tool, "provenance", st)
		text, st := redact.Text(c.Text)
		r.logRedaction(tool, "provenance", st)
		var params []string
		for _, p := range c.ParamsRedacted {
			p, st = redact.Text(p)
			r.logRedaction(tool, "provenance", st)
			params = append(params, p)
		}
		out[i] = events.ToolCall{
			Kind: c.Kind, Target: target, Text: text, ParamsRedacted: params,
			At: c.At, TookMS: c.TookMS,
		}
	}
	return out
}
