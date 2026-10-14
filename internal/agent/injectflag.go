package agent

import (
	"strings"

	"piperoom/internal/injectscan"
	"piperoom/internal/tools/runbooks"
)

// suspectedAttr marks a tool_output whose data contains text that looks like
// instructions. It sits in the opening tag, outside the data: the data is
// escaped (& < >), so text inside it cannot forge the attribute.
const suspectedAttr = ` suspected_instructions="true"`

// untrustedAttr ends the opening tag wrapToolOutput writes.
const untrustedAttr = ` untrusted="true">`

// wrapReadToolOutput wraps a read tool's output like wrapToolOutput. When
// injectscan flags instruction-like text in it, the opening tag gets
// suspected_instructions="true" and the runner logs the tool, step and finding
// kinds. The data is unchanged (it may be the evidence) and the matched text
// is never logged: it is untrusted and may hold record values.
//
// runbook_lookup is not scanned: its text is curated repo content
// (internal/tools/runbooks/library), full of legitimate imperatives.
func (r *runner) wrapReadToolOutput(tool, stepID, text string) string {
	wrapped := wrapToolOutput(tool, stepID, text)
	if tool == runbooks.ToolName {
		return wrapped
	}
	findings := injectscan.Scan(text)
	if len(findings) == 0 {
		return wrapped
	}
	r.log.Warn("agent: tool output contains instruction-like text",
		"tool", tool, "step", stepID, "kinds", injectscan.Kinds(findings), "count", len(findings))
	return markSuspected(wrapped)
}

// markSuspected adds suspectedAttr to the opening tag of a wrapToolOutput
// result. The first untrustedAttr is the opening tag's: tool and step are
// names from code, and the data after it is escaped, so it has no '>'.
func markSuspected(wrapped string) string {
	i := strings.Index(wrapped, untrustedAttr)
	if i < 0 {
		return wrapped
	}
	return wrapped[:i] + ` untrusted="true"` + suspectedAttr + ">" + wrapped[i+len(untrustedAttr):]
}
