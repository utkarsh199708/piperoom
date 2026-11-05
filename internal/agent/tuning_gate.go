package agent

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"piperoom/internal/tools"
)

// The tuning gate (X6c). A performance step (connect_tune_throughput) is a
// change to how fast a working pipeline moves, so the agent may propose one
// only on a measured bottleneck: the step's description (or verify) must
// cite the step id of a read of this run that measures throughput, task
// balance or snapshot progress and returned a result. A tuning is never a fix
// for a failing connector: the action itself refuses a FAILED connector or
// task (its Validate), so that rule holds for the driver's proposals too.

// tuningEvidenceTools are the reads that can show a throughput bottleneck.
var tuningEvidenceTools = []string{"kafka_throughput", "connect_task_balance", "debezium_snapshot_progress"}

// citedStepRe finds step ids (newStepID) in free text.
var citedStepRe = regexp.MustCompile(`s_[0-9A-HJKMNP-TV-Z]{26}`)

// tuningEvidence returns nil when text cites a step of this run that is a
// tuningEvidenceTools read with a result. The error wraps tools.ErrBadInput:
// the model sees it as a refused step and nothing is stored. A nil ledger (a
// harness without a runner) knows no step, so it refuses.
func (l *stepLedger) tuningEvidence(step int, action, text string) error {
	for _, id := range citedStepRe.FindAllString(text, -1) {
		if l == nil {
			break
		}
		if f, ok := l.get(id); ok && !f.isError && slices.Contains(tuningEvidenceTools, f.tool) {
			return nil
		}
	}
	return fmt.Errorf("%w: step %d (%s): a tuning needs measured evidence: cite in the step description the step id (s_...) of a %s result of this run that shows the bottleneck; a tuning is never the fix for a failing connector",
		tools.ErrBadInput, step, action, strings.Join(tuningEvidenceTools, ", "))
}
