package agent

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"piperoom/internal/setup/knowledge"
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

// The setup tuning gate (U10b). In a setup room a step is a tuning step when
// its config keys map to a tuning entry of the knowledge index for the room's
// source connector (knowledge.Tuning, row U10a) — no new step kind, op or
// contract. Such a step is allowed only when the plan cites a symptom step by
// id: a slow or stalled snapshot, or an OOM or heap error read in the logs.
// A new pipeline is never tuned by default.

// setupTuningEvidenceTools are the reads that can show a setup tuning
// symptom: the snapshot progress metric, and the logs an OOM or heap error
// lands in (the container's, the Connect worker's, or a host unit's journal).
var setupTuningEvidenceTools = []string{"debezium_snapshot_progress", "container_logs", "connect_worker_logs", "journal_tail"}

// tuningIndex loads the shipped knowledge index once. On a load error the
// gate never fires (no key can map to an entry), which is fail-open only in
// theory: the same index ships in this binary, and loading it is what every
// setup_config_help call does.
var tuningIndex = sync.OnceValue(func() *knowledge.Index {
	ix, err := knowledge.LoadIndex()
	if err != nil {
		return nil
	}
	return ix
})

// setupTuningKeys returns, sorted, the step's config keys that map to a
// tuning entry for the room's source connector: the entries of a
// properties_write (worker and env scopes) and the config of a connector
// create or update (connector scope). An unparsable params set gates nothing
// here — the action's own Validate refuses it with the real reason.
func setupTuningKeys(source string, params json.RawMessage) []string {
	ix := tuningIndex()
	if ix == nil {
		return nil
	}
	var p struct {
		Entries map[string]string `json:"entries"`
		Config  map[string]any    `json:"config"`
	}
	if err := json.Unmarshal(params, &p); err != nil || len(p.Entries)+len(p.Config) == 0 {
		return nil
	}
	tuned := map[string]bool{}
	for _, e := range ix.Tuning(source) {
		for _, k := range e.Keys {
			tuned[k.Name] = true
		}
	}
	var out []string
	for k := range p.Entries {
		if tuned[k] {
			out = append(out, k)
		}
	}
	for k := range p.Config {
		if tuned[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// setupTuningEvidence returns nil when text cites a step of this run that is
// a setupTuningEvidenceTools read with a result. The error wraps
// tools.ErrBadInput, so the model sees a refused step and nothing is stored;
// a nil ledger (a harness without a runner) knows no step, so it refuses.
func (l *stepLedger) setupTuningEvidence(step int, action string, keys []string, text string) error {
	for _, id := range citedStepRe.FindAllString(text, -1) {
		if l == nil {
			break
		}
		if f, ok := l.get(id); ok && !f.isError && slices.Contains(setupTuningEvidenceTools, f.tool) {
			return nil
		}
	}
	return fmt.Errorf("%w: step %d (%s): %s is a tuning key: a new pipeline is tuned only after a symptom — cite in the step description the step id (s_...) of a read of this run that shows it (a slow or stalled snapshot in debezium_snapshot_progress, or an OOM or heap error in container_logs, connect_worker_logs or journal_tail); never tune by default",
		tools.ErrBadInput, step, action, strings.Join(keys, ", "))
}
