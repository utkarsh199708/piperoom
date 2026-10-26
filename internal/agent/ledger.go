package agent

import (
	"strings"
	"sync"

	"piperoom/internal/tools/runbooks"
)

// maxLedgerSummaryBytes caps the summary kept per step. It is the summary the
// model saw (already capped by tools.MaxSummaryBytes); the cap here is a
// second bound so the ledger's size does not depend on a tool's discipline.
const maxLedgerSummaryBytes = 8000

// maxLedgerSteps caps the steps a ledger keeps besides the triage seeds; the
// oldest go first (#447). The runner lives across episodes, so without a cap
// a long room grows by up to 16 KB a read for its whole life. 256 is several
// times the reads one episode makes (defaultMaxTurns turns, a few reads
// each), so a step the model saw recently is still known; an evicted
// citation gets the "not a step of this run" refusal, which tells the model
// to re-read. Worst case about 4 MB a room.
const maxLedgerSteps = 256

// stepLedger remembers the steps this run opened, so update_hypothesis can
// check the steps a hypothesis cites. It holds no room state: after a restart
// it is empty and the model must re-read, which is the intended behaviour.
type stepLedger struct {
	mu    sync.Mutex
	steps map[string]stepFact
	order []string // step ids, oldest first
	// pinned are the triage seeds: the first message tells the model to cite
	// them for the whole room, so they are never evicted.
	pinned map[string]bool
	// background is the text the model was given up front (alert, pipeline map),
	// lower-cased once. A term in it needs no step to back it (see groundTerms).
	background string

	// scanNames, flagged and cited serve the echo guard (echoguard.go):
	// the names ScanNames looks for in a read, the reads injectscan flagged
	// by step id, and the steps the latest hypothesis cites.
	scanNames []string
	flagged   map[string]flaggedRead
	cited     []string
}

// stepFact is what the ledger keeps of one step.
type stepFact struct {
	tool    string
	input   string // the tool input as the model wrote it
	summary string
	isError bool
}

func newStepLedger() *stepLedger {
	return &stepLedger{steps: map[string]stepFact{}, pinned: map[string]bool{}}
}

// add records a step with no input. A nil ledger ignores it.
func (l *stepLedger) add(stepID, tool, summary string, isError bool) {
	l.addWithInput(stepID, tool, "", summary, isError)
}

// addWithInput records a step and the input its tool was called with. A name the
// model typed into the input (a connector, a topic) is backed by the step it ran in.
func (l *stepLedger) addWithInput(stepID, tool, input, summary string, isError bool) {
	l.put(stepID, tool, input, summary, isError, false)
}

// put records a step, pinned or not, and evicts the oldest unpinned steps
// past maxLedgerSteps.
func (l *stepLedger) put(stepID, tool, input, summary string, isError, pin bool) {
	if l == nil || stepID == "" {
		return
	}
	if len(summary) > maxLedgerSummaryBytes {
		cut := maxLedgerSummaryBytes
		for cut > 0 && s8(summary[cut]) {
			cut--
		}
		summary = summary[:cut]
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.steps[stepID]; !ok {
		l.order = append(l.order, stepID)
	}
	if len(input) > maxLedgerSummaryBytes {
		input = cutBytes(input, maxLedgerSummaryBytes)
	}
	l.steps[stepID] = stepFact{tool: tool, input: input, summary: summary, isError: isError}
	if pin {
		l.pinned[stepID] = true
	}
	for len(l.order)-len(l.pinned) > maxLedgerSteps {
		l.evictOldest()
	}
}

// evictOldest drops the oldest unpinned step. The caller holds mu.
func (l *stepLedger) evictOldest() {
	for i, id := range l.order {
		if l.pinned[id] {
			continue
		}
		delete(l.steps, id)
		delete(l.flagged, id)
		l.order = append(l.order[:i], l.order[i+1:]...)
		return
	}
}

// get returns the step's fact.
func (l *stepLedger) get(stepID string) (stepFact, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.steps[stepID]
	return f, ok
}

// ledgerStep is one step with its id, as snapshot and citedSteps return it.
type ledgerStep struct {
	id   string
	fact stepFact
}

// snapshot returns every step, newest first.
func (l *stepLedger) snapshot() []ledgerStep {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ledgerStep, 0, len(l.order))
	for i := len(l.order) - 1; i >= 0; i-- {
		id := l.order[i]
		out = append(out, ledgerStep{id: id, fact: l.steps[id]})
	}
	return out
}

// citedSteps returns the known steps among ids, in ids order. Unknown ids are left
// out (checkSteps refuses them first).
func (l *stepLedger) citedSteps(ids []string) []ledgerStep {
	out := make([]ledgerStep, 0, len(ids))
	for _, id := range ids {
		if f, ok := l.get(id); ok {
			out = append(out, ledgerStep{id: id, fact: f})
		}
	}
	return out
}

// recent returns up to n of the most recent step ids, newest last.
func (l *stepLedger) recent(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.order) <= n {
		return append([]string(nil), l.order...)
	}
	return append([]string(nil), l.order[len(l.order)-n:]...)
}

// s8 reports whether b is a UTF-8 continuation byte.
func s8(b byte) bool { return b&0xC0 == 0x80 }

// seedTriage adds the reads of the triage pass, which the first message shows as
// <tool_output step="s_..."> and tells the model to cite instead of repeating.
// It applies firstLookBlock's own filter (a real step id, at most triagePassReads),
// so the ledger holds exactly the ids the model was shown.
//
// Each seeded read is also scanned for the echo guard (scanRead), exactly as
// a live read is by wrapReadToolOutput: the triage pass ran in its own runner,
// so its flagged spans never reach this ledger otherwise, and with the pass on
// an injection in its first connect_status read would be unguarded. Call
// setScanNames first, or the spans carry no names. runbook_lookup is skipped
// as wrapReadToolOutput skips it.
func (l *stepLedger) seedTriage(t *TriageResult) {
	if t == nil {
		return
	}
	n := 0
	for _, s := range t.Steps {
		if n == triagePassReads {
			break
		}
		if !triageStepID.MatchString(s.StepID) {
			continue
		}
		n++
		text := s.Summary
		if s.IsError && !strings.HasPrefix(text, "error: ") {
			text = "error: " + text
		}
		l.put(s.StepID, s.Tool, "", text, s.IsError, true)
		if s.Tool != runbooks.ToolName {
			l.scanRead(s.StepID, s.Tool, s.Summary)
		}
	}
}

// setBackground records the text the model was given up front.
func (l *stepLedger) setBackground(text string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.background = strings.ToLower(text)
}

// backgroundText returns the text set by setBackground.
func (l *stepLedger) backgroundText() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.background
}

// alertBackground is the alert text that goes into the first message: title,
// pipeline, description, labels, watcher evidence and the pipeline map. A hypothesis
// that names a component from there is not making it up.
func alertBackground(a Alert) string {
	parts := []string{a.Title, a.PipelineID, alertDescription(a), alertLabels(a), watcherEvidence(a.Raw), a.PipelineMap}
	return strings.Join(parts, "\n")
}
