package agent

import (
	"strings"
	"sync"
)

// maxLedgerSummaryBytes caps the summary kept per step. It is the summary the
// model saw (already capped by tools.MaxSummaryBytes); the cap here is a
// second bound so the ledger's size does not depend on a tool's discipline.
const maxLedgerSummaryBytes = 8000

// stepLedger remembers the steps this run opened, so update_hypothesis can
// check the steps a hypothesis cites. It holds no room state: after a restart
// it is empty and the model must re-read, which is the intended behaviour.
type stepLedger struct {
	mu    sync.Mutex
	steps map[string]stepFact
	order []string // step ids, oldest first
}

// stepFact is what the ledger keeps of one step.
type stepFact struct {
	tool    string
	summary string
	isError bool
}

func newStepLedger() *stepLedger {
	return &stepLedger{steps: map[string]stepFact{}}
}

// add records a step. A nil ledger ignores it.
func (l *stepLedger) add(stepID, tool, summary string, isError bool) {
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
	l.steps[stepID] = stepFact{tool: tool, summary: summary, isError: isError}
}

// get returns the step's fact.
func (l *stepLedger) get(stepID string) (stepFact, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.steps[stepID]
	return f, ok
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
		l.add(s.StepID, s.Tool, text, s.IsError)
	}
}
