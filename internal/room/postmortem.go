package room

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/version"
)

// PostmortemOption changes RenderPostmortem.
type PostmortemOption func(*pmOptions)

type pmOptions struct{ withHash bool }

// WithPostmortemHash adds the export's SHA-256 to the facts table. It costs a
// second pass over the log, so it is off by default (GET ...?with_hash=1).
func WithPostmortemHash() PostmortemOption { return func(o *pmOptions) { o.withHash = true } }

// pmProposal is what the log says about one proposal.
type pmProposal struct {
	ev        events.ProposalCreatedPayload
	approvals []string // "layer — name (grant, method) at"
	rejected  bool
	ran       map[int]bool // step index -> ok
	failed    bool
	withdrawn bool
	// withdrawReason is the withdrawer's one line (proposal.withdrawn.reason).
	withdrawReason string
}

// pmFixNote is a person's note of a fix made outside Piperoom (a comment.added
// of kind fix_note).
type pmFixNote struct {
	seq  int64
	at   time.Time
	who  events.Actor
	text string
}

// pmHandoff is what the log says about one hand-off: when it was written and
// what it said.
type pmHandoff struct {
	seq     int64
	at      time.Time
	summary events.HandoffSummary
}

// pmFold is the one-pass fold of a room log into the facts of the draft.
type pmFold struct {
	n                                      int64
	id, title, severity, pipeline, source  string
	openedAt                               time.Time
	closed                                 bool
	closedAt                               time.Time
	closedSeq                              int64
	closedBy                               events.Actor
	resolution                             string
	rows                                   []string
	hyp                                    *events.AgentHypothesisPayload
	hypSeq                                 int64
	verdicts                               []string // for the final hypothesis
	props                                  map[string]*pmProposal
	propOrder                              []string
	verification                           []string
	handoffs                               map[string]pmHandoff
	fixNotes                               []pmFixNote
	execSeq                                int64 // newest action.executed that succeeded; 0 when none
	execAt                                 time.Time
	draft                                  *events.PostmortemDraftedPayload
	draftSeq                               int64
	draftAt                                time.Time
	steps, toolCalls, toolFails, modelCall int
	inTok, outTok                          int64
	cost                                   float64
	priced, unpriced                       int
	firstHypS                              int64
}

func newPMFold() *pmFold {
	return &pmFold{props: map[string]*pmProposal{}, handoffs: map[string]pmHandoff{}, firstHypS: -1}
}

func (f *pmFold) row(e events.Event, text string) {
	d := e.At.Sub(f.openedAt)
	if d < 0 {
		d = 0
	}
	f.rows = append(f.rows, fmt.Sprintf("| %s | +%02d:%02d | %s |", e.At.UTC().Format("15:04:05"), int(d.Minutes()), int(d.Seconds())%60, text))
}

func actorName(a events.Actor) string { return mdTextN(a.Name, 80) }

func identityText(id *events.Identity) string {
	if id == nil {
		return ""
	}
	var parts []string
	if id.Grant != "" {
		parts = append(parts, mdTextN(id.Grant, 80))
	}
	if id.Method != "" {
		parts = append(parts, mdTextN(id.Method, 20))
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

// add folds one event. A payload that does not decode is left out: a draft must
// not fail because one event is odd.
func (f *pmFold) add(e events.Event) error {
	f.n++
	switch e.Type {
	case events.RoomCreated:
		if p, ok := decode[events.RoomCreatedPayload](e); ok {
			f.id, f.title, f.severity, f.pipeline, f.source = e.RoomID, p.Title, p.Severity, p.PipelineID, p.AlertSource
		}
		f.openedAt = e.At
		src := f.source
		if src == "" {
			src = "opened by hand"
		}
		f.row(e, "room opened ("+mdTextN(src, 80)+")")
	case events.RoomClosed:
		f.closed, f.closedAt, f.closedSeq, f.closedBy = true, e.At, e.Seq, e.Actor
		if p, ok := decode[events.RoomClosedPayload](e); ok {
			f.resolution = p.Resolution
		}
		f.row(e, "room closed by "+actorName(e.Actor))
	case events.CommentAdded:
		if p, ok := decode[events.CommentAddedPayload](e); ok {
			if p.Kind == events.CommentKindFixNote {
				f.fixNotes = append(f.fixNotes, pmFixNote{seq: e.Seq, at: e.At, who: e.Actor, text: p.Text})
				f.row(e, actorName(e.Actor)+" logged a fix note: "+mdTextN(p.Text, pmLineMax))
			} else {
				f.row(e, actorName(e.Actor)+" commented: "+mdTextN(p.Text, pmLineMax))
			}
		}
	case events.AgentStepStart:
		f.steps++
	case events.AgentToolCall:
		f.toolCalls++
	case events.AgentToolResult:
		if p, ok := decode[events.AgentToolResultPayload](e); ok && p.IsError {
			f.toolFails++
		}
	case events.AgentUsage:
		if p, ok := decode[events.AgentUsagePayload](e); ok {
			f.modelCall++
			f.inTok += p.InputTokens + p.CacheCreationInputTokens + p.CacheReadInputTokens
			f.outTok += p.OutputTokens
			if p.CostUSD != nil {
				f.cost += *p.CostUSD
				f.priced++
			} else {
				f.unpriced++
			}
		}
	case events.AgentHypothesis:
		if p, ok := decode[events.AgentHypothesisPayload](e); ok {
			f.hyp, f.hypSeq, f.verdicts = &p, e.Seq, nil
			if f.firstHypS < 0 {
				f.firstHypS = int64(max(e.At.Sub(f.openedAt), 0) / time.Second)
			}
			f.row(e, fmt.Sprintf("hypothesis (%s): %s", mdTextN(p.Confidence, 20), mdTextN(p.Text, pmLineMax)))
		}
	case events.HypothesisVerdict:
		if p, ok := decode[events.HypothesisVerdictPayload](e); ok {
			v := fmt.Sprintf("%s by %s", mdTextN(p.Verdict, 20), actorName(e.Actor))
			if p.HypothesisSeq == f.hypSeq {
				f.verdicts = append(f.verdicts, v)
			}
			f.row(e, "hypothesis "+v)
		}
	case events.AgentMessage:
		if p, ok := decode[events.AgentMessagePayload](e); ok && strings.HasPrefix(p.Text, "[verification] ") {
			t := mdTextN(strings.TrimPrefix(p.Text, "[verification] "), pmLineMax)
			f.verification = append(f.verification, t)
			f.row(e, "verification: "+t)
		}
	case events.AgentVerification:
		// A repeated last-step result ran no check: it adds no record.
		if p, ok := decode[events.AgentVerificationPayload](e); ok && !p.Repeat {
			t := mdCode(p.ProposalID) + ": " + mdTextN(p.Verdict, pmLineMax)
			f.verification = append(f.verification, t)
			f.row(e, "verification: "+t)
		}
	case events.ProposalCreated:
		if p, ok := decode[events.ProposalCreatedPayload](e); ok {
			if _, dup := f.props[p.ProposalID]; !dup {
				f.propOrder = append(f.propOrder, p.ProposalID)
			}
			f.props[p.ProposalID] = &pmProposal{ev: p, ran: map[int]bool{}}
			f.row(e, "proposal "+mdCode(p.ProposalID)+" proposed: "+mdTextN(p.Title, pmLineMax))
		}
	case events.ProposalWithdrawn:
		if p, ok := decode[events.ProposalWithdrawnPayload](e); ok {
			if pr := f.props[p.ProposalID]; pr != nil {
				pr.withdrawn, pr.withdrawReason = true, p.Reason
			}
			f.row(e, fmt.Sprintf("proposal %s withdrawn by %s: %s", mdCode(p.ProposalID), actorName(e.Actor), mdTextN(p.Reason, pmLineMax)))
		}
	case events.ApprovalGiven:
		if p, ok := decode[events.ApprovalGivenPayload](e); ok {
			a := fmt.Sprintf("%s — %s%s at %s", mdTextN(p.Layer, 40), actorName(e.Actor), identityText(p.Identity), e.At.UTC().Format("15:04:05"))
			if pr := f.props[p.ProposalID]; pr != nil {
				pr.approvals = append(pr.approvals, a)
			}
			f.row(e, fmt.Sprintf("approval given for %s: %s%s (%s)", mdCode(p.ProposalID), actorName(e.Actor), identityText(p.Identity), mdTextN(p.Layer, 40)))
		}
	case events.ApprovalRejected:
		if p, ok := decode[events.ApprovalRejectedPayload](e); ok {
			if pr := f.props[p.ProposalID]; pr != nil {
				pr.rejected = true
			}
			f.row(e, fmt.Sprintf("approval rejected for %s: %s%s (%s)", mdCode(p.ProposalID), actorName(e.Actor), identityText(p.Identity), mdTextN(p.Layer, 40)))
		}
	case events.ActionExecuted:
		if p, ok := decode[events.ActionExecutedPayload](e); ok {
			res := "ok"
			if !p.OK {
				res = "failed"
				if p.OutcomeUnknown {
					res = "outcome unknown"
				}
			} else {
				f.execSeq, f.execAt = e.Seq, e.At
			}
			if pr := f.props[p.ProposalID]; pr != nil {
				pr.ran[p.StepIndex] = p.OK
				if !p.OK {
					pr.failed = true
				}
			}
			f.row(e, fmt.Sprintf("%s step %d %s", mdCode(p.ProposalID), p.StepIndex+1, res))
		}
	case events.HandoffRequested:
		if p, ok := decode[events.HandoffRequestedPayload](e); ok {
			f.row(e, fmt.Sprintf("hand-off requested by %s to %s", actorName(e.Actor), actorName(p.To)))
		}
	case events.HandoffCreated:
		if p, ok := decode[events.HandoffCreatedPayload](e); ok {
			f.handoffs[p.HandoffID] = pmHandoff{seq: e.Seq, at: e.At, summary: p.Summary}
			by := ""
			if p.Manual {
				by = " (summary written by hand)"
			}
			f.row(e, fmt.Sprintf("hand-off %s created: %s to %s%s", mdCode(p.HandoffID), actorName(e.Actor), actorName(p.To), by))
		}
	case events.HandoffAccepted:
		if p, ok := decode[events.HandoffAcceptedPayload](e); ok {
			f.row(e, fmt.Sprintf("hand-off %s accepted by %s", mdCode(p.HandoffID), actorName(e.Actor)))
		}
	case events.HandoffFailed:
		if p, ok := decode[events.HandoffFailedPayload](e); ok {
			f.row(e, "hand-off failed ("+mdTextN(p.Reason, 40)+")")
		}
	case events.PostmortemDrafted:
		if p, ok := decode[events.PostmortemDraftedPayload](e); ok {
			f.draft, f.draftSeq, f.draftAt = &p, e.Seq, e.At
		}
	}
	return nil
}

// RenderPostmortem renders the room as a Markdown post-mortem draft. It reads
// the log once (the export's walk); the output is deterministic for a log.
// An open room renders the draft so far. Every string from an event is
// escaped (postmortem_escape.go), so the result is safe to show as text.
func (s *Service) RenderPostmortem(ctx context.Context, roomID string, opts ...PostmortemOption) ([]byte, error) {
	var o pmOptions
	for _, fn := range opts {
		fn(&o)
	}
	row, err := s.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	limit := s.exportMax
	if limit <= 0 {
		limit = DefaultExportMaxEvents
	}
	if row.LastSeq > limit {
		return nil, &Error{
			Status: 413, Code: "export_too_large",
			Msg: fmt.Sprintf("room has %d events; a post-mortem draft reads at most %d (page through GET /api/rooms/{id}/events instead)", row.LastSeq, limit),
		}
	}
	ex := &Export{svc: s, row: row, upTo: row.LastSeq}
	f := newPMFold()
	if _, err := ex.walk(ctx, f.add); err != nil {
		return nil, fmt.Errorf("room: postmortem %s: %w", roomID, err)
	}
	sum := ""
	if o.withHash {
		full, err := s.PrepareExport(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("room: postmortem %s: hash: %w", roomID, err)
		}
		sum = full.SHA256()
	}
	return f.render(sum), nil
}

// pmProse is the summary the draft's prose sections come from, with what the
// draft needs to judge whether it is still current.
type pmProse struct {
	sum    events.HandoffSummary
	have   bool
	source string // footer label
	// what names the summary in a label ("hand-off", "closing summary"); asOfSeq
	// and asOfAt say when it was written.
	what       string
	asOfSeq    int64
	asOfAt     time.Time
	violations []string // grounding rules it failed
	// dropped counts the summary lines left out as hand-off bookkeeping.
	dropped int
}

// prose returns the summary the draft's prose sections come from (see
// postmortem.drafted). Lines that only talk about the hand-off process, not the
// incident, are taken out of its lists (isHandoffNoise) and counted.
func (f *pmFold) prose() pmProse {
	var p pmProse
	if f.draft == nil {
		return pmProse{source: "none"}
	}
	switch f.draft.Source {
	case "handoff":
		if h, ok := f.handoffs[f.draft.SummaryRef]; ok {
			p = pmProse{sum: h.summary, have: true, source: "handoff " + mdCode(f.draft.SummaryRef), what: "hand-off", asOfSeq: h.seq, asOfAt: h.at}
		}
	case "model":
		if f.draft.Summary != nil {
			// The model read the log once, right after the close, and the
			// call can take minutes: a fix note logged meanwhile lands below
			// postmortem.drafted but was never seen (QA F13). The close is the
			// newest event the summary surely covers.
			seq, at := f.draftSeq, f.draftAt
			if f.closed && f.closedSeq > 0 && f.closedSeq < seq {
				seq, at = f.closedSeq, f.closedAt
			}
			p = pmProse{sum: *f.draft.Summary, have: true, source: "model", what: "closing summary", asOfSeq: seq, asOfAt: at, violations: f.draft.Violations}
		}
	}
	if !p.have {
		return pmProse{source: "none"}
	}
	for _, list := range []*[]string{&p.sum.Done, &p.sum.RuledOut, &p.sum.Open, &p.sum.Risks} {
		kept := make([]string, 0, len(*list))
		for _, it := range *list {
			if isHandoffNoise(it) {
				p.dropped++
				continue
			}
			kept = append(kept, it)
		}
		*list = kept
	}
	return p
}

// isHandoffNoise reports whether a line of a summary is about the hand-off
// process ("One handoff failed", "Agent summary lines were truncated") and not
// about the incident. The log already shows those events in the timeline; in
// "Still open" or "Risks" they read as problems of the pipeline. The match is
// deliberately narrow: a line must name the summary or the hand-off as a word
// (not part of an identifier such as orders_summary, and not followed by a
// data noun such as "table": QA F12) together with a failure or a cut.
func isHandoffNoise(line string) bool {
	l := strings.ToLower(line)
	cut := containsAny(l, "truncat", "cut off", "clipped", "shortened")
	if cut && hasProcessWord(l, "summary", "summaries") {
		return true
	}
	if hasProcessWord(l, "hand-off", "handoff", "hand off", "hand-offs", "handoffs") &&
		(cut || containsAny(l, "fail", "time out", "timed out", "timeout", "retried", "retry")) {
		return true
	}
	return false
}

// dataNouns follow a word that names a data object, not the agent's summary
// or the hand-off ("summary table", "summary rows").
var dataNouns = map[string]bool{
	"table": true, "tables": true, "view": true, "views": true, "row": true, "rows": true,
	"column": true, "columns": true, "partition": true, "partitions": true, "topic": true,
	"topics": true, "job": true, "jobs": true, "load": true, "loads": true, "data": true,
	"record": true, "records": true, "schema": true, "file": true, "files": true,
}

// hasProcessWord reports whether l (lower case) holds one of words as a word
// of its own: no identifier character (letter, digit, _ . - /) right before or
// after it, and the next word not a data noun.
func hasProcessWord(l string, words ...string) bool {
	for _, w := range words {
		for from := 0; from < len(l); {
			i := strings.Index(l[from:], w)
			if i < 0 {
				break
			}
			i += from
			end := i + len(w)
			from = i + 1
			if i > 0 && isIdentByte(l[i-1]) {
				continue
			}
			if end < len(l) && isIdentByte(l[end]) {
				continue
			}
			next := strings.Fields(strings.TrimLeft(l[end:], " \t,:;"))
			if len(next) > 0 && dataNouns[strings.Trim(next[0], ".,;:!?()")] {
				continue
			}
			return true
		}
	}
	return false
}

func isIdentByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_' || b == '.' || b == '-' || b == '/'
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// pmEvidence is a sign that someone fixed the problem: when it happened (seq,
// time) and what it was.
type pmEvidence struct {
	seq  int64
	at   time.Time
	what string
}

// fixEvidence returns the newest sign of a fix: a successful executed step, a
// fix note, or, in a room a person closed with a resolution where nothing ran
// from Piperoom and nobody logged a fix, the resolution itself (a person fixed
// it outside and said so on closing; see resolutionIsFix).
func (f *pmFold) fixEvidence() (pmEvidence, bool) {
	var ev pmEvidence
	if f.execSeq > 0 {
		ev = pmEvidence{seq: f.execSeq, at: f.execAt, what: "executed step"}
	}
	if n := len(f.fixNotes); n > 0 && f.fixNotes[n-1].seq > ev.seq {
		last := f.fixNotes[n-1]
		ev = pmEvidence{seq: last.seq, at: last.at, what: "fix note"}
	}
	if ev.seq == 0 && f.resolutionIsFix() {
		ev = pmEvidence{seq: f.closedSeq, at: f.closedAt, what: "resolution"}
	}
	return ev, ev.seq > 0
}

func (f *pmFold) hasResolution() bool {
	return f.closed && strings.TrimSpace(f.resolution) != ""
}

// resolutionIsFix says the resolution counts as a sign of a fix: a person
// closed the room with it. The system's own close (the open-room limit's
// "retention: ...") records why the room closed, not that anything was fixed
// (QA F11), so it never does.
func (f *pmFold) resolutionIsFix() bool {
	return f.hasResolution() && f.closedBy.Kind == events.ActorHuman
}

// fixedByPerson says the problem was fixed outside Piperoom: no step ran from
// it, and a person logged a fix or closed the room with a resolution.
func (f *pmFold) fixedByPerson() bool {
	return f.execSeq == 0 && (len(f.fixNotes) > 0 || f.resolutionIsFix())
}

func hhmm(t time.Time) string { return t.UTC().Format("15:04") }

// moneyText is a cost as "$0.12"; an amount that rounds to zero says so.
func moneyText(v float64) string {
	if v > 0 && v < 0.005 {
		return "less than $0.01"
	}
	return fmt.Sprintf("$%.2f", v)
}

func (f *pmFold) render(hash string) []byte {
	var b bytes.Buffer
	w := func(format string, a ...any) { fmt.Fprintf(&b, format, a...) }
	p := f.prose()
	sum := p.sum
	resolution := "none recorded"
	if strings.TrimSpace(f.resolution) != "" {
		resolution = mdText(f.resolution)
	}

	w("# Post-mortem: %s\n\n", mdText(f.title))
	status := "open (draft so far)"
	if f.closed {
		status = "closed"
	}
	w("Status: %s\n\n| | |\n|---|---|\n", status)
	w("| Severity | %s |\n| Pipeline | %s |\n", mdTextN(f.severity, 20), mdTextN(f.pipeline, 80))
	w("| Opened (UTC) | %s |\n", f.openedAt.UTC().Format("2006-01-02 15:04:05"))
	if f.closed {
		w("| Closed (UTC) | %s |\n| Duration | %s |\n| Closed by | %s |\n| Resolution | %s |\n",
			f.closedAt.UTC().Format("2006-01-02 15:04:05"), durationText(f.closedAt.Sub(f.openedAt)), actorName(f.closedBy), resolution)
	}
	w("| Room id | %s |\n", mdCode(f.id))
	if hash != "" {
		w("| Export sha256 | %s |\n", mdCode(hash))
	}

	w("\n## Timeline\n\n| time (UTC) | +mm:ss | what |\n|---|---|---|\n")
	for _, r := range f.rows {
		w("%s\n", r)
	}

	// Paragraphs are joined, so the section ends with one newline whatever it holds.
	w("\n## Root cause\n\n")
	var paras []string
	if f.hyp != nil {
		paras = append(paras, fmt.Sprintf("Final hypothesis (%s confidence): %s", mdTextN(f.hyp.Confidence, 20), mdText(f.hyp.Text)))
		if len(f.verdicts) > 0 {
			paras = append(paras, "Verdicts: "+strings.Join(f.verdicts, "; "))
		}
	} else {
		paras = append(paras, "No hypothesis recorded.")
	}
	if p.have && sum.Why != "" {
		paras = append(paras, "Why: "+mdText(sum.Why))
	}
	if f.closed {
		paras = append(paras, "Resolution: "+resolution)
	}
	if len(p.violations) > 0 {
		paras = append(paras, fmt.Sprintf("> Grounding check: the closing summary mentions details the log does not contain (%s). Verify them before relying on it.", mdText(strings.Join(p.violations, ", "))))
	}
	w("%s\n", strings.Join(paras, "\n\n"))

	f.writeDone(w)
	f.writeVerification(w)

	// Open items and risks go stale when the problem was fixed after the
	// summary that listed them was written: the hand-off said "restart the
	// consumer", a person restarted it and the room closed as fixed. Such lines
	// are set apart and labelled, never shown as current.
	ev, haveEv := f.fixEvidence()
	stale := p.have && haveEv && ev.seq > p.asOfSeq
	staleLabel := ""
	if stale {
		staleLabel = fmt.Sprintf("Earlier, from the %s at %s (before the fix: %s at %s), possibly resolved since:",
			p.what, hhmm(p.asOfAt), ev.what, hhmm(ev.at))
	}
	w("\n## Still open\n\n")
	shown := 0
	if !stale {
		shown = f.list(&b, sum.Open, p.have)
	}
	for _, id := range f.propOrder {
		if pr := f.props[id]; f.outcome(pr) == "waiting for approval" {
			if f.closed {
				w("- proposal %s was still waiting for approval when the room closed\n", mdCode(id))
			} else {
				w("- proposal %s is still waiting for approval\n", mdCode(id))
			}
			shown++
		}
	}
	f.writeProseTail(w, &b, sum.Open, shown, stale, staleLabel)

	w("\n## Risks\n\n")
	shown = 0
	if !stale {
		shown = f.list(&b, sum.Risks, p.have)
	}
	f.writeProseTail(w, &b, sum.Risks, shown, stale, staleLabel)

	w("\n## Ruled out\n\n")
	if f.list(&b, sum.RuledOut, p.have) == 0 {
		w("none recorded\n")
	}

	w("\n## Agent\n\n")
	first := "no hypothesis"
	if f.firstHypS >= 0 {
		first = fmt.Sprintf("first hypothesis after %d s", f.firstHypS)
	}
	cost := "not priced"
	switch {
	case f.priced > 0 && f.unpriced == 0:
		cost = moneyText(f.cost)
	case f.priced > 0:
		cost = moneyText(f.cost) + " (calls without a price not counted)"
	}
	w("- steps: %d\n- tool calls: %d (%d failed)\n- %s\n- model calls: %d\n- tokens: %d in, %d out\n- cost: %s\n",
		f.steps, f.toolCalls, f.toolFails, first, f.modelCall, f.inTok, f.outTok, cost)

	w("\nDrafted by Piperoom %s from %d events; summary source: %s\n", mdTextN(version.String(), 40), f.n, p.source)
	if p.dropped > 0 {
		// Hand-off bookkeeping is process, not incident: one line here, the
		// timeline has the events.
		lines, verb := "line", "was"
		if p.dropped > 1 {
			lines, verb = "lines", "were"
		}
		w("\nNote: %d summary %s about the hand-off process (a failed or truncated hand-off) %s left out of the sections above; the timeline shows the hand-off events.\n", p.dropped, lines, verb)
	}
	return b.Bytes()
}

// writeProseTail finishes "Still open" or "Risks" after its current lines
// (shown of them) are written: the "none recorded" line when nothing is
// current, and, for a stale summary, its items under the label.
func (f *pmFold) writeProseTail(w func(string, ...any), b *bytes.Buffer, items []string, shown int, stale bool, label string) {
	if shown == 0 {
		if stale && len(items) > 0 {
			w("none recorded after the fix\n")
		} else {
			w("none recorded\n")
		}
	}
	if stale && len(items) > 0 {
		w("\n%s\n", label)
		f.list(b, items, true)
	}
}

// writeDone writes "What was done": every proposal with its steps, approvals
// and outcome; the fix notes people logged; and, when nothing ran from
// Piperoom and nobody logged a fix, the resolution the room closed with.
func (f *pmFold) writeDone(w func(string, ...any)) {
	w("\n## What was done\n\n")
	wrote := false
	// gap puts one blank line between blocks.
	gap := func() {
		if wrote {
			w("\n")
		}
		wrote = true
	}
	for _, id := range f.propOrder {
		p := f.props[id]
		gap()
		w("### %s %s\n\n", mdCode(id), mdText(p.ev.Title))
		for i, st := range p.ev.Steps {
			w("%d. %s · %s · %s\n", i+1, mdCode(st.Action), mdTextN(st.Layer, 40), mdTextN(st.Description, pmLineMax))
		}
		for i, st := range p.ev.Steps {
			if len(st.Params) == 0 {
				continue
			}
			var c bytes.Buffer
			if json.Compact(&c, st.Params) != nil {
				continue
			}
			j := c.String()
			if j == "null" { // a step with no params is stored as JSON null
				continue
			}
			if len(j) > 2000 {
				j = j[:2000] + "…"
			}
			w("\nStep %d params:\n\n%s\n", i+1, mdFence("json", j))
		}
		w("\nApprovals:")
		if len(p.approvals) == 0 {
			w(" none\n")
		} else {
			w("\n")
			for _, a := range p.approvals {
				w("- %s\n", a)
			}
		}
		w("\nOutcome: %s\n", f.outcome(p))
	}
	if len(f.fixNotes) > 0 {
		gap()
		w("### Fix notes (done outside Piperoom)\n\n")
		for _, n := range f.fixNotes {
			w("- %s · %s: %s\n", n.at.UTC().Format("15:04:05"), actorName(n.who), mdText(n.text))
		}
	}
	if f.execSeq == 0 && len(f.fixNotes) == 0 && f.hasResolution() {
		gap()
		ran := "Nothing was run from Piperoom."
		for _, p := range f.props {
			if len(p.ran) > 0 {
				ran = "No step succeeded from Piperoom."
			}
		}
		w("%s %s closed the room at %s with the resolution: %s\n",
			ran, actorName(f.closedBy), f.closedAt.UTC().Format("15:04:05"), mdText(f.resolution))
	}
	if !wrote {
		w("No fix was proposed.\n")
	}
}

// writeVerification writes "Verification": the agent's records, or, for a room
// a person fixed, the fact that the fix was made outside Piperoom and nothing
// verified it, so the section does not contradict "What was done".
func (f *pmFold) writeVerification(w func(string, ...any)) {
	w("\n## Verification\n\n")
	switch {
	case len(f.verification) > 0:
		for _, v := range f.verification {
			w("- %s\n", v)
		}
	case f.fixedByPerson():
		src := "per the resolution"
		switch {
		case len(f.fixNotes) > 0 && f.resolutionIsFix():
			src = "per the fix notes and the resolution"
		case len(f.fixNotes) > 0:
			src = "per the fix notes"
		}
		w("No agent verification ran: the fix was made outside Piperoom, %s.\n", src)
	default:
		w("No verification recorded.\n")
	}
}

// list writes items as bullets (when the prose exists) and returns the count.
func (f *pmFold) list(b *bytes.Buffer, items []string, have bool) int {
	if !have {
		return 0
	}
	for _, it := range items {
		fmt.Fprintf(b, "- %s\n", mdText(it))
	}
	return len(items)
}

// outcome says where a proposal ended, from the log alone.
func (f *pmFold) outcome(p *pmProposal) string {
	switch {
	case p.rejected:
		return "rejected"
	case p.failed:
		return "a step failed"
	case len(p.ev.Steps) > 0 && len(p.ran) >= len(p.ev.Steps):
		return "executed"
	case len(p.ran) > 0:
		return "partly executed"
	case p.withdrawn:
		if strings.TrimSpace(p.withdrawReason) == "" {
			return "withdrawn"
		}
		return "withdrawn (" + mdTextN(p.withdrawReason, 120) + ")"
	}
	return "waiting for approval"
}

func durationText(d time.Duration) string {
	d = max(d, 0)
	return fmt.Sprintf("%d min %d s", int(d.Minutes()), int(d.Seconds())%60)
}
