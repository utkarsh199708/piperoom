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
}

// pmFold is the one-pass fold of a room log into the facts of the draft.
type pmFold struct {
	n                                      int64
	id, title, severity, pipeline, source  string
	openedAt                               time.Time
	closed                                 bool
	closedAt                               time.Time
	closedBy                               events.Actor
	resolution                             string
	rows                                   []string
	hyp                                    *events.AgentHypothesisPayload
	hypSeq                                 int64
	verdicts                               []string // for the final hypothesis
	props                                  map[string]*pmProposal
	propOrder                              []string
	verification                           []string
	handoffs                               map[string]events.HandoffSummary
	draft                                  *events.PostmortemDraftedPayload
	steps, toolCalls, toolFails, modelCall int
	inTok, outTok                          int64
	cost                                   float64
	priced, unpriced                       int
	firstHypS                              int64
}

func newPMFold() *pmFold {
	return &pmFold{props: map[string]*pmProposal{}, handoffs: map[string]events.HandoffSummary{}, firstHypS: -1}
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
		f.closed, f.closedAt, f.closedBy = true, e.At, e.Actor
		if p, ok := decode[events.RoomClosedPayload](e); ok {
			f.resolution = p.Resolution
		}
		f.row(e, "room closed by "+actorName(e.Actor))
	case events.CommentAdded:
		if p, ok := decode[events.CommentAddedPayload](e); ok {
			f.row(e, actorName(e.Actor)+" commented: "+mdTextN(p.Text, pmLineMax))
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
		// A repeat carries no new check, only the first verdict again.
		if p, ok := decode[events.AgentVerificationPayload](e); ok && !p.Repeat {
			t := verificationLine(p)
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
			f.handoffs[p.HandoffID] = p.Summary
			f.row(e, fmt.Sprintf("hand-off %s created: %s to %s", mdCode(p.HandoffID), actorName(e.Actor), actorName(p.To)))
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
			f.draft = &p
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

// prose returns the summary the draft's prose sections come from, the source
// label for the footer and the grounding rules it failed.
func (f *pmFold) prose() (sum events.HandoffSummary, have bool, source string, violations []string) {
	if f.draft == nil {
		return sum, false, "none", nil
	}
	switch f.draft.Source {
	case "handoff":
		if h, ok := f.handoffs[f.draft.SummaryRef]; ok {
			return h, true, "handoff " + mdCode(f.draft.SummaryRef), nil
		}
	case "model":
		if f.draft.Summary != nil {
			return *f.draft.Summary, true, "model", f.draft.Violations
		}
	}
	return sum, false, "none", nil
}

func (f *pmFold) render(hash string) []byte {
	var b bytes.Buffer
	w := func(format string, a ...any) { fmt.Fprintf(&b, format, a...) }
	sum, haveSum, source, violations := f.prose()

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
			f.closedAt.UTC().Format("2006-01-02 15:04:05"), durationText(f.closedAt.Sub(f.openedAt)), actorName(f.closedBy), mdText(f.resolution))
	}
	w("| Room id | %s |\n", mdCode(f.id))
	if hash != "" {
		w("| Export sha256 | %s |\n", mdCode(hash))
	}

	w("\n## Timeline\n\n| time (UTC) | +mm:ss | what |\n|---|---|---|\n")
	for _, r := range f.rows {
		w("%s\n", r)
	}

	w("\n## Root cause\n\n")
	if f.hyp != nil {
		w("Final hypothesis (%s confidence): %s\n\n", mdTextN(f.hyp.Confidence, 20), mdText(f.hyp.Text))
		if len(f.verdicts) > 0 {
			w("Verdicts: %s\n\n", strings.Join(f.verdicts, "; "))
		}
	} else {
		w("No hypothesis recorded.\n\n")
	}
	if haveSum && sum.Why != "" {
		w("Why: %s\n\n", mdText(sum.Why))
	}
	if f.closed {
		w("Resolution: %s\n", mdText(f.resolution))
	}
	if len(violations) > 0 {
		w("\n> Grounding check: the closing summary mentions details the log does not contain (%s). Verify them before relying on it.\n", mdText(strings.Join(violations, ", ")))
	}

	w("\n## What was done\n\n")
	if len(f.propOrder) == 0 {
		w("No fix was proposed.\n")
	}
	for _, id := range f.propOrder {
		p := f.props[id]
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
		w("\nOutcome: %s\n\n", f.outcome(p))
	}

	w("## Verification\n\n")
	if len(f.verification) == 0 {
		w("No verification recorded.\n")
	}
	for _, v := range f.verification {
		w("- %s\n", v)
	}

	w("\n## Still open\n\n")
	n := f.list(&b, sum.Open, haveSum)
	for _, id := range f.propOrder {
		if p := f.props[id]; f.outcome(p) == "waiting for approval" {
			w("- proposal %s is still waiting for approval\n", mdCode(id))
			n++
		}
	}
	if n == 0 {
		w("none recorded\n")
	}
	w("\n## Risks\n\n")
	if f.list(&b, sum.Risks, haveSum) == 0 {
		w("none recorded\n")
	}
	w("\n## Ruled out\n\n")
	if f.list(&b, sum.RuledOut, haveSum) == 0 {
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
		cost = fmt.Sprintf("$%.2f", f.cost)
	case f.priced > 0:
		cost = fmt.Sprintf("$%.2f (calls without a price not counted)", f.cost)
	}
	w("- steps: %d\n- tool calls: %d (%d failed)\n- %s\n- model calls: %d\n- tokens: %d in, %d out\n- cost: %s\n",
		f.steps, f.toolCalls, f.toolFails, first, f.modelCall, f.inTok, f.outTok, cost)

	w("\nDrafted by Piperoom %s from %d events; summary source: %s\n", mdTextN(version.String(), 40), f.n, source)
	return b.Bytes()
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
	}
	return "waiting for approval"
}

func durationText(d time.Duration) string {
	d = max(d, 0)
	return fmt.Sprintf("%d min %d s", int(d.Minutes()), int(d.Seconds())%60)
}

// verificationLine renders one agent.verification as a single escaped line:
// the verdict, then each failed layer check and the data-flow sinks. Every
// string is untrusted tool output, so it goes through mdTextN.
func verificationLine(p events.AgentVerificationPayload) string {
	res := "not recovered"
	if p.Recovered {
		res = "recovered"
	}
	parts := []string{mdCode(p.ProposalID) + " " + res}
	if p.Verdict != "" {
		parts = append(parts, mdTextN(p.Verdict, pmLineMax))
	}
	for _, c := range p.Checks {
		if !c.OK {
			parts = append(parts, fmt.Sprintf("failed %s/%s: %s", mdTextN(c.Layer, 40), mdTextN(c.Name, 60), mdTextN(c.Detail, pmLineMax)))
		}
	}
	if d := p.DataFlow; d != nil {
		switch {
		case !d.Checked:
			parts = append(parts, "data flow not checked: "+mdTextN(d.Detail, pmLineMax))
		default:
			fresh := "stale"
			if d.Fresh {
				fresh = "fresh"
			}
			sinks := make([]string, 0, len(d.Sinks))
			for _, s := range d.Sinks {
				r := "failed"
				if s.Passed {
					r = "passed"
				}
				sinks = append(sinks, mdTextN(s.Table, 60)+" "+r)
			}
			line := "data flow " + fresh
			if len(sinks) > 0 {
				line += " (" + strings.Join(sinks, ", ") + ")"
			}
			parts = append(parts, line)
		}
	}
	return strings.Join(parts, "; ")
}
