package similarincidents

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/store"
)

// Limits on what one past room contributes to the index. Every kept field is
// clipped here, when the room is read, so the cache is bounded by
// rooms x pipelines x these caps whatever a room contains.
const (
	rareEventLimit    = 16  // room.created, room.closed, shadow.labelled: one each in practice
	handoffEventLimit = 64  // newest hand-offs read; the last one is used
	resultEventLimit  = 100 // newest agent.tool_result events read per room
	maxFingerprints   = 8   // distinct traces kept per room
	maxFieldBytes     = 2000
	maxTraceTextKeep  = 1500 // masked trace text indexed per room
	maxSummaryField   = 300
	maxListItems      = 4
	maxTerms          = 8
	maxTermBytes      = 60
	fixEventLimit     = 64 // newest proposal.created / action.executed events read per room
	maxFixActions     = 6
)

// minIncidentEvents is how many events a closed room needs to count as a real
// incident (G7). A test room has a handful of events; a room someone worked has
// far more, so this is a floor, not a target.
const minIncidentEvents = 8

var (
	rareTypes    = []events.Type{events.RoomCreated, events.RoomClosed, events.ShadowLabelled, events.RoomLabelled}
	hypoTypes    = []events.Type{events.AgentHypothesis}
	handoffTypes = []events.Type{events.HandoffCreated}
	resTypes     = []events.Type{events.AgentToolResult}
	fixTypes     = []events.Type{events.ProposalCreated, events.ActionExecuted}
)

// pastRoom is what is kept of one closed room. Everything in it is text that
// people or tools wrote, so it is untrusted data wherever it is shown.
type pastRoom struct {
	RoomID     string
	PipelineID string
	Title      string
	ClosedAt   time.Time
	// Events is the room's event count (its last seq), for isIncident.
	Events int
	// Hypothesis is true when the agent wrote a hypothesis. It is read only
	// for rooms whose agent text is trusted, like the rest of the agent output.
	Hypothesis bool
	// Labelled is true when a person labelled the room (room.labelled or
	// shadow.labelled).
	Labelled bool
	// HasResolution is true when the room was closed with a non-blank
	// resolution, whoever closed it. Resolution below may still be empty.
	HasResolution bool
	// Resolution is the text the closing person wrote.
	Resolution string
	// Layer and Terms are the human label (shadow.labelled): the layer that
	// was actually broken first, and the root-cause words. Empty if unlabelled.
	Layer string
	Terms []string
	// Cause, FixThatWorked and FixProposalID come from the last room.labelled
	// (typed by a person). FixProposal is the proposal whose every executed
	// step succeeded: its title and the action names of its steps.
	Cause         string
	FixThatWorked string
	FixProposalID string
	FixTitle      string
	FixActions    []string
	// Duration is room open to close; ClosedBy is the closer's name, kept
	// only when the server vouched for it (events.Actor.Verified).
	Duration time.Duration
	ClosedBy string
	// Hand-off summary fields of the last hand-off (index only, never returned).
	WhatBroke string
	Why       string
	Open      []string
	Risks     []string
	// Traces are the fingerprints of error traces found in tool results.
	Traces []fingerprints
	// traceText is the masked template text of those traces, clipped, so error
	// words in a query can match even when no fingerprint does. It is never the
	// raw trace: record values and DSNs must not become index terms.
	traceText string
	fetched   time.Time
	// lastSeq is the room's last event seq when it was read.
	lastSeq int64
}

// loadRoom reads one closed room into a pastRoom. Agent-written text (the
// hand-off and tool-result traces) is indexed only when room.created was read
// and decoded and says Shadow is false: a shadow room's agent output must never
// reach another room, and a missing or corrupt room.created must not be read as
// "not shadow". The shadow-agent run itself lives in the shadow log, which this
// tool never reads.
func loadRoom(ctx context.Context, src Source, r store.Room, now time.Time) (pastRoom, error) {
	pr := pastRoom{RoomID: r.ID, PipelineID: r.PipelineID, Title: clipRunes(scrub(r.Title), maxTitleBytes), Events: int(r.LastSeq), fetched: now, lastSeq: r.LastSeq}
	if r.ClosedAt != nil {
		pr.ClosedAt = *r.ClosedAt
	}
	rare, err := src.EventsOfTypes(ctx, r.ID, rareTypes, rareEventLimit)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	agentTextOK := false
	for _, e := range rare {
		switch e.Type {
		case events.RoomCreated:
			p, err := events.Decode[events.RoomCreatedPayload](e)
			if err != nil {
				agentTextOK = false
				continue
			}
			agentTextOK = !p.Shadow
			if p.Title != "" {
				pr.Title = clipRunes(scrub(p.Title), maxTitleBytes)
			}
		case events.RoomClosed:
			p, err := events.Decode[events.RoomClosedPayload](e)
			if err == nil && strings.TrimSpace(p.Resolution) != "" {
				pr.HasResolution = true
			}
			// Gap 10: a resolution typed by someone the server cannot vouch
			// for must not steer future rooms, so it is not indexed or shown.
			if e.Actor.Verified {
				pr.ClosedBy = clipBytes(oneLine(scrub(e.Actor.Name)), 60)
				if err == nil {
					pr.Resolution = clipBytes(scrub(p.Resolution), maxFieldBytes)
				}
			}
		case events.RoomLabelled:
			// Re-labelling appends another; the last wins.
			p, err := events.Decode[events.RoomLabelledPayload](e)
			if err != nil {
				continue
			}
			pr.Labelled = true
			pr.Cause = clipBytes(scrub(p.Cause), maxFieldBytes)
			pr.FixThatWorked = clipBytes(scrub(p.FixThatWorked), maxFieldBytes)
			pr.FixProposalID = p.FixProposalID
			if slices.Contains(events.Layers, p.ActualLayer) {
				pr.Layer = p.ActualLayer
			}
		case events.ShadowLabelled:
			// One label per room; the last wins if there were ever two.
			p, err := events.Decode[events.ShadowLabelledPayload](e)
			if err != nil {
				continue
			}
			pr.Labelled = true
			if !slices.Contains(events.Layers, p.ActualLayer) {
				continue
			}
			pr.Layer, pr.Terms = p.ActualLayer, nil
			for _, x := range p.RootCauseTerms[:min(len(p.RootCauseTerms), maxTerms)] {
				pr.Terms = append(pr.Terms, clipBytes(scrub(x), maxTermBytes))
			}
		}
	}
	if !r.OpenedAt.IsZero() && pr.ClosedAt.After(r.OpenedAt) {
		pr.Duration = pr.ClosedAt.Sub(r.OpenedAt)
	}
	if !agentTextOK {
		return pr, nil
	}
	hyp, err := src.EventsOfTypes(ctx, r.ID, hypoTypes, 1)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	pr.Hypothesis = len(hyp) > 0
	fix, err := src.EventsOfTypes(ctx, r.ID, fixTypes, fixEventLimit)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	pr.addFix(fix)
	hand, err := src.EventsOfTypes(ctx, r.ID, handoffTypes, handoffEventLimit)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	for _, e := range hand {
		if p, err := events.Decode[events.HandoffCreatedPayload](e); err == nil {
			pr.WhatBroke = clipBytes(scrub(p.Summary.WhatBroke), maxSummaryField)
			pr.Why = clipBytes(scrub(p.Summary.Why), maxSummaryField)
			pr.Open = clipList(p.Summary.Open)
			pr.Risks = clipList(p.Summary.Risks)
		}
	}
	res, err := src.EventsOfTypes(ctx, r.ID, resTypes, resultEventLimit)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	pr.addTraces(res)
	return pr, nil
}

// isIncident says whether a closed room is a real incident worth offering as
// history: it was closed with a resolution, it has a hypothesis or a human
// label, and it has enough events to have been worked. A test room titled
// "test" with a closing note has neither of the last two (G7).
func isIncident(pr pastRoom) bool {
	return pr.HasResolution && (pr.Hypothesis || pr.Labelled) && pr.Events >= minIncidentEvents
}

func clipList(xs []string) []string {
	var out []string
	for _, x := range xs[:min(len(xs), maxListItems)] {
		out = append(out, clipBytes(scrub(x), maxSummaryField))
	}
	return out
}

// scrub masks secrets in room text before it is indexed. Rooms closed before
// the room log was redacted (#96) still hold raw hand-offs and tool output;
// indexing them raw would let a query that guesses a secret confirm it by
// matching. It runs before clipping so a clip cannot cut a secret into a
// prefix the redactor no longer recognises. Text already redacted is unchanged
// (redact.Text is idempotent).
func scrub(s string) string {
	out, _ := redact.Text(s)
	return out
}

// addTraces fingerprints the error traces in tool results, first seen first.
// Results of this tool are skipped: they quote other rooms, and indexing them
// would make a room "similar" to the rooms it merely searched.
func (pr *pastRoom) addTraces(res []events.Event) {
	var text strings.Builder
	for _, e := range res {
		if len(pr.Traces) >= maxFingerprints {
			break
		}
		p, err := events.Decode[events.AgentToolResultPayload](e)
		if err != nil || p.Tool == ToolName {
			continue
		}
		fp, masked, ok := analyze(scrub(p.Summary))
		if !ok || slices.Contains(pr.Traces, fp) {
			continue
		}
		pr.Traces = append(pr.Traces, fp)
		if text.Len() < maxTraceTextKeep {
			text.WriteString(masked)
			text.WriteByte('\n')
		}
	}
	pr.traceText = clipBytes(text.String(), maxTraceTextKeep)
}

// addFix finds the proposal that fixed the room: every action.executed of it
// is ok and at least one ran; the label's fix_proposal_id picks it when set,
// otherwise the last such proposal wins. Its title is agent text, so this runs
// only for rooms whose agent text is trusted (not shadow).
func (pr *pastRoom) addFix(evs []events.Event) {
	type prop struct {
		title   string
		actions []string
		ran, ok int
	}
	props := map[string]*prop{}
	var order []string
	for _, e := range evs {
		switch e.Type {
		case events.ProposalCreated:
			p, err := events.Decode[events.ProposalCreatedPayload](e)
			if err != nil {
				continue
			}
			x := &prop{title: clipBytes(oneLine(scrub(p.Title)), maxSummaryField)}
			for _, st := range p.Steps[:min(len(p.Steps), maxFixActions)] {
				x.actions = append(x.actions, clipBytes(oneLine(scrub(st.Action)), maxTermBytes))
			}
			if _, seen := props[p.ProposalID]; !seen {
				order = append(order, p.ProposalID)
			}
			props[p.ProposalID] = x
		case events.ActionExecuted:
			p, err := events.Decode[events.ActionExecutedPayload](e)
			if err != nil {
				continue
			}
			if x := props[p.ProposalID]; x != nil {
				x.ran++
				if p.OK && !p.OutcomeUnknown {
					x.ok++
				}
			}
		}
	}
	chosen := ""
	for _, id := range order {
		x := props[id]
		if x.ran == 0 || x.ran != x.ok {
			continue
		}
		if id == pr.FixProposalID {
			chosen = id
			break
		}
		chosen = id
	}
	if chosen != "" {
		pr.FixTitle, pr.FixActions = props[chosen].title, props[chosen].actions
	}
}
