package similarincidents

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Limits on what one past room contributes to the index. Every kept field is
// clipped here, when the room is read, so the cache is bounded by
// rooms x pipelines x these caps whatever a room contains.
const (
	rareEventLimit    = 16  // room.created, room.closed, shadow.labelled: one each in practice
	handoffEventLimit = 64  // hand-offs; the last one is used
	resultEventLimit  = 100 // agent.tool_result events read per room
	maxFingerprints   = 8   // distinct traces kept per room
	maxFieldBytes     = 2000
	maxTraceTextKeep  = 1500 // masked trace text indexed per room
	maxSummaryField   = 300
	maxListItems      = 4
	maxTerms          = 8
	maxTermBytes      = 60
)

var (
	rareTypes    = []events.Type{events.RoomCreated, events.RoomClosed, events.ShadowLabelled}
	handoffTypes = []events.Type{events.HandoffCreated}
	resTypes     = []events.Type{events.AgentToolResult}
)

// pastRoom is what is kept of one closed room. Everything in it is text that
// people or tools wrote, so it is untrusted data wherever it is shown.
type pastRoom struct {
	RoomID     string
	PipelineID string
	Title      string
	ClosedAt   time.Time
	// Resolution is the text the closing person wrote.
	Resolution string
	// Layer and Terms are the human label (shadow.labelled): the layer that
	// was actually broken first, and the root-cause words. Empty if unlabelled.
	Layer string
	Terms []string
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
}

// loadRoom reads one closed room into a pastRoom. Agent-written text (the
// hand-off and tool-result traces) is indexed only when room.created was read
// and decoded and says Shadow is false: a shadow room's agent output must never
// reach another room, and a missing or corrupt room.created must not be read as
// "not shadow". The shadow-agent run itself lives in the shadow log, which this
// tool never reads.
func loadRoom(ctx context.Context, src Source, r store.Room, now time.Time) (pastRoom, error) {
	pr := pastRoom{RoomID: r.ID, PipelineID: r.PipelineID, Title: clipRunes(r.Title, maxTitleBytes), fetched: now}
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
				pr.Title = clipRunes(p.Title, maxTitleBytes)
			}
		case events.RoomClosed:
			if p, err := events.Decode[events.RoomClosedPayload](e); err == nil {
				pr.Resolution = clipBytes(p.Resolution, maxFieldBytes)
			}
		case events.ShadowLabelled:
			// One label per room; the last wins if there were ever two.
			p, err := events.Decode[events.ShadowLabelledPayload](e)
			if err != nil || !slices.Contains(events.Layers, p.ActualLayer) {
				continue
			}
			pr.Layer, pr.Terms = p.ActualLayer, nil
			for _, x := range p.RootCauseTerms[:min(len(p.RootCauseTerms), maxTerms)] {
				pr.Terms = append(pr.Terms, clipBytes(x, maxTermBytes))
			}
		}
	}
	if !agentTextOK {
		return pr, nil
	}
	hand, err := src.EventsOfTypes(ctx, r.ID, handoffTypes, handoffEventLimit)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	for _, e := range hand {
		if p, err := events.Decode[events.HandoffCreatedPayload](e); err == nil {
			pr.WhatBroke = clipBytes(p.Summary.WhatBroke, maxSummaryField)
			pr.Why = clipBytes(p.Summary.Why, maxSummaryField)
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

func clipList(xs []string) []string {
	var out []string
	for _, x := range xs[:min(len(xs), maxListItems)] {
		out = append(out, clipBytes(x, maxSummaryField))
	}
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
		fp, masked, ok := analyze(p.Summary)
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
