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

// Limits on what one past room contributes to the index.
const (
	keyEventLimit    = 64  // room.created, room.closed, hand-offs, labels
	resultEventLimit = 100 // agent.tool_result events read per room
	maxFingerprints  = 8   // distinct traces kept per room
	maxFieldBytes    = 2000
	maxTraceTextKeep = 1500 // trace text indexed per room
)

var (
	keyTypes = []events.Type{events.RoomCreated, events.RoomClosed, events.HandoffCreated, events.ShadowLabelled}
	resTypes = []events.Type{events.AgentToolResult}
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
	// Hand-off summary fields of the last hand-off.
	WhatBroke string
	Why       string
	Open      []string
	Risks     []string
	// Traces are the fingerprints of error traces found in tool results.
	Traces []fingerprints
	// traceText is the text of those traces, clipped, so error words in a query
	// can match even when no fingerprint does.
	traceText string
	fetched   time.Time
}

// loadRoom reads one closed room into a pastRoom. Shadow rooms contribute
// human text only: the main log of a shadow room holds no shadow-agent output
// (it goes to the shadow log, which this tool never reads), and the agent-
// written hand-off and tool results are skipped as well so the blind run can
// never reach another room through this tool.
func loadRoom(ctx context.Context, src Source, r store.Room, now time.Time) (pastRoom, error) {
	pr := pastRoom{RoomID: r.ID, PipelineID: r.PipelineID, Title: r.Title, fetched: now}
	if r.ClosedAt != nil {
		pr.ClosedAt = *r.ClosedAt
	}
	keys, err := src.EventsOfTypes(ctx, r.ID, keyTypes, keyEventLimit)
	if err != nil {
		return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
	}
	shadow := false
	for _, e := range keys {
		switch e.Type {
		case events.RoomCreated:
			p, err := events.Decode[events.RoomCreatedPayload](e)
			if err != nil {
				continue
			}
			shadow = p.Shadow
			if p.Title != "" {
				pr.Title = p.Title
			}
		case events.RoomClosed:
			if p, err := events.Decode[events.RoomClosedPayload](e); err == nil {
				pr.Resolution = p.Resolution
			}
		case events.ShadowLabelled:
			// One label per room; the last wins if there were ever two.
			p, err := events.Decode[events.ShadowLabelledPayload](e)
			if err != nil || !slices.Contains(events.Layers, p.ActualLayer) {
				continue
			}
			pr.Layer, pr.Terms = p.ActualLayer, p.RootCauseTerms
		}
	}
	if !shadow {
		for _, e := range keys {
			if e.Type != events.HandoffCreated {
				continue
			}
			if p, err := events.Decode[events.HandoffCreatedPayload](e); err == nil {
				pr.WhatBroke, pr.Why = p.Summary.WhatBroke, p.Summary.Why
				pr.Open, pr.Risks = p.Summary.Open, p.Summary.Risks
			}
		}
		res, err := src.EventsOfTypes(ctx, r.ID, resTypes, resultEventLimit)
		if err != nil {
			return pastRoom{}, fmt.Errorf("similarincidents: room %s: %w", r.ID, err)
		}
		pr.addTraces(res)
	}
	return pr, nil
}

// addTraces fingerprints the error traces in tool results, first seen first.
func (pr *pastRoom) addTraces(res []events.Event) {
	var text strings.Builder
	for _, e := range res {
		if len(pr.Traces) >= maxFingerprints {
			break
		}
		p, err := events.Decode[events.AgentToolResultPayload](e)
		if err != nil {
			continue
		}
		fp, ok := fingerprint(p.Summary)
		if !ok || slices.Contains(pr.Traces, fp) {
			continue
		}
		pr.Traces = append(pr.Traces, fp)
		if text.Len() < maxTraceTextKeep {
			text.WriteString(extractTrace(p.Summary))
			text.WriteByte('\n')
		}
	}
	pr.traceText = clipBytes(text.String(), maxTraceTextKeep)
}
