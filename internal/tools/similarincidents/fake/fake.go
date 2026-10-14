// Package fake seeds an in-memory room store with past closed rooms, for tests
// of similar_incidents and for the eval fixtures that need the real tool's
// output for a known history.
package fake

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// ToolResult is one recorded tool result of a past room.
type ToolResult struct {
	Tool    string
	Summary string
	IsError bool
}

// Room describes one closed room.
type Room struct {
	ID         string
	PipelineID string
	Title      string
	ClosedAt   time.Time
	Resolution string
	// Shadow marks a room opened under shadow mode.
	Shadow bool
	// NoCreated leaves out room.created; CorruptCreated writes one whose
	// payload does not decode. Both must make the tool treat the room as
	// possibly shadow.
	NoCreated      bool
	CorruptCreated bool
	// Layer and LabelTerms, when Layer is set, add a shadow.labelled event.
	Layer      string
	LabelTerms []string
	// Handoff, when set, adds a handoff.created event (a person starts it).
	Handoff *events.HandoffSummary
	// MoreHandoffs are written after Handoff, in order.
	MoreHandoffs []events.HandoffSummary
	ToolResults  []ToolResult
	// UnverifiedCloser closes the room as a person the server cannot vouch
	// for (events.Actor.Verified false). The default closer is verified.
	UnverifiedCloser bool
	// Label, when set, adds a room.labelled event after the room's other events.
	Label *events.RoomLabelledPayload
	// Fix, when set, adds a proposal and its executed steps before closing.
	Fix *Fix
}

// Fix is one proposal of a past room and how its steps ended.
type Fix struct {
	ProposalID string
	Title      string
	Actions    []string
	// Failed marks the last executed step as failed (ok=false).
	Failed bool
	// NotRun creates the proposal without executing any step.
	NotRun bool
}

var (
	human    = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	verified = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun", Verified: true}
	sys      = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
)

// Seed creates the room in st, appends its events and closes it.
func Seed(ctx context.Context, st store.Store, r Room) error {
	pipeline := r.PipelineID
	if pipeline == "" {
		pipeline = "orders"
	}
	opened := r.ClosedAt.Add(-2 * time.Hour)
	if err := st.CreateRoom(ctx, store.Room{ID: r.ID, Title: r.Title, Severity: "sev2", PipelineID: pipeline, Status: "open", OpenedAt: opened}); err != nil {
		return fmt.Errorf("fake: create room %s: %w", r.ID, err)
	}
	app := func(typ events.Type, a events.Actor, p any) error {
		if _, err := st.Append(ctx, r.ID, typ, a, p); err != nil {
			return fmt.Errorf("fake: append %s to %s: %w", typ, r.ID, err)
		}
		return nil
	}
	switch {
	case r.NoCreated:
	case r.CorruptCreated:
		if err := app(events.RoomCreated, sys, []int{1}); err != nil {
			return err
		}
	default:
		if err := app(events.RoomCreated, sys, events.RoomCreatedPayload{Title: r.Title, Severity: "sev2", PipelineID: pipeline, Shadow: r.Shadow}); err != nil {
			return err
		}
	}
	for i, tr := range r.ToolResults {
		if err := app(events.AgentToolResult, events.AgentActor, events.AgentToolResultPayload{
			StepID: fmt.Sprintf("s%d", i), ToolUseID: fmt.Sprintf("u%d", i), Tool: tr.Tool, Summary: tr.Summary, IsError: tr.IsError,
			Data: []byte(`{"bulky":"detail that must not be read"}`),
		}); err != nil {
			return err
		}
	}
	if r.Handoff != nil {
		if err := app(events.HandoffCreated, human, events.HandoffCreatedPayload{HandoffID: "h-" + r.ID, To: human, Summary: *r.Handoff}); err != nil {
			return err
		}
	}
	for i, h := range r.MoreHandoffs {
		if err := app(events.HandoffCreated, human, events.HandoffCreatedPayload{HandoffID: fmt.Sprintf("h%d-%s", i, r.ID), To: human, Summary: h}); err != nil {
			return err
		}
	}
	if r.Layer != "" {
		if err := app(events.ShadowLabelled, human, events.ShadowLabelledPayload{ActualLayer: r.Layer, RootCauseTerms: r.LabelTerms}); err != nil {
			return err
		}
	}
	if r.Fix != nil {
		f := r.Fix
		var steps []events.ProposalStep
		for _, a := range f.Actions {
			steps = append(steps, events.ProposalStep{Action: a, Layer: "kafka-connect", Params: []byte(`{}`)})
		}
		if err := app(events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{ProposalID: f.ProposalID, Title: f.Title, Steps: steps}); err != nil {
			return err
		}
		for i := range steps {
			if f.NotRun {
				break
			}
			ok := !(f.Failed && i == len(steps)-1)
			if err := app(events.ActionExecuted, sys, events.ActionExecutedPayload{ProposalID: f.ProposalID, StepIndex: i, OK: ok, Output: "done"}); err != nil {
				return err
			}
		}
	}
	if r.Label != nil {
		if err := app(events.RoomLabelled, human, *r.Label); err != nil {
			return err
		}
	}
	closer := verified
	if r.UnverifiedCloser {
		closer = human
	}
	if err := app(events.RoomClosed, closer, events.RoomClosedPayload{Resolution: r.Resolution}); err != nil {
		return err
	}
	if err := st.CloseRoom(ctx, r.ID, r.ClosedAt); err != nil {
		return fmt.Errorf("fake: close room %s: %w", r.ID, err)
	}
	return nil
}

// NewStore returns a memstore holding rooms.
func NewStore(ctx context.Context, rooms ...Room) (*memstore.Store, error) {
	st := memstore.New()
	for _, r := range rooms {
		if err := Seed(ctx, st, r); err != nil {
			return nil, err
		}
	}
	return st, nil
}
