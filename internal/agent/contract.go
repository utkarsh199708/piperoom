// Package agent runs the investigation loop with Claude.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
// The implementation (run.go and others) is owned by agent A4.
package agent

import (
	"context"
	"encoding/json"
	"log/slog"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// HumanInput is something a person said that the agent must read.
type HumanInput struct {
	Seq       int64        `json:"seq"` // room event seq it came from
	Actor     events.Actor `json:"actor"`
	Kind      string       `json:"kind"` // "steer" | "comment" | "approval_result" | "action_result"
	StepID    string       `json:"step_id,omitempty"`
	CommentID string       `json:"comment_id,omitempty"`
	Text      string       `json:"text"`
	// OK is set only for Kind "action_result": true when the step succeeded,
	// false when it failed. Nil means the outcome is unknown, and the agent must
	// then never claim the pipeline recovered.
	OK *bool `json:"ok,omitempty"`
	// ProposalID, Layer and StepIndex identify what an "approval_result" or
	// "action_result" is about, so the runner never parses Text for them.
	// Layer is set for approval_result; StepIndex for action_result.
	ProposalID string `json:"proposal_id,omitempty"`
	Layer      string `json:"layer,omitempty"`
	StepIndex  *int   `json:"step_index,omitempty"`
	// Approved is set only for Kind "approval_result": true for an approval,
	// false for a rejection.
	Approved *bool `json:"approved,omitempty"`
}

// Inbox delivers human input to the agent between model turns.
type Inbox interface {
	// Drain returns all pending inputs (possibly none) without blocking.
	Drain(ctx context.Context) ([]HumanInput, error)
	// Wait blocks until at least one input is pending or ctx ends.
	Wait(ctx context.Context) error
}

// Sink receives every event the agent produces. The room service implements
// it by appending to the room log; the CLI prints to the terminal.
type Sink interface {
	Emit(ctx context.Context, typ events.Type, payload any) error
}

// Proposals creates fix proposals that need human approval.
type Proposals interface {
	// Create validates every step and stores the proposal. It returns the
	// proposal ID. It must not execute anything.
	Create(ctx context.Context, p events.ProposalCreatedPayload) (string, error)
}

// Alert is what starts an investigation.
type Alert struct {
	RoomID     string          `json:"room_id"`
	Title      string          `json:"title"`
	PipelineID string          `json:"pipeline_id"`
	Raw        json.RawMessage `json:"raw,omitempty"`
	// PipelineMap is a text rendering of the topology, given to the model up front.
	PipelineMap string `json:"pipeline_map"`
}

// Deps are the agent's collaborators.
type Deps struct {
	Tools     *tools.Registry   // read-only tools from A2 and A3
	Actions   *actions.Registry // write actions the agent may PROPOSE (never run)
	Inbox     Inbox
	Sink      Sink
	Proposals Proposals
	Logger    *slog.Logger
	// Usage, when set, is called exactly once when Run ends, however it ends
	// (nil, error or panic), with the model tokens the room has used in this
	// process (input + output, cache included; the count behind
	// PIPEROOM_ROOM_TOKEN_BUDGET). Only an Endpoint run counts tokens; the
	// plain Run and the demo runner never call it. Shadow mode records the
	// value. Nil = not wanted.
	Usage func(used int64)
}

// Options tune the model call.
type Options struct {
	Model    string // default "claude-opus-5-5"
	Effort   string // default "high"
	MaxTurns int    // safety stop; default 40
	// TokenBudget caps model tokens for this run's room. 0 = the Endpoint's
	// default (PIPEROOM_ROOM_TOKEN_BUDGET). A value > 0 becomes the room's
	// limit when Run starts, even if the room's count already exists (a
	// hand-off may have created it); the count so far is kept. Shadow rooms
	// set it lower.
	TokenBudget int64
}

// Runner is implemented by A4 in run.go:
//
//	func Run(ctx context.Context, alert Alert, deps Deps, opts Options) error
//
// Run investigates until ctx ends or the room is closed. When the model
// finishes a turn it emits AgentStatus "idle" and calls Inbox.Wait.
// An Inbox whose Wait returns ErrInboxClosed ends the run cleanly (nil) after
// the current turn. When the room's token budget stops the run, Run returns an
// error wrapping *BudgetError after it has emitted the idle status and the
// message; a caller must treat it as a stop, not a failure (no retry, no
// non-zero exit).
type Runner func(ctx context.Context, alert Alert, deps Deps, opts Options) error

// HandoffWriter is implemented by A4 in handoff.go:
//
//	func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error)
//
// It makes one model call that turns the room log into a hand-off summary.
type HandoffWriter func(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error)
