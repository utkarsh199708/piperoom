// Package events defines the incident room event log: every change in a room
// is one append-only event. The UI, Slack, hand-off and audit all read it.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Type names an event. Values are stable strings stored in the database and
// sent to the browser.
type Type string

const (
	RoomCreated Type = "room.created"
	RoomClosed  Type = "room.closed"

	AgentStatus     Type = "agent.status"
	AgentStepStart  Type = "agent.step.started"
	AgentToolCall   Type = "agent.tool_call"
	AgentToolResult Type = "agent.tool_result"
	AgentMessage    Type = "agent.message"
	AgentHypothesis Type = "agent.hypothesis"

	CommentAdded   Type = "comment.added"
	SteerSuggested Type = "steer.suggested"
	SteerSent      Type = "steer.sent"

	ControlRequested Type = "control.requested"
	ControlGranted   Type = "control.granted"

	ProposalCreated  Type = "proposal.created"
	ApprovalGiven    Type = "approval.given"
	ApprovalRejected Type = "approval.rejected"
	ActionExecuted   Type = "action.executed"

	HandoffCreated  Type = "handoff.created"
	HandoffAccepted Type = "handoff.accepted"

	TracePinned Type = "trace.pinned"
)

// ActorKind says who caused an event.
type ActorKind string

const (
	ActorHuman  ActorKind = "human"
	ActorAgent  ActorKind = "agent"
	ActorSystem ActorKind = "system"
)

// Actor is who caused an event.
type Actor struct {
	Kind ActorKind `json:"kind"`
	ID   string    `json:"id"`
	Name string    `json:"name"`
}

// AgentActor is the actor used for every event the agent emits.
var AgentActor = Actor{Kind: ActorAgent, ID: "agent", Name: "Agent"}

// Event is one entry in a room's log. Seq starts at 1 and has no gaps
// within a room.
type Event struct {
	RoomID  string          `json:"room_id"`
	Seq     int64           `json:"seq"`
	Type    Type            `json:"type"`
	Actor   Actor           `json:"actor"`
	Payload json.RawMessage `json:"payload"`
	At      time.Time       `json:"at"`
}

// Store is the append-only event log.
type Store interface {
	// Append stores one event and assigns the next Seq for the room atomically.
	// payload is marshalled to JSON.
	Append(ctx context.Context, roomID string, typ Type, actor Actor, payload any) (Event, error)
	// Since returns events with Seq > afterSeq in order, at most limit.
	Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]Event, error)
	// Subscribe replays events after afterSeq, then streams new ones live.
	// The channel closes when ctx ends. No event may be skipped or repeated.
	Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan Event, error)
}

// Decode unmarshals an event payload into T.
func Decode[T any](e Event) (T, error) {
	var v T
	if err := json.Unmarshal(e.Payload, &v); err != nil {
		return v, fmt.Errorf("events: decode %s payload: %w", e.Type, err)
	}
	return v, nil
}

// ---- Payloads (one struct per Type) ----

// RoomCreatedPayload starts a room.
type RoomCreatedPayload struct {
	Title      string          `json:"title"`
	Severity   string          `json:"severity"` // "sev1".."sev4"
	PipelineID string          `json:"pipeline_id"`
	Alert      json.RawMessage `json:"alert,omitempty"` // raw alert from the webhook
}

// RoomClosedPayload ends a room.
type RoomClosedPayload struct {
	Resolution string `json:"resolution"`
}

// AgentStatusPayload reports the agent's state.
type AgentStatusPayload struct {
	State  string `json:"state"` // "working" | "idle" | "waiting_approval" | "error" | "done"
	Detail string `json:"detail,omitempty"`
}

// AgentStepStartPayload opens one visible investigation step.
type AgentStepStartPayload struct {
	StepID string `json:"step_id"`
	Title  string `json:"title"`
}

// AgentToolCallPayload records a tool call inside a step.
type AgentToolCallPayload struct {
	StepID    string          `json:"step_id"`
	ToolUseID string          `json:"tool_use_id"`
	Tool      string          `json:"tool"`
	Input     json.RawMessage `json:"input"`
}

// AgentToolResultPayload records a tool result inside a step.
type AgentToolResultPayload struct {
	StepID    string          `json:"step_id"`
	ToolUseID string          `json:"tool_use_id"`
	Tool      string          `json:"tool"`
	Summary   string          `json:"summary"`
	Data      json.RawMessage `json:"data,omitempty"`
	IsError   bool            `json:"is_error"`
	Truncated bool            `json:"truncated,omitempty"`
	TookMS    int64           `json:"took_ms"`
}

// AgentMessagePayload is plain text the agent says to the room.
// ReplyTo is set when it answers a comment.
type AgentMessagePayload struct {
	Text    string `json:"text"`
	StepID  string `json:"step_id,omitempty"`
	ReplyTo string `json:"reply_to,omitempty"` // CommentID
}

// AgentHypothesisPayload is the agent's current best guess.
type AgentHypothesisPayload struct {
	Text         string   `json:"text"`
	BasedOnSteps []string `json:"based_on_steps"`
	Confidence   string   `json:"confidence"` // "low" | "medium" | "high"
	Confirmed    bool     `json:"confirmed"`  // true only after a human confirms
}

// CommentAddedPayload is a human comment, optionally anchored to a step.
type CommentAddedPayload struct {
	CommentID     string `json:"comment_id"`
	StepID        string `json:"step_id,omitempty"`
	Text          string `json:"text"`
	MentionsAgent bool   `json:"mentions_agent"`
}

// SteerSuggestedPayload is a non-driver's suggestion waiting for the driver.
type SteerSuggestedPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Text         string `json:"text"`
}

// SteerSentPayload is an instruction delivered to the agent by the driver.
type SteerSentPayload struct {
	SuggestionID string `json:"suggestion_id,omitempty"` // set when forwarding a suggestion
	Text         string `json:"text"`
}

// ControlRequestedPayload asks the driver for control.
type ControlRequestedPayload struct{}

// ControlGrantedPayload moves the driver role.
type ControlGrantedPayload struct {
	To         Actor     `json:"to"`
	LeaseUntil time.Time `json:"lease_until"`
}

// ProposalStep is one write action in a proposed fix.
type ProposalStep struct {
	Action      string          `json:"action"` // an actions.Action name
	Layer       string          `json:"layer"`  // see Layers
	Params      json.RawMessage `json:"params"`
	Description string          `json:"description"`
}

// ProposalCreatedPayload is a fix waiting for approvals.
type ProposalCreatedPayload struct {
	ProposalID     string         `json:"proposal_id"`
	Title          string         `json:"title"`
	Steps          []ProposalStep `json:"steps"`
	Risk           string         `json:"risk"`
	Rollback       string         `json:"rollback"`
	RequiredLayers []string       `json:"required_layers"`
}

// ApprovalGivenPayload is one layer owner's approval.
type ApprovalGivenPayload struct {
	ProposalID string `json:"proposal_id"`
	Layer      string `json:"layer"`
}

// ApprovalRejectedPayload is one layer owner's rejection.
type ApprovalRejectedPayload struct {
	ProposalID string `json:"proposal_id"`
	Layer      string `json:"layer"`
	Reason     string `json:"reason"`
}

// ActionExecutedPayload is the result of running one approved step.
type ActionExecutedPayload struct {
	ProposalID string `json:"proposal_id"`
	StepIndex  int    `json:"step_index"`
	OK         bool   `json:"ok"`
	Output     string `json:"output"`
}

// HandoffSummary is the agent-written shift hand-off.
type HandoffSummary struct {
	WhatBroke string   `json:"what_broke"`
	Why       string   `json:"why"`
	Done      []string `json:"done"`
	RuledOut  []string `json:"ruled_out"`
	Open      []string `json:"open"`
	Risks     []string `json:"risks"`
}

// HandoffCreatedPayload proposes moving the driver role to another person.
type HandoffCreatedPayload struct {
	HandoffID     string         `json:"handoff_id"`
	To            Actor          `json:"to"`
	Summary       HandoffSummary `json:"summary"`
	StayAsWatcher bool           `json:"stay_as_watcher"`
}

// HandoffAcceptedPayload completes a hand-off.
type HandoffAcceptedPayload struct {
	HandoffID string `json:"handoff_id"`
}

// TraceStage is one stage of a record trace.
type TraceStage struct {
	Layer  string            `json:"layer"`
	Name   string            `json:"name"`
	Status string            `json:"status"` // "seen" | "not_seen" | "stopped_here" | "unknown"
	Facts  map[string]string `json:"facts,omitempty"`
}

// TracePinnedPayload pins a record trace to the room.
type TracePinnedPayload struct {
	Table  string       `json:"table"`
	Key    string       `json:"key"`
	Stages []TraceStage `json:"stages"`
}

// Layers are the pipeline layers that own nodes and approve fixes.
var Layers = []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse"}
