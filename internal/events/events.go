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

	// Room limits (docs/design/retention.md). The first three are stored in
	// the room log; room.deleted never is (see RoomDeletedPayload).
	RoomRetentionWarning Type = "room.retention_warning"
	RoomExtended         Type = "room.extended"
	RoomLogCapWarning    Type = "room.log_cap_warning"
	RoomDeleted          Type = "room.deleted"

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

	// Shadow mode (docs/design/shadow.md). All three are written only to a
	// closed shadow room, after room.closed.
	ShadowRevealed Type = "shadow.revealed"
	ShadowLabelled Type = "shadow.labelled"
	ShadowGraded   Type = "shadow.graded"
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
	// AlertSource is the route that opened the room: "webhook" (dev X-User
	// route), a signed source ("alertmanager", "grafana", "pagerduty",
	// "datadog", "generic") or the built-in watcher ("watcher"; dedup key
	// "watcher:<pipeline>", no HTTP route). Empty for rooms not opened by an alert.
	AlertSource string `json:"alert_source,omitempty"`
	// DedupKey is the exact index key the room was opened under, so a restart
	// rebuilds the same alert index instead of re-deriving it from Alert.
	DedupKey string `json:"dedup_key,omitempty"`
	// Shadow is true when the room's pipeline has shadow enabled in owners.yaml
	// at open time: the agent runs silently, proposes nothing, and its output
	// is revealed after the room closes. Frozen here so a restart keeps it.
	// Never set by CreateRoom (POST /api/rooms).
	Shadow bool `json:"shadow,omitempty"`
}

// RoomClosedPayload ends a room. A room closed by the open-room limit
// (PIPEROOM_MAX_OPEN_DAYS) is closed by the system actor with the resolution
// "retention: open longer than N days".
type RoomClosedPayload struct {
	Resolution string `json:"resolution"`
}

// RoomRetentionWarningPayload says the open-room limit will close the room at
// CloseAt. Written by the system actor once per deadline, 24 h before it (at
// once when less is left); an extension moves the deadline and gets its own
// warning.
type RoomRetentionWarningPayload struct {
	CloseAt  time.Time `json:"close_at"`
	OpenDays int       `json:"open_days"` // days from opened_at to CloseAt
}

// RoomExtendedPayload records the room's one extension of the open-room limit
// (POST /api/rooms/{id}/extend, driver only). CloseAt is the new deadline:
// the old one plus Days.
type RoomExtendedPayload struct {
	Days    int       `json:"days"`
	CloseAt time.Time `json:"close_at"`
}

// RoomLogCapWarningPayload says the room log reached 90% of a cap
// (PIPEROOM_ROOM_MAX_EVENTS or PIPEROOM_ROOM_MAX_BYTES, bytes counted on the
// stored payloads). Written once per room by the system actor. At 100% agent
// events are refused and the agent stops (agent.status "log_full").
type RoomLogCapWarningPayload struct {
	Events    int64 `json:"events"`
	MaxEvents int64 `json:"max_events"`
	Bytes     int64 `json:"bytes"`
	MaxBytes  int64 `json:"max_bytes"`
}

// RoomDeletedPayload says an admin deleted the room (DELETE /api/rooms/{id}).
// It is never stored: the log is deleted with the room. The WebSocket sends it
// as the last "event" frame of each open connection to the room, with Seq =
// the room's last seq + 1, the admin as actor, and then closes with the close
// reason "room deleted".
type RoomDeletedPayload struct {
	Reason string `json:"reason"`
}

// AgentStatusPayload reports the agent's state. "log_full" is written by the
// system actor when the room log reached its cap: the agent has stopped and
// cannot be woken in this room.
type AgentStatusPayload struct {
	State  string `json:"state"` // "working" | "idle" | "waiting_approval" | "error" | "done" | "queued" | "log_full"
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

// AgentToolResultPayload records a tool result inside a step. A Data larger
// than PIPEROOM_EVENT_MAX_DATA_BYTES is stored as a TruncatedData object
// instead, and Truncated is then true.
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

// TruncatedData replaces a tool_result.data over PIPEROOM_EVENT_MAX_DATA_BYTES.
// Preview is the start of the original JSON text, cut at a UTF-8 boundary so
// the whole object stays under the limit; it is not valid JSON on its own.
type TruncatedData struct {
	Truncated     bool   `json:"truncated"` // always true
	OriginalBytes int64  `json:"original_bytes"`
	Preview       string `json:"preview"`
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
	// ParamsHash is hex SHA-256 of the RFC 8785 (JCS) form of the frozen
	// proposal (docs/design/auth.md §3.1). Empty on events written before auth PR 4.
	ParamsHash string `json:"params_hash,omitempty"`
}

// Identity says who made a decision and how they logged in
// (docs/design/auth.md §3.2). SessionID is the public session id, never the token.
type Identity struct {
	Iss       string    `json:"iss,omitempty"`
	Sub       string    `json:"sub,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	Method    string    `json:"method"`          // "oidc" | "dev"
	Grant     string    `json:"grant,omitempty"` // "user:…" | "group:…" | "backup:…" | "solo"
	AuthTime  time.Time `json:"auth_time,omitzero"`
}

// ApprovalGivenPayload is one layer owner's approval.
type ApprovalGivenPayload struct {
	ProposalID string    `json:"proposal_id"`
	Layer      string    `json:"layer"`
	ParamsHash string    `json:"params_hash,omitempty"`
	Identity   *Identity `json:"identity,omitempty"`
}

// ApprovalRejectedPayload is one layer owner's rejection.
type ApprovalRejectedPayload struct {
	ProposalID string    `json:"proposal_id"`
	Layer      string    `json:"layer"`
	Reason     string    `json:"reason"`
	ParamsHash string    `json:"params_hash,omitempty"`
	Identity   *Identity `json:"identity,omitempty"`
}

// ActionExecutedPayload is the result of running one approved step. An Output
// over PIPEROOM_EVENT_MAX_DATA_BYTES is cut at a UTF-8 boundary and ends with
// "… [truncated: N bytes]", N being the original size.
type ActionExecutedPayload struct {
	ProposalID string `json:"proposal_id"`
	StepIndex  int    `json:"step_index"`
	OK         bool   `json:"ok"`
	Output     string `json:"output"`
	// ParamsHash is the proposal's params_hash the executor checked before Execute.
	ParamsHash string `json:"params_hash,omitempty"`
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

// ---- Shadow mode ----

// ShadowRevealedPayload says a shadow run's result exists. Written once per
// room, by the system actor, when both the room is closed and the run has
// ended. It carries numbers only: the hypothesis itself is read from
// GET /api/rooms/{id}/shadow after the blind label, so this event cannot
// un-blind a labeller.
type ShadowRevealedPayload struct {
	Status string `json:"status"` // "hypothesised" | "no_hypothesis" | "error" | "budget" | "interrupted"
	// Seconds from rooms.opened_at; -1 when there was no hypothesis.
	TimeToFirstHypothesisS int64 `json:"time_to_first_hypothesis_s"`
	TimeToFinalHypothesisS int64 `json:"time_to_final_hypothesis_s"`
	// HumanMTTRS is closed_at - opened_at in seconds.
	HumanMTTRS         int64 `json:"human_mttr_s"`
	FinishedAfterClose bool  `json:"finished_after_close"`
	ToolCalls          int   `json:"tool_calls"`
	Tokens             int64 `json:"tokens"` // 0 when the runner counts no tokens (demo, plain Run)
}

// ShadowLabelledPayload is the blind label a pipeline member gives after the
// room is closed and before they may read the run. One per room.
type ShadowLabelledPayload struct {
	ActualLayer    string    `json:"actual_layer"`     // one of Layers
	RootCauseTerms []string  `json:"root_cause_terms"` // 1..20, each 1..200 chars
	Identity       *Identity `json:"identity,omitempty"`
}

// ShadowGradedPayload is a pipeline member's grade of the revealed run against
// the resolution and the label. Re-grading appends another; the last wins.
type ShadowGradedPayload struct {
	LayerCorrect bool      `json:"layer_correct"`
	RootCause    string    `json:"root_cause"` // "yes" | "partial" | "no"
	Notes        string    `json:"notes,omitempty"`
	Identity     *Identity `json:"identity,omitempty"`
}

// Layers are the pipeline layers that own nodes and approve fixes.
var Layers = []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse"}
