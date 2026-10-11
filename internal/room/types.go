package room

import (
	"errors"
	"net/http"
	"time"

	"piperoom/internal/events"
)

// Error is a rule violation the HTTP layer maps to a status and error code
// (docs/api.md §Errors).
type Error struct {
	Status  int    // HTTP status
	Code    string // e.g. "not_driver"
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, msg string) *Error {
	return &Error{Status: status, Code: code, Message: msg}
}

// AsError returns the *Error inside err, if any.
func AsError(err error) (*Error, bool) {
	var re *Error
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// User is an authenticated person (dev/users.yaml).
type User struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Layers []string `json:"layers"`
}

// Actor converts u to an event actor.
func (u User) Actor() events.Actor {
	return events.Actor{Kind: events.ActorHuman, ID: u.ID, Name: u.Name}
}

// Person is an id/name pair.
type Person struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Driver is the current driver and when their lease ends.
type Driver struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	LeaseUntil time.Time `json:"lease_until"`
}

// AgentStatus is the agent's latest status.
type AgentStatus struct {
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// Hypothesis is the agent's latest hypothesis.
type Hypothesis struct {
	Text         string   `json:"text"`
	BasedOnSteps []string `json:"based_on_steps"`
	Confidence   string   `json:"confidence"`
	Confirmed    bool     `json:"confirmed"`
}

// OpenProposal is a proposal that has not finished (not rejected, not fully executed).
type OpenProposal struct {
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Steps          []events.ProposalStep `json:"steps"`
	Risk           string                `json:"risk"`
	Rollback       string                `json:"rollback"`
	RequiredLayers []string              `json:"required_layers"`
	Approvals      map[string]string     `json:"approvals"` // layer -> pending|approved|rejected
}

// Suggestion is a non-driver steer waiting for the driver.
type Suggestion struct {
	SuggestionID string `json:"suggestion_id"`
	From         Person `json:"from"`
	Text         string `json:"text"`
}

// Member is a room participant shown in the snapshot.
type Member struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"` // "driver" | "watching"
}

// Snapshot is docs/api.md RoomSnapshot: state folded from the event log.
type Snapshot struct {
	ID                 string         `json:"id"`
	Title              string         `json:"title"`
	Severity           string         `json:"severity"`
	PipelineID         string         `json:"pipeline_id"`
	Status             string         `json:"status"` // "open" | "closed"
	OpenedAt           time.Time      `json:"opened_at"`
	ClosedAt           *time.Time     `json:"closed_at"`
	Resolution         string         `json:"resolution"`
	LastSeq            int64          `json:"last_seq"`
	Driver             *Driver        `json:"driver"`
	AgentStatus        AgentStatus    `json:"agent_status"`
	Hypothesis         *Hypothesis    `json:"hypothesis"`
	OpenProposals      []OpenProposal `json:"open_proposals"`
	PendingSuggestions []Suggestion   `json:"pending_suggestions"`
	Members            []Member       `json:"members"`
}

// Timeouts and lease rules from docs/api.md.
const (
	LeaseDuration    = 30 * time.Minute
	LeaseRenewBelow  = 25 * time.Minute
	LeaseRenewMinGap = 5 * time.Minute
	StatusOpen       = "open"
	StatusClosed     = "closed"
	approvalPending  = "pending"
	approvalApproved = "approved"
	approvalRejected = "rejected"
	httpConflict     = http.StatusConflict
	httpForbidden    = http.StatusForbidden
	httpBadRequest   = http.StatusBadRequest
	httpNotFound     = http.StatusNotFound
)

// ---- Service inputs ----

// CreateRoomInput is POST /api/rooms.
type CreateRoomInput struct {
	Title      string `json:"title"`
	Severity   string `json:"severity"`
	PipelineID string `json:"pipeline_id"`
}

// CommentInput is POST /api/rooms/{id}/comments.
type CommentInput struct {
	StepID string `json:"step_id"`
	Text   string `json:"text"`
}

// HandoffInput is POST /api/rooms/{id}/handoffs.
type HandoffInput struct {
	To            string `json:"to"`
	StayAsWatcher bool   `json:"stay_as_watcher"`
}

// TraceInput is POST /api/rooms/{id}/trace.
type TraceInput struct {
	Table string `json:"table"`
	Key   string `json:"key"`
}

// EventsPage is the GET /api/rooms/{id}/events result.
type EventsPage struct {
	Events  []events.Event `json:"events"`
	LastSeq int64          `json:"last_seq"`
}
