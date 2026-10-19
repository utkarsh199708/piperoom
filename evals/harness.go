package evals

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// RecordedEvent is one event the agent produced during a run.
type RecordedEvent struct {
	Type    events.Type     `json:"type"`
	Payload json.RawMessage `json:"payload"`
	At      time.Time       `json:"at"`
	// Actor is who wrote the event in the room log. The room also writes
	// agent.message events as the system (a note for an unknown pipeline, a
	// backup approval); the scorer reads agent.* events from the agent only.
	// Zero in transcripts written before the room path.
	Actor events.Actor `json:"actor,omitzero"`
}

// fromAgent reports whether ev was written by the agent (or by an old
// transcript that did not record actors).
func fromAgent(ev RecordedEvent) bool {
	return ev.Actor.Kind == "" || ev.Actor.Kind == events.ActorAgent
}

// agentOnly reports whether only the agent's own events of this type are
// the agent's work: the room writes agent.message notes as the system too.
func agentOnly(t events.Type) bool {
	return strings.HasPrefix(string(t), "agent.") || t == events.ProposalCreated
}

// Transcript is everything one run produced, in order. Proposals appear in
// Events as proposal.created, the way the room service would log them.
type Transcript struct {
	Events []RecordedEvent `json:"events"`
	Misses []Miss          `json:"misses,omitempty"`
	// Attempts are every action Validate call, including the rejected ones
	// that never became a proposal.
	Attempts []ActionAttempt `json:"action_attempts,omitempty"`
	// Writes are the writes the replay clients took and the steps the room
	// executed, performed nowhere (replay_actions.go).
	Writes []ReplayWrite `json:"replay_writes,omitempty"`
	Idles  int           `json:"idles"`
	// Replay is what a phased or executing scenario's replay did: phases,
	// executed steps, transitions and phase-tagged answers. Nil for a static
	// scenario that executed nothing.
	Replay *ReplayTrace `json:"replay,omitempty"`
	// Room is how the room path ran: the room, how it opened, and the alert
	// the room service built for the agent. Nil for a run that never opened
	// a room.
	Room *RoomTrace `json:"room,omitempty"`
}

// RoomTrace records what the room service handed the agent.
type RoomTrace struct {
	RoomID string `json:"room_id"`
	// Entry is how the room opened: "alert" or "watcher".
	Entry string      `json:"entry"`
	Alert agent.Alert `json:"alert"`
}

// paramsMatch reports whether every scripted param is in the step's params
// with an equal value (the recording input rule).
func paramsMatch(want map[string]any, params json.RawMessage) bool {
	var got map[string]any
	if err := json.Unmarshal(params, &got); err != nil {
		return false
	}
	return matches(want, got)
}

// compactJSON renders step params for a result text.
func compactJSON(raw json.RawMessage) string {
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return string(raw)
	}
	return b.String()
}
