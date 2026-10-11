package room

import (
	"time"

	"piperoom/internal/events"
)

// Person is a bare identity (no role/state), used inside suggestions.
type Person struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Member is someone who has participated in a room.
type Member struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"` // "driver" | "watching"
}

// Driver is the current driver with their lease.
type Driver struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	LeaseUntil time.Time `json:"lease_until"`
}

// AgentStatus mirrors events.AgentStatusPayload.
type AgentStatus struct {
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// Hypothesis mirrors events.AgentHypothesisPayload.
type Hypothesis struct {
	Text         string   `json:"text"`
	BasedOnSteps []string `json:"based_on_steps"`
	Confidence   string   `json:"confidence"`
	Confirmed    bool     `json:"confirmed"`
}

// Proposal is a pending fix as shown in a snapshot.
type Proposal struct {
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Steps          []events.ProposalStep `json:"steps"`
	Risk           string                `json:"risk"`
	Rollback       string                `json:"rollback"`
	RequiredLayers []string              `json:"required_layers"`
	Approvals      map[string]string     `json:"approvals"` // layer -> approved|pending|rejected
}

// Suggestion is a non-driver's pending steer.
type Suggestion struct {
	SuggestionID string `json:"suggestion_id"`
	From         Person `json:"from"`
	Text         string `json:"text"`
}

// RoomSnapshot is the derived room state (docs/api.md), folded from events.
type RoomSnapshot struct {
	ID                 string       `json:"id"`
	Title              string       `json:"title"`
	Severity           string       `json:"severity"`
	PipelineID         string       `json:"pipeline_id"`
	Status             string       `json:"status"`
	OpenedAt           time.Time    `json:"opened_at"`
	LastSeq            int64        `json:"last_seq"`
	Driver             *Driver      `json:"driver"`
	AgentStatus        AgentStatus  `json:"agent_status"`
	Hypothesis         *Hypothesis  `json:"hypothesis"`
	OpenProposals      []Proposal   `json:"open_proposals"`
	PendingSuggestions []Suggestion `json:"pending_suggestions"`
	Members            []Member     `json:"members"`
}

// NewSnapshot returns a zero snapshot with non-nil slices so the JSON is []
// rather than null.
func NewSnapshot() RoomSnapshot {
	return RoomSnapshot{
		OpenProposals:      []Proposal{},
		PendingSuggestions: []Suggestion{},
		Members:            []Member{},
	}
}

// Clone returns a deep copy. The service returns clones so callers can read or
// encode a snapshot after the service has moved on (and folded more events).
func (s RoomSnapshot) Clone() RoomSnapshot {
	c := s
	if s.Driver != nil {
		d := *s.Driver
		c.Driver = &d
	}
	if s.Hypothesis != nil {
		h := *s.Hypothesis
		c.Hypothesis = &h
	}
	c.OpenProposals = cloneProposals(s.OpenProposals)
	if s.PendingSuggestions != nil {
		c.PendingSuggestions = append([]Suggestion{}, s.PendingSuggestions...)
	}
	if s.Members != nil {
		c.Members = append([]Member{}, s.Members...)
	}
	return c
}

func cloneProposals(in []Proposal) []Proposal {
	out := make([]Proposal, len(in))
	for i, p := range in {
		out[i] = p
		out[i].Steps = append([]events.ProposalStep(nil), p.Steps...)
		out[i].RequiredLayers = append([]string(nil), p.RequiredLayers...)
		if p.Approvals != nil {
			out[i].Approvals = make(map[string]string, len(p.Approvals))
			for k, v := range p.Approvals {
				out[i].Approvals[k] = v
			}
		}
	}
	return out
}

// Apply folds one event into the snapshot and returns the updated snapshot.
// It is a pure function: the input is not mutated.
func Apply(snap RoomSnapshot, e events.Event) RoomSnapshot {
	snap.LastSeq = e.Seq
	// Copy the slice headers whose elements Apply may mutate, so the input
	// snapshot is never modified (Apply is a pure function).
	snap.Members = append([]Member(nil), snap.Members...)
	snap.OpenProposals = append([]Proposal(nil), snap.OpenProposals...)
	snap.PendingSuggestions = append([]Suggestion(nil), snap.PendingSuggestions...)
	if e.Actor.Kind == events.ActorHuman {
		snap.Members = upsertMember(snap.Members, e.Actor.ID, e.Actor.Name, "watching")
	}
	switch e.Type {
	case events.RoomCreated:
		p, ok := decode[events.RoomCreatedPayload](e)
		if !ok {
			return snap
		}
		snap.ID = e.RoomID
		snap.Title = p.Title
		snap.Severity = p.Severity
		snap.PipelineID = p.PipelineID
		snap.Status = "open"
		snap.OpenedAt = e.At

	case events.RoomClosed:
		snap.Status = "closed"

	case events.AgentStatus:
		p, ok := decode[events.AgentStatusPayload](e)
		if !ok {
			return snap
		}
		snap.AgentStatus = AgentStatus{State: p.State, Detail: p.Detail}

	case events.AgentHypothesis:
		p, ok := decode[events.AgentHypothesisPayload](e)
		if !ok {
			return snap
		}
		snap.Hypothesis = &Hypothesis{
			Text:         p.Text,
			BasedOnSteps: p.BasedOnSteps,
			Confidence:   p.Confidence,
			Confirmed:    p.Confirmed,
		}

	case events.SteerSuggested:
		p, ok := decode[events.SteerSuggestedPayload](e)
		if !ok {
			return snap
		}
		snap.PendingSuggestions = append(snap.PendingSuggestions, Suggestion{
			SuggestionID: p.SuggestionID,
			From:         Person{ID: e.Actor.ID, Name: e.Actor.Name},
			Text:         p.Text,
		})

	case events.SteerSent:
		p, ok := decode[events.SteerSentPayload](e)
		if !ok {
			return snap
		}
		if p.SuggestionID != "" {
			snap.PendingSuggestions = removeSuggestion(snap.PendingSuggestions, p.SuggestionID)
		}

	case events.ControlGranted:
		p, ok := decode[events.ControlGrantedPayload](e)
		if !ok {
			return snap
		}
		if snap.Driver != nil && snap.Driver.ID != p.To.ID {
			snap.Members = setRole(snap.Members, snap.Driver.ID, "watching")
		}
		snap.Driver = &Driver{ID: p.To.ID, Name: p.To.Name, LeaseUntil: p.LeaseUntil}
		snap.Members = upsertMember(snap.Members, p.To.ID, p.To.Name, "driver")

	case events.ProposalCreated:
		p, ok := decode[events.ProposalCreatedPayload](e)
		if !ok {
			return snap
		}
		approvals := map[string]string{}
		for _, layer := range p.RequiredLayers {
			approvals[layer] = "pending"
		}
		snap.OpenProposals = append(snap.OpenProposals, Proposal{
			ProposalID:     p.ProposalID,
			Title:          p.Title,
			Steps:          p.Steps,
			Risk:           p.Risk,
			Rollback:       p.Rollback,
			RequiredLayers: p.RequiredLayers,
			Approvals:      approvals,
		})

	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](e)
		if !ok {
			return snap
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "approved")

	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](e)
		if !ok {
			return snap
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "rejected")
		// A rejection decides the proposal; it is no longer open.
		snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)

	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok {
			return snap
		}
		// The proposal leaves the open list when its last step runs, or when
		// any step fails (the executor stops there, so it is decided).
		if !p.OK || isLastStep(snap.OpenProposals, p.ProposalID, p.StepIndex) {
			snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)
		}
	}

	return snap
}

func decode[T any](e events.Event) (T, bool) {
	v, err := events.Decode[T](e)
	if err != nil {
		var zero T
		return zero, false
	}
	return v, true
}

// upsertMember adds or updates a member without mutating the input slice.
func upsertMember(members []Member, id, name, role string) []Member {
	for i := range members {
		if members[i].ID == id {
			if members[i].Name == name && (role != "driver" || members[i].Role == "driver") {
				return members
			}
			// Copy-on-write so a shared backing array is not mutated.
			out := append([]Member(nil), members...)
			out[i].Name = name
			if role == "driver" {
				out[i].Role = "driver"
			}
			return out
		}
	}
	return append(members, Member{ID: id, Name: name, Role: role})
}

// setRole changes a member's role without mutating the input slice.
func setRole(members []Member, id, role string) []Member {
	for i := range members {
		if members[i].ID == id {
			if members[i].Role == role {
				return members
			}
			out := append([]Member(nil), members...)
			out[i].Role = role
			return out
		}
	}
	return members
}

func removeSuggestion(suggestions []Suggestion, id string) []Suggestion {
	out := make([]Suggestion, 0, len(suggestions))
	for _, s := range suggestions {
		if s.SuggestionID != id {
			out = append(out, s)
		}
	}
	return out
}

func removeProposal(proposals []Proposal, id string) []Proposal {
	out := make([]Proposal, 0, len(proposals))
	for _, p := range proposals {
		if p.ProposalID != id {
			out = append(out, p)
		}
	}
	return out
}

func isLastStep(proposals []Proposal, proposalID string, stepIndex int) bool {
	for _, p := range proposals {
		if p.ProposalID == proposalID {
			return len(p.Steps) == 0 || stepIndex >= len(p.Steps)-1
		}
	}
	return false
}

// setApproval records a decision on a proposal's approvals map without
// mutating a shared map.
func setApproval(proposals []Proposal, proposalID, layer, decision string) {
	for i := range proposals {
		if proposals[i].ProposalID == proposalID {
			m := make(map[string]string, len(proposals[i].Approvals)+1)
			for k, v := range proposals[i].Approvals {
				m[k] = v
			}
			m[layer] = decision
			proposals[i].Approvals = m
			return
		}
	}
}
