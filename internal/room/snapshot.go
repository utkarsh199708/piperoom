package room

import (
	"time"

	"piperoom/internal/events"
)

// Person is a bare identity (no role/state), used inside suggestions.
// Verified mirrors events.Actor.Verified (docs/design/auth.md section 2.1),
// as do the Verified fields of Member and Driver.
type Person struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Verified bool   `json:"verified"`
}

// Member is someone who has participated in a room.
type Member struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Verified bool   `json:"verified"`
	Role     string `json:"role"` // "driver" | "watching"
}

// Driver is the current driver with their lease.
type Driver struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Verified   bool      `json:"verified"`
	LeaseUntil time.Time `json:"lease_until"`
	// LeaseExpired is the "nobody is driving" signal (docs/api.md, Lease
	// expiry): the last control.lease_expired names this driver and the
	// lease_until of the control.granted that set the current lease. The
	// next control.granted clears it. Omitted when false.
	LeaseExpired bool `json:"lease_expired,omitempty"`
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
	// ParamsHash is the proposal's params_hash (design section 3.1); the UI
	// sends it back with approve and reject. "" only for a proposal that
	// predates the hash and cannot be hashed.
	ParamsHash string `json:"params_hash"`
}

// Suggestion is a non-driver's pending steer.
type Suggestion struct {
	SuggestionID string `json:"suggestion_id"`
	From         Person `json:"from"`
	Text         string `json:"text"`
}

// RoomSnapshot is the derived room state (docs/api.md), folded from events.
type RoomSnapshot struct {
	ID         string     `json:"id"`
	Title      string     `json:"title"`
	Severity   string     `json:"severity"`
	PipelineID string     `json:"pipeline_id"`
	Status     string     `json:"status"`
	OpenedAt   time.Time  `json:"opened_at"`
	ClosedAt   *time.Time `json:"closed_at"`
	Resolution string     `json:"resolution"`
	// Shadow is room.created.shadow: the room's pipeline had shadow mode on at
	// open, so the agent ran silently and proposes nothing
	// (docs/design/shadow.md).
	Shadow             bool         `json:"shadow"`
	LastSeq            int64        `json:"last_seq"`
	Driver             *Driver      `json:"driver"`
	AgentStatus        AgentStatus  `json:"agent_status"`
	Hypothesis         *Hypothesis  `json:"hypothesis"`
	OpenProposals      []Proposal   `json:"open_proposals"`
	PendingSuggestions []Suggestion `json:"pending_suggestions"`
	Members            []Member     `json:"members"`

	// Room limits (docs/design/retention.md). AutoCloseAt is set by the
	// service, not by Apply: it depends on PIPEROOM_MAX_OPEN_DAYS. LogCap is
	// "ok", "warning" (room.log_cap_warning) or "full" (agent.status
	// log_full).
	AutoCloseAt *time.Time `json:"auto_close_at"`
	Extended    bool       `json:"extended"`
	LogCap      string     `json:"log_cap"`

	// extendedUntil is room.extended.close_at; warnedFor is the close_at of
	// the newest room.retention_warning. Only the service reads them.
	extendedUntil *time.Time
	warnedFor     *time.Time

	// grantedLease is lease_until of the last control.granted: the lease as
	// the log knows it. Driver.LeaseUntil can be later (unlogged renewals,
	// see touchDriverLease); control.lease_expired names grantedLease, so
	// every client folding the log matches it (lease_expiry.go).
	grantedLease time.Time

	// Shadow facts folded from the log after room.closed. Unexported: they are
	// not part of the snapshot JSON (docs/api.md), only the service reads them.
	shadowRevealed bool
	shadowLabelled bool
	shadowGrade    *events.ShadowGradedPayload // the last grade; nil before any
}

// Log cap states of RoomSnapshot.LogCap, and the agent state the system
// writes when the log is full.
const (
	LogCapOK      = "ok"
	LogCapWarning = "warning"
	LogCapFull    = "full"
	AgentLogFull  = "log_full"
)

// NewSnapshot returns a zero snapshot with non-nil slices and the agent state
// "idle" (docs/api.md: a new room's agent is idle; arrays are never null).
func NewSnapshot() RoomSnapshot {
	return RoomSnapshot{
		AgentStatus:        AgentStatus{State: "idle"},
		LogCap:             LogCapOK,
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
		if s.Hypothesis.BasedOnSteps != nil {
			h.BasedOnSteps = append([]string{}, s.Hypothesis.BasedOnSteps...)
		}
		c.Hypothesis = &h
	}
	if s.shadowGrade != nil {
		g := *s.shadowGrade
		c.shadowGrade = &g
	}
	c.AutoCloseAt = cloneTime(s.AutoCloseAt)
	c.extendedUntil = cloneTime(s.extendedUntil)
	c.warnedFor = cloneTime(s.warnedFor)
	c.OpenProposals = cloneProposals(s.OpenProposals)
	c.PendingSuggestions = append([]Suggestion{}, s.PendingSuggestions...)
	c.Members = append([]Member{}, s.Members...)
	return c
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func cloneProposals(in []Proposal) []Proposal {
	out := make([]Proposal, len(in))
	for i, p := range in {
		out[i] = p
		out[i].Steps = append([]events.ProposalStep{}, p.Steps...)
		out[i].RequiredLayers = append([]string{}, p.RequiredLayers...)
		if p.Approvals != nil {
			out[i].Approvals = make(map[string]string, len(p.Approvals))
			for k, v := range p.Approvals {
				out[i].Approvals[k] = v
			}
		} else {
			out[i].Approvals = map[string]string{}
		}
	}
	return out
}

// Apply folds one event into the snapshot and returns the updated snapshot.
// It is a pure function: the input is not mutated. Arrays are normalised to
// [] and the agent state defaults to "idle" so the JSON never contains null
// (docs/api.md).
func Apply(snap RoomSnapshot, e events.Event) RoomSnapshot {
	snap.LastSeq = e.Seq
	// Copy the slice headers whose elements Apply may mutate, so the input
	// snapshot is never modified (Apply is a pure function).
	snap.Members = append([]Member(nil), snap.Members...)
	snap.OpenProposals = append([]Proposal(nil), snap.OpenProposals...)
	snap.PendingSuggestions = append([]Suggestion(nil), snap.PendingSuggestions...)
	if e.Actor.Kind == events.ActorHuman {
		snap.Members = upsertMember(snap.Members, e.Actor, "watching")
	}
	switch e.Type {
	case events.RoomCreated:
		p, ok := decode[events.RoomCreatedPayload](e)
		if !ok {
			break
		}
		snap.ID = e.RoomID
		snap.Title = p.Title
		snap.Severity = p.Severity
		snap.PipelineID = p.PipelineID
		snap.Shadow = p.Shadow
		snap.Status = "open"
		snap.OpenedAt = e.At

	case events.ShadowRevealed:
		snap.shadowRevealed = true

	case events.ShadowLabelled:
		snap.shadowLabelled = true

	case events.ShadowGraded:
		if p, ok := decode[events.ShadowGradedPayload](e); ok {
			snap.shadowGrade = &p
		}

	case events.RoomClosed:
		p, ok := decode[events.RoomClosedPayload](e)
		if !ok {
			break
		}
		at := e.At
		snap.Status = "closed"
		snap.ClosedAt = &at
		snap.Resolution = p.Resolution

	case events.AgentStatus:
		p, ok := decode[events.AgentStatusPayload](e)
		if !ok {
			break
		}
		snap.AgentStatus = AgentStatus{State: p.State, Detail: p.Detail}
		if p.State == AgentLogFull {
			snap.LogCap = LogCapFull
		}

	case events.RoomLogCapWarning:
		if snap.LogCap != LogCapFull {
			snap.LogCap = LogCapWarning
		}

	case events.RoomRetentionWarning:
		if p, ok := decode[events.RoomRetentionWarningPayload](e); ok {
			at := p.CloseAt.UTC()
			snap.warnedFor = &at
		}

	case events.RoomExtended:
		if p, ok := decode[events.RoomExtendedPayload](e); ok {
			at := p.CloseAt.UTC()
			snap.Extended = true
			snap.extendedUntil = &at
		}

	case events.AgentHypothesis:
		p, ok := decode[events.AgentHypothesisPayload](e)
		if !ok {
			break
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
			break
		}
		snap.PendingSuggestions = append(snap.PendingSuggestions, Suggestion{
			SuggestionID: p.SuggestionID,
			From:         Person{ID: e.Actor.ID, Name: e.Actor.Name, Verified: e.Actor.Verified},
			Text:         p.Text,
		})

	case events.SteerSent:
		p, ok := decode[events.SteerSentPayload](e)
		if !ok {
			break
		}
		if p.SuggestionID != "" {
			snap.PendingSuggestions = removeSuggestion(snap.PendingSuggestions, p.SuggestionID)
		}

	case events.ControlGranted:
		p, ok := decode[events.ControlGrantedPayload](e)
		if !ok {
			break
		}
		if snap.Driver != nil && snap.Driver.ID != p.To.ID {
			snap.Members = setRole(snap.Members, snap.Driver.ID, "watching")
		}
		snap.Driver = &Driver{ID: p.To.ID, Name: p.To.Name, Verified: p.To.Verified, LeaseUntil: p.LeaseUntil}
		snap.grantedLease = p.LeaseUntil
		snap.Members = upsertMember(snap.Members, p.To, "driver")

	case events.ControlLeaseExpired:
		applyLeaseExpired(&snap, e)

	case events.ProposalCreated:
		p, ok := decode[events.ProposalCreatedPayload](e)
		if !ok {
			break
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
			ParamsHash:     eventParamsHash(e.RoomID, p),
		})

	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](e)
		if !ok {
			break
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "approved")

	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](e)
		if !ok {
			break
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "rejected")
		// A rejection decides the proposal; it is no longer open.
		snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)

	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok {
			break
		}
		// The proposal leaves the open list when its last step runs, or when
		// any step fails (the executor stops there, so it is decided).
		if !p.OK || isLastStep(snap.OpenProposals, p.ProposalID, p.StepIndex) {
			snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)
		}
	}

	return normalize(snap)
}

// eventParamsHash is the params_hash a proposal.created event carries. An event
// written before auth PR 4 has none: the hash is then computed from the event's
// own fields, which is the value creation would have stored (params are frozen).
// It is "" only when the proposal cannot be hashed (see ComputeParamsHash).
func eventParamsHash(roomID string, p events.ProposalCreatedPayload) string {
	if p.ParamsHash != "" {
		return p.ParamsHash
	}
	h, err := ComputeParamsHash(roomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if err != nil {
		return ""
	}
	return h
}

// normalize returns the snapshot with every array non-nil and the agent state
// defaulted, so encoding it never produces null or an empty state
// (docs/api.md: arrays are never null, a new room's agent is "idle").
func normalize(snap RoomSnapshot) RoomSnapshot {
	if snap.Members == nil {
		snap.Members = []Member{}
	}
	if snap.OpenProposals == nil {
		snap.OpenProposals = []Proposal{}
	}
	for i := range snap.OpenProposals {
		// The slice was copied at the top of Apply, so writing fields of its
		// elements (never shared backing arrays) keeps Apply pure.
		if snap.OpenProposals[i].Steps == nil {
			snap.OpenProposals[i].Steps = []events.ProposalStep{}
		}
		if snap.OpenProposals[i].RequiredLayers == nil {
			snap.OpenProposals[i].RequiredLayers = []string{}
		}
		if snap.OpenProposals[i].Approvals == nil {
			snap.OpenProposals[i].Approvals = map[string]string{}
		}
	}
	if snap.PendingSuggestions == nil {
		snap.PendingSuggestions = []Suggestion{}
	}
	if snap.AgentStatus.State == "" {
		snap.AgentStatus.State = "idle"
	}
	if snap.LogCap == "" {
		snap.LogCap = LogCapOK
	}
	if snap.Hypothesis != nil && snap.Hypothesis.BasedOnSteps == nil {
		h := *snap.Hypothesis
		h.BasedOnSteps = []string{}
		snap.Hypothesis = &h
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

// upsertMember adds or updates a member without mutating the input slice. The
// name and verified flag follow the latest event of that person.
func upsertMember(members []Member, a events.Actor, role string) []Member {
	for i := range members {
		if members[i].ID == a.ID {
			if members[i].Name == a.Name && members[i].Verified == a.Verified && (role != "driver" || members[i].Role == "driver") {
				return members
			}
			// Copy-on-write so a shared backing array is not mutated.
			out := append([]Member(nil), members...)
			out[i].Name = a.Name
			out[i].Verified = a.Verified
			if role == "driver" {
				out[i].Role = "driver"
			}
			return out
		}
	}
	return append(members, Member{ID: a.ID, Name: a.Name, Verified: a.Verified, Role: role})
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
