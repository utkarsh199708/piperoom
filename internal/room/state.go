package room

import (
	"slices"
	"time"

	"piperoom/internal/events"
)

// proposalState is a proposal as the log describes it.
type proposalState struct {
	payload   events.ProposalCreatedPayload
	approvals map[string]string // layer -> pending|approved|rejected
	rejected  bool
	failed    bool
	done      map[int]bool // step index -> action.executed seen
	// started is set (in memory only) once the executor took the proposal, so
	// a second approval path can never start it twice.
	started bool
}

func (p *proposalState) allApproved() bool {
	for _, l := range p.payload.RequiredLayers {
		if p.approvals[l] != approvalApproved {
			return false
		}
	}
	return true
}

// finished reports whether nothing more can happen to the proposal.
func (p *proposalState) finished() bool {
	return p.rejected || p.failed || len(p.done) >= len(p.payload.Steps)
}

type suggestionState struct {
	Suggestion
	sent bool
}

type handoffState struct {
	payload  events.HandoffCreatedPayload
	creator  string
	voided   bool
	accepted bool
}

// State is one room folded from its event log: the Snapshot plus the
// bookkeeping the rules need. Apply is the only way it changes, so replaying the
// log always gives the same State.
type State struct {
	snap        Snapshot
	proposals   map[string]*proposalState
	propOrder   []string
	suggestions map[string]*suggestionState
	suggOrder   []string
	handoffs    map[string]*handoffState
	hoOrder     []string
	members     []Person
	// lastGrant is the time of the last event that set the driver lease
	// (room.created or control.granted); renewals are rate-limited against it.
	lastGrant time.Time
}

// NewState returns an empty state; the first event must be room.created.
func NewState(roomID string) *State {
	return &State{
		snap: Snapshot{
			ID: roomID, Status: StatusOpen, AgentStatus: AgentStatus{State: "idle"},
			OpenProposals: []OpenProposal{}, PendingSuggestions: []Suggestion{}, Members: []Member{},
		},
		proposals:   map[string]*proposalState{},
		suggestions: map[string]*suggestionState{},
		handoffs:    map[string]*handoffState{},
	}
}

// Fold builds the state for roomID from its events.
func Fold(roomID string, evs []events.Event) *State {
	st := NewState(roomID)
	for _, e := range evs {
		st.Apply(e)
	}
	return st
}

func (s *State) touch(a events.Actor) {
	if a.Kind != events.ActorHuman {
		return
	}
	for _, m := range s.members {
		if m.ID == a.ID {
			return
		}
	}
	s.members = append(s.members, Person{ID: a.ID, Name: a.Name})
}

// Apply folds one event into the state. Events whose payload cannot be decoded
// are skipped: the log is append-only and must never stop a room from loading.
func (s *State) Apply(e events.Event) {
	s.snap.LastSeq = e.Seq
	s.touch(e.Actor)
	switch e.Type {
	case events.RoomCreated:
		p, err := events.Decode[events.RoomCreatedPayload](e)
		if err != nil {
			return
		}
		s.snap.Title, s.snap.Severity, s.snap.PipelineID = p.Title, p.Severity, p.PipelineID
		s.snap.OpenedAt = e.At
		if e.Actor.Kind == events.ActorHuman {
			s.snap.Driver = &Driver{ID: e.Actor.ID, Name: e.Actor.Name, LeaseUntil: e.At.Add(LeaseDuration)}
			s.lastGrant = e.At
		}
	case events.RoomClosed:
		p, _ := events.Decode[events.RoomClosedPayload](e)
		at := e.At
		s.snap.Status, s.snap.ClosedAt, s.snap.Resolution = StatusClosed, &at, p.Resolution
	case events.AgentStatus:
		if p, err := events.Decode[events.AgentStatusPayload](e); err == nil {
			s.snap.AgentStatus = AgentStatus{State: p.State, Detail: p.Detail}
		}
	case events.AgentHypothesis:
		if p, err := events.Decode[events.AgentHypothesisPayload](e); err == nil {
			s.snap.Hypothesis = &Hypothesis{Text: p.Text, BasedOnSteps: nonNil(p.BasedOnSteps), Confidence: p.Confidence, Confirmed: p.Confirmed}
		}
	case events.SteerSuggested:
		if p, err := events.Decode[events.SteerSuggestedPayload](e); err == nil {
			s.suggestions[p.SuggestionID] = &suggestionState{Suggestion: Suggestion{
				SuggestionID: p.SuggestionID, From: Person{ID: e.Actor.ID, Name: e.Actor.Name}, Text: p.Text}}
			s.suggOrder = append(s.suggOrder, p.SuggestionID)
		}
	case events.SteerSent:
		if p, err := events.Decode[events.SteerSentPayload](e); err == nil && p.SuggestionID != "" {
			if g := s.suggestions[p.SuggestionID]; g != nil {
				g.sent = true
			}
		}
	case events.ControlGranted:
		if p, err := events.Decode[events.ControlGrantedPayload](e); err == nil {
			s.snap.Driver = &Driver{ID: p.To.ID, Name: p.To.Name, LeaseUntil: p.LeaseUntil}
			s.lastGrant = e.At
			s.touch(p.To)
		}
	case events.ProposalCreated:
		p, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			return
		}
		ps := &proposalState{payload: p, approvals: map[string]string{}, done: map[int]bool{}}
		for _, l := range p.RequiredLayers {
			ps.approvals[l] = approvalPending
		}
		s.proposals[p.ProposalID] = ps
		s.propOrder = append(s.propOrder, p.ProposalID)
	case events.ApprovalGiven:
		if p, err := events.Decode[events.ApprovalGivenPayload](e); err == nil {
			if ps := s.proposals[p.ProposalID]; ps != nil {
				ps.approvals[p.Layer] = approvalApproved
			}
		}
	case events.ApprovalRejected:
		if p, err := events.Decode[events.ApprovalRejectedPayload](e); err == nil {
			if ps := s.proposals[p.ProposalID]; ps != nil {
				ps.approvals[p.Layer] = approvalRejected
				ps.rejected = true
			}
		}
	case events.ActionExecuted:
		if p, err := events.Decode[events.ActionExecutedPayload](e); err == nil {
			if ps := s.proposals[p.ProposalID]; ps != nil {
				if p.OK {
					ps.done[p.StepIndex] = true
				} else {
					ps.failed = true
				}
			}
		}
	case events.HandoffCreated:
		if p, err := events.Decode[events.HandoffCreatedPayload](e); err == nil {
			// A newer hand-off voids every earlier pending one.
			for _, h := range s.handoffs {
				if !h.accepted {
					h.voided = true
				}
			}
			s.handoffs[p.HandoffID] = &handoffState{payload: p, creator: e.Actor.ID}
			s.hoOrder = append(s.hoOrder, p.HandoffID)
		}
	case events.HandoffAccepted:
		if p, err := events.Decode[events.HandoffAcceptedPayload](e); err == nil {
			if h := s.handoffs[p.HandoffID]; h != nil {
				h.accepted = true
				if !h.payload.StayAsWatcher {
					s.dropMember(h.creator)
				}
			}
		}
	}
}

func (s *State) dropMember(id string) {
	s.members = slices.DeleteFunc(s.members, func(m Person) bool { return m.ID == id })
}

// Snapshot returns an independent copy of the room snapshot.
func (s *State) Snapshot() Snapshot {
	out := s.snap
	if s.snap.Driver != nil {
		d := *s.snap.Driver
		out.Driver = &d
	}
	if s.snap.Hypothesis != nil {
		h := *s.snap.Hypothesis
		out.Hypothesis = &h
	}
	if s.snap.ClosedAt != nil {
		t := *s.snap.ClosedAt
		out.ClosedAt = &t
	}
	out.OpenProposals = []OpenProposal{}
	for _, id := range s.propOrder {
		ps := s.proposals[id]
		if ps.finished() {
			continue
		}
		ap := make(map[string]string, len(ps.approvals))
		for k, v := range ps.approvals {
			ap[k] = v
		}
		out.OpenProposals = append(out.OpenProposals, OpenProposal{
			ProposalID: id, Title: ps.payload.Title, Steps: slices.Clone(ps.payload.Steps),
			Risk: ps.payload.Risk, Rollback: ps.payload.Rollback,
			RequiredLayers: slices.Clone(ps.payload.RequiredLayers), Approvals: ap,
		})
	}
	out.PendingSuggestions = []Suggestion{}
	for _, id := range s.suggOrder {
		if g := s.suggestions[id]; !g.sent {
			out.PendingSuggestions = append(out.PendingSuggestions, g.Suggestion)
		}
	}
	out.Members = []Member{}
	seen := map[string]bool{}
	add := func(p Person, role string) {
		if !seen[p.ID] {
			seen[p.ID] = true
			out.Members = append(out.Members, Member{ID: p.ID, Name: p.Name, Role: role})
		}
	}
	if d := s.snap.Driver; d != nil {
		add(Person{ID: d.ID, Name: d.Name}, "driver")
	}
	for _, m := range s.members {
		add(m, "watching")
	}
	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
