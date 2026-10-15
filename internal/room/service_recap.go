package room

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"unicode/utf8"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// A run that starts after another run of the room (a restart, a takeover, a
// steer after the run ended, an executed fix whose proposing run is gone)
// knows nothing of the earlier investigation: the runner keeps no memory
// across runs. The room gives it a recap as its first input (Q5 T1), built
// from the log: the newest hypothesis, the step summaries so far, every
// proposal with its steps and status, and the newest hand-off summary.
//
// The recap travels as an agent.HumanInput of Kind "comment" from the system
// actor (the actor openProposalsInput used before; no person can write a
// system input) whose Text is the Recap as one JSON object. The runner already
// renders a comment as untrusted data, which is right: the recap quotes tool
// summaries, titles and agent text. The agent package reads the JSON back to
// track the proposals of earlier runs, so it can verify one that executes.
// See the Contract change request in the Q5 report for a typed field instead.

// RecapKind is Recap.Kind: the agent tells the recap from a comment by it,
// together with the system actor.
const RecapKind = "piperoom.room_recap"

const (
	// recapMaxSteps is how many step summaries the recap keeps, newest last.
	recapMaxSteps = 30
	// recapSummaryBytes caps one step summary, like the hand-off's lines.
	recapSummaryBytes = 300
	// recapTextBytes caps the free text of a hypothesis, a title or a
	// hand-off line.
	recapTextBytes = 1024
	// recapMaxBytes bounds the encoded recap; the oldest steps go first.
	recapMaxBytes = 24 << 10
	// recapMaxProposals bounds the proposals listed, newest kept.
	recapMaxProposals = 20
)

// Recap is the room recap a resumed run reads first.
type Recap struct {
	Kind    string `json:"kind"`    // RecapKind
	Version int    `json:"version"` // 1
	RoomID  string `json:"room_id"`
	// AsOfSeq is the newest event the recap read.
	AsOfSeq int64 `json:"as_of_seq"`
	// Note says what the recap is, for the model.
	Note       string           `json:"note"`
	Hypothesis *RecapHypothesis `json:"hypothesis"`
	Steps      []RecapStep      `json:"steps"`
	// StepsOmitted counts older step summaries left out.
	StepsOmitted int             `json:"steps_omitted"`
	Proposals    []RecapProposal `json:"proposals"`
	Handoff      *RecapHandoff   `json:"handoff"`
}

// RecapHypothesis is the newest agent.hypothesis.
type RecapHypothesis struct {
	Seq          int64    `json:"seq"`
	Text         string   `json:"text"`
	Confidence   string   `json:"confidence"`
	BasedOnSteps []string `json:"based_on_steps"`
}

// RecapStep is one tool result of an earlier run (or of the triage pass).
type RecapStep struct {
	Seq     int64  `json:"seq"`
	StepID  string `json:"step_id"`
	Tool    string `json:"tool"`
	Summary string `json:"summary"`
	IsError bool   `json:"is_error"`
}

// Recap proposal statuses (RecapProposal.Status).
const (
	RecapOpen      = "open"      // waiting for approvals
	RecapApproved  = "approved"  // every layer approved; nothing ran yet
	RecapExecuting = "executing" // some steps succeeded, more to run
	RecapExecuted  = "executed"  // every step succeeded
	RecapFailed    = "failed"    // a step failed
	RecapUnknown   = "outcome_unknown"
	RecapRejected  = "rejected"
	RecapWithdrawn = "withdrawn"
)

// RecapProposal is one proposal as the log tells it.
type RecapProposal struct {
	Seq            int64                 `json:"seq"`
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Status         string                `json:"status"`
	ByAgent        bool                  `json:"by_agent"`
	Steps          []events.ProposalStep `json:"steps"`
	RequiredLayers []string              `json:"required_layers"`
	// StepsOK is how many steps succeeded; LastStepSeq the seq of the newest
	// action.executed (0 when none ran).
	StepsOK     int   `json:"steps_ok"`
	LastStepSeq int64 `json:"last_step_seq"`
	// Verified is set once an agent.verification judged the proposal, with
	// its one-line Verdict.
	Verified bool   `json:"verified"`
	Verdict  string `json:"verdict,omitempty"`
}

// RecapHandoff is the newest handoff.created.
type RecapHandoff struct {
	Seq       int64                 `json:"seq"`
	HandoffID string                `json:"handoff_id"`
	Summary   events.HandoffSummary `json:"summary"`
}

const recapNote = "This run resumes the room's investigation; the earlier runs' context is below, read from the room log by the server. " +
	"Proposals with status open or approved are already proposed: do not propose them again. " +
	"A proposal with status executed and verified false still needs its verification."

// recapFold folds a room log into a Recap.
type recapFold struct {
	rc       Recap
	steps    []RecapStep
	props    map[string]*RecapProposal
	order    []string
	approved map[string]map[string]bool // proposal -> layer -> approved
	any      bool                       // the log holds agent work to recap
}

func newRecapFold(roomID string) *recapFold {
	return &recapFold{
		rc:       Recap{Kind: RecapKind, Version: 1, RoomID: roomID, Note: recapNote, Steps: []RecapStep{}, Proposals: []RecapProposal{}},
		props:    map[string]*RecapProposal{},
		approved: map[string]map[string]bool{},
	}
}

func clipBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n - len("…")
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func clipList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = append(out, clipBytes(s, recapTextBytes))
	}
	return out
}

func (f *recapFold) add(e events.Event) {
	f.rc.AsOfSeq = e.Seq
	switch e.Type {
	case events.AgentToolResult:
		if p, ok := decode[events.AgentToolResultPayload](e); ok {
			f.any = true
			f.steps = append(f.steps, RecapStep{Seq: e.Seq, StepID: p.StepID, Tool: p.Tool, Summary: clipBytes(p.Summary, recapSummaryBytes), IsError: p.IsError})
		}
	case events.AgentHypothesis:
		if p, ok := decode[events.AgentHypothesisPayload](e); ok {
			f.any = true
			based := append([]string{}, p.BasedOnSteps...)
			f.rc.Hypothesis = &RecapHypothesis{Seq: e.Seq, Text: clipBytes(p.Text, recapTextBytes), Confidence: p.Confidence, BasedOnSteps: based}
		}
	case events.ProposalCreated:
		if p, ok := decode[events.ProposalCreatedPayload](e); ok {
			f.any = true
			if _, dup := f.props[p.ProposalID]; !dup {
				f.order = append(f.order, p.ProposalID)
			}
			steps := append([]events.ProposalStep{}, p.Steps...)
			f.props[p.ProposalID] = &RecapProposal{
				Seq: e.Seq, ProposalID: p.ProposalID, Title: clipBytes(p.Title, recapTextBytes), Status: RecapOpen,
				ByAgent: e.Actor.Kind == events.ActorAgent, Steps: steps, RequiredLayers: append([]string{}, p.RequiredLayers...),
			}
		}
	case events.ApprovalGiven:
		if p, ok := decode[events.ApprovalGivenPayload](e); ok && f.props[p.ProposalID] != nil {
			if f.approved[p.ProposalID] == nil {
				f.approved[p.ProposalID] = map[string]bool{}
			}
			f.approved[p.ProposalID][p.Layer] = true
			pr := f.props[p.ProposalID]
			if pr.Status == RecapOpen && f.allApproved(pr) {
				pr.Status = RecapApproved
			}
		}
	case events.ApprovalRejected:
		if p, ok := decode[events.ApprovalRejectedPayload](e); ok && f.props[p.ProposalID] != nil {
			f.props[p.ProposalID].Status = RecapRejected
		}
	case events.ProposalWithdrawn:
		if p, ok := decode[events.ProposalWithdrawnPayload](e); ok && f.props[p.ProposalID] != nil {
			if pr := f.props[p.ProposalID]; pr.Status == RecapOpen || pr.Status == RecapApproved {
				pr.Status = RecapWithdrawn
			}
		}
	case events.ActionExecuted:
		if p, ok := decode[events.ActionExecutedPayload](e); ok && f.props[p.ProposalID] != nil {
			pr := f.props[p.ProposalID]
			pr.LastStepSeq = e.Seq
			switch {
			case p.OutcomeUnknown:
				pr.Status = RecapUnknown
			case !p.OK:
				pr.Status = RecapFailed
			default:
				pr.StepsOK++
				pr.Status = RecapExecuting
				if pr.StepsOK >= len(pr.Steps) {
					pr.Status = RecapExecuted
				}
			}
		}
	case events.AgentVerification:
		if p, ok := decode[events.AgentVerificationPayload](e); ok && f.props[p.ProposalID] != nil {
			pr := f.props[p.ProposalID]
			pr.Verified, pr.Verdict = true, clipBytes(p.Verdict, recapTextBytes)
		}
	case events.HandoffCreated:
		if p, ok := decode[events.HandoffCreatedPayload](e); ok {
			f.any = true
			sum := p.Summary
			sum.WhatBroke, sum.Why = clipBytes(sum.WhatBroke, recapTextBytes), clipBytes(sum.Why, recapTextBytes)
			sum.Done, sum.RuledOut, sum.Open, sum.Risks = clipList(sum.Done), clipList(sum.RuledOut), clipList(sum.Open), clipList(sum.Risks)
			sum.Violations = nil
			f.rc.Handoff = &RecapHandoff{Seq: e.Seq, HandoffID: p.HandoffID, Summary: sum}
		}
	}
}

func (f *recapFold) allApproved(pr *RecapProposal) bool {
	for _, l := range pr.RequiredLayers {
		if !f.approved[pr.ProposalID][l] {
			return false
		}
	}
	return len(pr.RequiredLayers) > 0
}

// done returns the recap, cut to recapMaxBytes by dropping the oldest steps,
// and whether the log held anything to recap.
func (f *recapFold) done() (Recap, bool) {
	rc := f.rc
	ids := f.order
	if len(ids) > recapMaxProposals {
		ids = ids[len(ids)-recapMaxProposals:]
	}
	for _, id := range ids {
		rc.Proposals = append(rc.Proposals, *f.props[id])
	}
	steps := f.steps
	if len(steps) > recapMaxSteps {
		rc.StepsOmitted = len(steps) - recapMaxSteps
		steps = steps[len(steps)-recapMaxSteps:]
	}
	rc.Steps = slices.Clone(steps)
	for len(rc.Steps) > 0 {
		b, err := json.Marshal(rc)
		if err == nil && len(b) <= recapMaxBytes {
			break
		}
		rc.Steps = rc.Steps[1:]
		rc.StepsOmitted++
	}
	return rc, f.any
}

// buildRecap reads the room log and returns the recap input, or nil when the
// log holds no agent work yet. It reads the store: call it outside s.mu.
func (s *Service) buildRecap(ctx context.Context, roomID string) (*agent.HumanInput, error) {
	f := newRecapFold(roomID)
	after := int64(0)
	for {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil {
			return nil, fmt.Errorf("room: recap %s: %w", roomID, err)
		}
		for _, e := range evs {
			f.add(e)
			after = e.Seq
		}
		if len(evs) < 1000 {
			break
		}
	}
	rc, ok := f.done()
	if !ok {
		return nil, nil
	}
	b, err := json.Marshal(rc)
	if err != nil {
		return nil, fmt.Errorf("room: recap %s: encode: %w", roomID, err)
	}
	return &agent.HumanInput{Seq: rc.AsOfSeq, Actor: systemActor, Kind: "comment", Text: string(b)}, nil
}
