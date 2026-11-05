package room

import (
	"encoding/json"
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
)

// Layered and Deferred steps (Q7k; docs/design/fix-pr.md §7, docs/api.md
// "Q7 pull-request fix").
//
// One rule decides a step's approving layer everywhere in the room service:
// actions.StepLayer with the room's pipeline (the owners.yaml pipeline, or a
// setup room's provisional one). It answers Layered.LayerFor (the repo glob's
// layer of a fix PR), the pipeline's source layer for a SourceLayered action
// (Q8o) and Layer() for every other action. The proposer's layer is compared
// with it at creation and again right before Execute, so the proposer never
// picks the approver.

// maxOpenFixPRs caps the fix PRs one room has open or about to open
// (docs/design/fix-pr.md §7): a fourth PR proposal is refused until one merges
// or closes.
const maxOpenFixPRs = 3

// stepPipeline is the pipeline a step's layer and scope are judged against:
// a setup room's provisional pipeline, else the owners.yaml pipeline. A
// pipeline no config defines answers its id alone, so a plain action still
// gets its Layer(), a source action the postgres layer (source_kind ""), and
// a Layered action fails closed (it lists no repos).
func (s *Service) stepPipeline(snap *RoomSnapshot, pipelineID string) owners.Pipeline {
	if p, ok := s.roomPipelineDef(snap, pipelineID); ok {
		return p
	}
	return owners.Pipeline{ID: pipelineID}
}

// stepLayer is the layer whose owner approves step a with params in the room
// whose snapshot is snap (nil outside a setup room) and pipeline pipelineID.
func (s *Service) stepLayer(snap *RoomSnapshot, pipelineID string, a actions.Action, params json.RawMessage) (string, error) {
	return actions.StepLayer(a, s.stepPipeline(snap, pipelineID), params)
}

// isDeferred reports whether a is an actions.Deferred action.
func isDeferred(a actions.Action) bool {
	_, ok := a.(actions.Deferred)
	return ok
}

// checkDeferredSteps applies the proposal rule of actions.Deferred to the
// actions of one proposal's steps: a proposal with a Deferred step holds only
// Deferred steps, at most actions.MaxDeferredSteps. A live stop-gap is a
// separate proposal (design §14 D4). It returns the number of Deferred steps.
func checkDeferredSteps(acts []actions.Action) (int, error) {
	deferred := 0
	for _, a := range acts {
		if isDeferred(a) {
			deferred++
		}
	}
	switch {
	case deferred == 0:
		return 0, nil
	case deferred != len(acts):
		return deferred, validation(fmt.Sprintf(
			"a proposal with a pull-request step holds only pull-request steps (%d of %d steps are); propose a live stop-gap separately",
			deferred, len(acts)))
	case deferred > actions.MaxDeferredSteps:
		return deferred, validation(fmt.Sprintf("a proposal holds at most %d pull-request steps, not %d", actions.MaxDeferredSteps, deferred))
	}
	return deferred, nil
}

// hasDeferredStep reports whether a step of steps is a Deferred action of the
// pipeline's registry. A step whose action the registry does not know counts
// as Deferred: the callers are the policy schedule checks, which must fail
// closed.
func (s *Service) hasDeferredStep(pipelineID string, steps []events.ProposalStep) bool {
	acts := s.actionsFor(pipelineID)
	for _, st := range steps {
		a, ok := acts.Get(st.Action)
		if !ok || isDeferred(a) {
			return true
		}
	}
	return false
}

// pendingFixPRs counts the fix PRs a room has open (pull_requests with state
// open) plus the Deferred steps of its open proposals that have not opened one
// yet: what maxOpenFixPRs caps. Caller holds the room's lock (or reads a
// published snapshot).
func (s *Service) pendingFixPRs(snap *RoomSnapshot) int {
	n := 0
	for _, pr := range snap.PullRequests {
		if pr.followed() {
			n++
		}
	}
	acts := s.actionsFor(snap.PipelineID)
	for _, p := range snap.OpenProposals {
		for i, st := range p.Steps {
			if i < len(p.outcomes) && p.outcomes[i] != outcomeNotRun {
				continue // ran: its PR (if it opened one) is counted above
			}
			if a, ok := acts.Get(st.Action); ok && isDeferred(a) {
				n++
			}
		}
	}
	return n
}

// stepRef is the actions.StepRef of step i of proposal p (Q7): the room,
// proposal and step, the proposal title as stored, and "layer: display name"
// of each layer's approval.given in required_layers order, from the room's
// snapshot (a layer with no approval folded yet is left out).
func (s *Service) stepRef(snap *RoomSnapshot, p store.ProposalRow, i int) actions.StepRef {
	ref := actions.StepRef{RoomID: p.RoomID, ProposalID: p.ProposalID, StepIndex: i, Title: p.Title}
	if snap == nil {
		return ref
	}
	prop, ok := openProposal(*snap, p.ProposalID)
	if !ok {
		return ref
	}
	for _, l := range p.RequiredLayers {
		if name, ok := prop.approvers[l]; ok && prop.Approvals[l] == "approved" {
			ref.Approvals = append(ref.Approvals, l+": "+name)
		}
	}
	return ref
}
