package room

import (
	"slices"

	"piperoom/internal/events"
)

// The room-service side of config_drift (X3b): the action.executed hook the
// watcher moves a connector's approved config with, and the check of POST
// /api/pipelines/{pid}/connectors/{name}/accept-config.

// configLayer is the layer whose owners may accept a connector's config.
const configLayer = "kafka-connect"

// actionExecutedLocked hands an action.executed, with the step that ran, to
// Deps.OnActionExecuted on its own goroutine. The step comes from the room's
// open proposals, so it must run before Apply removes the proposal. An event
// whose proposal or step the snapshot does not hold (a rebuilt room) is
// skipped. Caller holds st's lock.
func (s *Service) actionExecutedLocked(st *roomState, e events.Event) {
	if s.onActionExec == nil || e.Type != events.ActionExecuted {
		return
	}
	p, ok := decode[events.ActionExecutedPayload](e)
	if !ok {
		return
	}
	i := slices.IndexFunc(st.snap.OpenProposals, func(pr Proposal) bool { return pr.ProposalID == p.ProposalID })
	if i < 0 || p.StepIndex < 0 || p.StepIndex >= len(st.snap.OpenProposals[i].Steps) {
		return
	}
	step := st.snap.OpenProposals[i].Steps[p.StepIndex]
	step.Params = append([]byte(nil), step.Params...)
	pipelineID := st.snap.PipelineID
	if !s.addWorker() {
		return
	}
	go func() {
		defer s.wg.Done()
		s.onActionExec(s.ctx, pipelineID, step)
	}()
}

// AuthorizeAcceptConfig is the check of POST
// /api/pipelines/{pid}/connectors/{name}/accept-config (docs/api.md, X3):
// 404 not_found for an unknown pipeline, one outside the person's tenants or
// a connector that is not one of the pipeline's; then 403 not_layer_owner
// unless the person may approve the pipeline's kafka-connect layer (an
// approver or backup in owners.yaml, the users.yaml layers of dev mode, or
// PIPEROOM_SOLO_USER). A viewer never gets here (the viewer guard answers
// 403 viewer first).
func (s *Service) AuthorizeAcceptConfig(u User, pipelineID, connector string) error {
	if !s.UsagePipelineVisible(u, pipelineID) {
		return notFound("pipeline not found")
	}
	p, _ := s.owners.Pipeline(pipelineID)
	if !slices.Contains(p.Connectors, connector) {
		return notFound("connector not found")
	}
	if _, err := s.authorizeLayer(pipelineID, u, configLayer, "", false); err != nil {
		return err
	}
	return nil
}
