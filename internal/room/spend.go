package room

import (
	"context"

	"piperoom/internal/events"
)

// EmitAgentUsage appends the agent.usage event of a model request made outside
// an agent run (the hand-off summary; docs/design/cost.md section 2) through
// the agent sink, so it gets the same checks and, in a shadow room, goes to the
// shadow log.
func (s *Service) EmitAgentUsage(ctx context.Context, roomID string, p events.AgentUsagePayload) error {
	return s.agentEmit(ctx, roomID, events.AgentUsage, p)
}

// CanReadPipelineUsage decides who may read GET /api/usage for a pipeline
// (docs/api.md): an auditors: principal, or a member of the pipeline as the
// export and shadow-grade rules define it (an approver or backup of any of
// its layers, a users.yaml user whose layers grant one, the solo user).
func (s *Service) CanReadPipelineUsage(user User, pipelineID string) bool {
	if s.IsAuditor(user.ID, user.Groups) {
		return true
	}
	_, ok := s.canGradeShadow(pipelineID, user)
	return ok
}
