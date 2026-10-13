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

// UsageRoomVisible decides whether user may see a room on the usage route.
// It is the tenancy adapter until #112 merges: there are no tenants yet, so
// every room a caller can name is visible. After #112 it becomes the
// roomVisible check (CheckRoomVisible), so a room of another tenant answers
// 404 like a missing one.
func (s *Service) UsageRoomVisible(user User, snap RoomSnapshot) bool { return true }

// UsagePipelineVisible decides whether user may see a pipeline on GET
// /api/usage, before any role check. The adapter until #112: a pipeline is
// visible when owners.yaml defines it. After #112 it becomes CanSeePipeline,
// which also hides other tenants' pipelines, from auditors too.
func (s *Service) UsagePipelineVisible(user User, pipelineID string) bool {
	if s.owners == nil {
		return false
	}
	_, ok := s.owners.Pipeline(pipelineID)
	return ok
}
