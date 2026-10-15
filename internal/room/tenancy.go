package room

import (
	"context"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/tools"
)

// Tenancy (docs/design/tenancy.md): which client set a room's agent and
// executor use, and which rooms a person may see.

// PipelineStack is what a room uses for its pipeline: the read tools and the
// write actions built over that pipeline's own clients.
type PipelineStack struct {
	Tools   *tools.Registry
	Actions *actions.Registry
}

// toolsFor returns the read tools of the pipeline's own client set. Without
// Deps.StackFor every room uses Deps.Tools, as before tenancy.
func (s *Service) toolsFor(pipelineID string) *tools.Registry {
	if s.stackFor == nil {
		return s.tools
	}
	if t := s.stackFor(pipelineID).Tools; t != nil {
		return t
	}
	return tools.NewRegistry()
}

// actionsFor returns the write actions of the pipeline's own client set.
func (s *Service) actionsFor(pipelineID string) *actions.Registry {
	if s.stackFor == nil {
		return s.actions
	}
	if a := s.stackFor(pipelineID).Actions; a != nil {
		return a
	}
	return actions.NewRegistry()
}

// TenancyOn reports whether owners.yaml has a tenants: block. Without one
// every signed-in user sees every room.
func (s *Service) TenancyOn() bool { return s.owners.TenancyOn() }

// CanSeePipeline reports whether the person may see rooms of the pipeline:
// always with the filter off; with it on, the solo user sees every tenant and
// everyone else the tenants they belong to (owners.InTenant). A pipeline that
// owners.yaml does not define is in no tenant.
func (s *Service) CanSeePipeline(u User, pipelineID string) bool {
	if !s.TenancyOn() {
		return true
	}
	if s.IsSolo(u.ID) {
		return true
	}
	return s.owners.CanSeePipeline(pipelineID, u.ID, u.Groups)
}

// errHidden is the answer for an object outside the person's tenants: the
// same 404 as a room, proposal or hand-off that does not exist (mapStoreErr),
// so ids do not leak.
func errHidden() *Error { return notFound("not found") }

// CheckRoomVisible answers 404 not_found when the room does not exist or is
// outside the person's tenants. With the filter off it does nothing, so every
// route keeps its own answers.
func (s *Service) CheckRoomVisible(ctx context.Context, u User, roomID string) error {
	return s.checkVisible(ctx, u, roomID)
}

// checkVisible is CheckRoomVisible's check, also for a caller that has just
// loaded a row (proposal, hand-off) of roomID. It must run before any answer
// that depends on the row (409 already decided, 403 not the recipient), so a
// person outside the tenant learns nothing from it. It takes no lock.
func (s *Service) checkVisible(ctx context.Context, u User, roomID string) error {
	if !s.TenancyOn() {
		return nil
	}
	pipelineID, err := s.roomPipelineID(ctx, roomID)
	if err != nil {
		return err
	}
	if !s.CanSeePipeline(u, pipelineID) {
		return errHidden()
	}
	return nil
}

// roomPipelineID returns a room's pipeline id from its published snapshot,
// else from its rooms row: no lock is waited for, and no snapshot is cloned or
// rebuilt for a visibility check. A missing room is the 404 of mapStoreErr.
func (s *Service) roomPipelineID(ctx context.Context, roomID string) (string, error) {
	if p := s.published(roomID); p != nil {
		return p.PipelineID, nil
	}
	row, err := s.store.GetRoom(ctx, roomID)
	if err != nil {
		return "", mapStoreErr(err)
	}
	return row.PipelineID, nil
}

// checkTargetVisible refuses to give control or a hand-off to a person who
// cannot see the room, with the same answer as an unknown person.
func (s *Service) checkTargetVisible(to User, pipelineID string) error {
	if !s.CanSeePipeline(to, pipelineID) {
		return errNotAPerson()
	}
	return nil
}

// ListRoomsFor is ListRooms without the rooms outside the person's tenants.
func (s *Service) ListRoomsFor(ctx context.Context, u User) ([]RoomSnapshot, error) {
	all, err := s.ListRooms(ctx)
	if err != nil || !s.TenancyOn() || s.IsSolo(u.ID) {
		return all, err
	}
	visible, _ := s.owners.VisiblePipelines(u.ID, u.Groups)
	out := make([]RoomSnapshot, 0, len(all))
	for _, r := range all {
		if visible[r.PipelineID] {
			out = append(out, r)
		}
	}
	return out, nil
}

// ShadowMetricsFor is ShadowMetrics over the rooms the person may see. A
// pipeline filter outside their tenants counts nothing, as an unknown
// pipeline does.
func (s *Service) ShadowMetricsFor(ctx context.Context, u User, pipelineID string, since time.Time) (ShadowMetrics, error) {
	if !s.TenancyOn() || s.IsSolo(u.ID) {
		return s.ShadowMetrics(ctx, pipelineID, since)
	}
	visible, _ := s.owners.VisiblePipelines(u.ID, u.Groups)
	return s.shadowMetrics(ctx, pipelineID, since, func(p string) bool { return visible[p] })
}
