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
	// Staging is true when Actions hold the pipeline's staging client set
	// (wiring.Stack.HasStaging: owners.yaml staging: and write mode on), so
	// its Stagers can try a step on staging (X8).
	Staging bool
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

// actionsFor returns the write actions of the pipeline's own client set, or
// the setup stack's for the pipeline an open setup room sets up
// (setup_agent.go), so a setup proposal is validated, routed, previewed and
// executed by the action the setup run proposed.
func (s *Service) actionsFor(pipelineID string) *actions.Registry {
	if acts, ok := s.setupActionsFor(pipelineID); ok {
		return acts
	}
	return s.pipelineActionsFor(pipelineID)
}

// pipelineActionsFor is actionsFor without the setup stack: the actions an
// incident room's agent run is given, unchanged by setup rooms.
func (s *Service) pipelineActionsFor(pipelineID string) *actions.Registry {
	if s.stackFor == nil {
		return s.actions
	}
	if a := s.stackFor(pipelineID).Actions; a != nil {
		return a
	}
	return actions.NewRegistry()
}

// AllActions returns the full write-action catalogue in registration order:
// what GET /api/actions lists (docs/api.md, P3c), whether or not write mode is
// on. Without per-pipeline client sets it is the process-wide registry; with
// them (StackFor) it is the union of every set's actions (Deps.ActionCatalogue),
// deduplicated by name with the first registration kept.
func (s *Service) AllActions() []actions.Action {
	if s.stackFor != nil {
		if s.actionCatalogue == nil {
			return nil
		}
		return dedupeActions(s.actionCatalogue())
	}
	return s.actions.All()
}

// dedupeActions keeps the first action of each name, in order. The same action
// is registered in every pipeline's client set, so the catalogue carries it
// once per set; GET /api/actions lists each action once.
func dedupeActions(all []actions.Action) []actions.Action {
	out := make([]actions.Action, 0, len(all))
	seen := make(map[string]bool, len(all))
	for _, a := range all {
		if a == nil {
			continue
		}
		name := a.Name()
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, a)
	}
	return out
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
//
// The room's own snapshot answers, not its row's pipeline id: a setup room's
// pipeline is provisional and absent from owners.yaml, so its tenant comes
// from room.created (SetupRequest.tenant) until pipeline_register. A room this
// replica has not published is loaded first, exactly as the handler would.
func (s *Service) checkVisible(ctx context.Context, u User, roomID string) error {
	if !s.TenancyOn() {
		return nil
	}
	snap, err := s.readRoom(ctx, roomID)
	if err != nil {
		return mapStoreErr(err)
	}
	if !s.canSeeRoom(u, snap) {
		return errHidden()
	}
	return nil
}

// canSeeRoom reports whether the person may see the room: the setup room's
// tenant, or the pipeline's tenant for every other room.
func (s *Service) canSeeRoom(u User, snap *RoomSnapshot) bool {
	if !s.TenancyOn() || s.IsSolo(u.ID) {
		return true
	}
	if snap.IsSetupRoom() {
		tenant := snap.SetupTenant()
		return tenant != "" && s.owners.InTenant(tenant, u.ID, u.Groups)
	}
	return s.owners.CanSeePipeline(snap.PipelineID, u.ID, u.Groups)
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
// It filters the ordered list, so the rooms it keeps are in the same order as
// ListRooms (see roomListLess); GET /api/rooms and the room list socket both
// answer through this method and therefore agree (KAN-382). Setup rooms are
// filtered by their own tenant (canSeeRoom), and every row carries only the
// setup fields the list documents (trimSetupForList).
func (s *Service) ListRoomsFor(ctx context.Context, u User) ([]RoomSnapshot, error) {
	all, err := s.ListRooms(ctx)
	if err != nil {
		return all, err
	}
	out := all
	if s.TenancyOn() && !s.IsSolo(u.ID) {
		out = make([]RoomSnapshot, 0, len(all))
		for i := range all {
			if s.canSeeRoom(u, &all[i]) {
				out = append(out, all[i])
			}
		}
	}
	for i := range out {
		trimSetupForList(&out[i])
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
