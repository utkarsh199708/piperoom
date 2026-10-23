package room

import (
	"context"

	"piperoom/internal/tools"
)

// roomIDKey carries a room's id in an action call's context. The real write
// clients ignore it; --demo's in-memory clients use it to keep one state per
// room (KAN-392), so a second demo room replays the scripted story instead of
// inheriting the first room's executed fix.
type roomIDKey struct{}

// WithRoomID returns ctx carrying roomID. An empty id leaves ctx unchanged, so
// a caller that does not know the room cannot erase one already carried.
func WithRoomID(ctx context.Context, roomID string) context.Context {
	if roomID == "" {
		return ctx
	}
	return context.WithValue(ctx, roomIDKey{}, roomID)
}

// RoomID returns the room id carried by ctx, or "" when it carries none.
func RoomID(ctx context.Context) string {
	id, _ := ctx.Value(roomIDKey{}).(string)
	return id
}

// actionCtx scopes ctx to the room and its pipeline for an action call. The
// pipeline part is what the real write clients require (pipelineCtx); the room
// id lets a per-room client implementation tell concurrent demo rooms apart.
func (s *Service) actionCtx(ctx context.Context, roomID, pipelineID string) context.Context {
	return WithRoomID(s.pipelineCtx(ctx, pipelineID), roomID)
}

// actionCtxFor is actionCtx with the room's snapshot: a setup room's action
// calls are scoped to the provisional pipeline before registration (design
// §3.2), so its Validate, Scope and Execute see the pipeline the person
// described, not an owners.yaml entry that does not exist yet.
func (s *Service) actionCtxFor(ctx context.Context, roomID string, snap *RoomSnapshot, pipelineID string) context.Context {
	base := s.actionCtx(ctx, roomID, pipelineID)
	if snap != nil && snap.IsSetupRoom() {
		if prov, ok := snap.SetupProvisional(); ok {
			base = tools.WithPipeline(base, prov)
		}
	}
	return base
}
