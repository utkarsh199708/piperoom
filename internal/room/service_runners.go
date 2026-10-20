package room

import (
	"context"

	"piperoom/internal/events"
)

// EmitSetup is the runner registry's audit sink and the escape hatch for a
// protocol refusal that happens before a code resolves (a version mismatch at
// Pair or Hello): it appends setup.* events to the room's log when the event
// names a room, and logs (never loses) an attempt whose room is unknown, such
// as a bad pairing code or a global rate limit. The code and the token never
// reach this method.
func (s *Service) EmitSetup(ctx context.Context, roomID string, typ events.Type, payload any) error {
	if roomID == "" {
		s.log.Info("room: setup pairing audit (no room)", "type", typ)
		return nil
	}
	st := s.lockCachedRoom(roomID)
	if st == nil {
		s.log.Info("room: setup audit for an uncached room", "type", typ, "room", roomID)
		return nil
	}
	defer s.unlockRoom(st)
	_, err := s.emitLocked(ctx, st, roomID, typ, systemActor, payload)
	return err
}

// IssuePairingCode issues a single-use pairing code bound to (tenant, roomID,
// creator), valid for protocol.CodeTTL. POST /api/setup (U1-5b) is its caller.
func (s *Service) IssuePairingCode(tenant, roomID string, creator events.Actor) (string, error) {
	return s.runners.IssuePairingCode(tenant, roomID, creator)
}

// PairRunner validates one pairing attempt (a Pair frame) and returns its
// outcome. It emits setup.pairing for the attempt.
func (s *Service) PairRunner(ctx context.Context, code, source string) (PairResult, error) {
	return s.runners.PairCode(ctx, code, source)
}

// AttachRunner registers a paired runner (after Hello) and consumes its code.
// It emits setup.runner_attached, and setup.pairing {outcome: replaced} when it
// replaces a runner of the same (tenant, room, creator).
func (s *Service) AttachRunner(ctx context.Context, code string, rn Runner) error {
	return s.runners.Attach(ctx, code, rn)
}

// RunnerLookup returns the runner bound to (tenant, roomID, creatorID), if any.
func (s *Service) RunnerLookup(tenant, roomID, creatorID string) (*Runner, bool) {
	return s.runners.Runner(tenant, roomID, creatorID)
}

// RunnerLost detaches the runner bound to (tenant, roomID, creatorID) and emits
// setup.runner_lost.
func (s *Service) RunnerLost(ctx context.Context, tenant, roomID, creatorID string) {
	s.runners.Lost(ctx, tenant, roomID, creatorID)
}

// RunnerTouch updates the runner's last-seen time after a frame arrives.
func (s *Service) RunnerTouch(tenant, roomID, creatorID string) {
	s.runners.Touch(tenant, roomID, creatorID)
}

// RunnerRefused records that the runner refused an op, emitting
// setup.runner_refused.
func (s *Service) RunnerRefused(ctx context.Context, tenant, roomID, creatorID string, payload events.SetupRunnerRefusedPayload) {
	s.runners.Refuse(ctx, tenant, roomID, creatorID, payload)
}
