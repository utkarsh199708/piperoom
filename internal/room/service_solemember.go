package room

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"piperoom/internal/events"
)

// Sole-member driver (P2b, docs/api.md item 4): an alert room opens with no
// driver, and the first member who walks in is made its driver by the system,
// with control.granted{reason: "sole_member"}, so a team of one never has to
// "request control" from nobody. Never in a shadow room, never for a viewer,
// never once the room has had a driver or someone asked for control.

// errNotSoleMember ends a sole-member grant whose rule no longer holds when
// the event would land (another replica granted first, the room closed). It
// never reaches a caller: the join simply changes nothing.
var errNotSoleMember = errors.New("room: not the sole member")

// soleMemberTimeout bounds the grant a WebSocket join triggers: the join must
// not hang on a slow store (AGENTS.md §4 rule 4).
const soleMemberTimeout = 10 * time.Second

// soleMemberCounter counts the sole-member grants this replica appended, for
// /metrics. The zero value is ready to use.
type soleMemberCounter struct{ n atomic.Uint64 }

// SoleMemberGrants returns how many control.granted{reason: "sole_member"}
// events this replica appended since it started, for the
// piperoom_sole_member_grants_total counter.
func (s *Service) SoleMemberGrants() uint64 {
	return s.soleMembers.n.Load()
}

// soleMemberRoom reports whether snap is a room whose next member becomes its
// driver: open, from an alert, not shadow, never driven, no control.requested.
// Driver never goes back to nil once set (a lapsed lease keeps it, marked
// expired), so "never driven" is Driver == nil, and every fact here only
// moves towards "no": a published snapshot that says no is final.
func soleMemberRoom(snap *RoomSnapshot) bool {
	return snap.Status == "open" && snap.alertRoom && !snap.Shadow && snap.Driver == nil && !snap.controlRequested
}

// JoinRoom is called when a person opens the room's live stream (the
// WebSocket). If the room is a driverless alert room and the person is its
// first member (soleMemberRoom), they become its driver: the event is
// returned with granted true. Anything else changes nothing and returns
// granted false with a nil error; err is only a store failure of the grant.
func (s *Service) JoinRoom(ctx context.Context, user User, roomID string) (e events.Event, granted bool, err error) {
	if !s.soleMemberCandidate(user) {
		return events.Event{}, false, nil
	}
	// The common case (the room has a driver) answers from the published
	// snapshot without the room's lock or a store read.
	if p := s.published(roomID); p != nil && !soleMemberRoom(p) {
		return events.Event{}, false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, soleMemberTimeout)
	defer cancel()
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if isNotFound(err) {
		// Deleted since the connection looked it up; the gateway closes it.
		return events.Event{}, false, nil
	}
	if err != nil {
		return events.Event{}, false, err
	}
	defer s.unlockRoom(st)
	return s.claimSoleMemberLocked(ctx, st, roomID, user)
}

// soleMemberCandidate reports whether user may become a driver at all: a
// member (a viewer never drives, docs/design/auth.md section 2.1).
func (s *Service) soleMemberCandidate(user User) bool {
	if user.ID == "" {
		return false
	}
	role, ok := s.RoleFor(user.ID, user.Groups)
	return ok && role == RoleMember
}

// claimSoleMemberLocked makes user the room's driver when the room is a
// sole-member room and user a member who may see its pipeline. The append is
// checked (emitCheckedLocked): with a shared store, two first joiners on two
// replicas race on the room's last seq, the loser catches up, sees the
// winner's grant and gets nothing. Caller holds st's lock.
func (s *Service) claimSoleMemberLocked(ctx context.Context, st *roomState, roomID string, user User) (events.Event, bool, error) {
	if !soleMemberRoom(&st.snap) || st.deleting || !s.soleMemberCandidate(user) || !s.CanSeePipeline(user, st.snap.PipelineID) {
		return events.Event{}, false, nil
	}
	lease := s.now().UTC().Add(DriverLease)
	e, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		if !soleMemberRoom(&st.snap) {
			return errNotSoleMember
		}
		return nil
	}, events.ControlGranted, systemActor, events.ControlGrantedPayload{
		To:         s.humanActor(user),
		LeaseUntil: lease,
		Reason:     events.ControlReasonSoleMember,
	})
	if err != nil {
		if errors.Is(err, errNotSoleMember) {
			return events.Event{}, false, nil
		}
		return events.Event{}, false, err
	}
	s.soleMembers.n.Add(1)
	s.afterAppend(roomID, "persist sole-member driver", s.store.UpdateDriver(ctx, roomID, user.ID, user.Name, lease))
	s.log.Info("room: sole member became the driver", "room", roomID, "driver", user.ID)
	return e, true, nil
}
