package room

import (
	"context"
	"errors"
	"sort"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Driver lease expiry (docs/api.md, Lease expiry; events.ControlLeaseExpired).
//
// When the driver's lease has passed with no renewal and no new driver, the
// system actor appends control.lease_expired {driver, lease_until} once for
// that lease. It moves nothing: the driver stays snapshot.driver, their next
// action renews the lease with a control.granted to themselves (which ends the
// signal), and anyone may take control (GrantControl, leaseExpired).
//
// Which lease: the service enforces Driver.LeaseUntil, which can be later
// than the log's lease because a renewal with 25 minutes or more left is only
// persisted on the rooms row (touchDriverLease). The event is written once
// that enforced lease has passed, but it names the lease of the last
// control.granted (RoomSnapshot.grantedLease): docs/api.md says lease_until
// equals the control.granted that set it, clients folding the log only know
// that value, and every replica derives the same value from the log.
//
// Once: the fold (applyLeaseExpired) marks Driver.LeaseExpired when the event
// names the current driver and grantedLease, and a room so marked is not due.
// The mark comes from the log, so a restart (rebuild) does not repeat it.

// LeaseCheckEvery is how often the service looks for expired driver leases.
// The signal pages people (Slack) and shows in the room, so a delay of up to
// one period after the 30-minute lease ends is fine; a check costs one pass
// over the cached rooms under s.mu and no I/O unless a lease is due.
const LeaseCheckEvery = 15 * time.Second

// leaseCheckTimeout bounds one check: the coordination read and the appends.
const leaseCheckTimeout = 30 * time.Second

// errLeaseNotDue is the check of the conditional append: the room is no
// longer due (folded a renewal, a new driver, a close or the event itself).
var errLeaseNotDue = errors.New("room: driver lease no longer due to expire")

// applyLeaseExpired folds control.lease_expired: it marks the driver when the
// event is for the current driver and lease. An event for an older lease or
// another driver changes nothing.
func applyLeaseExpired(snap *RoomSnapshot, e events.Event) {
	p, ok := decode[events.ControlLeaseExpiredPayload](e)
	if !ok || snap.Driver == nil {
		return
	}
	if p.Driver.ID != snap.Driver.ID || !p.LeaseUntil.Equal(snap.grantedLease) {
		return
	}
	// A new Driver value: Apply must not modify the input snapshot.
	d := *snap.Driver
	d.LeaseExpired = true
	snap.Driver = &d
}

// leaseExpiryDueLocked returns the control.lease_expired payload of a room
// whose driver lease has passed and has not been signalled yet. Not for a
// closed room, a room being deleted or a room with no driver. Caller holds
// s.mu.
func (s *Service) leaseExpiryDueLocked(st *roomState) (events.ControlLeaseExpiredPayload, bool) {
	d := st.snap.Driver
	if st.deleting || !s.roomOpen(st) || d == nil || d.LeaseExpired || st.snap.grantedLease.IsZero() {
		return events.ControlLeaseExpiredPayload{}, false
	}
	if s.now().Before(d.LeaseUntil) {
		return events.ControlLeaseExpiredPayload{}, false
	}
	return events.ControlLeaseExpiredPayload{
		Driver:     events.Actor{Kind: events.ActorHuman, ID: d.ID, Name: d.Name, Verified: d.Verified},
		LeaseUntil: st.snap.grantedLease,
	}, true
}

// startLeaseExpiry starts the lease expiry loop once, after Load. It stops
// when the service lifetime ends (Close).
func (s *Service) startLeaseExpiry() {
	s.leaseOnce.Do(func() {
		s.mu.Lock()
		closed := s.closed
		if !closed {
			s.wg.Add(1)
		}
		s.mu.Unlock()
		if closed {
			return
		}
		go func() {
			defer s.wg.Done()
			s.runLeaseExpiry()
		}()
	})
}

// runLeaseExpiry checks the driver leases at every tick until Close.
func (s *Service) runLeaseExpiry() {
	newTicker := s.leaseTicker
	if newTicker == nil {
		newTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
	tick, stop := newTicker(LeaseCheckEvery)
	defer stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick:
		}
		ctx, cancel := context.WithTimeout(s.ctx, leaseCheckTimeout)
		s.checkLeases(ctx)
		cancel()
		if s.leaseTickDone != nil {
			s.leaseTickDone()
		}
	}
}

// checkLeases appends control.lease_expired to every open room whose driver
// lease is due. With a shared store (docs/design/replicas.md) a room whose
// agent lease another replica holds is left to that replica; a room nobody
// holds (an idle agent) is written by whichever replica gets there first, and
// the conditional append (emitCheckedLocked) makes sure only one event lands:
// the others fold it, find the room no longer due and write nothing.
func (s *Service) checkLeases(ctx context.Context) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var due []string
	for id, st := range s.rooms {
		if _, ok := s.leaseExpiryDueLocked(st); ok {
			due = append(due, id)
		}
	}
	s.mu.Unlock()
	if len(due) == 0 {
		return
	}
	sort.Strings(due) // a stable order for logs and tests

	var held map[string]store.RoomLease
	if s.coord != nil {
		cctx, cancel := context.WithTimeout(ctx, coordTimeout)
		leases, err := s.coord.RoomLeases(cctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("room: lease expiry: list agent leases failed; retry at the next check", "err", err)
			}
			return
		}
		held = leases
	}
	for _, id := range due {
		if ctx.Err() != nil {
			return
		}
		if l, ok := held[id]; ok && l.Owner != s.replica {
			continue // the replica holding the room writes it
		}
		// Shared store: adopt a renewal another replica persisted on the
		// rooms row and fold its events before judging the lease.
		s.syncRoom(ctx, id)
		s.emitLeaseExpired(ctx, id)
	}
}

// emitLeaseExpired re-checks one room under s.mu and appends its
// control.lease_expired as the system actor.
func (s *Service) emitLeaseExpired(ctx context.Context, roomID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	st := s.rooms[roomID]
	if st == nil {
		return
	}
	p, ok := s.leaseExpiryDueLocked(st)
	if !ok {
		return
	}
	_, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		q, ok := s.leaseExpiryDueLocked(st)
		if !ok || q.Driver.ID != p.Driver.ID || !q.LeaseUntil.Equal(p.LeaseUntil) {
			return errLeaseNotDue
		}
		return nil
	}, events.ControlLeaseExpired, systemActor, p)
	switch {
	case errors.Is(err, errLeaseNotDue):
		return
	case err != nil:
		s.log.Warn("room: lease expiry: append control.lease_expired failed; retry at the next check", "room", roomID, "err", err)
		return
	}
	s.log.Info("room: driver lease expired", "room", roomID, "driver", p.Driver.ID, "lease_until", p.LeaseUntil)
}
