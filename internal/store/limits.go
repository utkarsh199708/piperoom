package store

import (
	"context"
	"errors"

	"piperoom/internal/events"
)

// ErrLogFull is returned (wrapped) by Append when the event would take the
// room's log over its cap (docs/design/retention.md section 5). Nothing is
// written and no seq is used.
var ErrLogFull = errors.New("store: room log is full")

// ReservePercent is how far over a cap events outside the agent class may go,
// so people can still comment, approve, hand off and close a full room.
const ReservePercent = 2

// WarnPercent is the share of a cap at which the room gets its one
// room.log_cap_warning.
const WarnPercent = 90

// Limits are the per-room log caps a store enforces. A zero field means no
// cap on that measure (unit tests and tools that never set limits).
type Limits struct {
	MaxEvents int64 // events in one room log
	MaxBytes  int64 // sum of the stored payload JSON sizes of one room log
}

// reserve is ReservePercent of n, rounded up, at least 1; 0 when n is 0.
func reserve(n int64) int64 {
	if n <= 0 {
		return 0
	}
	r := (n*ReservePercent + 99) / 100
	if r < 1 {
		r = 1
	}
	return r
}

// capClass says which limit an event is held to.
type capClass int

const (
	// classAgent is every event of the agent actor: refused at the cap.
	classAgent capClass = iota
	// classReserve is every human and system event: refused past cap + reserve.
	classReserve
	// classAlways is room.closed: never refused, so a full room can be closed.
	classAlways
)

func classOf(typ events.Type, actor events.Actor) capClass {
	switch {
	case typ == events.RoomClosed:
		return classAlways
	case actor.Kind == events.ActorAgent:
		return classAgent
	default:
		return classReserve
	}
}

// bounds returns the event and byte limits for a class; 0 means no limit.
func (l Limits) bounds(c capClass) (maxEvents, maxBytes int64) {
	switch c {
	case classAlways:
		return 0, 0
	case classReserve:
		me, mb := l.MaxEvents, l.MaxBytes
		if me > 0 {
			me += reserve(me)
		}
		if mb > 0 {
			mb += reserve(mb)
		}
		return me, mb
	default:
		return l.MaxEvents, l.MaxBytes
	}
}

// Fits reports whether one more event of this type and actor, with a payload
// of size bytes, stays within the limit of its class given the room's current
// counts. It is the rule the Postgres UPDATE applies; the memstore calls it.
func (l Limits) Fits(typ events.Type, actor events.Actor, curEvents, curBytes, size int64) bool {
	me, mb := l.bounds(classOf(typ, actor))
	if me > 0 && curEvents+1 > me {
		return false
	}
	if mb > 0 && curBytes+size > mb {
		return false
	}
	return true
}

// Usage returns the counts with these caps.
func (l Limits) Usage(events, bytes int64) Usage {
	return Usage{Events: events, Bytes: bytes, MaxEvents: l.MaxEvents, MaxBytes: l.MaxBytes}
}

// Usage is one room's log size after an append, with the caps it is held to.
type Usage struct {
	Events    int64
	Bytes     int64
	MaxEvents int64 // 0 = no cap
	MaxBytes  int64 // 0 = no cap
}

// AtWarning reports whether either measure reached WarnPercent of its cap.
func (u Usage) AtWarning() bool {
	return (u.MaxEvents > 0 && u.Events*100 >= u.MaxEvents*WarnPercent) ||
		(u.MaxBytes > 0 && u.Bytes*100 >= u.MaxBytes*WarnPercent)
}

// Full reports whether either measure reached its cap: agent events are
// refused from here on.
func (u Usage) Full() bool {
	return (u.MaxEvents > 0 && u.Events >= u.MaxEvents) ||
		(u.MaxBytes > 0 && u.Bytes >= u.MaxBytes)
}

// Capped is the cap surface of a store: Append enforces the limits set with
// SetLimits, and RoomUsage reads a room's counters. The bytes counted for an
// event are len(Event.Payload) as Append returns it, and the event count is the
// seq (the log has no gaps), so a caller that read RoomUsage once can follow
// the usage from the events it appends without another query. Both *PG and
// memstore.Store implement it.
type Capped interface {
	SetLimits(l Limits)
	RoomUsage(ctx context.Context, roomID string) (Usage, error)
}
