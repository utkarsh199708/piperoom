package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Room limits (docs/design/retention.md): the open-room limit, the log cap
// reactions and the data cap. The caps themselves are enforced by the store.

// retentionWarnLead is how long before the open-room deadline the room is
// warned.
const retentionWarnLead = 24 * time.Hour

// errRoomDeleting refuses a write to a room that DeleteRoom is removing.
func errRoomDeleting() *Error {
	return notFound("room is being deleted")
}

func alreadyExtended() *Error {
	return &Error{Status: 409, Code: "already_extended", Msg: "this room was already extended once"}
}

// usage is the room log's size with the caps of PIPEROOM_ROOM_MAX_EVENTS and
// PIPEROOM_ROOM_MAX_BYTES (the store enforces the same values). The event
// count is the last seq: the log has no gaps.
func (s *Service) usage(st *roomState) store.Usage {
	return store.Usage{
		Events: st.snap.LastSeq, Bytes: st.logBytes,
		MaxEvents: s.limits.RoomMaxEvents, MaxBytes: s.limits.RoomMaxBytes,
	}
}

// loadUsage reads a room's byte counter from the store, for a room that
// enters the cache from the log. A store without counters, or a failed read,
// gives 0: the store still enforces its caps, only the warning comes late.
func (s *Service) loadUsage(ctx context.Context, roomID string) int64 {
	c, ok := s.store.(store.Capped)
	if !ok {
		return 0
	}
	u, err := c.RoomUsage(ctx, roomID)
	if err != nil {
		s.log.Warn("room: read log usage failed", "room", roomID, "err", err)
		return 0
	}
	return u.Bytes
}

// ReloadUsage reads every cached room's byte counter from the store again.
// The server calls it after BackfillByteSize has counted the rooms from
// before migration 00012, so their 90% warning and log_full stop use the
// counted size instead of the bytes appended since the start.
func (s *Service) ReloadUsage(ctx context.Context) {
	for id := range s.cachedRooms() {
		if ctx.Err() != nil {
			return
		}
		if st := s.lockCachedRoom(id); st != nil {
			st.logBytes = s.loadUsage(ctx, id)
			s.unlockRoom(st)
		}
	}
}

// checkCapLocked reacts to the usage after an append: the one 90% warning,
// then the stop at 100%. Both are system events, held to the reserve, and both
// are folded into the snapshot (LogCap), so neither repeats, not even after a
// restart. Caller holds st's lock.
func (s *Service) checkCapLocked(ctx context.Context, st *roomState, roomID string) {
	u := s.usage(st)
	if u.AtWarning() && st.snap.LogCap == LogCapOK {
		if _, err := s.emitLocked(ctx, st, roomID, events.RoomLogCapWarning, systemActor, events.RoomLogCapWarningPayload{
			Events: u.Events, MaxEvents: u.MaxEvents, Bytes: u.Bytes, MaxBytes: u.MaxBytes,
		}); err != nil {
			s.log.Warn("room: log cap warning failed", "room", roomID, "err", err)
		}
	}
	if u.Full() {
		s.logFullLocked(ctx, st, roomID)
	}
}

// logFullLocked stops a room whose log reached its cap: the agent run is
// cancelled and the system writes agent.status log_full once. A shadow run
// writes to its own log, so it is left alone. If even the status cannot be
// stored (the reserve is used up), the snapshot is still marked full so the
// agent is not started again in this process. Caller holds st's lock.
func (s *Service) logFullLocked(ctx context.Context, st *roomState, roomID string) {
	if st.snap.LogCap == LogCapFull {
		return
	}
	u := s.usage(st)
	// Marked before the status append: if that append is refused too, the
	// refusal comes back here and must find the room already full.
	st.snap.LogCap = LogCapFull
	st.changed = true
	if !st.snap.Shadow && st.agentCancel != nil {
		st.agentCancel()
	}
	detail := fmt.Sprintf("room log is full (%d of %d events, %d of %d bytes); the agent has stopped", u.Events, u.MaxEvents, u.Bytes, u.MaxBytes)
	if _, err := s.emitLocked(ctx, st, roomID, events.AgentStatus, systemActor, events.AgentStatusPayload{
		State: AgentLogFull, Detail: detail,
	}); err != nil {
		s.log.Warn("room: log full status failed", "room", roomID, "err", err)
	}
	// Counts and ids only.
	s.log.Warn("room: log full; agent stopped", "room", roomID, "events", u.Events, "bytes", u.Bytes)
}

// openDeadline is the open-room limit's view of a room.
type openDeadline struct {
	// due is opened_at plus PIPEROOM_MAX_OPEN_DAYS, or the room.extended
	// deadline.
	due time.Time
	// warned is true when a room.retention_warning covers due: its close_at
	// is due or later.
	warned bool
	// closeAt is when the room is closed: the warned close_at, which is never
	// less than 24 h after the warning, or due while no warning was given.
	closeAt time.Time
}

// deadline returns the room's open-room deadline. ok is false when the limit
// is off or the room is not open. A room is never closed less than 24 h after
// its warning: a room that is already past due when the limit first sees it
// (the limit was just turned on, or lowered) is warned now and closed a day
// later.
func (s *Service) deadline(snap RoomSnapshot) (openDeadline, bool) {
	n := s.limits.MaxOpenDays
	if n <= 0 || snap.Status != "open" || snap.OpenedAt.IsZero() {
		return openDeadline{}, false
	}
	d := openDeadline{due: snap.OpenedAt.UTC().AddDate(0, 0, n)}
	if snap.extendedUntil != nil {
		d.due = snap.extendedUntil.UTC()
	}
	d.closeAt = d.due
	if w := snap.warnedFor; w != nil && !w.Before(d.due) {
		d.warned, d.closeAt = true, w.UTC()
	}
	return d, true
}

// setDeadline fills snapshot.auto_close_at.
func (s *Service) setDeadline(snap *RoomSnapshot) {
	if d, ok := s.deadline(*snap); ok {
		at := d.closeAt
		snap.AutoCloseAt = &at
		return
	}
	snap.AutoCloseAt = nil
}

// OpenLimitResult counts what one EnforceOpenLimit pass did.
type OpenLimitResult struct {
	Warned int
	Closed int
	Failed int
}

// EnforceOpenLimit applies PIPEROOM_MAX_OPEN_DAYS to every open room: a room
// within 24 h of its deadline gets one room.retention_warning per deadline,
// and a room past it is closed by the system actor with the resolution
// "retention: open longer than N days". The server calls it every few minutes;
// tests call it with a fake clock. A room that fails is logged and counted and
// tried again on the next pass.
func (s *Service) EnforceOpenLimit(ctx context.Context) (OpenLimitResult, error) {
	var res OpenLimitResult
	if s.limits.MaxOpenDays <= 0 {
		return res, nil
	}
	if err := s.ensureAllLoaded(ctx, false); err != nil {
		return res, fmt.Errorf("room: open-room limit: %w", err)
	}
	rooms := s.cachedRooms()
	ids := make([]string, 0, len(rooms))
	for id := range rooms {
		ids = append(ids, id)
	}
	sort.Strings(ids) // a stable order for logs and tests
	var expired []string
	for _, id := range ids {
		st := rooms[id]
		s.lockState(st)
		var warned, due bool
		var err error
		if !st.gone {
			warned, due, err = s.warnOpenLimitLocked(ctx, id, st)
		}
		s.unlockRoom(st)
		switch {
		case err != nil:
			s.log.Warn("room: retention warning failed", "room", id, "err", err)
			res.Failed++
		case warned:
			res.Warned++
		case due:
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("room: open-room limit: %w", err)
		}
		closed, err := s.autoClose(ctx, id)
		if err != nil {
			s.log.Warn("room: open-room limit could not close a room", "room", id, "err", err)
			res.Failed++
			continue
		}
		if closed {
			res.Closed++
		}
	}
	return res, nil
}

// warnOpenLimitLocked gives an open room its retention warning when its
// deadline is within a day (warned), or reports it due to close (due) when it
// was warned and its close time has passed. Caller holds st's lock.
func (s *Service) warnOpenLimitLocked(ctx context.Context, roomID string, st *roomState) (warned, due bool, err error) {
	if st.deleting {
		return false, false, nil
	}
	d, ok := s.deadline(st.snap)
	if !ok {
		return false, false, nil
	}
	// Per room: a pass over many rooms (each warning is a write) must not
	// judge the last room by the time the pass started.
	now := s.now().UTC()
	if d.warned {
		return false, !now.Before(d.closeAt), nil
	}
	if now.Before(d.due.Add(-retentionWarnLead)) {
		return false, false, nil
	}
	// The warning names the close time, which is at least 24 h away.
	closeAt := d.due
	if lead := now.Add(retentionWarnLead); closeAt.Before(lead) {
		closeAt = lead
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.RoomRetentionWarning, systemActor, events.RoomRetentionWarningPayload{
		CloseAt: closeAt, OpenDays: openDays(st.snap.OpenedAt, closeAt),
	}); err != nil {
		return false, false, err
	}
	return true, false, nil
}

// openDays is the whole number of days from opened to deadline.
func openDays(opened, deadline time.Time) int {
	return int((deadline.Sub(opened) + 12*time.Hour) / (24 * time.Hour))
}

// autoClose closes a room past its warned close time as the system actor.
// The check runs again under the lock: an extension or a close may have
// happened since the scan. closed is false when the room was no longer due.
func (s *Service) autoClose(ctx context.Context, roomID string) (closed bool, err error) {
	_, err = s.closeRoom(ctx, roomID, systemActor, func(st *roomState) (string, error) {
		now := s.now().UTC() // read under the lock, with the room's latest state
		d, ok := s.deadline(st.snap)
		if !ok || !d.warned || now.Before(d.closeAt) {
			return "", errNotDue
		}
		return "retention: open longer than " + strconv.Itoa(openDays(st.snap.OpenedAt, d.closeAt)) + " days", nil
	})
	if errors.Is(err, errNotDue) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s.log.Info("room: closed by the open-room limit", "room", roomID)
	return true, nil
}

var errNotDue = errors.New("room: not due")

// ExtendRoom is POST /api/rooms/{id}/extend: the driver moves the open-room
// deadline by PIPEROOM_MAX_OPEN_DAYS days, once per room.
func (s *Service) ExtendRoom(ctx context.Context, user User, roomID string) (events.Event, error) {
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	if !s.isDriver(st, user.ID) {
		return events.Event{}, ErrNotDriver()
	}
	n := s.limits.MaxOpenDays
	if n <= 0 {
		return events.Event{}, validation("the open-room limit is off (PIPEROOM_MAX_OPEN_DAYS=0); there is nothing to extend")
	}
	if st.snap.Extended {
		return events.Event{}, alreadyExtended()
	}
	d, ok := s.deadline(st.snap)
	if !ok {
		return events.Event{}, validation("this room has no open-room deadline")
	}
	// From the close time the room shows (auto_close_at), so a late warning's
	// extra day is not lost.
	e, err := s.emitLocked(ctx, st, roomID, events.RoomExtended, s.humanActor(user), events.RoomExtendedPayload{
		Days: n, CloseAt: d.closeAt.AddDate(0, 0, n),
	})
	if err != nil {
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}

// ---- data cap ----

// truncatedSuffix ends an action output that was cut.
const truncatedSuffix = "… [truncated: %d bytes]"

// capToolData replaces a tool_result.data over PIPEROOM_EVENT_MAX_DATA_BYTES
// with an events.TruncatedData object and sets the payload's truncated flag.
// raw is the sanitized payload JSON.
func (s *Service) capToolData(payload any) (any, error) {
	max := s.limits.EventMaxDataBytes
	raw, ok := payload.(json.RawMessage)
	if max <= 0 || !ok || int64(len(raw)) <= max {
		// The whole payload fits, so its data does too.
		return payload, nil
	}
	var p events.AgentToolResultPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("room: decode tool result: %w", err)
	}
	if int64(len(p.Data)) <= max {
		return payload, nil
	}
	data, err := truncateData(p.Data, max)
	if err != nil {
		return nil, err
	}
	p.Data, p.Truncated = data, true
	out, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("room: encode tool result: %w", err)
	}
	return json.RawMessage(out), nil
}

// truncateData builds the TruncatedData that replaces data: as much of the
// JSON text as fits in max bytes once encoded (escaping can grow it, so the
// preview is halved until it fits).
func truncateData(data json.RawMessage, max int64) (json.RawMessage, error) {
	td := events.TruncatedData{Truncated: true, OriginalBytes: int64(len(data))}
	n := int(max)
	for {
		td.Preview = cutUTF8(string(data), n)
		out, err := json.Marshal(td)
		if err != nil {
			return nil, fmt.Errorf("room: encode truncated data: %w", err)
		}
		if int64(len(out)) <= max || n == 0 {
			return out, nil
		}
		n /= 2
	}
}

// capOutput cuts an action output over PIPEROOM_EVENT_MAX_DATA_BYTES and says
// how long it was.
func (s *Service) capOutput(out string) string {
	max := s.limits.EventMaxDataBytes
	if max <= 0 || int64(len(out)) <= max {
		return out
	}
	suffix := fmt.Sprintf(truncatedSuffix, len(out))
	keep := int(max) - len(suffix)
	if keep < 0 {
		keep = 0
	}
	return cutUTF8(out, keep) + suffix
}

// cutUTF8 returns at most n bytes of s, never splitting a rune.
func cutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
