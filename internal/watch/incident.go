package watch

import (
	"slices"
	"strings"
	"time"

	"piperoom/internal/owners"
)

// Incidents, not pipelines (QA G1; docs/api.md "Watcher alerts"). An incident
// is the set of signal keys ("connector_down:pg-orders",
// "slot_wal:debezium_orders") a room carried. Before Q1 the cooldown after a
// closed room and the close-while-firing ack covered the whole pipeline, so a
// connector-auth break 19 s after an unrelated room closed opened nothing.
// Now:
//
//   - the cooldown covers the closed room's keys only: a firing signal with any
//     other key opens (or joins) a room at once;
//   - a signal that fires EscalateAfter with no room (acked at a close, held
//     back by the cooldown, the room cap, a failing open) logs a warning once
//     and, in rooms mode, re-opens a room once, unless the cap is reached;
//   - an open room is joined only when it is recent and came from an alert, or
//     a person opened it recently (RoomInfoSink); a join that brings keys the
//     room was not told about goes through IncidentSink, which wakes the agent.
//
// A close the watcher did not see (the room service knows it, the watcher
// restarted since) is recovered from the room service when it can tell the
// room's keys (ClosedRoomKeysSink); otherwise it covers only the signals that
// were already firing at the close (QA S1).

const (
	// maxToldRooms caps the rooms a pipeline remembers telling. A room leaves
	// the list when it closes; the oldest goes first past the cap.
	maxToldRooms = 8
	// maxToldKeys caps the keys remembered per room.
	maxToldKeys = 64
)

// toldRoom is what one room has been told about one pipeline.
type toldRoom struct {
	roomID string
	keys   []string // sorted
}

// cooledKey is one key of a closed room's incident.
type cooledKey struct {
	at   time.Time // when its room closed
	room string
}

// sigKey is the incident key of one signal target.
func sigKey(signal, target string) string { return signal + ":" + target }

// toldIndex is the index of roomID in ps.told, or -1.
func (ps *pipeState) toldIndex(roomID string) int {
	return slices.IndexFunc(ps.told, func(t toldRoom) bool { return t.roomID == roomID })
}

// untold returns the keys roomID has not been told about. known is false when
// the pipeline never posted into roomID (every key is then untold).
func (ps *pipeState) untold(roomID string, keys []string) (fresh []string, known bool) {
	i := ps.toldIndex(roomID)
	for _, k := range keys {
		if i < 0 || !slices.Contains(ps.told[i].keys, k) {
			fresh = append(fresh, k)
		}
	}
	return fresh, i >= 0
}

// tell records that roomID now carries keys.
func (ps *pipeState) tell(roomID string, keys []string) {
	if roomID == "" {
		return
	}
	i := ps.toldIndex(roomID)
	if i < 0 {
		if len(ps.told) >= maxToldRooms {
			ps.told = slices.Delete(ps.told, 0, 1)
		}
		ps.told = append(ps.told, toldRoom{roomID: roomID})
		i = len(ps.told) - 1
	}
	tr := &ps.told[i]
	for _, k := range keys {
		if !slices.Contains(tr.keys, k) && len(tr.keys) < maxToldKeys {
			tr.keys = append(tr.keys, k)
		}
	}
	slices.Sort(tr.keys)
}

// cool puts keys into cooldown from at, the close of room.
func (ps *pipeState) cool(keys []string, room string, at time.Time) {
	if ps.cooled == nil {
		ps.cooled = map[string]cooledKey{}
	}
	for _, k := range keys {
		if c, ok := ps.cooled[k]; !ok || at.After(c.at) {
			ps.cooled[k] = cooledKey{at: at, room: room}
		}
	}
}

// coolRoom moves the keys roomID was told about into cooldown and forgets the
// room. It reports whether the pipeline had told the room anything.
func (ps *pipeState) coolRoom(roomID string, at time.Time) bool {
	i := ps.toldIndex(roomID)
	if i < 0 {
		return false
	}
	ps.cool(ps.told[i].keys, roomID, at)
	ps.told = slices.Delete(ps.told, i, i+1)
	return true
}

// quiet is the cooldown verdict for an open.
type quiet struct {
	on   bool
	room string
	ago  time.Duration
	// unknown: the closed room's incident is not known (a close the watcher
	// did not see and could not recover), so the cooldown covers the signals
	// that were already firing at that close.
	unknown bool
}

// cooldownOf decides whether the pending signals pend stay quiet. last,
// closedAt and found are the room service's answer for the pipeline's own
// dedup key (LastRoomForKey). Every key of every pending signal must be in a
// closed incident for the open to wait; one outside opens at once. A close
// the watcher did not see (and recoverClosed could not explain) covers only
// a signal that fired at or before the close (QA S1): one that fired later,
// such as after a restart, was not part of what the driver closed. Caller
// holds w.mu.
func (w *Watcher) cooldownOf(now time.Time, id string, pend []sigSnap, last string, closedAt *time.Time, found bool) quiet {
	if found && closedAt == nil {
		return quiet{} // the key's room is still open: it is joined, not waited out
	}
	ps := w.pipes[id]
	if ps == nil || len(pend) == 0 {
		return quiet{}
	}
	for k, c := range ps.cooled {
		if now.Sub(c.at) >= w.cfg.Cooldown {
			delete(ps.cooled, k)
		}
	}
	unseen := closedAt != nil && !w.closed[last] && now.Sub(*closedAt) < w.cfg.Cooldown
	q := quiet{on: true}
	for _, s := range pend {
		if unseen && !s.fired.IsZero() && !s.fired.After(*closedAt) {
			if ago := now.Sub(*closedAt); q.room == "" || ago < q.ago {
				q.room, q.ago = last, ago
			}
			q.unknown = true
			continue
		}
		for _, k := range s.keys {
			c, ok := ps.cooled[k]
			if !ok {
				return quiet{} // a signal outside every closed incident
			}
			if q.room == "" || now.Sub(c.at) < q.ago {
				q.room, q.ago = c.room, now.Sub(c.at)
			}
		}
	}
	return q
}

// recoverClosed asks the room service (ClosedRoomKeysSink) what a closed
// watcher room the watcher did not see carried, and cools those keys from
// its close. The room then counts as seen. A sink without the extension, or
// a room it cannot explain, leaves the fallback rule of cooldownOf. Called
// without w.mu.
func (w *Watcher) recoverClosed(now time.Time, id, last string, closedAt *time.Time) {
	ks, ok := w.rooms.(ClosedRoomKeysSink)
	if !ok || last == "" || closedAt == nil || now.Sub(*closedAt) >= w.cfg.Cooldown {
		return
	}
	w.mu.Lock()
	seen := w.closed[last]
	w.mu.Unlock()
	if seen {
		return
	}
	keys, ok := ks.RoomSignalKeys(last)
	if !ok || len(keys) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed[last] {
		return
	}
	w.markClosedLocked(last)
	w.pipe(id).cool(keys, last, *closedAt)
	w.log.Info("watch: recovered the incident of a room closed before the watcher saw it",
		"mode", string(w.cfg.Mode), "pipeline", id, "room", last, "keys", len(keys),
		"closed_ago", now.Sub(*closedAt).Round(time.Second).String())
}

// inCooldown: the pending signal sig of the pipeline belongs to an incident
// whose room closed less than the cooldown ago.
func (w *Watcher) inCooldown(now time.Time, id, sig string) bool {
	last, closedAt, found := w.rooms.LastRoomForKey(id, DedupKey(id))
	w.recoverClosed(now, id, last, closedAt)
	w.mu.Lock()
	defer w.mu.Unlock()
	var pend []sigSnap
	if ps := w.pipes[id]; ps != nil && ps.sigs[sig] != nil {
		ss := ps.sigs[sig]
		pend = []sigSnap{{id: sig, keys: ss.keys(sig), fired: ss.firedAt}}
	}
	return w.cooldownOf(now, id, pend, last, closedAt, found).on
}

// joinable applies the Q1 join rule to an open room: its newest event is
// younger than maxAge, and it came from an alert or the watcher, or a person
// opened it less than maxAge ago. A zero time counts as old.
func joinable(now time.Time, info RoomInfo, maxAge time.Duration) bool {
	if info.LastEventAt.IsZero() || now.Sub(info.LastEventAt) >= maxAge {
		return false
	}
	return info.AlertSource != "" || (!info.OpenedAt.IsZero() && now.Sub(info.OpenedAt) < maxAge)
}

// titleFlags are the injection flags of a room's title (RoomTitleFlagsSink);
// nil without the extension or for an unknown room.
func (w *Watcher) titleFlags(roomID string) []string {
	fs, ok := w.rooms.(RoomTitleFlagsSink)
	if !ok {
		return nil
	}
	flags, ok := fs.RoomTitleFlags(roomID)
	if !ok {
		return nil
	}
	return flags
}

// roomToJoin finds an open room of the pipeline that takes the evidence
// instead of a new room (B1). With a RoomInfoSink an old room, or an old one a
// person opened, is passed over and the watcher opens its own (QA G1: an old
// manual room titled "test" swallowed an outage); so is a room whose title is
// flagged as a prompt injection (RoomTitleFlagsSink, QA S2). Every open goes
// through here, an escalation's included.
func (w *Watcher) roomToJoin(now time.Time, id string) (string, bool) {
	ris, ok := w.rooms.(RoomInfoSink)
	if !ok {
		return w.rooms.OpenRoomForPipeline(id)
	}
	info, ok := ris.OpenRoomInfo(id)
	if !ok || info.RoomID == "" {
		return "", false
	}
	if w.mayJoin(now, id, info) {
		return info.RoomID, true
	}
	return "", false
}

// mayJoin applies the join rules to info and logs a refusal once per episode.
func (w *Watcher) mayJoin(now time.Time, id string, info RoomInfo) bool {
	if !joinable(now, info, w.cfg.JoinMaxAge) {
		w.once(id, func(ps *pipeState) *bool { return &ps.warnedStale }, func() {
			by := "alert"
			if info.AlertSource == "" {
				by = "person"
			}
			args := []any{"mode", string(w.cfg.Mode), "pipeline", id, "room", info.RoomID, "opened_by", by,
				"join_max_age", w.cfg.JoinMaxAge.String()}
			if !info.LastEventAt.IsZero() {
				args = append(args, "last_event_ago", now.Sub(info.LastEventAt).Round(time.Second).String())
			}
			w.log.Info("watch: not joining an old or manual room, opening the watcher's own", args...)
		})
		return false
	}
	if flags := w.titleFlags(info.RoomID); len(flags) > 0 {
		w.once(id, func(ps *pipeState) *bool { return &ps.warnedFlagged }, func() {
			w.log.Warn("watch: not joining a room whose title is flagged as a possible prompt injection, opening the watcher's own",
				"mode", string(w.cfg.Mode), "pipeline", id, "room", info.RoomID, "flags", strings.Join(flags, ","))
		})
		return false
	}
	return true
}

// Reasons an open opened nothing (pipeState.blocked), for the escalation line.
const (
	blockedCooldown = "cooldown after a closed room"
	blockedMax      = "max_open_watcher_rooms reached"
	blockedOpen     = "the room service could not open a room"
	blockedStorm    = "storm: waiting for the leader's room"
	blockedAck      = "acked: its room was closed while it still fired"
)

// block records why the pipeline's open opened nothing.
func (w *Watcher) block(id, why string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ps := w.pipes[id]; ps != nil {
		ps.blocked = why
	}
}

// escalateLocked handles signals that fire EscalateAfter with no room (QA G1):
// acked at a close, held back by the cooldown, the cap, a failing open or a
// storm leader. Each logs one warning per episode. An acked one is un-acked,
// so it is posted again with a note that it kept firing after the close; the
// open that follows skips the cooldown. The cap still holds (openRoom logs an
// error then). Each signal escalates once per episode: closed again while it
// fires, it stays acked until it clears. An escalation that was blocked by
// the cap, a failing open or a storm wait opened nothing, so it does not spend
// that budget: the next room that carries the signal gives it back (recordPost).
// Caller holds w.mu.
func (w *Watcher) escalateLocked(now time.Time, id string, ps *pipeState, t target) {
	e := w.cfg.EscalateAfter
	if e <= 0 {
		return
	}
	for _, sid := range signalIDs {
		sc := signalCfg(t.cfg, sid)
		ss := ps.sigs[sid]
		if ss == nil || !sc.Enabled || sc.Action != owners.ActionRoom || ss.escalated || !ss.clearingSince.IsZero() {
			continue
		}
		var since time.Time
		why := ps.blocked
		switch {
		case ss.phase == phaseAcked:
			since, why = ss.ackedAt, blockedAck
		case ss.phase == phaseFiring && !ss.notified && ps.roomID == "":
			since = ss.firedAt
		default:
			continue
		}
		if since.IsZero() || now.Sub(since) < e {
			continue
		}
		ss.escalated = true
		ss.escFree = ss.phase != phaseAcked && why != blockedCooldown
		if why == "" {
			why = "no room yet"
		}
		w.log.Warn("watch: signal firing with no room", "mode", string(w.cfg.Mode), "pipeline", id, "signal", sid,
			"firing_for", now.Sub(since).Round(time.Second).String(), "escalate_after", e.String(), "reason", why)
		if ss.phase == phaseAcked {
			ss.phase, ss.notified = phaseFiring, false
			ps.note = "kept firing " + fmtDur(now.Sub(since).Round(time.Second)) + " after its room was closed"
		} else if ps.note == "" {
			ps.note = "firing " + fmtDur(now.Sub(since).Round(time.Second)) + " with no room"
		}
		ps.bypassCooldown = true
	}
}

// SignalLayer is the pipeline layer (one of events.Layers) a watcher signal
// points at, "" for an unknown signal. Every signal's evidence carries it as
// "layer", so the room service can page the owners of the first signal's
// layer (QA G2) without its own copy of this table.
func SignalLayer(signal string) string {
	switch signal {
	case owners.SignalConnectorDown, owners.SignalConfigRisk, owners.SignalDLQGrowth, SignalConnectUnreachable:
		return "kafka-connect"
	case owners.SignalSlotInactive, owners.SignalSlotWAL, SignalSlotStall:
		return "postgres"
	case owners.SignalConsumerLag, owners.SignalGroupUnstable, owners.SignalOffsetBelowLogStart:
		return "consumer"
	}
	return ""
}
