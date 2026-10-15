package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/alertin"
	"piperoom/internal/owners"
)

// maxEvidenceBytes keeps the evidence small. Past it, task traces are dropped.
const maxEvidenceBytes = 4096

// maxAffected caps the pipeline list of a storm in the evidence.
const maxAffected = 20

// sigSnap is a copy of one firing signal, safe to read without the lock.
type sigSnap struct {
	id      string
	sev     string
	label   string
	withDur bool
	since   time.Time
	ev      map[string]any
	pending bool     // not yet in a room
	keys    []string // incident keys (sigState.keys)
	// fired is when the signal last fired (sigState.firedAt).
	fired time.Time
	// clearedFrom is when the signal last started to clear (sigState.clearedFrom).
	clearedFrom time.Time
}

// view is a copy of what the output step needs to know about one pipeline.
type view struct {
	roomID   string
	owned    bool
	follower bool
	// leaderGone: a follower whose storm room has no leader any more (the
	// leader resolved). The follower then repeats "still firing" itself.
	leaderGone bool
	active     []sigSnap // firing and not acked, action room
	hasAny     bool      // some room signal is firing or acked
	episode    bool
	sev        string // most severe of active
	postedSev  string
	lastPost   time.Time
	bypass     bool             // skip the cooldown (a severity rise or an escalation re-opened the incident)
	risks      []map[string]any // current config findings (codes only)
	// note is the escalation's reason, for the title and the evidence.
	note string
}

// keys are the incident keys of the active signals.
func (v view) keys() []string { return snapKeys(v.active) }

func snapKeys(ss []sigSnap) []string {
	var out []string
	for _, s := range ss {
		out = append(out, s.keys...)
	}
	return out
}

func (v view) pending() []sigSnap {
	var out []sigSnap
	for _, s := range v.active {
		if s.pending {
			out = append(out, s)
		}
	}
	return out
}

func (v view) hasPending(sig string) bool {
	return slices.ContainsFunc(v.active, func(s sigSnap) bool { return s.pending && s.id == sig })
}

// signalCfg returns the shared settings of a signal.
func signalCfg(cfg owners.WatchConfig, id string) owners.WatchSignal {
	switch id {
	case owners.SignalConnectorDown:
		return cfg.ConnectorDown
	case owners.SignalSlotInactive:
		return cfg.SlotInactive
	case owners.SignalDLQGrowth:
		return cfg.DLQGrowth.WatchSignal
	case owners.SignalConfigRisk:
		return cfg.ConfigRisk
	case owners.SignalConsumerLag:
		return cfg.ConsumerLag.WatchSignal
	case owners.SignalGroupUnstable:
		return cfg.GroupUnstable.WatchSignal
	case owners.SignalOffsetBelowLogStart:
		return cfg.OffsetBelowLogStart
	case SignalSlotStall:
		return slotStallCfg(cfg)
	case SignalConnectUnreachable:
		return connectUnreachableCfg(cfg)
	}
	return cfg.SlotWAL.WatchSignal
}

// viewOf copies the state of a pipeline. Caller holds w.mu.
func (w *Watcher) viewOf(t target, ps *pipeState) view {
	v := view{roomID: ps.roomID, owned: ps.owned, follower: ps.follower, episode: ps.episode,
		postedSev: ps.postedSev, lastPost: ps.lastPost, bypass: ps.bypassCooldown, sev: "sev9", risks: ps.risks,
		note: ps.note}
	v.leaderGone = ps.follower && ps.roomID != "" && !w.roomLedLocked(ps.roomID)
	for _, id := range signalIDs {
		ss := ps.sigs[id]
		sc := signalCfg(t.cfg, id)
		if ss == nil || !sc.Enabled || sc.Action != owners.ActionRoom {
			continue
		}
		switch ss.phase {
		case phaseFiring:
			v.hasAny = true
			v.sev = moreSevere(v.sev, ss.last.sev)
			v.active = append(v.active, sigSnap{
				id: id, sev: ss.last.sev, label: ss.last.label, withDur: ss.last.withDur,
				since: ss.breachingSince, ev: ss.last.ev, pending: !ss.notified, keys: ss.keys(id),
				fired: ss.firedAt, clearedFrom: ss.clearedFrom,
			})
		case phaseAcked:
			v.hasAny = true
		}
	}
	return v
}

// roomLedLocked: some pipeline that is not a storm follower (the leader, whether
// it opened the room or joined one that was already open) still posts into roomID.
// Caller holds w.mu.
func (w *Watcher) roomLedLocked(roomID string) bool {
	for _, ps := range w.pipes {
		if !ps.follower && ps.roomID == roomID {
			return true
		}
	}
	return false
}

// reviveWorse re-opens an acked incident when it got worse than what the
// driver saw when they closed the room: any signal that rose to sev1 (a slot
// about to be lost), and a connector_down that rose at all (a PAUSED
// connector, sev3, that went FAILED or crash-loops, sev2). connector_down's
// severity changes only with the failure class; the other signals' sev3 ->
// sev2 follows volume or duration (a DLQ trickle, lag rising for 2×for), which
// the driver already saw, so those stay acked. The open skips the cooldown.
// ackSev moves to the new severity at the next close, so each level re-opens at
// most once. Caller holds w.mu.
func (w *Watcher) reviveWorse(id string, ps *pipeState, t target) {
	for _, sid := range signalIDs {
		sc := signalCfg(t.cfg, sid)
		ss := ps.sigs[sid]
		if ss == nil || !sc.Enabled || sc.Action != owners.ActionRoom || ss.phase != phaseAcked {
			continue
		}
		rose := sevRank(ss.last.sev) < sevRank(ss.ackSev)
		worse := ss.last.sev == "sev1" || sid == owners.SignalConnectorDown
		if rose && worse && ss.clearingSince.IsZero() {
			ss.phase, ss.notified = phaseFiring, false
			ps.bypassCooldown = true
			w.log.Warn("watch: severity rose after the room was closed, re-opening",
				"mode", string(w.cfg.Mode), "pipeline", id, "signal", sid, "was", ss.ackSev, "now", ss.last.sev)
			return
		}
	}
}

// stormInfo is one storm: the signal fired on several pipelines in one tick.
type stormInfo struct {
	// members are the pipelines that wait for the storm room, in owners order.
	// Pipelines in cooldown are not members: they stay quiet on their own.
	members []string
	// leader opens the room: the first member that is not in cooldown.
	leader string
}

// waitingLocked: the signal fires on the pipeline and no room carries it yet.
func (w *Watcher) waitingLocked(id, sig string) bool {
	ps := w.pipes[id]
	if ps == nil || ps.roomID != "" {
		return false
	}
	ss := ps.sigs[sig]
	return ss != nil && ss.phase == phaseFiring && !ss.notified
}

// stormGroups finds the storms of this tick: a signal that fired on at least
// stormThreshold pipelines in one tick (one Connect worker died). A storm whose
// leader could not get a room is carried to the next tick (w.storms), so its
// members do not each open a room of their own.
func (w *Watcher) stormGroups(now time.Time, ts []target, fired map[string][]string) map[string]*stormInfo {
	pos := map[string]int{}
	for i, t := range ts {
		pos[t.p.ID] = i
	}
	w.mu.Lock()
	cands := map[string][]string{}
	for sig, ids := range w.storms { // carried over
		for _, id := range ids {
			if w.waitingLocked(id, sig) {
				cands[sig] = append(cands[sig], id)
			}
		}
	}
	for sig, ids := range fired {
		var fresh []string
		for _, id := range ids {
			if w.waitingLocked(id, sig) {
				fresh = append(fresh, id)
			}
		}
		if len(fresh) >= stormThreshold {
			cands[sig] = append(cands[sig], fresh...)
		}
	}
	bypass := map[string]bool{}
	for id, ps := range w.pipes {
		bypass[id] = ps.bypassCooldown
	}
	w.mu.Unlock()

	out := map[string]*stormInfo{}
	next := map[string][]string{}
	for sig, ids := range cands {
		slices.SortFunc(ids, func(a, b string) int { return pos[a] - pos[b] })
		ids = slices.Compact(ids)
		var members []string
		for _, id := range ids {
			if !bypass[id] && w.inCooldown(now, id, sig) {
				continue
			}
			members = append(members, id)
		}
		if len(members) == 0 {
			continue
		}
		out[sig] = &stormInfo{members: members, leader: members[0]}
		next[sig] = members
	}
	w.mu.Lock()
	w.storms = next
	w.mu.Unlock()
	return out
}

// emit writes to rooms for this tick: opens, joins, "still firing" and
// resolved. Sink calls are made without the lock (a room hook may call back
// into OnRoomClosed).
func (w *Watcher) emit(ctx context.Context, now time.Time, ts []target, fired map[string][]string) {
	storm := w.stormGroups(now, ts, fired)
	for _, t := range ts {
		if ctx.Err() != nil {
			return
		}
		w.emitPipeline(ctx, now, t, storm)
	}
}

func (w *Watcher) emitPipeline(ctx context.Context, now time.Time, t target, storm map[string]*stormInfo) {
	id := t.p.ID
	w.mu.Lock()
	ps := w.pipes[id]
	if ps == nil {
		w.mu.Unlock()
		return
	}
	w.reviveWorse(id, ps, t)
	w.escalateLocked(now, id, ps, t)
	v := w.viewOf(t, ps)
	w.mu.Unlock()

	// A follower of a storm waits until its leader has a room: then the leader
	// has put it into that room already. If the leader failed, nobody opens a
	// room of their own; the storm is carried to the next tick.
	if v.roomID == "" {
		for sig, si := range storm {
			if si.leader != id && slices.Contains(si.members, id) && v.hasPending(sig) {
				w.block(id, blockedStorm)
				w.log.Debug("watch: storm, waiting for the leader's room", "pipeline", id, "signal", sig, "leader", si.leader)
				return
			}
		}
	}

	switch {
	case !v.hasAny && v.episode:
		w.resolve(ctx, now, t, v)
	case len(v.active) == 0:
		// Nothing firing that is not acked: stay quiet.
	case v.roomID != "":
		w.updateRoom(ctx, now, t, v)
	case len(v.pending()) > 0:
		w.openRoom(ctx, now, t, v, storm)
	}
}

// updateRoom joins the room the pipeline already has: a new signal (or a new
// target of a firing one, such as a second connector), a higher severity, or
// the 15-minute "still firing". Keys the room was not told about make it an
// incident post (IncidentSink), which wakes the agent; the rest are repeats.
func (w *Watcher) updateRoom(ctx context.Context, now time.Time, t target, v view) {
	w.mu.Lock()
	fresh, _ := w.pipes[t.p.ID].untold(v.roomID, v.keys())
	w.mu.Unlock()
	// A new incident in a room the pipeline only joined is a join too: the
	// join rules apply again (QA S2). Repeats keep going to the room.
	if (len(fresh) > 0 || len(v.pending()) > 0) && !v.owned && !v.follower && w.leaveJoined(now, t, v, fresh) {
		w.mu.Lock()
		v = w.viewOf(t, w.pipes[t.p.ID])
		w.mu.Unlock()
		if v.roomID == "" && len(v.pending()) > 0 {
			w.openRoom(ctx, now, t, v, nil)
		}
		return
	}
	reason := ""
	switch {
	case len(v.pending()) > 0 || len(fresh) > 0:
		reason = "new signal"
	case sevRank(v.sev) < sevRank(v.postedSev):
		reason = "severity raised to " + v.sev
	case (!v.follower || v.leaderGone) && now.Sub(v.lastPost) >= stillFiringEvery:
		reason = "still firing"
	default:
		return
	}
	title, ev := compose(t.p.ID, now, v, reason, nil)
	if !w.post(ctx, v.roomID, fmt.Sprintf("watcher: %s (%s)\nevidence: %s", title, reason, ev), ev, len(fresh) > 0) {
		return
	}
	if w.recordPost(now, t.p.ID, v, v.roomID, v.owned, v.follower) {
		w.log.Info("watch: joined room", "mode", string(w.cfg.Mode), "pipeline", t.p.ID, "room", v.roomID, "reason", reason, "severity", v.sev)
	}
}

// leaveJoined reports whether the pipeline leaves the room it joined (v.roomID,
// not its own) because that room no longer passes the join rules (old, a
// person's room past JoinMaxAge, a flagged title). The signals that bring the
// fresh keys become pending again, so the caller opens or joins another room
// for them. A room that is not the pipeline's newest open one cannot be
// checked and is kept. Called without w.mu.
func (w *Watcher) leaveJoined(now time.Time, t target, v view, fresh []string) bool {
	ris, ok := w.rooms.(RoomInfoSink)
	if !ok {
		return false
	}
	info, ok := ris.OpenRoomInfo(t.p.ID)
	if !ok || info.RoomID != v.roomID || w.mayJoin(now, t.p.ID, info) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ps := w.pipes[t.p.ID]
	if ps == nil || ps.roomID != v.roomID {
		return false
	}
	for id, ss := range ps.sigs {
		if ss.phase == phaseFiring && ss.notified && slices.ContainsFunc(ss.keys(id), func(k string) bool { return slices.Contains(fresh, k) }) {
			ss.notified = false
		}
	}
	ps.roomID, ps.owned, ps.follower = "", false, false
	w.log.Info("watch: new signals do not join the room the pipeline joined earlier, it no longer passes the join rules",
		"mode", string(w.cfg.Mode), "pipeline", t.p.ID, "room", v.roomID)
	return true
}

// openRoom starts the incident for a pipeline: the cooldown of the closed
// incident (pending signals outside it skip it), then an already open room
// that may be joined, then the room cap, then a new room.
func (w *Watcher) openRoom(ctx context.Context, now time.Time, t target, v view, storm map[string]*stormInfo) {
	id := t.p.ID
	key := DedupKey(id)

	existing, closedAt, found := w.rooms.LastRoomForKey(id, key)
	if !v.bypass {
		w.recoverClosed(now, id, existing, closedAt)
		w.mu.Lock()
		q := w.cooldownOf(now, id, v.pending(), existing, closedAt, found)
		w.mu.Unlock()
		if q.on {
			w.block(id, blockedCooldown)
			w.once(id, func(ps *pipeState) *bool { return &ps.warnedCooldown }, func() {
				incident := "same signals"
				if q.unknown {
					incident = "unknown: closed before the watcher saw it"
				}
				w.log.Info("watch: cooldown after a closed watcher room, staying quiet",
					"mode", string(w.cfg.Mode), "pipeline", id, "room", q.room,
					"closed_ago", q.ago.Round(time.Second).String(), "cooldown", w.cfg.Cooldown.String(), "incident", incident)
			})
			return
		}
	}
	reuse := found && closedAt == nil // our watcher room is still open (from before a restart, or resolved)

	// Storm: the leader carries every affected pipeline in its evidence and the
	// followers join its room afterwards.
	var affected, led []string
	for sig, si := range storm {
		if si.leader == id && v.hasPending(sig) {
			affected = append(affected, si.members...)
			led = append(led, sig)
		}
	}
	slices.Sort(affected)
	affected = slices.Compact(affected)
	slices.Sort(led)

	if !reuse {
		// An open room for the pipeline, an Alertmanager one included, takes
		// the evidence instead of a second room (B1), when the join rules allow.
		if other, ok := w.roomToJoin(now, id); ok {
			title, ev := compose(id, now, v, "joined an open room", affected)
			w.mu.Lock()
			fresh, _ := w.pipes[id].untold(other, v.keys())
			w.mu.Unlock()
			if !w.post(ctx, other, fmt.Sprintf("watcher: %s\nevidence: %s", title, ev), ev, len(fresh) > 0) {
				return
			}
			if w.recordPost(now, id, v, other, false, false) {
				w.assignFollowers(now, id, other, storm, led)
				w.log.Info("watch: joined an open room", "mode", string(w.cfg.Mode), "pipeline", id, "room", other,
					"severity", v.sev, "new_incident", len(fresh) > 0)
			}
			return
		}
		w.mu.Lock()
		full := w.openOwned() >= w.maxRooms
		w.mu.Unlock()
		if full {
			w.block(id, blockedMax)
			w.once(id, func(ps *pipeState) *bool { return &ps.warnedMax }, func() {
				w.log.Warn("watch: max_open_watcher_rooms reached, not opening a room",
					"mode", string(w.cfg.Mode), "pipeline", id, "max", w.maxRooms)
			})
			if v.note != "" { // an escalation: a signal has fired EscalateAfter with no room
				w.once(id, func(ps *pipeState) *bool { return &ps.erroredMax }, func() {
					w.log.Error("watch: signal firing with no room, max_open_watcher_rooms reached",
						"mode", string(w.cfg.Mode), "pipeline", id, "max", w.maxRooms, "note", v.note)
				})
			}
			return
		}
	}

	title, ev := compose(id, now, v, "opened", affected)
	alert := alertin.Alert{
		Source:     alertin.Watcher,
		Status:     alertin.StatusFiring,
		Title:      title,
		Severity:   v.sev,
		PipelineID: id,
		DedupKey:   key,
		OccurredAt: now,
		Raw:        ev,
	}
	sctx, cancel := context.WithTimeout(ctx, w.cfg.CheckTimeout)
	roomID, created, err := w.rooms.OpenAlertRoom(sctx, alert)
	cancel()
	if err != nil {
		// Warn once per episode; the retry on the next tick is only a debug line.
		msg := clean(err.Error(), 300)
		w.mu.Lock()
		first := !w.pipes[id].warnedOpen
		w.pipes[id].warnedOpen = true
		w.pipes[id].blocked = blockedOpen
		w.mu.Unlock()
		if first {
			w.log.Warn("watch: could not open a room, will retry", "mode", string(w.cfg.Mode), "pipeline", id, "err", msg)
		} else {
			w.log.Debug("watch: could not open a room, will retry", "pipeline", id, "err", msg)
		}
		return
	}
	// What the room knew before this open: its keys when it is our room that
	// stayed open after its signals cleared. After a restart the watcher has
	// no told list; the room service's log says what the room carried (QA Q4
	// R2), so a key it never carried is a new incident that wakes the agent.
	stale := false
	w.mu.Lock()
	_, inProcess := w.pipes[id].untold(roomID, nil)
	w.mu.Unlock()
	if !created {
		stale = w.seedFromRoom(now, id, roomID)
	}
	w.mu.Lock()
	fresh, known := w.pipes[id].untold(roomID, v.keys())
	w.mu.Unlock()
	if !w.recordPost(now, id, v, roomID, true, false) {
		return
	}
	if !created {
		// The service only writes "fired again" with the title on a join; the
		// numbers go in as evidence. After a restart it is a repeat (the agent
		// is not woken, docs/api.md); in-process, keys the room has not seen
		// are a new incident.
		how := "re-attached after a restart"
		if inProcess {
			how = "fired again in the open room"
		}
		// A room that fails the join rules (old, flagged title) is a new
		// incident for whoever is in it, whatever it carried before.
		w.post(ctx, roomID, fmt.Sprintf("watcher: %s (%s)\nevidence: %s", title, how, ev), ev, (known && len(fresh) > 0) || stale)
	}
	w.assignFollowers(now, id, roomID, storm, led)
	if len(affected) > 1 {
		w.log.Info("watch: storm, one room for several pipelines", "mode", string(w.cfg.Mode), "pipeline", id, "affected", strings.Join(affected, ","), "room", roomID)
	}
	w.log.Info("watch: room "+map[bool]string{true: "opened", false: "joined"}[created],
		"mode", string(w.cfg.Mode), "pipeline", id, "room", roomID, "severity", v.sev)
}

// seedFromRoom learns what the watcher's own open room carried when the
// pipeline has no told list for it (a restart, a leadership change): the
// keys come from ClosedRoomKeysSink, which reads any room, open or not. It
// also reports whether the room fails the join rules (JoinMaxAge, flagged
// title), so the post wakes the agent even for a repeated key. Called
// without w.mu.
func (w *Watcher) seedFromRoom(now time.Time, id, roomID string) (stale bool) {
	if ris, ok := w.rooms.(RoomInfoSink); ok {
		if info, ok := ris.OpenRoomInfo(id); ok && info.RoomID == roomID &&
			(!joinable(now, info, w.cfg.JoinMaxAge) || len(w.titleFlags(roomID)) > 0) {
			stale = true
			w.log.Info("watch: re-attached room is old or flagged, telling the agent as a new incident",
				"mode", string(w.cfg.Mode), "pipeline", id, "room", roomID)
		}
	}
	w.mu.Lock()
	seeded := w.pipes[id].toldIndex(roomID) >= 0
	w.mu.Unlock()
	if seeded {
		return stale
	}
	if ks, ok := w.rooms.(ClosedRoomKeysSink); ok {
		if keys, ok := ks.RoomSignalKeys(roomID); ok && len(keys) > 0 {
			w.mu.Lock()
			w.pipes[id].tell(roomID, keys)
			w.mu.Unlock()
		}
	}
	return stale
}

// assignFollowers puts the other members of a storm into the leader's room
// (owned = false), so their later signals, severity changes and clear land
// there. It then drops the carried-over storm. Caller holds no lock.
func (w *Watcher) assignFollowers(now time.Time, leader, roomID string, storm map[string]*stormInfo, led []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, sig := range led {
		si := storm[sig]
		delete(w.storms, sig)
		for _, m := range si.members {
			if m == leader {
				continue
			}
			ps := w.pipes[m]
			if ps == nil || ps.roomID != "" {
				continue
			}
			ss := ps.sigs[sig]
			if ss == nil || ss.phase != phaseFiring {
				continue
			}
			ss.notified = true
			if w.closed[roomID] {
				at := w.clock.Now()
				ss.ack()
				ss.ackedAt = at
				ps.cool(ss.keys(sig), roomID, at)
				continue
			}
			ps.tell(roomID, ss.keys(sig))
			sev := "sev9"
			for _, other := range ps.sigs {
				if other.phase == phaseFiring {
					sev = moreSevere(sev, other.last.sev)
				}
			}
			ps.roomID, ps.owned, ps.follower = roomID, false, true
			ps.lastPost, ps.postedSev = now, sev
			w.log.Info("watch: storm, pipeline joined the leader's room", "mode", string(w.cfg.Mode),
				"pipeline", m, "signal", sig, "leader", leader, "room", roomID)
		}
	}
}

// once runs log at most once per episode for a flag in the pipeline state.
func (w *Watcher) once(id string, flag func(*pipeState) *bool, log func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f := flag(w.pipes[id])
	if *f {
		return
	}
	*f = true
	log()
}

// post writes one system message to a room under its own deadline. It reports
// success. ev, when not nil, is the evidence JSON of the message: a sink that
// implements EvidenceSink gets it as data (the service feeds it to the agent as
// an alert_update), any other sink gets the text alone. incident marks a post
// that brings keys the room was not told about: a sink that implements
// IncidentSink gets it there, so the service wakes or restarts the agent (QA
// G1). When the sink says the room is closed, the pipeline is acked, as if
// OnRoomClosed had arrived (a missed close event must not leave the watcher
// posting into a closed room).
func (w *Watcher) post(ctx context.Context, roomID, text string, ev json.RawMessage, incident bool) bool {
	sctx, cancel := context.WithTimeout(ctx, w.cfg.CheckTimeout)
	defer cancel()
	var err error
	is, isIncident := w.rooms.(IncidentSink)
	es, isEvidence := w.rooms.(EvidenceSink)
	switch {
	case incident && isIncident:
		err = is.PostIncidentJSON(sctx, roomID, text, ev)
	case isEvidence:
		err = es.PostEvidenceJSON(sctx, roomID, text, ev)
	default:
		err = w.rooms.PostEvidence(sctx, roomID, text)
	}
	if err == nil {
		return true
	}
	if errors.Is(err, ErrRoomClosed) {
		w.log.Info("watch: room is closed, treating it as closed by the driver", "mode", string(w.cfg.Mode), "room", roomID)
		w.OnRoomClosed(roomID, "", "")
		return false
	}
	w.log.Warn("watch: could not post to the room, will retry", "mode", string(w.cfg.Mode), "room", roomID, "err", clean(err.Error(), 300))
	return false
}

// recordPost remembers that the pending signals are now in roomID, and that
// the room carries the keys of every active signal. It returns false when the
// room was closed while the post or the open was in flight: the pipeline then
// does not take the closed room as its own (that would make every later tick
// post into it and never open another), and the keys go into cooldown.
func (w *Watcher) recordPost(now time.Time, pipelineID string, v view, roomID string, owned, follower bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	ps := w.pipes[pipelineID]
	closed := w.closed[roomID]
	at := w.clock.Now()
	for _, s := range v.pending() {
		if ss := ps.sigs[s.id]; ss != nil {
			ss.notified = true
			if ss.escFree && !closed {
				ss.escalated, ss.escFree = false, false
			}
			if closed && ss.phase == phaseFiring {
				ss.ack()
				ss.ackedAt = at
			}
		}
	}
	if closed {
		ps.cool(v.keys(), roomID, at)
		ps.roomID, ps.owned, ps.follower = "", false, false
		w.log.Info("watch: room closed while it was being written to, signals acked",
			"mode", string(w.cfg.Mode), "pipeline", pipelineID, "room", roomID)
		return false
	}
	ps.tell(roomID, v.keys())
	ps.roomID, ps.owned, ps.follower = roomID, owned, follower
	ps.lastPost, ps.postedSev = now, v.sev
	ps.bypassCooldown = false
	ps.warnedOpen = false
	ps.note, ps.blocked = "", ""
	return true
}

// resolve tells the room that every signal is clear, then forgets the incident.
// A room the watcher opened gets the resolved alert (the service posts the note
// and leaves the room open); a room it only joined, or a storm room, gets a
// plain message. A transient error is retried on the next tick; a room that is
// gone or closed ends the incident.
func (w *Watcher) resolve(ctx context.Context, now time.Time, t target, v view) {
	id := t.p.ID
	if v.roomID != "" {
		if v.owned {
			raw, _ := json.Marshal(map[string]any{
				"source": "watcher", "pipeline": id, "resolved": true, "at": now.UTC().Format(time.RFC3339),
			})
			sev := v.postedSev
			if sev == "" {
				sev = "sev3"
			}
			a := alertin.Alert{
				Source: alertin.Watcher, Status: alertin.StatusResolved,
				Title: clipTitle(id + ": all watcher signals are clear"), Severity: sev,
				PipelineID: id, DedupKey: DedupKey(id), OccurredAt: now, Raw: raw,
			}
			sctx, cancel := context.WithTimeout(ctx, w.cfg.CheckTimeout)
			_, _, err := w.rooms.OpenAlertRoom(sctx, a)
			cancel()
			switch {
			case err == nil:
			case errors.Is(err, ErrNoRoom), errors.Is(err, ErrRoomClosed):
				w.log.Info("watch: no open room to resolve", "mode", string(w.cfg.Mode), "pipeline", id, "room", v.roomID)
			default:
				// Warn once per episode; the retry on the next tick is only a debug line.
				msg := clean(err.Error(), 300)
				w.mu.Lock()
				first := !w.pipes[id].warnedResolve
				w.pipes[id].warnedResolve = true
				w.mu.Unlock()
				if first {
					w.log.Warn("watch: resolved alert failed, will retry", "mode", string(w.cfg.Mode), "pipeline", id,
						"room", v.roomID, "err", msg)
				} else {
					w.log.Debug("watch: resolved alert failed, will retry", "pipeline", id, "room", v.roomID, "err", msg)
				}
				return
			}
		} else if !w.post(ctx, v.roomID, "watcher: "+id+": all watcher signals are clear", nil, false) {
			// post acks the pipeline itself when the room is closed; then the
			// incident is over too.
			w.mu.Lock()
			still := w.pipes[id].roomID == v.roomID
			w.mu.Unlock()
			if still {
				return // transient: try again next tick
			}
		}
		w.log.Info("watch: all signals clear", "mode", string(w.cfg.Mode), "pipeline", id, "room", v.roomID)
	}
	w.mu.Lock()
	ps := w.pipes[id]
	ps.episode, ps.roomID, ps.owned, ps.follower = false, "", false, false
	ps.postedSev, ps.lastPost = "", time.Time{}
	ps.bypassCooldown = false
	ps.note, ps.blocked = "", ""
	ps.warnedCooldown, ps.warnedMax, ps.warnedOpen, ps.warnedResolve = false, false, false, false
	ps.warnedStale, ps.warnedFlagged, ps.erroredMax = false, false, false
	w.mu.Unlock()
}

// ---- text and evidence ----

// compose builds the room title and the evidence JSON for the active signals.
// The evidence is small and holds no usernames and no query text: it is built
// only from slot rows, connector states and a one-line task trace.
func compose(pipelineID string, now time.Time, v view, reason string, affected []string) (string, json.RawMessage) {
	var frags []string
	var sigs []map[string]any
	for _, id := range signalIDs {
		for _, s := range v.active {
			if s.id != id {
				continue
			}
			frag := s.label
			if s.withDur && !s.since.IsZero() {
				frag += " " + fmtDur(now.Sub(s.since).Round(time.Second))
			}
			frags = append(frags, frag)
			ev := make(map[string]any, len(s.ev)+3)
			for k, val := range s.ev {
				ev[k] = val
			}
			if _, ok := ev["layer"]; !ok && SignalLayer(s.id) != "" {
				ev["layer"] = SignalLayer(s.id)
			}
			if !s.since.IsZero() {
				ev["since"] = s.since.UTC().Format(time.RFC3339)
				ev["firing_for_seconds"] = int64(now.Sub(s.since).Seconds())
			}
			sigs = append(sigs, ev)
		}
	}
	title := pipelineID + ": " + strings.Join(frags, "; ")
	// Whatever signal opened the room, the agent should see the connector
	// settings that drop records. A config_risk signal lists them itself.
	riskInEv := slices.ContainsFunc(v.active, func(s sigSnap) bool { return s.id == owners.SignalConfigRisk })
	if len(affected) > 1 {
		title += fmt.Sprintf(" (and %d other pipelines)", len(affected)-1)
	}
	if v.note != "" {
		title += " (" + v.note + ")"
	}
	body := map[string]any{
		"source": "watcher", "pipeline": pipelineID, "severity": v.sev,
		"at": now.UTC().Format(time.RFC3339), "reason": reason, "signals": sigs,
	}
	// The incident keys, so the room service can tell a restarted watcher
	// what a closed room carried (ClosedRoomKeysSink, QA S1).
	if keys := evidenceKeys(v.keys()); len(keys) > 0 {
		body["keys"] = keys
	}
	if v.note != "" {
		// An escalation (QA G1): the signal kept firing with no room.
		body["escalated"] = true
		body["note"] = v.note
	}
	if len(v.risks) > 0 && !riskInEv {
		body["config_risks"] = v.risks
	}
	if len(affected) > 1 {
		body["affected_count"] = len(affected)
		body["affected_pipelines"] = affected[:min(len(affected), maxAffected)]
	}
	raw, err := json.Marshal(body)
	if err != nil {
		raw = []byte(`{"source":"watcher"}`)
	}
	if len(raw) > maxEvidenceBytes {
		stripTraces(sigs)
		if raw2, err := json.Marshal(body); err == nil {
			raw = raw2
		}
	}
	return clipTitle(title), raw
}

// stripTraces removes the task trace lines from connector evidence. The task
// maps are shared with the stored reading (reading.ev is never changed), so it
// swaps in copies and leaves the originals alone. sigs holds per-call copies
// of the signal maps (see compose), so setting "connectors" on them is safe.
func stripTraces(sigs []map[string]any) {
	for _, s := range sigs {
		conns, ok := s["connectors"].([]map[string]any)
		if !ok {
			continue
		}
		out := make([]map[string]any, len(conns))
		for i, c := range conns {
			cc := copyMap(c)
			if tasks, ok := c["tasks"].([]map[string]any); ok {
				tt := make([]map[string]any, len(tasks))
				for j, t := range tasks {
					tc := copyMap(t)
					delete(tc, "trace")
					tt[j] = tc
				}
				cc["tasks"] = tt
			}
			out[i] = cc
		}
		s["connectors"] = out
	}
}

// copyMap is a shallow copy.
func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// clipTitle keeps a title within alertin.MaxTitleRunes.
func clipTitle(s string) string {
	s = clean(s, 1<<16)
	if utf8.RuneCountInString(s) <= alertin.MaxTitleRunes {
		return s
	}
	return string([]rune(s)[:alertin.MaxTitleRunes-1]) + "…"
}
