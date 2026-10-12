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
	pending bool // not yet in a room
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
	bypass     bool // skip the cooldown (a rise to sev1 re-opened the incident)
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

// signalCfg returns the shared settings of a v1 signal.
func signalCfg(cfg owners.WatchConfig, id string) owners.WatchSignal {
	switch id {
	case owners.SignalConnectorDown:
		return cfg.ConnectorDown
	case owners.SignalSlotInactive:
		return cfg.SlotInactive
	}
	return cfg.SlotWAL.WatchSignal
}

// viewOf copies the state of a pipeline. Caller holds w.mu.
func (w *Watcher) viewOf(t target, ps *pipeState) view {
	v := view{roomID: ps.roomID, owned: ps.owned, follower: ps.follower, episode: ps.episode,
		postedSev: ps.postedSev, lastPost: ps.lastPost, bypass: ps.bypassCooldown, sev: "sev9"}
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
				since: ss.breachingSince, ev: ss.last.ev, pending: !ss.notified,
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

// reviveSev1 re-opens an acked incident once per episode when its severity has
// risen to sev1 after the driver closed the room. The slot is really about to
// be lost, so this skips the cooldown. Caller holds w.mu.
func (w *Watcher) reviveSev1(id string, ps *pipeState, t target) {
	if ps.reopened {
		return
	}
	for _, sid := range signalIDs {
		sc := signalCfg(t.cfg, sid)
		ss := ps.sigs[sid]
		if ss == nil || !sc.Enabled || sc.Action != owners.ActionRoom || ss.phase != phaseAcked {
			continue
		}
		if ss.last.sev == "sev1" && ss.ackSev != "sev1" && ss.clearingSince.IsZero() {
			ss.phase, ss.notified = phaseFiring, false
			ps.reopened, ps.bypassCooldown = true, true
			w.log.Warn("watch: severity rose to sev1 after the room was closed, re-opening",
				"mode", string(w.cfg.Mode), "pipeline", id, "signal", sid, "was", ss.ackSev)
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
			if !bypass[id] && w.inCooldown(now, id) {
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

// closedAtFor is when the pipeline's last watcher room closed: the later of the
// room service's closedAt for its own key and the close the watcher saw for any
// room the pipeline posted into (a storm follower has no room under its key).
// It is nil when nothing closed, and also while the key's room is still open:
// that room is joined, not waited out.
func (w *Watcher) closedAtFor(id string, closedAt *time.Time, found bool) *time.Time {
	if found && closedAt == nil {
		return nil
	}
	w.mu.Lock()
	var seen time.Time
	if ps := w.pipes[id]; ps != nil {
		seen = ps.lastClosed
	}
	w.mu.Unlock()
	switch {
	case closedAt != nil && closedAt.After(seen):
		return closedAt
	case !seen.IsZero():
		return &seen
	}
	return nil
}

// inCooldown: a watcher room for the pipeline closed less than `cooldown` ago.
func (w *Watcher) inCooldown(now time.Time, id string) bool {
	_, closedAt, found := w.rooms.LastRoomForKey(id, DedupKey(id))
	at := w.closedAtFor(id, closedAt, found)
	return at != nil && now.Sub(*at) < w.cfg.Cooldown
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
	w.reviveSev1(id, ps, t)
	v := w.viewOf(t, ps)
	w.mu.Unlock()

	// A follower of a storm waits until its leader has a room: then the leader
	// has put it into that room already. If the leader failed, nobody opens a
	// room of their own; the storm is carried to the next tick.
	if v.roomID == "" {
		for sig, si := range storm {
			if si.leader != id && slices.Contains(si.members, id) && v.hasPending(sig) {
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

// updateRoom joins the room the pipeline already has: a new signal, a higher
// severity, or the 15-minute "still firing".
func (w *Watcher) updateRoom(ctx context.Context, now time.Time, t target, v view) {
	reason := ""
	switch {
	case len(v.pending()) > 0:
		reason = "new signal"
	case sevRank(v.sev) < sevRank(v.postedSev):
		reason = "severity raised to " + v.sev
	case (!v.follower || v.leaderGone) && now.Sub(v.lastPost) >= stillFiringEvery:
		reason = "still firing"
	default:
		return
	}
	title, ev := compose(t.p.ID, now, v, reason, nil)
	if !w.post(ctx, v.roomID, fmt.Sprintf("watcher: %s (%s)\nevidence: %s", title, reason, ev)) {
		return
	}
	if w.recordPost(now, t.p.ID, v, v.roomID, v.owned, v.follower) {
		w.log.Info("watch: joined room", "mode", string(w.cfg.Mode), "pipeline", t.p.ID, "room", v.roomID, "reason", reason, "severity", v.sev)
	}
}

// openRoom starts the incident for a pipeline: cooldown, then an already open
// room (join it), then the room cap, then a new room.
func (w *Watcher) openRoom(ctx context.Context, now time.Time, t target, v view, storm map[string]*stormInfo) {
	id := t.p.ID
	key := DedupKey(id)

	existing, closedAt, found := w.rooms.LastRoomForKey(id, key)
	if last := w.closedAtFor(id, closedAt, found); last != nil && !v.bypass && now.Sub(*last) < w.cfg.Cooldown {
		w.once(id, func(ps *pipeState) *bool { return &ps.warnedCooldown }, func() {
			w.log.Info("watch: cooldown after a closed watcher room, staying quiet",
				"mode", string(w.cfg.Mode), "pipeline", id, "room", existing,
				"closed_ago", now.Sub(*last).Round(time.Second).String(), "cooldown", w.cfg.Cooldown.String())
		})
		return
	}
	reuse := found && closedAt == nil // a watcher room from before a restart is still open

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
		// Any open room for the pipeline, an Alertmanager one included, takes
		// the evidence instead of a second room (B1).
		if other, ok := w.rooms.OpenRoomForPipeline(id); ok {
			title, ev := compose(id, now, v, "joined an open room", affected)
			if !w.post(ctx, other, fmt.Sprintf("watcher: %s\nevidence: %s", title, ev)) {
				return
			}
			if w.recordPost(now, id, v, other, false, false) {
				w.assignFollowers(now, id, other, storm, led)
				w.log.Info("watch: joined an open room", "mode", string(w.cfg.Mode), "pipeline", id, "room", other, "severity", v.sev)
			}
			return
		}
		w.mu.Lock()
		full := w.openOwned() >= w.maxRooms
		w.mu.Unlock()
		if full {
			w.once(id, func(ps *pipeState) *bool { return &ps.warnedMax }, func() {
				w.log.Warn("watch: max_open_watcher_rooms reached, not opening a room",
					"mode", string(w.cfg.Mode), "pipeline", id, "max", w.maxRooms)
			})
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
		w.mu.Unlock()
		if first {
			w.log.Warn("watch: could not open a room, will retry", "mode", string(w.cfg.Mode), "pipeline", id, "err", msg)
		} else {
			w.log.Debug("watch: could not open a room, will retry", "pipeline", id, "err", msg)
		}
		return
	}
	if !w.recordPost(now, id, v, roomID, true, false) {
		return
	}
	if !created {
		// The service only writes "fired again" with the title on a join; the
		// numbers go in as evidence (a room that survived a restart).
		w.post(ctx, roomID, fmt.Sprintf("watcher: %s (re-attached after a restart)\nevidence: %s", title, ev))
	}
	w.assignFollowers(now, id, roomID, storm, led)
	if len(affected) > 1 {
		w.log.Info("watch: storm, one room for several pipelines", "mode", string(w.cfg.Mode), "pipeline", id, "affected", strings.Join(affected, ","), "room", roomID)
	}
	w.log.Info("watch: room "+map[bool]string{true: "opened", false: "joined"}[created],
		"mode", string(w.cfg.Mode), "pipeline", id, "room", roomID, "severity", v.sev)
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
				ss.phase, ss.ackSev = phaseAcked, ss.last.sev
				ps.lastClosed = w.clock.Now()
				continue
			}
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
// success. When the sink says the room is closed, the pipeline is acked, as if
// OnRoomClosed had arrived (a missed close event must not leave the watcher
// posting into a closed room).
func (w *Watcher) post(ctx context.Context, roomID, text string) bool {
	sctx, cancel := context.WithTimeout(ctx, w.cfg.CheckTimeout)
	defer cancel()
	err := w.rooms.PostEvidence(sctx, roomID, text)
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

// recordPost remembers that the pending signals are now in roomID. It returns
// false when the room was closed while the post or the open was in flight: the
// pipeline then does not take the closed room as its own (that would make every
// later tick post into it and never open another).
func (w *Watcher) recordPost(now time.Time, pipelineID string, v view, roomID string, owned, follower bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	ps := w.pipes[pipelineID]
	for _, s := range v.pending() {
		if ss := ps.sigs[s.id]; ss != nil {
			ss.notified = true
			if w.closed[roomID] && ss.phase == phaseFiring {
				ss.phase, ss.ackSev = phaseAcked, ss.last.sev
			}
		}
	}
	if w.closed[roomID] {
		ps.roomID, ps.owned, ps.follower = "", false, false
		ps.lastClosed = w.clock.Now()
		w.log.Info("watch: room closed while it was being written to, signals acked",
			"mode", string(w.cfg.Mode), "pipeline", pipelineID, "room", roomID)
		return false
	}
	ps.roomID, ps.owned, ps.follower = roomID, owned, follower
	ps.lastPost, ps.postedSev = now, v.sev
	ps.bypassCooldown = false
	ps.warnedOpen = false
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
		} else if !w.post(ctx, v.roomID, "watcher: "+id+": all watcher signals are clear") {
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
	ps.reopened, ps.bypassCooldown = false, false
	ps.warnedCooldown, ps.warnedMax, ps.warnedOpen, ps.warnedResolve = false, false, false, false
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
			ev := make(map[string]any, len(s.ev)+2)
			for k, val := range s.ev {
				ev[k] = val
			}
			if !s.since.IsZero() {
				ev["since"] = s.since.UTC().Format(time.RFC3339)
				ev["firing_for_seconds"] = int64(now.Sub(s.since).Seconds())
			}
			sigs = append(sigs, ev)
		}
	}
	title := pipelineID + ": " + strings.Join(frags, "; ")
	if len(affected) > 1 {
		title += fmt.Sprintf(" (and %d other pipelines)", len(affected)-1)
	}
	body := map[string]any{
		"source": "watcher", "pipeline": pipelineID, "severity": v.sev,
		"at": now.UTC().Format(time.RFC3339), "reason": reason, "signals": sigs,
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
