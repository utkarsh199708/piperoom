package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/alertin"
	"piperoom/internal/owners"
)

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
	roomID    string
	owned     bool
	active    []sigSnap // firing and not acked, action room
	hasAny    bool      // some room signal is firing or acked
	episode   bool
	sev       string // most severe of active
	postedSev string
	lastPost  time.Time
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
	v := view{roomID: ps.roomID, owned: ps.owned, episode: ps.episode, postedSev: ps.postedSev, lastPost: ps.lastPost, sev: "sev9"}
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
				since: ss.breachingSince, ev: ss.last.ev, pending: !ss.notified && !ss.suppressed,
			})
		case phaseAcked:
			v.hasAny = true
		}
	}
	return v
}

// stormGroups finds the signals that fired on at least stormThreshold pipelines
// in this tick and have no room yet (one Connect worker died: one room, not
// one per pipeline). The slice is in owners order, so [0] is the leader.
func (w *Watcher) stormGroups(fired map[string][]string) map[string][]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	storm := map[string][]string{}
	for sig, ids := range fired {
		var cand []string
		for _, id := range ids {
			if ps := w.pipes[id]; ps != nil && ps.roomID == "" {
				cand = append(cand, id)
			}
		}
		if len(cand) >= stormThreshold {
			storm[sig] = cand
		}
	}
	return storm
}

// emit writes to rooms for this tick: opens, joins, "still firing" and
// resolved. Sink calls are made without the lock (a room hook may call back
// into OnRoomClosed).
func (w *Watcher) emit(ctx context.Context, now time.Time, ts []target, fired map[string][]string) {
	storm := w.stormGroups(fired)
	suppress := map[string]bool{} // storm signals whose leader got a room
	for _, t := range ts {
		if ctx.Err() != nil {
			return
		}
		w.emitPipeline(ctx, now, t, storm, suppress)
	}
}

func (w *Watcher) emitPipeline(ctx context.Context, now time.Time, t target, storm map[string][]string, suppress map[string]bool) {
	id := t.p.ID
	w.mu.Lock()
	ps := w.pipes[id]
	if ps == nil {
		w.mu.Unlock()
		return
	}
	// Followers of a storm: the leader's room already names them.
	for sig, ids := range storm {
		if !suppress[sig] || ids[0] == id || !slices.Contains(ids, id) {
			continue
		}
		if ss := ps.sigs[sig]; ss != nil && ss.phase == phaseFiring && !ss.notified {
			ss.notified, ss.suppressed = true, true
			w.log.Info("watch: storm, signal left to the leader's room",
				"mode", string(w.cfg.Mode), "pipeline", id, "signal", sig, "leader", ids[0])
		}
	}
	v := w.viewOf(t, ps)
	w.mu.Unlock()

	switch {
	case !v.hasAny && v.episode:
		w.resolve(ctx, now, t, v)
	case len(v.active) == 0:
		// Nothing firing that is not acked: stay quiet.
	case v.roomID != "":
		w.updateRoom(ctx, now, t, v)
	case len(v.pending()) > 0:
		w.openRoom(ctx, now, t, v, storm, suppress)
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
	case now.Sub(v.lastPost) >= stillFiringEvery:
		reason = "still firing"
	default:
		return
	}
	title, ev := compose(t.p.ID, now, v, reason, nil)
	if !w.post(ctx, v.roomID, fmt.Sprintf("watcher: %s (%s)\nevidence: %s", title, reason, ev)) {
		return
	}
	w.recordPost(now, t.p.ID, v, v.roomID, v.owned)
	w.log.Info("watch: joined room", "mode", string(w.cfg.Mode), "pipeline", t.p.ID, "room", v.roomID, "reason", reason, "severity", v.sev)
}

// openRoom starts the incident for a pipeline: cooldown, then an already open
// room (join it), then the room cap, then a new room.
func (w *Watcher) openRoom(ctx context.Context, now time.Time, t target, v view, storm map[string][]string, suppress map[string]bool) {
	id := t.p.ID
	key := DedupKey(id)

	existing, closedAt, found := w.rooms.LastRoomForKey(id, key)
	if found && closedAt != nil && now.Sub(*closedAt) < w.cfg.Cooldown {
		w.once(id, func(ps *pipeState) *bool { return &ps.warnedCooldown }, func() {
			w.log.Info("watch: cooldown after a closed watcher room, staying quiet",
				"mode", string(w.cfg.Mode), "pipeline", id, "room", existing,
				"closed_ago", now.Sub(*closedAt).Round(time.Second).String(), "cooldown", w.cfg.Cooldown.String())
		})
		return
	}
	reuse := found && closedAt == nil // a watcher room from before a restart is still open

	// Storm: the leader carries every affected pipeline in its evidence.
	var affected []string
	var led []string
	for sig, ids := range storm {
		if ids[0] == id && slices.ContainsFunc(v.pending(), func(s sigSnap) bool { return s.id == sig }) {
			affected = append(affected, ids...)
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
			w.recordPost(now, id, v, other, false)
			w.markLeader(suppress, led)
			w.log.Info("watch: joined an open room", "mode", string(w.cfg.Mode), "pipeline", id, "room", other, "severity", v.sev)
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
		w.log.Warn("watch: could not open a room, will retry", "mode", string(w.cfg.Mode), "pipeline", id, "err", clean(err.Error(), 300))
		return
	}
	w.recordPost(now, id, v, roomID, true)
	w.markLeader(suppress, led)
	if len(affected) > 0 {
		w.log.Info("watch: storm, one room for several pipelines", "mode", string(w.cfg.Mode), "pipeline", id, "affected", strings.Join(affected, ","), "room", roomID)
	}
	w.log.Info("watch: room "+map[bool]string{true: "opened", false: "joined"}[created],
		"mode", string(w.cfg.Mode), "pipeline", id, "room", roomID, "severity", v.sev)
}

func (w *Watcher) markLeader(suppress map[string]bool, led []string) {
	for _, s := range led {
		suppress[s] = true
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

// post writes one system message to a room under its own deadline.
func (w *Watcher) post(ctx context.Context, roomID, text string) bool {
	sctx, cancel := context.WithTimeout(ctx, w.cfg.CheckTimeout)
	defer cancel()
	if err := w.rooms.PostEvidence(sctx, roomID, text); err != nil {
		w.log.Warn("watch: could not post to the room, will retry", "mode", string(w.cfg.Mode), "room", roomID, "err", clean(err.Error(), 300))
		return false
	}
	return true
}

// recordPost remembers that the pending signals are now in roomID.
func (w *Watcher) recordPost(now time.Time, pipelineID string, v view, roomID string, owned bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ps := w.pipes[pipelineID]
	ps.roomID, ps.owned = roomID, owned
	ps.lastPost, ps.postedSev = now, v.sev
	for _, s := range v.pending() {
		if ss := ps.sigs[s.id]; ss != nil {
			ss.notified = true
		}
	}
	if w.closedEarly[roomID] {
		delete(w.closedEarly, roomID)
		w.ackLocked(pipelineID, ps, roomID)
	}
}

// resolve tells the room that every signal is clear, then forgets the incident.
// A room the watcher opened gets the resolved alert (the service posts the note
// and leaves the room open); a room it only joined gets a plain message.
func (w *Watcher) resolve(ctx context.Context, now time.Time, t target, v view) {
	id := t.p.ID
	if v.roomID != "" {
		raw, _ := json.Marshal(map[string]any{
			"source": "watcher", "pipeline": id, "resolved": true, "at": now.UTC().Format(time.RFC3339),
		})
		if v.owned {
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
			if err != nil {
				// ErrResolvedAlertNoRoom: the room is gone. Nothing to resolve.
				w.log.Info("watch: resolved alert not delivered", "mode", string(w.cfg.Mode), "pipeline", id, "room", v.roomID, "err", clean(err.Error(), 300))
			}
		} else if !w.post(ctx, v.roomID, "watcher: "+id+": all watcher signals are clear") {
			return // try again next tick
		}
		w.log.Info("watch: all signals clear", "mode", string(w.cfg.Mode), "pipeline", id, "room", v.roomID)
	}
	w.mu.Lock()
	ps := w.pipes[id]
	ps.episode, ps.roomID, ps.owned = false, "", false
	ps.postedSev, ps.lastPost = "", time.Time{}
	ps.warnedCooldown, ps.warnedMax = false, false
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
		body["affected_pipelines"] = affected
	}
	raw, err := json.Marshal(body)
	if err != nil {
		raw = []byte(`{"source":"watcher"}`)
	}
	return clipTitle(title), raw
}

// clipTitle keeps a title within alertin.MaxTitleRunes.
func clipTitle(s string) string {
	s = clean(s, 1<<16)
	if utf8.RuneCountInString(s) <= alertin.MaxTitleRunes {
		return s
	}
	return string([]rune(s)[:alertin.MaxTitleRunes-1]) + "…"
}
