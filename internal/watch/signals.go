package watch

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
)

// maxTraceChars caps a task trace line in the evidence (design §5, I6).
const maxTraceChars = 300

// traceLine is what the watcher keeps of a task trace: the first line, with
// record values and credentials removed by the DLQ message rules (a Connect
// exception message quotes the failing record and connection URLs). The line
// is handed over longer than the cap, so RedactMessage makes the cut and
// knows a quote's closer may lie past it; its result is at most
// dlq.MaxMessageRunes (= maxTraceChars).
func traceLine(s string) string { return dlq.RedactMessage(firstLine(s, 4*maxTraceChars)) }

// slotNameRE is the owners rule for a slot name (the same check as watch.slot).
var slotNameRE = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// Evidence stays under 4 KiB: at most this many connectors and tasks are listed,
// and only the first few failed tasks carry a trace line.
const (
	maxEvidenceConnectors = 3
	maxEvidenceTasks      = 5
	maxEvidenceTraces     = 3
)

// evaluate turns the observations (one per client set) into signal readings and
// steps every state machine. Caller holds w.mu. It returns, per signal, the
// pipelines that fired in this tick (the storm cap needs that).
func (w *Watcher) evaluate(now time.Time, ts []target, bySet map[string]*observation) map[string][]string {
	fired := map[string][]string{}
	// DLQ rings are per set: a topic name is unique only within one cluster.
	perSet := map[string][]target{}
	for _, t := range ts {
		perSet[t.set] = append(perSet[t.set], t)
	}
	snapsBySet := make(map[string]map[string]dlqSnap, len(perSet))
	for set, sts := range perSet {
		if obs := bySet[set]; obs != nil {
			snapsBySet[set] = w.recordDLQ(now, sts, obs)
		}
	}
	for _, t := range ts {
		obs := bySet[t.set]
		if obs == nil {
			// observe reads every set it is given; this only guards a caller
			// that passes targets it did not observe.
			obs = &observation{set: t.set, slotsErr: errNotObserved, conns: map[string]*connObs{}}
		}
		snaps := snapsBySet[t.set]
		if snaps == nil {
			snaps = map[string]dlqSnap{}
		}
		ps := w.pipe(t.p.ID)
		var loops map[string]loopInfo
		if t.cfg.ConnectorDown.Enabled {
			loops = recordTasks(now, t, ps, obs)
		}
		cr := classifyConnectors(t.connectors, obs, loops, t.cfg.ConnectorCrashLoop.Window)
		if t.cfg.ConnectorDown.Enabled {
			w.apply(now, t, ps, owners.SignalConnectorDown, t.cfg.ConnectorDown, cr.reading(), fired)
		}
		if t.cfg.SlotInactive.Enabled || t.cfg.SlotWAL.Enabled || t.cfg.LongTx.Enabled {
			w.evaluateSlot(now, t, ps, cr, obs, fired)
		}
		if !t.cfg.LongTx.Enabled {
			ps.ltx = nil
		}
		w.evaluateDLQ(now, t, ps, obs, snaps, fired)
		w.evaluateLag(now, t, ps, obs, fired)
		w.evaluateConfigDrift(now, t, ps, obs, fired)
		w.logStill(now, t, ps)
	}
	return fired
}

func (w *Watcher) evaluateSlot(now time.Time, t target, ps *pipeState, cr connResult, obs *observation, fired map[string][]string) {
	row, err := slotRow(t, ps, obs)
	unknown := reading{v: vUnknown, err: err}
	if err != nil {
		if t.cfg.SlotInactive.Enabled {
			w.apply(now, t, ps, owners.SignalSlotInactive, t.cfg.SlotInactive, unknown, fired)
		}
		if t.cfg.SlotWAL.Enabled {
			w.apply(now, t, ps, owners.SignalSlotWAL, t.cfg.SlotWAL.WatchSignal, unknown, fired)
		}
		if t.cfg.LongTx.Enabled {
			w.apply(now, t, ps, owners.SignalLongTx, t.cfg.LongTx.WatchSignal, unknown, fired)
		}
		return
	}
	if t.cfg.SlotInactive.Enabled {
		w.apply(now, t, ps, owners.SignalSlotInactive, t.cfg.SlotInactive, slotInactiveReading(ps.slot, row), fired)
	}
	if t.cfg.SlotWAL.Enabled {
		ss := ps.sig(owners.SignalSlotWAL)
		r := w.slotWALReading(now, t, ps, ss, row, cr, heartbeatOf(t, obs))
		w.apply(now, t, ps, owners.SignalSlotWAL, t.cfg.SlotWAL.WatchSignal, r, fired)
	}
	if t.cfg.LongTx.Enabled {
		r := longTxReading(now, t, ps, ps.sig(owners.SignalLongTx), row)
		w.apply(now, t, ps, owners.SignalLongTx, t.cfg.LongTx.WatchSignal, r, fired)
	}
}

// apply steps one signal and logs the transition. Caller holds w.mu.
func (w *Watcher) apply(now time.Time, t target, ps *pipeState, id string, ws owners.WatchSignal, r reading, fired map[string][]string) {
	ss := ps.sig(id)
	if r.v == vUnknown {
		w.noteError(t.p.ID, id, ss, r.err)
		return
	}
	if ss.errText != "" {
		w.log.Info("watch: check works again", "mode", w.cfg.Mode, "pipeline", t.p.ID, "signal", id)
		ss.errText = ""
	}
	switch ss.step(now, r, ws.For) {
	case tFired:
		if ws.Action == owners.ActionRoom {
			ps.episode = true
			fired[id] = append(fired[id], t.p.ID)
		}
		ps.lastStill = now
		w.log.Info("watch: signal fired", w.fields(t.p.ID, id, ss, now, ws.Action)...)
	case tResolved:
		w.log.Info("watch: signal resolved", w.fields(t.p.ID, id, ss, now, ws.Action)...)
	}
}

// fields are the log fields of the shadow-mode metric (design §3, Logs).
func (w *Watcher) fields(pipeline, signal string, ss *sigState, now time.Time, action string) []any {
	f := []any{
		"mode", string(w.cfg.Mode),
		"pipeline", pipeline,
		"signal", signal,
		"value", ss.last.value,
		"threshold", ss.last.threshold,
		"severity", ss.last.sev,
		"action", action,
	}
	if !ss.breachingSince.IsZero() {
		f = append(f, "since", ss.breachingSince.UTC().Format(time.RFC3339))
	}
	return append(f, "room", w.pipes[pipeline].roomID)
}

// noteError records a failed check. The first failure is a warning; the same
// failure again is only a debug line, so a down database is not logged every tick.
func (w *Watcher) noteError(pipeline, signal string, ss *sigState, err error) {
	msg := "unknown"
	if err != nil {
		// A client error can carry a response body (a Connect 500 that echoes
		// a config value): mask credentials before it reaches a log line.
		msg, _ = redact.Text(clean(err.Error(), 300))
	}
	if ss.errText == msg {
		w.log.Debug("watch: check still failing", "pipeline", pipeline, "signal", signal, "err", msg)
		return
	}
	ss.errText = msg
	args := []any{"mode", string(w.cfg.Mode), "pipeline", pipeline, "signal", signal, "err", msg}
	// A standby has no pg_current_wal_lsn(); an old server has no safe_wal_size.
	// One clear line tells the operator what to fix (design §3, I8). It reads the
	// message text because the driver only gives a server error string.
	if strings.Contains(msg, "recovery is in progress") || strings.Contains(msg, "safe_wal_size") {
		args = append(args, "hint", "SOURCE_DB_URL must be the primary and PostgreSQL 13 or newer")
	}
	w.log.Warn("watch: check failed, signal state unchanged", args...)
}

// logStill writes the periodic "still firing" line (log mode and rooms mode).
func (w *Watcher) logStill(now time.Time, t target, ps *pipeState) {
	if now.Sub(ps.lastStill) < stillFiringEvery {
		return
	}
	for _, id := range ps.ids() {
		ss := ps.sigs[id]
		if ss == nil || ss.phase == phaseOK {
			continue
		}
		ps.lastStill = now
		w.log.Info("watch: signal still firing", w.fields(t.p.ID, id, ss, now, "")...)
	}
}

// ---- connector_down ----

// connResult is the verdict on a pipeline's connectors in one tick.
type connResult struct {
	anyBad     bool
	anyUnknown bool
	firstErr   error
	sev        string
	// parts are "pg-orders FAILED" style descriptions of the bad ones.
	parts []string
	ev    []map[string]any
}

// allRunning: every connector was read and is healthy.
func (c connResult) allRunning() bool { return !c.anyBad && !c.anyUnknown }

// classifyConnectors applies the devctl rule (health.go): healthy means the
// connector is RUNNING, has at least one task, and every task is RUNNING.
// FAILED and a missing connector are sev2; PAUSED/STOPPED alone are sev3 (often
// maintenance, but WAL still grows). A task in loops (crashloop.go) is bad at
// sev2 even while it reads RUNNING; window is the crash-loop window, for the label.
func classifyConnectors(names []string, obs *observation, loops map[string]loopInfo, window time.Duration) connResult {
	res := connResult{sev: "sev9"}
	for _, name := range names {
		co := obs.conns[name]
		switch {
		case co == nil:
			res.unknown(fmt.Errorf("watch: connector %s: not read", name))
		case errors.Is(co.statusErr, clients.ErrNotFound):
			res.bad("sev2", name+" missing", map[string]any{"name": name, "state": "MISSING"})
		case co.statusErr != nil:
			res.unknown(co.statusErr)
		default:
			sev, desc, ev := classifyOne(name, co.status, loops, window)
			if desc != "" {
				res.bad(sev, desc, ev)
			}
		}
	}
	return res
}

func (c *connResult) bad(sev, desc string, ev map[string]any) {
	c.anyBad = true
	c.sev = moreSevere(c.sev, sev)
	c.parts = append(c.parts, desc)
	c.ev = append(c.ev, ev)
}

func (c *connResult) unknown(err error) {
	c.anyUnknown = true
	if c.firstErr == nil {
		c.firstErr = err
	}
}

// classifyOne returns a description ("" = healthy) for one connector.
func classifyOne(name string, st clients.ConnectorStatus, loops map[string]loopInfo, window time.Duration) (sev, desc string, ev map[string]any) {
	var bad []string // non-RUNNING states seen
	var what []string
	if st.State != "RUNNING" {
		bad = append(bad, st.State)
		what = append(what, clean(st.State, 24))
	}
	tasks := make([]map[string]any, 0, min(len(st.Tasks), maxEvidenceTasks))
	traces, failedTasks := 0, 0
	for _, tk := range st.Tasks {
		li, looping := loops[taskKey(name, tk.ID)]
		if tk.State == "RUNNING" && !looping {
			continue // only the tasks that are not running go in the evidence
		}
		if looping && tk.State == "RUNNING" {
			bad = append(bad, "CRASH_LOOP")
		} else {
			bad = append(bad, tk.State)
		}
		failedTasks++
		if failedTasks <= 3 { // the title names a few tasks, not hundreds
			if looping {
				what = append(what, loopLabel(tk.ID, li, window))
			} else {
				what = append(what, fmt.Sprintf("task %d %s", tk.ID, clean(tk.State, 24)))
			}
		}
		if failedTasks > maxEvidenceTasks {
			continue
		}
		te := map[string]any{"id": tk.ID, "state": clean(tk.State, 24)}
		line := traceLine(tk.Trace)
		if looping {
			te["crash_loop"] = true
			te["restarts"] = li.restarts
			te["restart_window_seconds"] = int64(window.Seconds())
			if line == "" {
				line = li.trace // RUNNING now: the last FAILED trace
			}
		}
		if line != "" && traces < maxEvidenceTraces {
			te["trace"] = line
			traces++
		}
		tasks = append(tasks, te)
	}
	if len(st.Tasks) == 0 && st.State == "RUNNING" {
		bad = append(bad, "NO_TASKS")
		what = append(what, "has no tasks")
	}
	if len(bad) == 0 {
		return "", "", nil
	}
	sev = "sev3"
	for _, s := range bad {
		if s != "PAUSED" && s != "STOPPED" {
			sev = "sev2"
		}
	}
	ev = map[string]any{"name": name, "state": clean(st.State, 24), "tasks": tasks}
	if failedTasks > len(tasks) {
		ev["tasks_omitted"] = failedTasks - len(tasks)
	}
	return sev, name + " " + strings.Join(what, ", "), ev
}

func (c connResult) reading() reading {
	switch {
	case c.anyBad:
		shown := c.parts[:min(len(c.parts), maxEvidenceConnectors)]
		label := "connector " + strings.Join(shown, ", ")
		if len(c.parts) > 1 {
			label = "connectors " + strings.Join(shown, ", ")
		}
		if more := len(c.parts) - len(shown); more > 0 {
			label += fmt.Sprintf(" (+%d more)", more)
		}
		ev := map[string]any{
			"signal": owners.SignalConnectorDown, "severity": c.sev,
			"connectors": c.ev[:min(len(c.ev), maxEvidenceConnectors)],
		}
		if more := len(c.ev) - maxEvidenceConnectors; more > 0 {
			ev["connectors_omitted"] = more
		}
		return reading{
			v: vBreach, sev: c.sev, label: label, withDur: true,
			value: strings.Join(c.parts, ", "), threshold: "RUNNING", ev: ev,
		}
	case c.anyUnknown:
		return reading{v: vUnknown, err: c.firstErr}
	}
	return reading{v: vClear, value: "RUNNING", threshold: "RUNNING"}
}

// ---- slot lookup ----

// slotRow finds the pipeline's slot among the rows one query returned. The name
// is the watch.slot setting, else slot.name from the connector config (Debezium's
// default is "debezium"), else the name found in an earlier tick.
func slotRow(t target, ps *pipeState, obs *observation) (clients.ReplicationSlot, error) {
	if obs.slotsErr != nil {
		return clients.ReplicationSlot{}, obs.slotsErr
	}
	name := t.cfg.Slot
	if name == "" {
		var err error
		if name, err = slotFromConfigs(t, obs); err != nil {
			// A present but invalid name is never replaced by another slot.
			return clients.ReplicationSlot{}, err
		}
	}
	if name != "" {
		ps.slot = name
	} else {
		name = ps.slot
	}
	if name == "" {
		return clients.ReplicationSlot{}, fmt.Errorf("watch: pipeline %s: slot name unknown (no watch.slot and no slot.name in the connector config)", t.p.ID)
	}
	for _, r := range obs.slots {
		if r.Name == name {
			return r, nil
		}
	}
	return clients.ReplicationSlot{}, fmt.Errorf("watch: pipeline %s: slot %q not found in pg_replication_slots", t.p.ID, name)
}

// slotFromConfigs reads slot.name from the pipeline's connector configs. Only
// when the key is absent does a Debezium Postgres connector mean "debezium" (its
// default). A name that is present but not a valid slot name is an error: it
// must not turn into the default slot, which could belong to another pipeline.
func slotFromConfigs(t target, obs *observation) (string, error) {
	for _, c := range t.connectors {
		co := obs.conns[c]
		if co == nil || co.cfg == nil {
			continue
		}
		if n, present := co.cfg["slot.name"]; present {
			if !slotNameRE.MatchString(n) {
				return "", fmt.Errorf("watch: pipeline %s: connector %s has an invalid slot.name %q", t.p.ID, c, clean(n, 40))
			}
			return n, nil
		}
		if strings.Contains(co.cfg["connector.class"], "PostgresConnector") {
			return "debezium", nil
		}
	}
	return "", nil
}

// heartbeat says whether the pipeline's Debezium source has heartbeats. known is
// false when any config could not be read: the cap then does not apply, because
// guessing "idle database" would hide a real problem.
type heartbeat struct{ known, configured bool }

func heartbeatOf(t target, obs *observation) heartbeat {
	var hb heartbeat
	for _, c := range t.connectors {
		co := obs.conns[c]
		if co == nil || !co.cfgTried || co.cfgErr != nil {
			return heartbeat{}
		}
		if !isDebezium(co.cfg) {
			continue
		}
		hb.known = true
		if ms, err := strconv.Atoi(strings.TrimSpace(co.cfg["heartbeat.interval.ms"])); err == nil && ms > 0 {
			hb.configured = true
		}
	}
	return hb
}

func isDebezium(cfg map[string]string) bool {
	_, hasSlot := cfg["slot.name"]
	return hasSlot || strings.Contains(strings.ToLower(cfg["connector.class"]), "debezium")
}

// ---- slot_inactive ----

func slotInactiveReading(slot string, row clients.ReplicationSlot) reading {
	r := reading{
		sev:       "sev2",
		label:     "slot " + slot + " inactive",
		withDur:   true,
		value:     "active=" + strconv.FormatBool(row.Active),
		threshold: "active=true",
		ev: map[string]any{
			"signal": owners.SignalSlotInactive, "severity": "sev2", "slot": slot,
			"active": row.Active, "wal_status": clean(row.WALStatus, 24),
		},
	}
	if row.Active {
		r.v = vClear
		r.ev = nil
	} else {
		r.v = vBreach
	}
	return r
}

// ---- slot_wal ----

// slotWALReading records the sample, works out the rate and ETA, and decides
// the verdict and severity (design §3). Caller holds w.mu.
func (w *Watcher) slotWALReading(now time.Time, t target, ps *pipeState, ss *sigState, row clients.ReplicationSlot, cr connResult, hb heartbeat) reading {
	sw := t.cfg.SlotWAL
	ps.samples = trimSamples(append(ps.samples, sample{at: now, retained: row.RetainedWALBytes}), now)
	fast, fastOK := slope(ps.samples, now, fastWindow)
	slow, slowOK := slope(ps.samples, now, slowWindow)
	e := computeETA(slow, slowOK, row.RetainedWALBytes, row.SafeWALBytes, t.cfg.WALHeadroomBytes)

	// The estimate must stay under eta_sev1 for 2×for before it makes sev1, so a
	// short burst of growth does not page someone.
	if e.Known && e.D < sw.EtaSev1 {
		if ss.etaSince.IsZero() {
			ss.etaSince = now
		}
	} else {
		ss.etaSince = time.Time{}
	}

	// Slot active, connector running, no heartbeat: growth is most likely an idle
	// database or traffic on other tables (B3). Say so and cap the base sev2 at
	// sev3. The cap does not hide an ETA-based sev1: if the numbers say the slot
	// will be lost, it really will be.
	capped := row.Active && cr.allRunning() && hb.known && !hb.configured
	sev := "sev2"
	if capped {
		sev = "sev3"
	}
	etaHeld := !ss.etaSince.IsZero() && now.Sub(ss.etaSince) >= 2*sw.For
	if etaHeld {
		sev = "sev1"
	}
	// safe_wal_size <= 0 means the slot is already past max_slot_wal_keep_size
	// (it can be negative before the slot is lost): the same as "unreserved".
	pastLimit := row.SafeWALBytes != nil && *row.SafeWALBytes <= 0
	hardLost := row.WALStatus == "lost" || row.WALStatus == "unreserved" || pastLimit
	if hardLost {
		sev = "sev1" // lost means a re-snapshot; unreserved is one step before it
	}

	r := reading{
		sev:       sev,
		label:     slotWALLabel(ps.slot, row, fast, fastOK, slow, slowOK, e),
		value:     fmtBytes(row.RetainedWALBytes),
		threshold: fmt.Sprintf("fire %s clear %s", fmtBytes(sw.FireBytes), fmtBytes(sw.ClearBytes)),
	}
	switch {
	case hardLost:
		r.v = vBreachNow
		r.value += " wal_status=" + clean(row.WALStatus, 24)
	case row.RetainedWALBytes >= sw.FireBytes:
		r.v = vBreach
	case row.RetainedWALBytes < sw.ClearBytes:
		r.v = vClear
	default:
		r.v = vHold
	}

	ev := map[string]any{
		"signal": owners.SignalSlotWAL, "severity": sev, "slot": ps.slot,
		"active": row.Active, "wal_status": clean(row.WALStatus, 24),
		"retained_bytes": row.RetainedWALBytes,
		"fire_bytes":     sw.FireBytes, "clear_bytes": sw.ClearBytes,
	}
	if row.SafeWALBytes != nil {
		ev["safe_wal_bytes"] = *row.SafeWALBytes
	}
	if fastOK {
		ev["rate_1h_bytes_per_hour"] = int64(fast * 3600)
	}
	if slowOK {
		ev["rate_6h_bytes_per_hour"] = int64(slow * 3600)
	}
	if e.Known {
		ev["eta_hours"] = round1(e.D.Hours())
		ev["eta_basis"] = e.Basis
		if e.Basis == basisHeadroom {
			ev["eta_note"] = "rests on the configured wal_headroom_bytes, not on measured free space"
		}
	} else {
		ev["eta_note"] = e.Reason
	}
	if capped {
		ev["note"] = "no heartbeat configured: idle-database WAL growth is likely (base severity capped at sev3)"
	}
	r.ev = ev
	return r
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

// slotWALLabel is the title fragment: "slot debezium_orders 1.3 GB, +410 MB/h".
func slotWALLabel(slot string, row clients.ReplicationSlot, fast float64, fastOK bool, slow float64, slowOK bool, e eta) string {
	s := fmt.Sprintf("slot %s %s", slot, fmtBytes(row.RetainedWALBytes))
	switch {
	case fastOK && fast > 0:
		s += ", +" + fmtBytes(int64(fast*3600)) + "/h"
	case slowOK && slow > 0:
		s += ", +" + fmtBytes(int64(slow*3600)) + "/h"
	}
	if e.Known {
		what := "to slot loss"
		if e.Basis == basisHeadroom {
			what = "to disk full (configured)"
		}
		s += ", ~" + fmtDur(e.D.Round(time.Minute)) + " " + what
	}
	if row.WALStatus == "lost" || row.WALStatus == "unreserved" {
		s += ", wal_status " + clean(row.WALStatus, 24)
	} else if row.SafeWALBytes != nil && *row.SafeWALBytes <= 0 {
		s += ", past max_slot_wal_keep_size"
	}
	return s
}
