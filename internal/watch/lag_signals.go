package watch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// This file holds the consumer-group signals of tasks/F6-proactive-triage.md
// §Design 1: consumer_lag, group_unstable and offset_below_log_start. Per tick
// and client set the watcher reads GroupLag once per group of the set's
// pipelines and, when a committed offset could be below the log start, one
// DLQOffsets call (start/end offsets only, never a record) for the CDC topics
// the groups read. The evidence holds group, topic and partition names,
// offsets, counts and fixed state names, never a record.

// LagReader is the part of clients.KafkaReader the consumer-group signals use.
// Wire it from the same Kafka client as Deps.Kafka. Nil: consumer_lag,
// group_unstable and offset_below_log_start stay unknown, with one log line.
type LagReader interface {
	GroupLag(ctx context.Context, group string) (clients.GroupLag, error)
}

const (
	// lagRateWindow is how far back the lag slope looks.
	lagRateWindow = 30 * time.Minute
	// lagMinSpan: the slope needs samples over at least this long. Shorter
	// than the WAL minSpan: lag moves faster than retained WAL, and the
	// window itself is only 30 minutes.
	lagMinSpan = 5 * time.Minute
	// maxGroupStates is the ring of group states kept for group_unstable
	// (owners caps flaps at one less).
	maxGroupStates = 12
	// maxEvidenceStates, maxEvidencePartitions: evidence list caps.
	maxEvidenceStates     = 6
	maxEvidencePartitions = 3
	// flapReportWindow is the fixed window of the state_changes_15m evidence key.
	flapReportWindow = 15 * time.Minute
)

var errNoLagReader = errors.New("watch: no consumer-group lag reader configured")

// lagObs is one group's read in a tick.
type lagObs struct {
	lag clients.GroupLag
	err error
}

// groupState is one entry of the group_unstable ring.
type groupState struct {
	at      time.Time
	state   string
	members int
}

// groupMem is what the watcher remembers about one consumer group of a pipeline.
type groupMem struct {
	samples      []sample // retained = total lag
	risingSince  time.Time
	states       []groupState
	lastReadTick time.Time
}

// lagSignalsOn reports whether a target uses any consumer-group signal.
func lagSignalsOn(t target) bool {
	return len(t.groups) > 0 &&
		(t.cfg.ConsumerLag.Enabled || t.cfg.GroupUnstable.Enabled || t.cfg.OffsetBelowLogStart.Enabled)
}

// observeLag reads every group of the set's targets once, in parallel, then
// (for offset_below_log_start) the log-start offsets of the CDC topics those
// groups have committed offsets on, in one call. Each call has its own deadline.
func (w *Watcher) observeLag(ctx context.Context, ts []target, obs *observation, lr LagReader, kafka clients.DLQReader) {
	var groups []string
	offsetTopics := map[string]bool{}
	for _, t := range ts {
		if !lagSignalsOn(t) {
			continue
		}
		groups = append(groups, t.groups...)
		if t.cfg.OffsetBelowLogStart.Enabled {
			for _, tp := range t.cdcTopics {
				offsetTopics[tp] = true
			}
		}
	}
	if len(groups) == 0 {
		return
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	if lr == nil {
		obs.lagNoReader = true
		return
	}
	obs.lags = make(map[string]*lagObs, len(groups))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gl, err := call(ctx, w.cfg.CheckTimeout, "consumer group lag", func(c context.Context) (clients.GroupLag, error) {
				return lr.GroupLag(c, g)
			})
			mu.Lock()
			obs.lags[g] = &lagObs{lag: gl, err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Only the topics a group has a committed offset on are worth a call.
	var want []string
	for _, g := range groups {
		lo := obs.lags[g]
		if lo.err != nil {
			continue
		}
		for _, p := range lo.lag.Partitions {
			if p.Committed >= 0 && offsetTopics[p.Topic] && !slices.Contains(want, p.Topic) {
				want = append(want, p.Topic)
			}
		}
	}
	if len(want) == 0 {
		return
	}
	if kafka == nil {
		obs.logStartErr = errNoDLQReader
		return
	}
	slices.Sort(want)
	obs.logStartTried = true
	obs.logStart, obs.logStartErr = call(ctx, w.cfg.CheckTimeout, "read log start offsets", func(c context.Context) (map[string][]clients.PartitionOffsets, error) {
		return kafka.DLQOffsets(c, want...)
	})
}

// evaluateLag steps the three consumer-group signals of one pipeline. Caller
// holds w.mu.
func (w *Watcher) evaluateLag(now time.Time, t target, ps *pipeState, obs *observation, fired map[string][]string) {
	if !lagSignalsOn(t) {
		return
	}
	cfg := t.cfg
	if obs.lagNoReader {
		if !w.lagNoReaderLogged {
			w.lagNoReaderLogged = true
			w.log.Warn("watch: no consumer-group lag reader, consumer_lag, group_unstable and offset_below_log_start stay unknown",
				"mode", string(w.cfg.Mode))
		}
		return
	}
	lags, err := groupReads(t, obs)
	if err == nil {
		w.recordGroups(now, t, ps, lags)
	}
	if cfg.ConsumerLag.Enabled {
		r := reading{v: vUnknown, err: err}
		if err == nil {
			r = w.consumerLagReading(now, cfg.ConsumerLag, ps, lags)
		}
		w.apply(now, t, ps, owners.SignalConsumerLag, cfg.ConsumerLag.WatchSignal, r, fired)
	}
	if cfg.GroupUnstable.Enabled {
		r := reading{v: vUnknown, err: err}
		if err == nil {
			lagFiring := ps.sigs[owners.SignalConsumerLag] != nil && ps.sigs[owners.SignalConsumerLag].phase == phaseFiring
			r = groupUnstableReading(now, cfg.GroupUnstable, ps, lags, lagFiring)
		}
		w.apply(now, t, ps, owners.SignalGroupUnstable, cfg.GroupUnstable.WatchSignal, r, fired)
	}
	if cfg.OffsetBelowLogStart.Enabled {
		r := reading{v: vUnknown, err: err}
		if err == nil {
			r = offsetBelowReading(t, lags, obs)
		}
		// It fires at once (vBreachNow); its clear waits connector_down.for.
		ws := cfg.OffsetBelowLogStart
		ws.For = cfg.ConnectorDown.For
		w.apply(now, t, ps, owners.SignalOffsetBelowLogStart, ws, r, fired)
	}
}

// groupReads returns the pipeline's group reads in owners order. Any failed
// read makes the whole verdict unknown: a group we cannot see may be the one
// in trouble, and guessing "clear" would resolve a room.
func groupReads(t target, obs *observation) ([]clients.GroupLag, error) {
	out := make([]clients.GroupLag, 0, len(t.groups))
	for _, g := range t.groups {
		lo := obs.lags[g]
		switch {
		case lo == nil:
			return nil, fmt.Errorf("watch: consumer group %s: not read", g)
		case lo.err != nil:
			return nil, lo.err
		}
		gl := lo.lag
		gl.Group = g // the name we asked for, never one echoed back
		out = append(out, gl)
	}
	return out, nil
}

// recordGroups appends this tick's lag sample and group state to each group's
// memory. Caller holds w.mu.
func (w *Watcher) recordGroups(now time.Time, t target, ps *pipeState, lags []clients.GroupLag) {
	if ps.groups == nil {
		ps.groups = map[string]*groupMem{}
	}
	keep := max(lagRateWindow, t.cfg.GroupUnstable.Window)
	for _, gl := range lags {
		gm := ps.groups[gl.Group]
		if gm == nil {
			gm = &groupMem{}
			ps.groups[gl.Group] = gm
		}
		if gm.lastReadTick.Equal(now) {
			continue // one sample per tick, even if a test evaluates twice
		}
		gm.lastReadTick = now
		gm.samples = trimBefore(append(gm.samples, sample{at: now, retained: gl.TotalLag}), now.Add(-lagRateWindow))
		gm.states = append(gm.states, groupState{at: now, state: clean(gl.State, 32), members: gl.Members})
		if len(gm.states) > maxGroupStates {
			gm.states = gm.states[len(gm.states)-maxGroupStates:]
		}
		// Entries older than the window cannot count as flaps; keep the newest
		// one anyway, so the next change is still seen as a change.
		for len(gm.states) > 1 && gm.states[0].at.Before(now.Add(-keep)) && gm.states[1].at.Before(now.Add(-keep)) {
			gm.states = gm.states[1:]
		}
	}
	// Forget groups the pipeline no longer lists.
	for g := range ps.groups {
		if !slices.Contains(t.groups, g) {
			delete(ps.groups, g)
		}
	}
}

// trimBefore drops samples older than cutoff.
func trimBefore(s []sample, cutoff time.Time) []sample {
	i := 0
	for i < len(s) && s[i].at.Before(cutoff) {
		i++
	}
	if i == 0 {
		return s
	}
	return append(s[:0:0], s[i:]...)
}

// ---- consumer_lag ----

// consumerLagReading judges the worst group (by total lag). Severity sev3;
// sev2 when the group has no members while members are expected (nobody
// consumes), or when the lag has kept growing for 2×for. Caller holds w.mu.
func (w *Watcher) consumerLagReading(now time.Time, sc owners.ConsumerLagSignal, ps *pipeState, lags []clients.GroupLag) reading {
	worst := 0
	allBelowClear := true
	for i, gl := range lags {
		if gl.TotalLag > lags[worst].TotalLag {
			worst = i
		}
		if gl.TotalLag >= sc.ClearLag {
			allBelowClear = false
		}
		// The growth timer of every group runs every tick, not only the worst's.
		gm := ps.groups[gl.Group]
		if rate, ok := slopeMin(gm.samples, now, lagRateWindow, lagMinSpan); ok && rate > 0 {
			if gm.risingSince.IsZero() {
				gm.risingSince = now
			}
		} else {
			gm.risingSince = time.Time{}
		}
	}
	gl := lags[worst]
	gm := ps.groups[gl.Group]
	rate, rateOK := slopeMin(gm.samples, now, lagRateWindow, lagMinSpan)
	perMin := int64(rate * 60)

	empty := gl.Members == 0 && (gl.State == "Empty" || gl.State == "Dead")
	sev := "sev3"
	if (sc.MembersExpected && empty) || (!gm.risingSince.IsZero() && now.Sub(gm.risingSince) >= 2*sc.For) {
		sev = "sev2"
	}

	var extra []string
	if rateOK {
		extra = append(extra, fmt.Sprintf("%+d/min", perMin))
	}
	extra = append(extra, fmt.Sprintf("%d members", gl.Members))
	if gl.Members == 1 {
		extra[len(extra)-1] = "1 member"
	}
	label := fmt.Sprintf("group %s lag %d (%s)", gl.Group, gl.TotalLag, strings.Join(extra, ", "))

	r := reading{
		sev: sev, label: label,
		value:     strconv.FormatInt(gl.TotalLag, 10),
		threshold: fmt.Sprintf("fire %d clear %d", sc.FireLag, sc.ClearLag),
	}
	switch {
	case gl.TotalLag >= sc.FireLag:
		r.v = vBreach
		for _, o := range lags {
			if o.TotalLag >= sc.FireLag {
				r.targets = append(r.targets, o.Group)
			}
		}
	case allBelowClear:
		r.v = vClear
		return r
	default:
		r.v = vHold
	}
	ev := map[string]any{
		"signal": owners.SignalConsumerLag, "severity": sev, "group": gl.Group,
		"state": clean(gl.State, 32), "members": gl.Members, "total_lag": gl.TotalLag,
		"fire": sc.FireLag, "clear": sc.ClearLag, "worst_partitions": worstPartitions(gl.Partitions),
	}
	if rateOK {
		ev["lag_rate_per_min"] = perMin
	}
	var others []string
	for i, o := range lags {
		if i != worst && o.TotalLag >= sc.FireLag {
			others = append(others, o.Group)
		}
	}
	if len(others) > 0 {
		ev["other_groups_over_fire"] = others[:min(len(others), maxEvidencePartitions)]
	}
	r.ev = ev
	return r
}

// worstPartitions lists the partitions with the most lag, at most 3; never nil.
func worstPartitions(ps []clients.PartitionLag) []map[string]any {
	sorted := slices.Clone(ps)
	slices.SortStableFunc(sorted, func(a, b clients.PartitionLag) int {
		switch {
		case a.Lag > b.Lag:
			return -1
		case a.Lag < b.Lag:
			return 1
		}
		return 0
	})
	out := []map[string]any{}
	for _, p := range sorted {
		if len(out) == maxEvidencePartitions || p.Lag <= 0 {
			break
		}
		out = append(out, map[string]any{"topic": p.Topic, "partition": p.Partition, "lag": p.Lag})
	}
	return out
}

// ---- group_unstable ----

// unstable: the group is rebalancing (or in a state we do not know), or dead
// while it still has members. Stable, Empty and a memberless Dead are calm;
// a memberless group is consumer_lag's business.
func unstable(state string, members int) bool {
	switch state {
	case "Stable", "Empty":
		return false
	case "Dead":
		return members > 0
	}
	return true
}

// stateChanges counts the state changes in the ring whose newer side is at or
// after since.
func stateChanges(states []groupState, since time.Time) int {
	n := 0
	for i := 1; i < len(states); i++ {
		if states[i].state != states[i-1].state && !states[i].at.Before(since) {
			n++
		}
	}
	return n
}

// groupUnstableReading: breach while any group is unstable (for `for`), or at
// once when one changed state `flaps` times inside `window`. Severity sev3;
// sev2 while consumer_lag fires too. Caller holds w.mu.
func groupUnstableReading(now time.Time, sc owners.GroupUnstableSignal, ps *pipeState, lags []clients.GroupLag, lagFiring bool) reading {
	sev := "sev3"
	if lagFiring {
		sev = "sev2"
	}
	type cand struct {
		gl      clients.GroupLag
		flaps   int
		unstab  bool
		flapped bool
	}
	var best *cand
	for _, gl := range lags {
		gm := ps.groups[gl.Group]
		c := cand{gl: gl, unstab: unstable(gl.State, gl.Members)}
		c.flaps = stateChanges(gm.states, now.Add(-sc.Window))
		c.flapped = c.flaps >= sc.Flaps
		if !c.unstab && !c.flapped {
			continue
		}
		// A flapping group outranks one that is only unstable: it fires now.
		if best == nil || (c.flapped && !best.flapped) || (c.flapped == best.flapped && c.flaps > best.flaps) {
			cc := c
			best = &cc
		}
	}
	if best == nil {
		return reading{v: vClear, value: "Stable", threshold: "Stable|Empty"}
	}
	gl := best.gl
	gm := ps.groups[gl.Group]
	states := gm.states[max(0, len(gm.states)-maxEvidenceStates):]
	list := make([]map[string]any, 0, len(states))
	for _, s := range states {
		list = append(list, map[string]any{"at": s.at.UTC().Format(time.RFC3339), "state": s.state, "members": s.members})
	}
	r := reading{
		v: vBreach, sev: sev, withDur: !best.flapped, targets: []string{gl.Group},
		value:     fmt.Sprintf("%s, %d changes", clean(gl.State, 32), best.flaps),
		threshold: fmt.Sprintf("Stable|Empty, < %d changes in %s", sc.Flaps, fmtDur(sc.Window)),
		ev: map[string]any{
			"signal": owners.SignalGroupUnstable, "severity": sev, "group": gl.Group,
			"state": clean(gl.State, 32), "members": gl.Members,
			"state_changes_15m": stateChanges(gm.states, now.Add(-flapReportWindow)),
			"states":            list,
		},
	}
	if best.flapped {
		r.v = vBreachNow
		r.label = fmt.Sprintf("group %s flapping (%d state changes in %s)", gl.Group, best.flaps, fmtDur(sc.Window))
	} else {
		r.label = fmt.Sprintf("group %s %s", gl.Group, clean(gl.State, 32))
	}
	return r
}

// ---- offset_below_log_start ----

// offsetBelowReading: a committed offset below the partition's log start
// means retention deleted records the group never read. sev1 at once.
func offsetBelowReading(t target, lags []clients.GroupLag, obs *observation) reading {
	cdc := map[string]bool{}
	for _, tp := range t.cdcTopics {
		cdc[tp] = true
	}
	type hit struct {
		group, topic        string
		partition           int32
		committed, logStart int64
	}
	var hits []hit
	for _, gl := range lags {
		for _, p := range gl.Partitions {
			if !cdc[p.Topic] || p.Committed < 0 {
				continue
			}
			if obs.logStartErr != nil {
				return reading{v: vUnknown, err: obs.logStartErr}
			}
			if !obs.logStartTried {
				return reading{v: vUnknown, err: fmt.Errorf("watch: log start offsets of %s were not read", p.Topic)}
			}
			for _, po := range obs.logStart[p.Topic] { // a missing topic has no rows
				if po.Partition == p.Partition && p.Committed < po.Start {
					hits = append(hits, hit{gl.Group, p.Topic, p.Partition, p.Committed, po.Start})
				}
			}
		}
	}
	if len(hits) == 0 {
		return reading{v: vClear, value: "none below", threshold: "committed >= log start"}
	}
	slices.SortStableFunc(hits, func(a, b hit) int {
		la, lb := a.logStart-a.committed, b.logStart-b.committed
		switch {
		case la > lb:
			return -1
		case la < lb:
			return 1
		}
		return 0
	})
	h := hits[0]
	parts := make([]map[string]any, 0, min(len(hits), maxEvidencePartitions))
	for _, x := range hits[:min(len(hits), maxEvidencePartitions)] {
		parts = append(parts, map[string]any{
			"group": x.group, "topic": x.topic, "partition": x.partition,
			"committed": x.committed, "log_start": x.logStart, "records_lost": x.logStart - x.committed,
		})
	}
	lost := h.logStart - h.committed
	var groups []string
	for _, x := range hits {
		if !slices.Contains(groups, x.group) {
			groups = append(groups, x.group)
		}
	}
	ev := map[string]any{
		"signal": owners.SignalOffsetBelowLogStart, "severity": "sev1",
		"group": h.group, "topic": h.topic, "partition": h.partition,
		"committed": h.committed, "log_start": h.logStart, "records_lost": lost,
		"partitions": parts,
	}
	if len(hits) > len(parts) {
		ev["partitions_omitted"] = len(hits) - len(parts)
	}
	return reading{
		v: vBreachNow, sev: "sev1",
		label:     fmt.Sprintf("group %s committed below log start on %s/%d (%s records gone)", h.group, h.topic, h.partition, fmtCount(lost)),
		value:     fmt.Sprintf("%d partitions below", len(hits)),
		threshold: "committed >= log start",
		ev:        ev,
		targets:   groups,
	}
}

// fmtCount writes a record count short: 950, 12.4k, 17.2M, 3.1B.
func fmtCount(n int64) string {
	switch {
	case n < 1000:
		return strconv.FormatInt(n, 10)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	case n < 1_000_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1e9)
}
