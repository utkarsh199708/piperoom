package watch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
	"piperoom/internal/owners"
)

// This file holds the dlq_growth and config_risk signals
// (docs/design/dlq.md sections 3.1 and 3.4). The watcher reads offsets only,
// never a record, and puts counts, rates, offsets, topic names, connector
// names and fixed risk texts into the evidence, never a config value.

// Evidence limits.
const (
	maxEvidenceRisks = 8
	maxOtherTopics   = 5
	// troubleTrickle: under this many new records, with no rate yet, the
	// signal is sev3 (design 3.1.2).
	troubleTrickle = 10
	// maxDLQSamples bounds a ring whatever the window and interval are.
	maxDLQSamples = 50000
)

var (
	errNoDLQReader = errors.New("watch: the Kafka client cannot read DLQ offsets")
	errDLQConfig   = errors.New("watch: DLQ topics unknown: a connector config could not be read")
)

// dlqTopic is one DLQ topic of one pipeline.
type dlqTopic struct {
	name     string
	declared bool // in owners.yaml watch.dlq_topics
	shared   bool // declared shared, or named by another pipeline too
	writers  []string
	// cfgs are the parsed configs of the connectors that name the topic.
	cfgs []dlq.ErrorsConfig
}

// dlqPlan is which DLQ topics each pipeline looks at in a tick.
type dlqPlan struct {
	topics map[string][]dlqTopic // by pipeline id
	// cfgFail: a connector of the pipeline could not be read, so its DLQ topic
	// (if it has one) is not known.
	cfgFail map[string]bool
	// want is every topic to read, sorted and de-duplicated.
	want []string
}

// planDLQ lists the DLQ topics per pipeline: the declared ones first, then the
// ones sink connectors name in their config. A topic that is also a CDC topic
// of the pipeline is dropped: counting it would make every change look bad.
func planDLQ(ts []target, obs *observation) *dlqPlan {
	plan := &dlqPlan{topics: map[string][]dlqTopic{}, cfgFail: map[string]bool{}}
	users := map[string]map[string]bool{} // topic -> pipelines
	want := map[string]bool{}
	for _, t := range ts {
		if !t.cfg.DLQGrowth.Enabled && !t.cfg.ConfigRisk.Enabled {
			continue
		}
		cdc := map[string]bool{}
		for _, c := range t.cdcTopics {
			cdc[c] = true
		}
		var list []dlqTopic
		add := func(name, writer string, declared, shared bool, ec *dlq.ErrorsConfig) {
			i := slices.IndexFunc(list, func(d dlqTopic) bool { return d.name == name })
			if i < 0 {
				list = append(list, dlqTopic{name: name})
				i = len(list) - 1
			}
			d := &list[i]
			d.declared = d.declared || declared
			d.shared = d.shared || shared
			if writer != "" && !slices.Contains(d.writers, writer) {
				d.writers = append(d.writers, writer)
			}
			if ec != nil {
				d.cfgs = append(d.cfgs, *ec)
			}
		}
		for _, d := range t.cfg.DLQTopics {
			if !cdc[d.Topic] {
				add(d.Topic, d.WrittenBy, true, d.Shared, nil)
			}
		}
		for _, c := range t.connectors {
			co := obs.conns[c]
			if co == nil || !co.cfgTried || co.cfgErr != nil {
				plan.cfgFail[t.p.ID] = true
				continue
			}
			ec := dlq.Parse(co.cfg)
			if !ec.IsSink || ec.Topic == "" || cdc[ec.Topic] {
				continue
			}
			add(ec.Topic, c, false, false, &ec)
		}
		plan.topics[t.p.ID] = list
		for _, d := range list {
			if users[d.name] == nil {
				users[d.name] = map[string]bool{}
			}
			users[d.name][t.p.ID] = true
			want[d.name] = true
		}
	}
	for id, list := range plan.topics {
		for i := range list {
			if len(users[list[i].name]) > 1 {
				list[i].shared = true
			}
		}
		plan.topics[id] = list
	}
	for name := range want {
		plan.want = append(plan.want, name)
	}
	slices.Sort(plan.want)
	return plan
}

// observeDLQ makes the one DLQOffsets call of the tick, for every DLQ topic of
// every target, de-duplicated. No topics: no call (a client may list the whole
// cluster for an empty list). The call has its own deadline and its own error.
func (w *Watcher) observeDLQ(ctx context.Context, ts []target, obs *observation) {
	obs.plan = planDLQ(ts, obs)
	if w.kafka == nil {
		obs.dlqNoReader = true
		return
	}
	if len(obs.plan.want) == 0 {
		return
	}
	obs.dlqTried = true
	obs.dlqOffsets, obs.dlqErr = call(ctx, w.cfg.CheckTimeout, "read DLQ offsets", func(c context.Context) (map[string][]clients.PartitionOffsets, error) {
		return w.kafka.DLQOffsets(c, obs.plan.want...)
	})
}

// dlqTopicState is the memory of one DLQ topic: a ring of end-offset sums (the
// same sample shape as slot_wal) and whether records were there before the
// watcher looked.
type dlqTopicState struct {
	samples []sample // retained = sum of end offsets
	// backlog: the first sample already had records, so they are "older than
	// the watcher". A topic seen missing and then present has no backlog.
	backlog bool
	// absent: the last reading found the topic missing. The ring keeps the last
	// end sum, so a topic that comes back (a broker restart can answer
	// UNKNOWN_TOPIC_OR_PARTITION for a moment) does not make its old records new.
	absent bool
	// known/exists: the last good reading of the topic, used by config_risk
	// when a later read fails.
	known, exists bool
	// lastWanted is the last tick that asked for the topic; idle topics are
	// dropped after the ring window.
	lastWanted time.Time
}

// dlqSnap is what one tick learned about one topic.
type dlqSnap struct {
	known      bool // the offsets were read
	exists     bool
	partitions int
	records    int64 // sum of end - start: what is in the topic now
	endSum     int64
}

// recordDLQ appends this tick's sample to the ring of every topic read. It runs
// once per tick, so pipelines that share a topic do not add it twice. Caller
// holds w.mu.
func (w *Watcher) recordDLQ(now time.Time, ts []target, obs *observation) map[string]dlqSnap {
	snaps := map[string]dlqSnap{}
	if obs.plan == nil {
		return snaps
	}
	keep := time.Hour
	for _, t := range ts {
		keep = max(keep, t.cfg.DLQGrowth.Window)
	}
	// Forget topics nobody has asked for during the whole ring window.
	for _, name := range obs.plan.want {
		if st := w.dlqState[name]; st != nil {
			st.lastWanted = now
		}
	}
	for name, st := range w.dlqState {
		if now.Sub(st.lastWanted) > keep {
			delete(w.dlqState, name)
		}
	}
	if !obs.dlqTried || obs.dlqErr != nil {
		return snaps // unknown: rings and timers stay as they are
	}
	for _, name := range obs.plan.want {
		st := w.dlqState[name]
		if st == nil {
			st = &dlqTopicState{lastWanted: now}
			w.dlqState[name] = st
		}
		parts, ok := obs.dlqOffsets[name]
		if !ok {
			// Absent. A topic never seen before has zero records, so its first
			// appearance counts as new. A topic that had records keeps its last
			// end sum: one missing answer must not turn its backlog into news.
			if len(st.samples) == 0 {
				st.samples, st.backlog = []sample{{at: now}}, false
			}
			st.absent, st.known, st.exists = true, true, false
			snaps[name] = dlqSnap{known: true}
			continue
		}
		snap := dlqSnap{known: true, exists: true, partitions: len(parts)}
		for _, p := range parts {
			snap.endSum += p.End
			snap.records += max(p.End-p.Start, 0)
		}
		switch n := len(st.samples); {
		case n == 0:
			st.backlog = snap.records > 0 // the baseline: never a rise
		case snap.endSum < st.samples[n-1].retained:
			// Offsets went back: the topic was deleted and created again. All its
			// records are new.
			at := st.samples[n-1].at
			if st.absent {
				at = now
			}
			st.samples = []sample{{at: at}}
		}
		st.absent, st.known, st.exists = false, true, true
		st.samples = append(st.samples, sample{at: now, retained: snap.endSum})
		cutoff := now.Add(-keep)
		i := 0
		for i < len(st.samples)-1 && st.samples[i].at.Before(cutoff) {
			i++
		}
		if over := len(st.samples) - i - maxDLQSamples; over > 0 {
			i += over
		}
		st.samples = slices.Clone(st.samples[i:])
		snaps[name] = snap
	}
	return snaps
}

// dlqTopicReading is one topic's numbers for one pipeline.
type dlqTopicReading struct {
	t        dlqTopic
	snap     dlqSnap
	newN     int64
	rate     float64 // records per second
	rateOK   bool
	rising   bool
	first    time.Time
	backlog  bool
	contexts string // "true" | "false" | "" (unknown)
}

// windowed works out new_in_window and the rate for a topic.
// new_in_window = end_sum(now) - end_sum(oldest sample inside the window); the
// end offsets only grow, so it is exact. first is the time of the first rising
// sample inside the window.
//
// rising says growth is accelerating: the newer half of the window got at least
// two records more than the older half. Counts are integers and a slow trickle
// puts one or two records in a half, so a difference of one is noise.
func windowed(st *dlqTopicState, now time.Time, window, slack time.Duration) (newN int64, rate float64, rateOK bool, first time.Time, rising bool) {
	if st == nil || len(st.samples) == 0 {
		return 0, 0, false, time.Time{}, false
	}
	cutoff := now.Add(-window)
	base := 0
	for base < len(st.samples)-1 && st.samples[base].at.Before(cutoff) {
		base++
	}
	cur := st.samples[len(st.samples)-1]
	newN = max(cur.retained-st.samples[base].retained, 0)
	for j := base + 1; j < len(st.samples); j++ {
		if st.samples[j].retained > st.samples[j-1].retained {
			first = st.samples[j].at
			break
		}
	}
	rate, rateOK = slope(st.samples, now, window)
	mid := base
	for mid < len(st.samples)-1 && st.samples[mid].at.Before(now.Add(-window/2)) {
		mid++
	}
	older := st.samples[mid].retained - st.samples[base].retained
	newer := cur.retained - st.samples[mid].retained
	// Half a window of history cannot show acceleration (a fresh ring would
	// read every first record as a surge), so the ring must reach back to the
	// start of the window, give or take a tick.
	covered := st.samples[base].at.Sub(cutoff) <= slack
	return newN, rate, rateOK, first, covered && newer-older >= 2
}

// evaluateDLQ steps config_risk and dlq_growth for one pipeline. Caller holds w.mu.
func (w *Watcher) evaluateDLQ(now time.Time, t target, ps *pipeState, obs *observation, snaps map[string]dlqSnap, fired map[string][]string) {
	ps.risks = nil
	if t.cfg.ConfigRisk.Enabled {
		w.evaluateConfigRisk(now, t, ps, obs, snaps, fired)
	}
	if !t.cfg.DLQGrowth.Enabled {
		return
	}
	if obs.dlqNoReader {
		if !w.dlqNoReaderLogged {
			w.dlqNoReaderLogged = true
			w.log.Warn("watch: dlq_growth is off, the Kafka client cannot read DLQ offsets (clients.DLQReader)",
				"mode", string(w.cfg.Mode))
		}
		return
	}
	id := t.p.ID
	topics := obs.plan.topics[id]
	var r reading
	switch {
	case obs.dlqErr != nil:
		r = reading{v: vUnknown, err: obs.dlqErr}
	case len(topics) == 0 && obs.plan.cfgFail[id]:
		r = reading{v: vUnknown, err: errDLQConfig}
	case len(topics) == 0:
		r = reading{v: vClear, value: "no DLQ topic", threshold: dlqThreshold(t.cfg.DLQGrowth)}
	default:
		r = w.dlqGrowthReading(now, t, ps, obs, topics, snaps)
		if r.v != vBreach && obs.plan.cfgFail[id] {
			// A connector config could not be read, so a DLQ topic may be missing
			// from the list: clear or hold would be a guess.
			r = reading{v: vUnknown, err: errDLQConfig}
		}
	}
	ss := ps.sig(owners.SignalDLQGrowth)
	w.apply(now, t, ps, owners.SignalDLQGrowth, t.cfg.DLQGrowth.WatchSignal, r, fired)
	// A quiet window ends the episode: the next rise gets a new first_seen.
	if r.v == vClear && ss.phase == phaseOK {
		ps.dlqFirst = nil
	}
}

func dlqThreshold(s owners.DLQSignal) string {
	return fmt.Sprintf("fire %d new in %s", s.FireRecords, fmtDur(s.Window))
}

// dlqGrowthReading builds the verdict, label and evidence from the offsets.
func (w *Watcher) dlqGrowthReading(now time.Time, t target, ps *pipeState, obs *observation, topics []dlqTopic, snaps map[string]dlqSnap) reading {
	sc := t.cfg.DLQGrowth
	rs := make([]dlqTopicReading, 0, len(topics))
	for _, d := range topics {
		tr := dlqTopicReading{t: d, snap: snaps[d.name]}
		st := w.dlqState[d.name]
		var first time.Time
		if tr.snap.exists {
			tr.newN, tr.rate, tr.rateOK, first, tr.rising = windowed(st, now, sc.Window, 2*w.cfg.Interval)
			tr.backlog = st != nil && st.backlog
		}
		if tr.newN > 0 {
			if ps.dlqFirst == nil {
				ps.dlqFirst = map[string]time.Time{}
			}
			if ps.dlqFirst[d.name].IsZero() {
				if first.IsZero() {
					first = now
				}
				ps.dlqFirst[d.name] = first
			}
			tr.first = ps.dlqFirst[d.name]
		}
		tr.contexts = contextHeaders(d)
		rs = append(rs, tr)
	}
	// The topic with the most new records is the one the title and evidence name.
	pi := 0
	var most, total int64
	for i, tr := range rs {
		if tr.newN > rs[pi].newN {
			pi = i
		}
		most = max(most, tr.newN)
		total += tr.newN
	}
	p := rs[pi]

	r := reading{
		value:     fmt.Sprintf("%s +%d in %s", p.t.name, p.newN, fmtDur(sc.Window)),
		threshold: dlqThreshold(sc),
		withDur:   true,
	}
	switch {
	case most >= int64(sc.FireRecords):
		r.v = vBreach
	case most == 0:
		r.v = vClear
	default:
		r.v = vHold
	}
	// A trickle (under 10 new records) is sev3 unless it is accelerating; with no
	// rate yet it cannot be shown to accelerate. sev1 is never set: a DLQ does
	// not lose data by itself.
	r.sev = "sev2"
	if p.newN < troubleTrickle && (!p.rateOK || !p.rising) {
		r.sev = "sev3"
	}
	ratePerHour := int64(math.Round(p.rate * 3600))
	label := fmt.Sprintf("DLQ %s +%d in %s", p.t.name, p.newN, fmtDur(sc.Window))
	if p.rateOK && ratePerHour > 0 {
		label += fmt.Sprintf(", ~%d/h", ratePerHour)
	}
	if !p.first.IsZero() {
		label += ", first " + p.first.UTC().Format("15:04") + "Z"
	}
	label += ", " + commas(p.snap.records) + " present"
	if len(rs) > 1 {
		label += fmt.Sprintf(" (+%d more DLQ topics)", len(rs)-1)
	}
	r.label = label

	ev := map[string]any{
		"signal": owners.SignalDLQGrowth, "severity": r.sev,
		"topic": p.t.name, "writers": nonNil(p.t.writers), "declared": p.t.declared,
		"exists": p.snap.exists, "partitions": p.snap.partitions,
		"records": p.snap.records, "end_offset_sum": p.snap.endSum,
		"new_in_window": p.newN, "window": fmtDur(sc.Window),
		"older_than_watcher": p.backlog, "shared": p.t.shared,
	}
	if p.rateOK {
		ev["rate_per_hour"] = ratePerHour
	} else {
		ev["rate_per_hour"] = nil // unknown for the first 30 minutes
	}
	if !p.first.IsZero() {
		ev["first_seen"] = p.first.UTC().Format(time.RFC3339)
	}
	if p.contexts != "" {
		ev["context_headers"] = p.contexts == "true"
	}
	if codes := w.dlqRiskCodes(p.t, snaps); len(codes) > 0 && t.cfg.ConfigRisk.Enabled {
		ev["risks"] = codes
	}
	if len(rs) > 1 {
		var others []map[string]any
		for i, tr := range rs {
			if i == pi || len(others) >= maxOtherTopics {
				continue
			}
			others = append(others, map[string]any{"topic": tr.t.name, "new_in_window": tr.newN, "records": tr.snap.records, "exists": tr.snap.exists})
		}
		ev["other_topics"] = others
		ev["new_in_window_all"] = total
	}
	r.ev = ev
	return r
}

// contextHeaders says whether the writers' configs turn on DLQ context headers:
// "true" when all known ones do, "false" when one does not, "" when no writer
// config is known (a declared topic).
func contextHeaders(d dlqTopic) string {
	if len(d.cfgs) == 0 {
		return ""
	}
	for _, c := range d.cfgs {
		if !c.ContextHeaders {
			return "false"
		}
	}
	return "true"
}

// dlqRiskCodes are the risk codes of the connectors that write the topic.
func (w *Watcher) dlqRiskCodes(d dlqTopic, snaps map[string]dlqSnap) []string {
	var codes []string
	for _, c := range d.cfgs {
		for _, code := range dlq.Codes(dlq.Risks(c, w.existsOf(c, snaps))) {
			if !slices.Contains(codes, code) {
				codes = append(codes, code)
			}
		}
	}
	return codes
}

// existsOf is the watcher's last reading of the connector's DLQ topic: nil
// when the connector has none or the offsets were not read.
func (w *Watcher) existsOf(c dlq.ErrorsConfig, snaps map[string]dlqSnap) *bool {
	if c.Topic == "" {
		return nil
	}
	if s, ok := snaps[c.Topic]; ok && s.known {
		e := s.exists
		return &e
	}
	// This tick's read failed: the last good answer stands (offsets errors are
	// "unknown", never "resolved").
	if st := w.dlqState[c.Topic]; st != nil && st.known {
		e := st.exists
		return &e
	}
	return nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// commas writes 1204 as "1,204".
func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// ---- config_risk ----

// connRisk is one finding on one connector.
type connRisk struct {
	conn string
	risk dlq.Risk
}

// evaluateConfigRisk reads the risks of every connector config of the tick.
// Its verdict is breach on any sev2/sev3 finding (info never breaches), clear
// when none, unknown when nothing is breaching and a config could not be read.
// The findings (codes only) are kept on the pipeline so any room carries them.
func (w *Watcher) evaluateConfigRisk(now time.Time, t target, ps *pipeState, obs *observation, snaps map[string]dlqSnap, fired map[string][]string) {
	var found []connRisk
	var firstErr error
	existsUnknown := false
	for _, c := range t.connectors {
		co := obs.conns[c]
		switch {
		case co == nil || !co.cfgTried:
			if firstErr == nil {
				firstErr = fmt.Errorf("watch: connector %s: config not read", c)
			}
			continue
		case co.cfgErr != nil:
			if firstErr == nil {
				firstErr = co.cfgErr
			}
			continue
		}
		ec := dlq.Parse(co.cfg)
		ex := w.existsOf(ec, snaps)
		if ex == nil && ec.IsSink && ec.Topic != "" && obs.dlqErr != nil {
			existsUnknown = true // dlq_topic_missing cannot be judged this tick
		}
		for _, rk := range dlq.Risks(ec, ex) {
			found = append(found, connRisk{conn: c, risk: rk})
		}
	}
	for _, f := range found {
		if len(ps.risks) >= maxEvidenceRisks {
			break
		}
		ps.risks = append(ps.risks, map[string]any{"connector": f.conn, "code": f.risk.Code, "sev": f.risk.Sev})
	}

	var bad []connRisk
	sev := "sev9"
	for _, f := range found {
		if f.risk.Sev != dlq.SevInfo {
			bad = append(bad, f)
			sev = moreSevere(sev, f.risk.Sev)
		}
	}
	th := "errors.tolerance=none or a DLQ with logging"
	var r reading
	switch {
	case len(bad) > 0:
		parts := make([]string, 0, len(bad))
		evs := make([]map[string]any, 0, len(bad))
		for _, f := range bad {
			parts = append(parts, f.conn+": "+f.risk.Code)
			if len(evs) < maxEvidenceRisks {
				// Text is a fixed string per code; no config value reaches it.
				evs = append(evs, map[string]any{"connector": f.conn, "code": f.risk.Code, "sev": f.risk.Sev, "text": f.risk.Text})
			}
		}
		shown := parts[:min(len(parts), maxEvidenceConnectors)]
		label := "config risk " + strings.ReplaceAll(strings.Join(shown, ", "), ": ", " ")
		if more := len(parts) - len(shown); more > 0 {
			label += fmt.Sprintf(" (+%d more)", more)
		}
		r = reading{
			v: vBreach, sev: sev, label: label, value: strings.Join(parts, ", "), threshold: th,
			ev: map[string]any{"signal": owners.SignalConfigRisk, "severity": sev, "risks": evs},
		}
	case firstErr != nil:
		r = reading{v: vUnknown, err: firstErr}
	case existsUnknown:
		// A failed Kafka read must not resolve a finding that needs its answer.
		r = reading{v: vUnknown, err: obs.dlqErr}
	default:
		r = reading{v: vClear, value: "no risk", threshold: th}
	}
	w.apply(now, t, ps, owners.SignalConfigRisk, t.cfg.ConfigRisk, r, fired)
}
