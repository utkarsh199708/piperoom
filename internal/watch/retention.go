package watch

// Retention evidence (X7d; docs/design/lineage.md section 5, docs/api.md
// "Retention evidence (X7)"): the configured retention of a pipeline's topics
// (clients.TopicConfigReader) and, when the warehouse can say, of its sink
// tables (clients.TableRetentionReader) against owners.yaml
// declared_retention, in both directions, plus cleanup.policy=compact on a CDC
// topic. The findings are events.RetentionRisk entries of config_risk's
// "risks" list, not a signal of their own. They step their own state machine
// (retentionID), which the status rows name config_risk, so a finding is in
// /api/fleet signals_firing, but whose action is always "log": a retention
// finding opens no room, joins none and is never acked by a close; it rides
// along in the evidence when another signal opens or joins a room.
//
// Topic configs change rarely, so each topic or table is read at most every
// retentionRefresh, and the whole watcher makes at most maxRetentionReads
// reads per tick inside one budget. Only names, durations, the size cap and
// the policy words are kept: never another config value.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

const (
	// retentionID is the state machine of a pipeline's retention findings.
	retentionID = owners.SignalConfigRisk + ":retention"
	// retentionRefresh is how often one topic's or table's retention is read
	// again.
	retentionRefresh = 15 * time.Minute
	// maxRetentionReads caps the reads of one tick, all pipelines together;
	// the least recently read go first, so every object gets its turn.
	maxRetentionReads = 16
	// maxRetentionText caps RetentionRisk.Text (bytes).
	maxRetentionText = 200
)

// retentionSignal is config_risk's on/off and `for`, with action log whatever
// config_risk's action: a retention finding never opens a room on its own.
func retentionSignal(cfg owners.WatchConfig) owners.WatchSignal {
	return owners.WatchSignal{Enabled: cfg.ConfigRisk.Enabled, Action: owners.ActionLog, For: cfg.ConfigRisk.For}
}

// retentionIDs is retentionID when the pipeline has its state (for ps.ids).
func retentionIDs(ps *pipeState) []string {
	if ps.sigs[retentionID] != nil {
		return []string{retentionID}
	}
	return nil
}

// retentionMem is a pipeline's retention memory: the last good read of each
// judged topic or table, keyed "topic:<name>" or "table:<name>".
type retentionMem struct {
	objs map[string]*retObj
	// failing: the INFO line for an unreadable config was written; it is
	// written again only after a read worked.
	failing bool
	// missing are declared topics not in the pipeline's graph, logged once.
	missing map[string]bool
	// tablesOff: the warehouse answered ErrNotSupported, logged once.
	tablesOff bool
}

// retKeys are the memory's keys: topics before tables, each by name, so
// reads, evidence and titles keep one order.
func retKeys(mem *retentionMem) []string {
	keys := make([]string, 0, len(mem.objs))
	for k := range mem.objs {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if ta, tb := strings.HasPrefix(a, "table:"), strings.HasPrefix(b, "table:"); ta != tb {
			if ta {
				return 1
			}
			return -1
		}
		return strings.Compare(a, b)
	})
	return keys
}

// anyFailed: the latest read of some object failed.
func (mem *retentionMem) anyFailed() bool {
	for _, o := range mem.objs {
		if o.failed {
			return true
		}
	}
	return false
}

// retObj is one judged topic or table.
type retObj struct {
	topic, table string
	cdc          bool   // a CDC topic of the pipeline (the compact check)
	declared     string // owners.yaml declared_retention, "" = none
	node         string // topology node id, for the status row
	readAt       time.Time
	ok           bool // a read worked; cfg or tr hold it
	failed       bool // the latest read failed
	skip         bool // the reader cannot answer for it (ErrNotSupported)
	cfg          clients.TopicConfig
	tr           clients.TableRetention
}

func (o *retObj) key() string {
	if o.table != "" {
		return "table:" + o.table
	}
	return "topic:" + o.topic
}

// retentionObjects lists what the pipeline judges: every CDC topic (compact
// check, and its declaration), every DLQ topic of the graph with a
// declaration, and every sink table with a declaration when the warehouse can
// answer. Names come from owners.yaml, the scope and the connector configs of
// the pipeline's own connectors only. missing are declared topics that are
// none of these. Caller holds w.mu.
func retentionObjects(t target, obs *observation, tables bool) (objs []*retObj, missing []string) {
	p := t.p
	inGraph := map[string]bool{}
	for _, topic := range t.cdcTopics {
		inGraph[topic] = true
		d, _ := p.DeclaredTopicRetention(topic)
		objs = append(objs, &retObj{topic: topic, cdc: true, declared: d, node: "topic:" + topic})
	}
	var dlqs []string
	for _, d := range t.cfg.DLQTopics {
		dlqs = append(dlqs, d.Topic)
	}
	if obs != nil && obs.plan != nil {
		for _, d := range obs.plan.topics[p.ID] {
			dlqs = append(dlqs, d.name)
		}
	}
	for _, topic := range dlqs {
		if inGraph[topic] {
			continue
		}
		inGraph[topic] = true
		if d, ok := p.DeclaredTopicRetention(topic); ok {
			objs = append(objs, &retObj{topic: topic, declared: d, node: "dlq:" + topic})
		}
	}
	if p.DeclaredRetention != nil {
		for topic := range p.DeclaredRetention.Topics {
			if !inGraph[topic] {
				missing = append(missing, topic)
			}
		}
		slices.Sort(missing)
	}
	if tables {
		for _, ref := range p.SinkRefs() {
			d, ok := p.DeclaredTableRetention(ref.Table)
			if !ok || t.scope.CheckSinkTable(ref.Table) != nil || slices.ContainsFunc(objs, func(o *retObj) bool { return o.table == ref.Table }) {
				continue
			}
			objs = append(objs, &retObj{table: ref.Table, declared: d, node: "wh:" + ref.Key})
		}
	}
	return objs, missing
}

// topicConfigReader is the pipeline's topic config reader: on the Kafka
// client behind the DLQ or the lag interface (asserted, so the watcher stays
// off the kafka package).
func topicConfigReader(t target) clients.TopicConfigReader {
	if r, ok := t.kafka.(clients.TopicConfigReader); ok {
		return r
	}
	r, _ := t.lag.(clients.TopicConfigReader)
	return r
}

// retJob is one read of one tick.
type retJob struct {
	t   target
	obj *retObj
	at  time.Time // readAt before the tick, for the order
	tr  clients.TopicConfigReader
	wr  clients.TableRetentionReader
	// results
	cfg  clients.TopicConfig
	tret clients.TableRetention
	err  error
	done bool
}

// readRetention refreshes the retention memory of every pipeline: it syncs
// the judged objects with owners.yaml and the graph, then reads the ones that
// are due, at most maxRetentionReads, inside one budget (half the interval, at
// least one check timeout), without the lock. A read the budget cut stays due.
func (w *Watcher) readRetention(ctx context.Context, now time.Time, ts []target, bySet map[string]*observation) {
	var jobs []*retJob
	w.mu.Lock()
	for _, t := range ts {
		if !t.cfg.ConfigRisk.Enabled {
			continue
		}
		ps := w.pipe(t.p.ID)
		tr := topicConfigReader(t)
		wr, _ := t.wh.(clients.TableRetentionReader)
		if ps.ret == nil {
			ps.ret = &retentionMem{objs: map[string]*retObj{}, missing: map[string]bool{}}
		}
		mem := ps.ret
		objs, missing := retentionObjects(t, bySet[t.set], wr != nil && !mem.tablesOff)
		for _, m := range missing {
			if !mem.missing[m] {
				mem.missing[m] = true
				w.log.Info("watch: declared_retention names a topic that is not in the pipeline's graph, not judged",
					"mode", string(w.cfg.Mode), "pipeline", t.p.ID, "topic", m)
			}
		}
		keep := map[string]bool{}
		for _, o := range objs {
			k := o.key()
			keep[k] = true
			if old := mem.objs[k]; old != nil {
				// The read stays; the declaration and the graph may have moved.
				old.cdc, old.declared, old.node = o.cdc, o.declared, o.node
				continue
			}
			mem.objs[k] = o
		}
		for k := range mem.objs {
			if !keep[k] {
				delete(mem.objs, k)
			}
		}
		if tr == nil && slices.ContainsFunc(objs, func(o *retObj) bool { return o.topic != "" }) && !w.retNoReaderLogged {
			w.retNoReaderLogged = true
			w.log.Info("watch: retention check off for topics, the Kafka client cannot read topic configs (clients.TopicConfigReader)",
				"mode", string(w.cfg.Mode))
		}
		for _, k := range retKeys(mem) {
			o := mem.objs[k]
			if o.skip || (!o.readAt.IsZero() && now.Sub(o.readAt) < retentionRefresh) {
				continue
			}
			switch {
			case o.topic != "" && tr != nil:
				jobs = append(jobs, &retJob{t: t, obj: o, at: o.readAt, tr: tr})
			case o.table != "" && wr != nil:
				jobs = append(jobs, &retJob{t: t, obj: o, at: o.readAt, wr: wr})
			}
		}
	}
	w.mu.Unlock()
	if len(jobs) == 0 {
		return
	}
	// Never read first, then the oldest read; owners order breaks ties.
	slices.SortStableFunc(jobs, func(a, b *retJob) int { return a.at.Compare(b.at) })
	jobs = jobs[:min(len(jobs), maxRetentionReads)]

	bctx, cancel := context.WithTimeout(ctx, max(w.cfg.Interval/2, w.cfg.CheckTimeout))
	defer cancel()
	for _, j := range jobs {
		if bctx.Err() != nil {
			break
		}
		// The readers scope on the pipeline in ctx too; the names are the
		// pipeline's own already.
		pctx := tools.WithPipeline(bctx, j.t.p)
		if j.tr != nil {
			j.cfg, j.err = call(pctx, w.cfg.CheckTimeout, "topic config", func(c context.Context) (clients.TopicConfig, error) {
				return j.tr.DescribeTopicConfigs(c, j.obj.topic)
			})
		} else {
			j.tret, j.err = call(pctx, w.cfg.CheckTimeout, "table retention", func(c context.Context) (clients.TableRetention, error) {
				return j.wr.TableRetention(c, j.obj.table)
			})
		}
		// A read the budget or the shutdown cut says nothing about the topic.
		j.done = j.err == nil || bctx.Err() == nil
	}
	if ctx.Err() != nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, j := range jobs {
		if !j.done {
			continue
		}
		ps := w.pipes[j.t.p.ID]
		if ps == nil || ps.ret == nil || ps.ret.objs[j.obj.key()] != j.obj {
			continue // the object left the pipeline while it was read
		}
		mem, o := ps.ret, j.obj
		o.readAt, o.failed = now, j.err != nil
		switch {
		case j.err == nil:
			o.ok = true
			if j.tr != nil {
				// Only the three keys the check needs are kept.
				o.cfg = clients.TopicConfig{Topic: o.topic, RetentionMS: j.cfg.RetentionMS,
					RetentionBytes: j.cfg.RetentionBytes, CleanupPolicy: j.cfg.CleanupPolicy}
			} else {
				o.tr = j.tret
			}
			if !mem.anyFailed() {
				mem.failing = false // every config reads again: the next failure logs
			}
		case o.table != "" && errors.Is(j.err, clients.ErrNotSupported):
			// The warehouse has no retention setting to read: leave tables out.
			o.skip = true
			if !mem.tablesOff {
				mem.tablesOff = true
				w.log.Info("watch: retention check off for sink tables, the warehouse cannot say",
					"mode", string(w.cfg.Mode), "pipeline", j.t.p.ID)
			}
		default:
			// No DescribeConfigs ACL, a topic not created yet, a broker down:
			// one INFO line per pipeline, no finding, the last good read stays.
			if !mem.failing {
				mem.failing = true
				msg, _ := redact.Text(clean(j.err.Error(), 300))
				what := "topic"
				name := o.topic
				if o.table != "" {
					what, name = "table", o.table
				}
				w.log.Info("watch: retention check cannot read a config, no finding for it",
					"mode", string(w.cfg.Mode), "pipeline", j.t.p.ID, what, name, "err", msg)
			}
		}
	}
}

// evaluateRetention steps the pipeline's retention state from its memory and
// returns the current findings as evidence entries (config_risk lists them
// next to the connector risks). With nothing judged it drops the state.
// Caller holds w.mu.
func (w *Watcher) evaluateRetention(now time.Time, t target, ps *pipeState, fired map[string][]string) []map[string]any {
	if !t.cfg.ConfigRisk.Enabled || ps.ret == nil {
		ps.ret = nil
		delete(ps.sigs, retentionID)
		return nil
	}
	keys := retKeys(ps.ret)
	var risks []events.RetentionRisk
	node := ""
	unread := false
	for _, k := range keys {
		o := ps.ret.objs[k]
		if o.skip {
			continue
		}
		if !o.ok {
			// Never read yet (no reader, not due, or unreadable).
			unread = true
			continue
		}
		rs := judgeRetention(o)
		if len(rs) > 0 && node == "" {
			node = o.node
		}
		risks = append(risks, rs...)
	}
	var r reading
	switch {
	case len(risks) > 0:
		evs := make([]map[string]any, 0, min(len(risks), maxEvidenceRisks))
		codes := make([]string, 0, len(risks))
		for _, rk := range risks {
			codes = append(codes, rk.Code)
			if len(evs) < maxEvidenceRisks {
				evs = append(evs, retentionEntry(rk))
			}
		}
		label := "retention: " + risks[0].Text
		if len(risks) > 1 {
			label += fmt.Sprintf(" (+%d more)", len(risks)-1)
		}
		r = reading{
			v: vBreach, sev: events.RetentionRiskSeverity, label: label, node: node,
			value: strings.Join(codes, ", "), threshold: "declared_retention",
			ev: map[string]any{"signal": owners.SignalConfigRisk, "severity": events.RetentionRiskSeverity, "risks": evs},
		}
	case unread:
		// A config that could not be read is no evidence either way: the
		// state is left alone (the INFO line was written by readRetention).
		return nil
	case len(keys) == 0 && (ps.sigs[retentionID] == nil || ps.sigs[retentionID].phase == phaseOK):
		delete(ps.sigs, retentionID)
		return nil
	default:
		r = reading{v: vClear, value: "no retention finding", threshold: "declared_retention"}
	}
	w.apply(now, t, ps, retentionID, retentionSignal(t.cfg), r, fired)
	if r.v != vBreach {
		return nil
	}
	return r.ev["risks"].([]map[string]any)
}

// judgeRetention compares one object's last read with its declaration.
func judgeRetention(o *retObj) []events.RetentionRisk {
	var out []events.RetentionRisk
	name := o.topic
	if o.table != "" {
		name = o.table
	}
	var conf time.Duration
	var confInf, known bool
	var bytes int64
	policy := ""
	if o.table != "" {
		// No retention setting on a table means its rows are kept forever.
		conf, confInf, known = o.tr.After, o.tr.Infinite || !o.tr.Set, true
	} else {
		words := policyWords(o.cfg.CleanupPolicy)
		policy = strings.Join(words, ",")
		compactOnly := slices.Contains(words, "compact") && !slices.Contains(words, "delete")
		if o.cdc && compactOnly {
			rk := events.RetentionRisk{Code: events.RiskCompactCDCTopic, Sev: events.RetentionRiskSeverity,
				Topic: o.topic, CleanupPolicy: policy,
				Text: name + " has cleanup.policy=compact: deletes are compacted away and a replay cannot see them"}
			if o.declared != "" {
				rk.Declared = o.declared
			}
			out = append(out, rk)
		}
		if b, err := strconv.ParseInt(strings.TrimSpace(o.cfg.RetentionBytes), 10, 64); err == nil && b > 0 {
			bytes = b
		}
		switch ms, err := strconv.ParseInt(strings.TrimSpace(o.cfg.RetentionMS), 10, 64); {
		case compactOnly:
			// retention.ms deletes nothing without the delete policy: the
			// latest record of every key is kept forever.
			confInf, known = true, true
		case err != nil:
		case ms < 0:
			confInf, known = true, true
		case ms > 0:
			conf, known = time.Duration(ms)*time.Millisecond, true
		}
	}
	if o.declared == "" || !known {
		return out
	}
	decl, declInf, err := topology.ParseRetention(o.declared)
	if err != nil {
		return out // owners.Load refuses it; never judge a guess
	}
	conf = conf.Truncate(time.Second)
	var code string
	switch {
	case confInf && declInf:
	case confInf:
		code = events.RiskRetentionLonger
	case declInf:
		code = events.RiskRetentionShorter
	case conf < decl:
		code = events.RiskRetentionShorter
	case conf > decl:
		code = events.RiskRetentionLonger
	}
	if code == "" {
		return out
	}
	configured := topology.FormatRetention(conf, confInf)
	declared := topology.FormatRetention(decl, declInf)
	rk := events.RetentionRisk{Code: code, Sev: events.RetentionRiskSeverity, Topic: o.topic, Table: o.table,
		Configured: configured, Declared: declared, RetentionBytes: bytes, CleanupPolicy: policy}
	if o.table != "" {
		rk.Text = fmt.Sprintf("%s keeps rows %s; declared %s", name, configured, declared)
	} else {
		rk.Text = fmt.Sprintf("%s retains %s; declared %s", name, configured, declared)
		if bytes > 0 {
			rk.Text += " (a retention.bytes cap is set)"
		}
	}
	return append(out, rk)
}

// policyWords are the known cleanup.policy words of a value, in order; any
// other text is dropped, so no other config value reaches the evidence.
func policyWords(v string) []string {
	var out []string
	for _, w := range strings.Split(v, ",") {
		w = strings.TrimSpace(w)
		if (w == "compact" || w == "delete") && !slices.Contains(out, w) {
			out = append(out, w)
		}
	}
	return out
}

// retentionEntry is the evidence form of a RetentionRisk (its JSON keys,
// empty fields left out), with Text cut to maxRetentionText bytes.
func retentionEntry(r events.RetentionRisk) map[string]any {
	m := map[string]any{"code": r.Code, "sev": r.Sev, "text": cutBytes(r.Text, maxRetentionText)}
	for k, v := range map[string]string{"topic": r.Topic, "table": r.Table, "configured": r.Configured,
		"declared": r.Declared, "cleanup_policy": r.CleanupPolicy} {
		if v != "" {
			m[k] = v
		}
	}
	if r.RetentionBytes > 0 {
		m["retention_bytes"] = r.RetentionBytes
	}
	return m
}

// cutBytes cuts s to at most n bytes on a rune boundary.
func cutBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
