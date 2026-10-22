package watch

// Drift signals (X1b; docs/api.md "Drift (X1: silent corruption)"). A "green
// but wrong" pipeline raises no other symptom, so every reconcile_interval
// the watcher compares the two ends of each source -> sink table pair and
// scans the CDC topics:
//
//   - count_drift: CountBetween on both sides over the last closed hour,
//     shifted back by the sink's lag (now - its newest sink_ts_column), or,
//     on an idle sink, over the hour ending at the sink's newest row
//     (drift_landed.go); fires when |source - sink| / source >
//     count_tolerance on two checks in a row.
//   - row_drift: SampleKeys (50) on the source over the hour before
//     now - lag - settle (rows that had time to reach the sink), RowTimes on
//     both sides for the same keys; fires when a settled key is missing on
//     the sink, or one of its timestamp columns differs. It also fires on
//     the breaks the timestamps cannot show (drift_landed.go): recently
//     landed rows with sink_ts_column NULL, Debezium's unavailable-value
//     placeholder in a toastable column, a column gone from the sink.
//   - dup_keys and order_skew: one DupScan per CDC topic over the time since
//     the last scan; counts copied into the signal as docs/api.md maps them.
//
// Every reader is optional (clients.Reconciler, clients.DupScanner,
// clients.ColumnReader, clients.ToastColumnReader, clients.PlaceholderCounter
// and the source's Freshness, found by type assertion); one that is missing
// or answers ErrNotSupported turns its checks off for the pipeline with one
// INFO line. Primary-key values stay in
// this file's local variables: no log line, error or evidence carries one.
// Each signal id holds the table or topic ("count_drift:public.orders"), so
// state and cooldown are per (pipeline, table, kind).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const (
	// driftWindow is the width of count_drift's window: the last closed hour.
	driftWindow = time.Hour
	// maxDriftLag bounds the shift: a sink that has not written for a day is
	// compared on the hour a day ago, not on one further back.
	maxDriftLag = 24 * time.Hour
	// driftSampleKeys is how many source keys row_drift samples.
	driftSampleKeys = 50
	// rowSettle is how long a source row may still be on its way to the sink
	// on top of the measured lag (the reconcile tool's default settle).
	rowSettle = 2 * time.Minute
	// driftTimeTolerance: two timestamps closer than this are equal. A sink
	// that keeps milliseconds of a microsecond source is not corrupt; a time
	// zone or a lost update is hours or minutes off.
	driftTimeTolerance = time.Second
	// countDriftChecks is how many failing checks in a row fire count_drift.
	countDriftChecks = 2
	// Bounded work per pipeline and run: at most this many table pairs and
	// topics, rotated across runs, and this many timestamp columns per pair.
	maxDriftPairs   = 10
	maxDriftTopics  = 8
	maxDriftColumns = 8
	// minDupScan is the least records one topic's scan may read when the
	// DupScanMaxScan budget is split over the topics.
	minDupScan = 1000
	// driftConcurrency is how many pipelines are checked at once.
	driftConcurrency = 4
	// maxDriftDetail caps DriftSignal.Detail (bytes).
	maxDriftDetail = 200
)

// The off keys of driftMem: a check whose reader is missing or answered
// ErrNotSupported stays off for the pipeline (one INFO line, no retry loop).
const (
	offReconcile = "reconcile"
	offDup       = "dup_scan"
	offRowTable  = "row_drift:" // + source table: no usable primary key
)

// driftKinds are the drift signal names, in title order.
var driftKinds = []string{events.SignalCountDrift, events.SignalRowDrift, events.SignalDupKeys, events.SignalOrderSkew}

// driftID is the state-machine id of one drift signal on one table or topic.
func driftID(kind, object string) string { return kind + ":" + object }

// driftKind is the kind of a drift id, "" for any other signal.
func driftKind(id string) string {
	k, _, ok := strings.Cut(id, ":")
	if ok && slices.Contains(driftKinds, k) {
		return k
	}
	return ""
}

// driftSignalCfg is the owners settings of a drift kind.
func driftSignalCfg(cfg owners.WatchConfig, kind string) owners.WatchSignal {
	switch kind {
	case events.SignalCountDrift:
		return cfg.CountDrift
	case events.SignalRowDrift:
		return cfg.RowDrift
	case events.SignalDupKeys:
		return cfg.DupKeys
	}
	return cfg.OrderSkew
}

// driftOn: some drift signal is on for the pipeline.
func driftOn(cfg owners.WatchConfig) bool {
	return cfg.CountDrift.Enabled || cfg.RowDrift.Enabled || cfg.DupKeys.Enabled || cfg.OrderSkew.Enabled
}

// driftIDs are the drift ids the pipeline has state for, sorted (kind order,
// then object), so titles and logs are stable. Caller holds w.mu.
func (ps *pipeState) driftIDs() []string {
	var out []string
	for id := range ps.sigs {
		if driftKind(id) != "" {
			out = append(out, id)
		}
	}
	slices.SortFunc(out, func(a, b string) int {
		ka, kb := slices.Index(driftKinds, driftKind(a)), slices.Index(driftKinds, driftKind(b))
		if ka != kb {
			return ka - kb
		}
		return strings.Compare(a, b)
	})
	return out
}

// ids are every signal id of the pipeline in title order: the fixed ones,
// then its drift ids, then its config_drift ids (config_drift.go). Caller
// holds w.mu.
func (ps *pipeState) ids() []string {
	return slices.Concat(signalIDs, ps.driftIDs(), configDriftIDs(ps))
}

// primaryKeyer is implemented by the source and the warehouse clients that
// can name a table's primary key (postgres, warehouse; composite keys answer
// ErrNotSupported).
type primaryKeyer interface {
	PrimaryKeyColumn(ctx context.Context, table string) (string, error)
}

// driftMem is a pipeline's drift memory. Only the drift goroutine of the
// pipeline touches it while a check runs, and the tick waits for that
// goroutine, so it needs no lock; at is read and set under w.mu.
type driftMem struct {
	at        time.Time      // start of the last run
	nextPair  int            // rotation over the pairs (maxDriftPairs per run)
	nextTopic int            // rotation over the topics
	fails     map[string]int // count_drift id -> failing checks in a row
	scanned   map[string]time.Time
	pk        map[string]string // "src|table" / "sink|table" -> key column
	off       map[string]bool
	cols      map[string]map[string]bool // source table -> columns both sides had (structurePart)
	nextPh    map[string]int             // source table -> placeholder column rotation
}

func newDriftMem() *driftMem {
	return &driftMem{fails: map[string]int{}, scanned: map[string]time.Time{}, pk: map[string]string{}, off: map[string]bool{},
		cols: map[string]map[string]bool{}, nextPh: map[string]int{}}
}

// driftResult is one reading of one drift id.
type driftResult struct {
	id string
	r  reading
}

// driftReaders are the optional readers of one pipeline's client set.
type driftReaders struct {
	src, snk     clients.Reconciler
	wh           clients.WarehouseReader
	pkSrc, pkSnk primaryKeyer
	colSrc       clients.ColumnReader
	colSnk       clients.ColumnReader
	dup          clients.DupScanner
	srcFresh     freshnessReader            // the source's newest row (idle sink)
	toast        clients.ToastColumnReader  // the source's toastable columns
	ph           clients.PlaceholderCounter // placeholder rows on the sink
}

func readersOf(t target) driftReaders {
	var rd driftReaders
	rd.src, _ = t.src.(clients.Reconciler)
	rd.pkSrc, _ = t.src.(primaryKeyer)
	rd.colSrc, _ = t.src.(clients.ColumnReader)
	rd.srcFresh, _ = t.src.(freshnessReader)
	rd.toast, _ = t.src.(clients.ToastColumnReader)
	if t.wh != nil {
		rd.ph, _ = t.wh.(clients.PlaceholderCounter)
		rd.wh = t.wh
		rd.snk, _ = t.wh.(clients.Reconciler)
		rd.pkSnk, _ = t.wh.(primaryKeyer)
		rd.colSnk, _ = t.wh.(clients.ColumnReader)
	}
	// The Kafka client is behind the DLQ and lag interfaces; asserted, not
	// imported, so the watcher stays off the kafka package.
	if rd.dup, _ = t.kafka.(clients.DupScanner); rd.dup == nil {
		rd.dup, _ = t.lag.(clients.DupScanner)
	}
	return rd
}

// checkDrift runs the drift checks of every pipeline whose reconcile_interval
// has passed, at most driftConcurrency pipelines at a time, all inside one
// budget (half the tick interval, at least one check timeout), then steps
// the state machines. A run that was cut leaves its unread checks unknown;
// the next run is a whole interval later, so a slow warehouse costs at most
// one budget per interval.
func (w *Watcher) checkDrift(ctx context.Context, now time.Time, ts []target, fired map[string][]string) {
	type job struct {
		t   target
		mem *driftMem
	}
	var jobs []job
	w.mu.Lock()
	for _, t := range ts {
		if !driftOn(t.cfg) {
			continue
		}
		ps := w.pipe(t.p.ID)
		if ps.drift == nil {
			ps.drift = newDriftMem()
		}
		if !ps.drift.at.IsZero() && now.Sub(ps.drift.at) < t.cfg.ReconcileInterval {
			continue
		}
		ps.drift.at = now
		jobs = append(jobs, job{t, ps.drift})
	}
	w.mu.Unlock()
	if len(jobs) == 0 {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, max(w.cfg.Interval/2, w.cfg.CheckTimeout))
	defer cancel()
	results := make([][]driftResult, len(jobs))
	sem := make(chan struct{}, driftConcurrency)
	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-dctx.Done():
				return
			}
			defer func() { <-sem }()
			// The readers check scope from ctx (clients.Reconciler); the
			// watcher scopes every table and topic itself first too.
			results[i] = w.driftPipeline(tools.WithPipeline(dctx, j.t.p), now, j.t, j.mem)
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, j := range jobs {
		ps := w.pipe(j.t.p.ID)
		for _, r := range results[i] {
			w.apply(now, j.t, ps, r.id, signalCfg(j.t.cfg, r.id), r.r, fired)
		}
	}
}

// driftOff turns a check off for the pipeline with one INFO line.
func (w *Watcher) driftOff(mem *driftMem, pipeline, key, checks, reason string) {
	if mem.off[key] {
		return
	}
	mem.off[key] = true
	w.log.Info("watch: drift check off for this pipeline", "mode", string(w.cfg.Mode), "pipeline", pipeline,
		"checks", checks, "reason", reason)
}

// driftPipeline runs one pipeline's drift checks.
func (w *Watcher) driftPipeline(ctx context.Context, now time.Time, t target, mem *driftMem) []driftResult {
	rd := readersOf(t)
	pid := t.p.ID
	var out []driftResult
	const tableChecks = "count_drift,row_drift"
	if (t.cfg.CountDrift.Enabled || t.cfg.RowDrift.Enabled) && len(t.p.SinkTables) > 0 && !mem.off[offReconcile] {
		switch {
		case t.p.SinkTSColumn == "":
			w.driftOff(mem, pid, offReconcile, tableChecks, "no sink_ts_column in owners.yaml")
		case rd.src == nil:
			w.driftOff(mem, pid, offReconcile, tableChecks, "the source client has no reconcile reader")
		case rd.wh == nil:
			w.driftOff(mem, pid, offReconcile, tableChecks, "no warehouse client")
		case rd.snk == nil:
			w.driftOff(mem, pid, offReconcile, tableChecks, "the warehouse client has no reconcile reader")
		default:
			pairs := driftPairs(t)
			for _, pr := range rotate(pairs, &mem.nextPair, maxDriftPairs) {
				res, stop := w.checkPair(ctx, now, t, mem, rd, pr)
				out = append(out, res...)
				if stop || ctx.Err() != nil {
					break
				}
			}
		}
	}
	if (t.cfg.DupKeys.Enabled || t.cfg.OrderSkew.Enabled) && len(t.cdcTopics) > 0 && !mem.off[offDup] {
		if rd.dup == nil {
			w.driftOff(mem, pid, offDup, "dup_keys,order_skew", "the Kafka client has no duplicate scan reader")
		} else {
			out = append(out, w.checkTopics(ctx, now, t, mem, rd.dup)...)
		}
	}
	return out
}

// rotate returns at most n items of list starting at *next, and moves *next
// on, so a pipeline with more tables than n has them all checked in turn.
func rotate[T any](list []T, next *int, n int) []T {
	if len(list) <= n {
		return list
	}
	start := *next % len(list)
	*next = start + n
	out := make([]T, 0, n)
	for i := range n {
		out = append(out, list[(start+i)%len(list)])
	}
	return out
}

// tablePair is a source table and the sink table it fills.
type tablePair struct{ source, sink string }

// driftPairs pairs the pipeline's source tables with its sink tables: the
// one sink of a one-table pipeline, else the sink with the same table name
// when exactly one has it. A table outside the scope or with `reconcile:
// off` (on either side) is never part of a pair, so it is never queried.
func driftPairs(t target) []tablePair {
	var out []tablePair
	for _, src := range t.p.SourceTables {
		if t.scope.CheckSourceTable(src) != nil || !t.cfg.Reconciles(src) {
			continue
		}
		sink := pairSink(src, t.p.SourceTables, t.p.SinkTables)
		if sink == "" || t.scope.CheckSinkTable(sink) != nil || !t.cfg.Reconciles(sink) {
			continue
		}
		out = append(out, tablePair{src, sink})
	}
	return out
}

func pairSink(src string, sources, sinks []string) string {
	if len(sources) == 1 && len(sinks) == 1 {
		return sinks[0]
	}
	name := src[strings.LastIndex(src, ".")+1:]
	match := ""
	for _, s := range sinks {
		if s[strings.LastIndex(s, ".")+1:] == name {
			if match != "" {
				return "" // two sinks with the name: cannot tell
			}
			match = s
		}
	}
	return match
}

func notSupported(err error) bool { return errors.Is(err, clients.ErrNotSupported) }

// checkPair runs count_drift and row_drift on one pair. stop is true when the
// Reconciler answered ErrNotSupported: the table checks are then off.
// row_drift is the merge of up to four parts (drift_landed.go): the sink's
// column set, the sampled key compare, a probe of recently landed rows and a
// count of Debezium's unavailable-value placeholder.
func (w *Watcher) checkPair(ctx context.Context, now time.Time, t target, mem *driftMem, rd driftReaders, pr tablePair) ([]driftResult, bool) {
	countID, rowID := driftID(events.SignalCountDrift, pr.source), driftID(events.SignalRowDrift, pr.source)
	wantCount, wantRow := t.cfg.CountDrift.Enabled, t.cfg.RowDrift.Enabled
	if !wantCount && !wantRow {
		return nil, false
	}
	var parts []rowPart
	var cat *catalogs
	if wantRow {
		var err error
		if cat, err = w.readCatalogs(ctx, rd, pr); err != nil {
			parts = append(parts, rowPart{v: vUnknown, err: err})
		} else if cat != nil {
			sp, tsGone := structurePart(now, mem, pr, t.p.SinkTSColumn, cat)
			if tsGone {
				// Every other read keys on the column the sink no longer has.
				return []driftResult{{rowID, mergeRow(t, pr, []rowPart{sp})}}, false
			}
			parts = append(parts, sp)
		}
	}
	unknownAll := func(err error) []driftResult {
		var out []driftResult
		if wantCount {
			out = append(out, driftResult{countID, reading{v: vUnknown, err: err}})
		}
		if wantRow {
			out = append(out, driftResult{rowID, reading{v: vUnknown, err: err}})
		}
		return out
	}
	fresh, err := call(ctx, w.cfg.CheckTimeout, "sink freshness", func(c context.Context) (time.Time, error) {
		return rd.wh.Freshness(c, pr.sink, t.p.SinkTSColumn)
	})
	if err != nil {
		return unknownAll(err), false
	}
	srcNewest, err := w.sourceNewest(ctx, now, t, mem, rd, pr, fresh)
	if err != nil {
		return unknownAll(err), false
	}
	win := driftWindows(now, fresh, srcNewest)
	var out []driftResult
	if wantCount {
		r, err := w.countReading(ctx, t, mem, rd, pr, win)
		if notSupported(err) {
			w.driftOff(mem, t.p.ID, offReconcile, "count_drift,row_drift", "the reconcile reader is not supported by this client")
			return out, true
		}
		out = append(out, driftResult{countID, r})
	}
	if wantRow {
		rp, err := w.rowParts(ctx, t, mem, rd, pr, win, cat)
		if notSupported(err) {
			w.driftOff(mem, t.p.ID, offReconcile, "count_drift,row_drift", "the reconcile reader is not supported by this client")
			return out, true
		}
		parts = append(parts, rp...)
		parts = append(parts, w.placeholderPart(ctx, t, mem, rd, pr, win, cat)...)
		if len(parts) > 0 {
			out = append(out, driftResult{rowID, mergeRow(t, pr, parts)})
		}
	}
	return out, false
}

// sinkLag is how far the sink's newest row is behind now, whole seconds,
// 0 for an empty sink or a clock ahead of ours, at most maxDriftLag.
func sinkLag(now, fresh time.Time) time.Duration {
	if fresh.IsZero() || !fresh.Before(now) {
		return 0
	}
	return min(now.Sub(fresh), maxDriftLag).Truncate(time.Second)
}

// countWindow is count_drift's window on a busy sink: the last closed hour,
// ending lag earlier.
func countWindow(now time.Time, lag time.Duration) (from, to time.Time) {
	to = now.UTC().Truncate(time.Hour).Add(-lag)
	return to.Add(-driftWindow), to
}

// countReading compares the counts of one pair. A non-nil error is only
// ErrNotSupported; any other failure is an unknown reading.
func (w *Watcher) countReading(ctx context.Context, t target, mem *driftMem, rd driftReaders, pr tablePair, win pairWindows) (reading, error) {
	id := driftID(events.SignalCountDrift, pr.source)
	ts := t.p.SinkTSColumn
	from, to := win.countFrom, win.countTo
	srcN, err := call(ctx, w.cfg.CheckTimeout, "count source rows", func(c context.Context) (int64, error) {
		return rd.src.CountBetween(c, pr.source, ts, from, to)
	})
	if err == nil {
		var snkN int64
		snkN, err = call(ctx, w.cfg.CheckTimeout, "count sink rows", func(c context.Context) (int64, error) {
			return rd.snk.CountBetween(c, pr.sink, ts, from, to)
		})
		if err == nil {
			return countVerdict(t, mem, id, pr, win, srcN, snkN), nil
		}
	}
	if notSupported(err) {
		return reading{}, err
	}
	return reading{v: vUnknown, err: err}, nil
}

// countVerdict judges two counts: over the tolerance on countDriftChecks
// checks in a row is a breach; the first is a hold. At the tolerance itself
// is not drift.
func countVerdict(t target, mem *driftMem, id string, pr tablePair, win pairWindows, srcN, snkN int64) reading {
	tol := t.cfg.CountTolerance
	diff := srcN - snkN
	if diff < 0 {
		diff = -diff
	}
	var ratio float64
	breach := false
	switch {
	case srcN > 0:
		ratio = float64(diff) / float64(srcN)
		breach = ratio > tol
	case snkN > 0:
		ratio, breach = 1, true // rows on the sink the source window does not have
	}
	r := reading{
		value:     fmt.Sprintf("source %d sink %d (%.2f%%)", srcN, snkN, ratio*100),
		threshold: fmt.Sprintf("tolerance %.2f%% on %d checks", tol*100, countDriftChecks),
	}
	if !breach {
		mem.fails[id] = 0
		r.v = vClear
		return r
	}
	mem.fails[id]++
	r.v = vHold
	if mem.fails[id] >= countDriftChecks {
		r.v = vBreach
	}
	r.sev = events.SignalSeverity(events.SignalCountDrift)
	r.label = fmt.Sprintf("count drift %s: source %d rows, sink %d (%.1f%%)", pr.source, srcN, snkN, ratio*100)
	detail := "sink " + pr.sink
	switch {
	case win.idle:
		detail += "; sink idle: window ends at its newest row"
	case win.lag > 0:
		detail += "; window shifted by the sink lag of " + fmtDur(win.lag)
	}
	r.ev = driftEvidence(events.DriftSignal{
		Kind: events.SignalCountDrift, Severity: r.sev, Table: pr.source, Topic: topicOf(t, pr.source),
		Window: events.DriftWindow{From: win.countFrom, To: win.countTo}, Source: srcN, Sink: snkN,
		Missing: int(max(srcN-snkN, 0)), Detail: detail,
	})
	return r
}

// rowParts runs row_drift's key sample on one pair: the settled compare and,
// when the sink looks behind, the probe of recently landed rows. nil when the
// table's sample is off (no usable key, or a key or sink_ts_column that looks
// like personal data); a non-nil error is only ErrNotSupported from the
// Reconciler.
func (w *Watcher) rowParts(ctx context.Context, t target, mem *driftMem, rd driftReaders, pr tablePair, win pairWindows, cat *catalogs) ([]rowPart, error) {
	if mem.off[offRowTable+pr.source] {
		return nil, nil
	}
	ts := t.p.SinkTSColumn
	unknown := func(err error) ([]rowPart, error) {
		if notSupported(err) {
			return nil, err
		}
		return []rowPart{{v: vUnknown, err: err}}, nil
	}
	if rd.pkSrc == nil || rd.pkSnk == nil {
		w.driftOff(mem, t.p.ID, offRowTable+pr.source, "row_drift", "a client cannot name the table's primary key")
		return nil, nil
	}
	if piiColumn(ts) {
		w.driftOff(mem, t.p.ID, offRowTable+pr.source, "row_drift", "sink_ts_column looks like personal data")
		return nil, nil
	}
	pkSrc, err := w.primaryKey(ctx, mem, rd.pkSrc, "src", pr.source)
	var pkSnk string
	if err == nil {
		pkSnk, err = w.primaryKey(ctx, mem, rd.pkSnk, "sink", pr.sink)
	}
	if errors.Is(err, clients.ErrNotFound) || notSupported(err) {
		// No primary key, or a composite one: no single-key lookup is one row.
		w.driftOff(mem, t.p.ID, offRowTable+pr.source, "row_drift", "no single-column primary key on "+pr.source+" or "+pr.sink)
		return nil, nil
	}
	if err != nil {
		return unknown(err)
	}
	if piiColumn(pkSrc) || piiColumn(pkSnk) {
		// A table keyed by personal data (an email, a phone) is never
		// sampled: the keys would be the personal data itself
		// (docs/design/lineage.md section 3).
		w.driftOff(mem, t.p.ID, offRowTable+pr.source, "row_drift", "the primary key column looks like personal data")
		return nil, nil
	}
	main, err := w.compareRows(ctx, t, rd, pr, win, pkSrc, pkSnk, timeColumns(cat, ts))
	if err != nil {
		return unknown(err)
	}
	parts := []rowPart{main}
	if win.probe {
		p, err := w.landedProbe(ctx, t, rd, pr, win, pkSrc, pkSnk)
		if notSupported(err) {
			return nil, err
		}
		if err != nil {
			p = rowPart{v: vUnknown, err: err}
		}
		parts = append(parts, p)
	}
	return parts, nil
}

func (w *Watcher) primaryKey(ctx context.Context, mem *driftMem, pk primaryKeyer, side, table string) (string, error) {
	if col, ok := mem.pk[side+"|"+table]; ok {
		return col, nil
	}
	col, err := call(ctx, w.cfg.CheckTimeout, "primary key", func(c context.Context) (string, error) {
		return pk.PrimaryKeyColumn(c, table)
	})
	if err != nil {
		return "", err
	}
	mem.pk[side+"|"+table] = col
	return col, nil
}

// compareRows samples settled source keys (sink_ts_column in the row window,
// the half-open [since, cutoff) SampleKeys bounds) and compares their
// timestamp columns cols with the sink's. Keys live only in this function.
func (w *Watcher) compareRows(ctx context.Context, t target, rd driftReaders, pr tablePair, win pairWindows, pkSrc, pkSnk string, cols []string) (rowPart, error) {
	ts := t.p.SinkTSColumn
	since, cutoff := win.rowSince, win.rowCutoff
	keys, err := call(ctx, w.cfg.CheckTimeout, "sample source keys", func(c context.Context) ([]string, error) {
		return rd.src.SampleKeys(c, pr.source, pkSrc, ts, since, cutoff, driftSampleKeys)
	})
	if err != nil {
		return rowPart{}, err
	}
	if len(keys) == 0 {
		return rowPart{v: vHold, value: "no source rows to sample"}, nil
	}
	srcT, err := call(ctx, w.cfg.CheckTimeout, "source row times", func(c context.Context) (map[string]map[string]time.Time, error) {
		return rd.src.RowTimes(c, pr.source, pkSrc, keys, cols)
	})
	if err != nil {
		return rowPart{}, err
	}
	snkT, err := call(ctx, w.cfg.CheckTimeout, "sink row times", func(c context.Context) (map[string]map[string]time.Time, error) {
		return rd.snk.RowTimes(c, pr.sink, pkSnk, keys, cols)
	})
	if err != nil {
		return rowPart{}, err
	}
	sample, missing, mismatched := 0, 0, 0
	for _, k := range keys {
		st, ok := srcT[k]
		at, has := st[ts]
		if !ok || !has || !at.Before(cutoff) {
			// Deleted since the sample, or updated since (its new time is
			// still on its way to the sink).
			continue
		}
		sample++
		kt, ok := snkT[k]
		if !ok {
			missing++
			continue
		}
		for _, c := range cols {
			if !sameTime(st, kt, c) {
				mismatched++
				break
			}
		}
	}
	p := rowPart{
		sample: sample, missing: missing, mismatched: mismatched, from: since, to: cutoff,
		value: fmt.Sprintf("%d of %d sampled keys missing, %d with differing timestamps", missing, sample, mismatched),
	}
	switch {
	case sample == 0:
		p.v = vHold
	case missing == 0 && mismatched == 0:
		p.v = vClear
	default:
		p.v = vBreach
		p.label = fmt.Sprintf("%d of %d sampled keys missing on the sink, %d with differing timestamps", missing, sample, mismatched)
		p.detail = fmt.Sprintf("%d timestamp columns compared", len(cols))
	}
	return p, nil
}

// sameTime: column c is absent on both rows, or present on both within
// driftTimeTolerance.
func sameTime(a, b map[string]time.Time, c string) bool {
	ta, oka := a[c]
	tb, okb := b[c]
	if oka != okb {
		return false
	}
	if !oka {
		return true
	}
	d := ta.Sub(tb)
	return d < driftTimeTolerance && d > -driftTimeTolerance
}

// timeColumns are the timestamp columns row_drift compares: ts first, then
// the date/time columns both tables have (when both catalogs were read),
// never one whose name looks like personal data, at most maxDriftColumns.
// Without the catalogs it is ts alone.
func timeColumns(cat *catalogs, ts string) []string {
	cols := []string{ts}
	if cat == nil {
		return cols
	}
	onSink := map[string]bool{}
	for _, c := range cat.snk {
		if timeType(c.Type) {
			onSink[c.Name] = true
		}
	}
	var more []string
	for _, c := range cat.src {
		if c.Name != ts && timeType(c.Type) && onSink[c.Name] && !piiColumn(c.Name) && validColumn(c.Name) {
			more = append(more, c.Name)
		}
	}
	slices.Sort(more)
	more = slices.Compact(more)
	return append(cols, more[:min(len(more), maxDriftColumns-1)]...)
}

// timeType: a catalog type that is a point in time (what RowTimes reads).
func timeType(t string) bool {
	t = strings.ToLower(strings.TrimSpace(t))
	return t == "date" || strings.HasPrefix(t, "timestamp") || strings.HasPrefix(t, "datetime")
}

// validColumn is the Reconciler's identifier rule, so a catalog name it would
// refuse never fails the whole call.
func validColumn(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

// piiWords are column-name words of X7c's personal-data classes (email,
// phone, name, address, national id, card, ip, date of birth). row_drift
// never reads a column whose name has one (a `birth_date` or
// `email_verified_at` is a date, but of a person): the name alone decides,
// so a long PII tag list that was cut cannot let one through.
var piiWords = []string{"email", "mail", "phone", "mobile", "tel", "fax", "name", "address", "addr", "street",
	"city", "zip", "postcode", "postal", "ssn", "sin", "nin", "national", "tax", "passport", "license", "licence",
	"card", "pan", "iban", "cvv", "ip", "birth", "birthday", "birthdate", "dob", "born"}

// piiSubstrings catch the same words run together ("dateofbirth", "emailat").
var piiSubstrings = []string{"birth", "email", "phone", "passport", "address"}

func piiColumn(name string) bool {
	n := strings.ToLower(name)
	for _, s := range piiSubstrings {
		if strings.Contains(n, s) {
			return true
		}
	}
	for _, word := range strings.FieldsFunc(n, func(r rune) bool { return r == '_' || r == '-' || r == '.' || r == ' ' }) {
		if slices.Contains(piiWords, word) {
			return true
		}
	}
	return false
}

// checkTopics scans the pipeline's CDC topics for dup_keys and order_skew.
func (w *Watcher) checkTopics(ctx context.Context, now time.Time, t target, mem *driftMem, ds clients.DupScanner) []driftResult {
	var topics []string
	for _, topic := range t.cdcTopics {
		if t.scope.CheckTopic(topic) == nil {
			topics = append(topics, topic)
		}
	}
	topics = rotate(topics, &mem.nextTopic, maxDriftTopics)
	if len(topics) == 0 {
		return nil
	}
	budget := max(clients.DupScanMaxScan/len(topics), minDupScan)
	var out []driftResult
	for _, topic := range topics {
		if ctx.Err() != nil {
			break
		}
		since := mem.scanned[topic]
		if since.IsZero() {
			since = now.Add(-t.cfg.ReconcileInterval)
		}
		// The scan cuts itself at DupScanTimeout and answers what it read;
		// the extra second lets that answer arrive before call gives up.
		rep, err := call(ctx, w.cfg.CheckTimeout+time.Second, "duplicate scan", func(c context.Context) (clients.DupReport, error) {
			return ds.DupScan(c, topic, since, budget)
		})
		if notSupported(err) {
			w.driftOff(mem, t.p.ID, offDup, "dup_keys,order_skew", "the duplicate scan is not supported by this client")
			return out
		}
		for _, kind := range []string{events.SignalDupKeys, events.SignalOrderSkew} {
			if !driftSignalCfg(t.cfg, kind).Enabled {
				continue
			}
			r := reading{v: vUnknown, err: err}
			if err == nil {
				r = dupReading(kind, rep, since, now)
			}
			out = append(out, driftResult{driftID(kind, topic), r})
		}
		if err == nil {
			mem.scanned[topic] = now
		}
	}
	return out
}

// dupReading maps a DupReport onto one signal as docs/api.md says: counts
// only; fingerprints and offsets stay out. Records with no usable key or
// position never fire, and a window with none decodable says nothing.
func dupReading(kind string, rep clients.DupReport, since, now time.Time) reading {
	count, keys, what := rep.Duplicates, rep.Keys, "duplicate events"
	if kind == events.SignalOrderSkew {
		count, keys, what = rep.OrderRegressions, rep.RegressionKeys, "position regressions"
	}
	r := reading{value: fmt.Sprintf("%d %s in %d records", count, what, rep.Scanned), threshold: "0"}
	switch {
	case rep.Scanned-rep.Undecodable-rep.Tombstones <= 0:
		r.v = vHold
		return r
	case count == 0:
		r.v = vClear
		return r
	}
	r.v = vBreach
	r.sev = events.SignalSeverity(kind)
	r.label = fmt.Sprintf("%s topic %s: %d %s (%d keys)", strings.ReplaceAll(kind, "_", " "), rep.Topic, count, what, keys)
	sig := events.DriftSignal{
		Kind: kind, Severity: r.sev, Topic: rep.Topic,
		Window: events.DriftWindow{From: since.UTC(), To: now.UTC()}, Source: int64(rep.Scanned), Sample: keys,
	}
	if kind == events.SignalDupKeys {
		sig.Missing = count
	} else {
		sig.Mismatched = count
	}
	var notes []string
	if rep.Capped {
		notes = append(notes, fmt.Sprintf("scan capped at %d records", rep.MaxScan))
	}
	if rep.Partial {
		notes = append(notes, "scan cut by its deadline")
	}
	if rep.Undecodable > 0 {
		notes = append(notes, fmt.Sprintf("%d records without a usable key or position", rep.Undecodable))
	}
	sig.Detail = strings.Join(notes, "; ")
	r.ev = driftEvidence(sig)
	return r
}

// topicOf is the CDC topic of a source table, "" without a topic prefix.
func topicOf(t target, table string) string {
	if t.p.TopicPrefix == "" {
		return ""
	}
	return t.p.TopicPrefix + "." + table
}

// driftEvidence turns a DriftSignal into the evidence map compose copies.
// Detail is cut to maxDriftDetail bytes on a rune boundary.
func driftEvidence(s events.DriftSignal) map[string]any {
	s.Detail = clean(s.Detail, maxDriftDetail)
	for len(s.Detail) > maxDriftDetail {
		_, size := utf8.DecodeLastRuneInString(s.Detail)
		s.Detail = s.Detail[:len(s.Detail)-size]
	}
	s.Window.From, s.Window.To = s.Window.From.UTC(), s.Window.To.UTC()
	raw, _ := json.Marshal(s) // names, numbers and times: cannot fail
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

// driftCooling: a room carrying this drift signal closed less than the
// cooldown ago. The memory is the watcher's own (ackLocked), so it starts
// empty at process start.
func (w *Watcher) driftCooling(now time.Time, pid, id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	ps := w.pipes[pid]
	if ps == nil {
		return false
	}
	at := ps.driftClosed[id]
	return !at.IsZero() && now.Sub(at) < w.cfg.Cooldown
}

// driftOnly: every active signal of the view is a drift signal.
func (v view) driftOnly() bool {
	for _, s := range v.active {
		if driftKind(s.id) == "" {
			return false
		}
	}
	return len(v.active) > 0
}

// driftOpen prepares a room open for a view of drift signals alone: the ones
// in their own (pipeline, table, kind) cooldown are left out. ok is false
// when nothing is left to open a room for.
func (w *Watcher) driftOpen(now time.Time, pid string, v view) (view, bool) {
	kept := v.active[:0:0]
	for _, s := range v.active {
		if s.pending && w.driftCooling(now, pid, s.id) {
			continue
		}
		kept = append(kept, s)
	}
	v.active = kept
	v.sev = "sev9"
	for _, s := range kept {
		v.sev = moreSevere(v.sev, s.sev)
	}
	return v, len(v.pending()) > 0
}
