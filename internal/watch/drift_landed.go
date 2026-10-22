package watch

// The parts of the drift checks that catch the silent-corruption breaks the
// first version missed on a live stack (X1b-fix, KAN-416; dev/scenarios.md
// sections 14 to 17):
//
//   - An idle sink. The windows were shifted back by the sink's lag measured
//     from its newest row, so with no new writes the lag grew every run and
//     the rows a consumer skipped never fell in a window. When the source has
//     no row newer than the sink's newest, and the sink's newest is older than
//     rowSettle, nothing is in flight: the windows end at the sink's newest
//     row instead (driftWindows). A sink still behind the source keeps the
//     shifted windows, so a row in flight is never judged.
//   - A sink that stopped writing sink_ts_column. Its rows land with the
//     column NULL, so the sink's newest row stays put and looks like lag, and
//     every shifted window slides behind the break. landedProbe samples the
//     source rows of the last hour by the wall clock and counts the ones the
//     sink holds with the column NULL (a row the sink does not hold may be in
//     flight and is not judged). structurePart compares the column sets: the
//     sink's catalog has lost sink_ts_column, or a column both sides had.
//   - A TOASTed column whose unchanged value Debezium sent as its
//     unavailable.value.placeholder. The timestamps agree, so only a count of
//     the placeholder in the sink's copy of the source's toastable columns
//     sees it (placeholderPart).
//
// All of them are row_drift: docs/api.md has no other kind for rows that are
// wrong on the sink. Counts and column names only, never a key or a value.

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
	"piperoom/internal/events"
)

const (
	// debeziumPlaceholder is Debezium's default unavailable.value.placeholder.
	// A connector that sets another one is not counted (see the report gaps).
	debeziumPlaceholder = "__debezium_unavailable_value"
	// maxPlaceholderColumns bounds the placeholder counts of one pair per run;
	// a table with more toastable columns has them counted in turn.
	maxPlaceholderColumns = 4
	// maxDriftNames bounds the column names one title or detail lists.
	maxDriftNames = 3
)

// More off keys of driftMem.
const (
	offSrcFresh    = "source_freshness"
	offPlaceholder = "placeholder"
)

// freshnessReader is the source client's newest-row read (the postgres
// client has it for warehouse_freshness; not in clients.SourceReader, so it
// is asserted like dbtools' sourceFreshnessReader).
type freshnessReader interface {
	Freshness(ctx context.Context, table, tsColumn string) (time.Time, error)
}

// pairWindows are the half-open windows one pair's checks read.
type pairWindows struct {
	lag  time.Duration // the sink's lag: now - its newest row
	idle bool          // the source has nothing newer than the sink's newest row
	// count_drift's window.
	countFrom, countTo time.Time
	// row_drift's sample window: rows that had time to reach the sink.
	rowSince, rowCutoff time.Time
	// landed is the window of the checks that judge rows already on the sink
	// (a NULL sink_ts_column, the placeholder): such a row is wrong whatever
	// the lag, so it is not shifted by it.
	landedSince, landedUntil time.Time
	// probe: the sink looks behind by more than rowSettle, so the sample
	// window misses the last hour and landedProbe looks at it.
	probe bool
}

// driftWindows are a pair's windows. On a busy sink count_drift reads the
// last closed hour and row_drift the hour before now - settle, both shifted
// back by the sink's lag. On an idle sink (sinkIdle) both read the hour that
// ends just after the sink's newest row (whole seconds), so a break on a
// stack with no traffic stays in the windows until new rows arrive.
func driftWindows(now, fresh, srcNewest time.Time) pairWindows {
	now = now.UTC()
	win := pairWindows{lag: sinkLag(now, fresh)}
	if sinkIdle(now, fresh, srcNewest) {
		end := fresh.UTC().Truncate(time.Second).Add(time.Second)
		from := end.Add(-driftWindow)
		win.idle = true
		win.countFrom, win.countTo = from, end
		win.rowSince, win.rowCutoff = from, end
		win.landedSince, win.landedUntil = from, end
		return win
	}
	win.countFrom, win.countTo = countWindow(now, win.lag)
	win.rowCutoff = now.Add(-win.lag - rowSettle).Truncate(time.Second)
	win.rowSince = win.rowCutoff.Add(-driftWindow)
	win.landedUntil = now.Add(-rowSettle).Truncate(time.Second)
	win.landedSince = win.landedUntil.Add(-driftWindow)
	win.probe = win.lag > rowSettle
	return win
}

// sinkIdle: the sink's newest row is older than rowSettle and the source has
// no row newer than it (beyond driftTimeTolerance). Nothing is then on its way
// to the sink, so the sink's newest row bounds the rows that must be there. A
// zero srcNewest (not read, or an empty source table) is never idle.
func sinkIdle(now, fresh, srcNewest time.Time) bool {
	return !fresh.IsZero() && !srcNewest.IsZero() && fresh.Before(now.Add(-rowSettle)) &&
		!srcNewest.After(fresh.Add(driftTimeTolerance))
}

// sourceNewest is the source table's newest sink_ts_column, read only when it
// can make the sink idle (the sink's newest row is older than rowSettle);
// zero when not read. A source client without the read leaves the windows
// shifted by the lag; one that answers ErrNotSupported too, after one INFO
// line. Any other error is the pair's unknown reading.
func (w *Watcher) sourceNewest(ctx context.Context, now time.Time, t target, mem *driftMem, rd driftReaders, pr tablePair, fresh time.Time) (time.Time, error) {
	if rd.srcFresh == nil || mem.off[offSrcFresh] || fresh.IsZero() || !fresh.Before(now.Add(-rowSettle)) {
		return time.Time{}, nil
	}
	at, err := call(ctx, w.cfg.CheckTimeout, "source freshness", func(c context.Context) (time.Time, error) {
		return rd.srcFresh.Freshness(c, pr.source, t.p.SinkTSColumn)
	})
	if notSupported(err) {
		w.driftOff(mem, t.p.ID, offSrcFresh, "idle-sink windows", "the source client cannot read a table's newest row")
		return time.Time{}, nil
	}
	return at, err
}

// catalogs are the column lists of one pair's two tables.
type catalogs struct{ src, snk []clients.ColumnInfo }

// readCatalogs reads both tables' columns when both clients can (nil
// otherwise, or when one answers ErrNotSupported).
func (w *Watcher) readCatalogs(ctx context.Context, rd driftReaders, pr tablePair) (*catalogs, error) {
	if rd.colSrc == nil || rd.colSnk == nil {
		return nil, nil
	}
	src, err := call(ctx, w.cfg.CheckTimeout, "source columns", func(c context.Context) ([]clients.ColumnInfo, error) {
		return rd.colSrc.Columns(c, pr.source)
	})
	if err == nil {
		var snk []clients.ColumnInfo
		if snk, err = call(ctx, w.cfg.CheckTimeout, "sink columns", func(c context.Context) ([]clients.ColumnInfo, error) {
			return rd.colSnk.Columns(c, pr.sink)
		}); err == nil {
			return &catalogs{src, snk}, nil
		}
	}
	if notSupported(err) {
		return nil, nil
	}
	return nil, err
}

func columnSet(cols []clients.ColumnInfo) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c.Name] = true
	}
	return m
}

// structurePart compares the sink's columns with the source's: sink_ts_column
// missing on the sink (tsGone: owners.yaml says the sink has it), or a column
// both sides had at an earlier read of this process gone from the sink while
// the source still has it. The baseline lives in mem and starts again at a
// restart, as config_drift's does; a column only one side ever had is a
// projection, not drift, and one gone from both sides is a schema change. A
// catalog fact has no row window: its window is the instant of the read.
func structurePart(now time.Time, mem *driftMem, pr tablePair, ts string, cat *catalogs) (rowPart, bool) {
	onSrc, onSnk := columnSet(cat.src), columnSet(cat.snk)
	base := mem.cols[pr.source]
	if base == nil {
		base = map[string]bool{}
		mem.cols[pr.source] = base
	}
	for n := range onSrc {
		if onSnk[n] {
			base[n] = true
		}
	}
	var gone []string
	for n := range base {
		switch {
		case !onSrc[n]:
			delete(base, n)
		case !onSnk[n]:
			gone = append(gone, n)
		}
	}
	// A sink that folds names (UPDATED_AT for updated_at) still has the
	// column: only a name missing in any case is gone.
	tsGone := !onSnk[ts] && !slices.ContainsFunc(cat.snk, func(c clients.ColumnInfo) bool { return strings.EqualFold(c.Name, ts) })
	if tsGone && !slices.Contains(gone, ts) {
		gone = append(gone, ts)
	}
	if len(gone) == 0 {
		return rowPart{v: vClear, value: "sink has the source's columns"}, false
	}
	slices.Sort(gone)
	at := now.UTC().Truncate(time.Second)
	shown := make([]string, len(gone))
	for i, n := range gone {
		shown[i] = columnLabel(n)
	}
	names := listNames(shown)
	return rowPart{
		v: vBreach, from: at, to: at,
		value:  fmt.Sprintf("%d %s missing on the sink", len(gone), plural(len(gone), "column", "columns")),
		label:  fmt.Sprintf("%d %s missing on the sink (%s)", len(gone), plural(len(gone), "column", "columns"), names),
		detail: "columns missing on the sink: " + names,
	}, tsGone
}

// landedProbe samples the source rows of the landed window (by the wall
// clock, not shifted by the lag) and counts those the sink holds with
// sink_ts_column NULL. A row the sink does not hold may be in flight and is
// not judged; nor is one it holds with another time (an update in flight).
// The source never has the column NULL for a sampled row (SampleKeys skips
// NULL), so a NULL on the sink is a load that dropped the column. Keys live
// only in this function.
func (w *Watcher) landedProbe(ctx context.Context, t target, rd driftReaders, pr tablePair, win pairWindows, pkSrc, pkSnk string) (rowPart, error) {
	ts := t.p.SinkTSColumn
	keys, err := call(ctx, w.cfg.CheckTimeout, "sample landed source keys", func(c context.Context) ([]string, error) {
		return rd.src.SampleKeys(c, pr.source, pkSrc, ts, win.landedSince, win.landedUntil, driftSampleKeys)
	})
	if err != nil {
		return rowPart{}, err
	}
	if len(keys) == 0 {
		return rowPart{v: vHold, value: "no recent source rows to probe"}, nil
	}
	snkT, err := call(ctx, w.cfg.CheckTimeout, "sink row times", func(c context.Context) (map[string]map[string]time.Time, error) {
		return rd.snk.RowTimes(c, pr.sink, pkSnk, keys, []string{ts})
	})
	if err != nil {
		return rowPart{}, err
	}
	landed, nulls := 0, 0
	for _, k := range keys {
		row, ok := snkT[k]
		if !ok {
			continue
		}
		landed++
		if _, has := row[ts]; !has {
			nulls++
		}
	}
	p := rowPart{
		sample: landed, from: win.landedSince, to: win.landedUntil,
		value: fmt.Sprintf("%d of %d recently landed rows without %s", nulls, landed, ts),
	}
	switch {
	case landed == 0:
		p.v = vHold
	case nulls == 0:
		p.v = vClear
	default:
		p.v, p.mismatched = vBreach, nulls
		p.label = fmt.Sprintf("%d of %d recently landed rows have %s NULL on the sink", nulls, landed, ts)
		p.detail = fmt.Sprintf("%s NULL on %d recently landed rows", ts, nulls)
	}
	return p, nil
}

// placeholderPart counts Debezium's unavailable-value placeholder in the
// sink's copy of the source table's toastable columns (ToastColumnReader on
// the source, PlaceholderCounter on the sink) over the landed window: a row
// on the sink holding it has lost the real value whatever the lag. At most
// maxPlaceholderColumns columns per run, in turn; never sink_ts_column, a
// column whose name looks like personal data, or one the sink's catalog does
// not have. nil when a reader is missing, the check is off, or no column was
// counted.
func (w *Watcher) placeholderPart(ctx context.Context, t target, mem *driftMem, rd driftReaders, pr tablePair, win pairWindows, cat *catalogs) []rowPart {
	if rd.toast == nil || rd.ph == nil || mem.off[offPlaceholder] {
		return nil
	}
	ts := t.p.SinkTSColumn
	tabs, err := call(ctx, w.cfg.CheckTimeout, "toast columns", func(c context.Context) ([]clients.ToastTable, error) {
		out, _, err := rd.toast.ToastColumns(c, []string{pr.source})
		return out, err
	})
	if notSupported(err) {
		w.driftOff(mem, t.p.ID, offPlaceholder, "row_drift placeholder count", "the source client cannot read TOAST columns")
		return nil
	}
	if err != nil {
		return []rowPart{{v: vUnknown, err: err}}
	}
	var onSnk map[string]bool
	if cat != nil {
		onSnk = columnSet(cat.snk)
	}
	var cols []string
	for _, tt := range tabs {
		if !tt.Found || tt.Table != pr.source {
			continue
		}
		for _, c := range tt.Columns {
			if c.Name != ts && validColumn(c.Name) && !piiColumn(c.Name) && (onSnk == nil || onSnk[c.Name]) {
				cols = append(cols, c.Name)
			}
		}
	}
	if len(cols) == 0 {
		return nil
	}
	next := mem.nextPh[pr.source]
	cols = rotate(cols, &next, maxPlaceholderColumns)
	mem.nextPh[pr.source] = next
	var hit []string
	var total int64
	counted := 0
	for _, col := range cols {
		if ctx.Err() != nil {
			break
		}
		n, err := call(ctx, w.cfg.CheckTimeout, "placeholder count", func(c context.Context) (int64, error) {
			return rd.ph.CountPlaceholder(c, pr.sink, col, debeziumPlaceholder, ts, win.landedSince, win.landedUntil)
		})
		switch {
		case notSupported(err):
			w.driftOff(mem, t.p.ID, offPlaceholder, "row_drift placeholder count", "the warehouse cannot count the placeholder")
			return nil
		case errors.Is(err, clients.ErrNotFound):
			continue // not a column of the sink: nothing there to lose
		case err != nil:
			return []rowPart{{v: vUnknown, err: err}}
		}
		counted++
		if n > 0 {
			hit = append(hit, col)
			total += n
		}
	}
	if counted == 0 {
		return nil
	}
	p := rowPart{
		from: win.landedSince, to: win.landedUntil,
		value: fmt.Sprintf("placeholder in %d of %d toastable columns", len(hit), counted),
	}
	if total == 0 {
		p.v = vClear
		return []rowPart{p}
	}
	names := listNames(hit)
	p.v, p.mismatched = vBreach, int(min(total, math.MaxInt32))
	p.label = fmt.Sprintf("%d sink rows hold Debezium's unavailable-value placeholder (%s)", total, names)
	p.detail = fmt.Sprintf("unavailable-value placeholder in %s: %d rows", names, total)
	return []rowPart{p}
}

// columnLabel shows a catalog column name in a title or detail: a plain
// identifier as it is, any other quoted in ASCII (cut to 63 bytes first), so
// a name with spaces, commas or bidirectional controls reads as one name and
// cannot reorder the room title.
func columnLabel(n string) string {
	if validColumn(n) {
		return n
	}
	if len(n) > 63 {
		n = strings.ToValidUTF8(n[:63], "")
	}
	return strconv.QuoteToASCII(n)
}

// listNames lists at most maxDriftNames names, then how many more.
func listNames(names []string) string {
	if len(names) <= maxDriftNames {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:maxDriftNames], ", ") + fmt.Sprintf(" +%d more", len(names)-maxDriftNames)
}

// rowPart is one part of a pair's row_drift reading.
type rowPart struct {
	v                           verdict // vClear, vHold, vBreach or vUnknown
	err                         error
	sample, missing, mismatched int
	from, to                    time.Time
	// label is the title fragment and detail the evidence fragment of a
	// breach; value is the log fragment.
	label, detail, value string
}

// mergeRow folds a pair's row_drift parts into one reading. A breach of any
// part is the signal: the numbers of every part that read are summed, the
// window is the first breaching part's, and each breaching part is named.
// Without a breach a failed part leaves the state alone (unknown), any clear
// part clears, and all holds hold.
func mergeRow(t target, pr tablePair, parts []rowPart) reading {
	r := reading{threshold: "0 missing, 0 differing"}
	var values, labels []string
	details := []string{"sink " + pr.sink}
	var first *rowPart
	var failed error
	clear := false
	sig := events.DriftSignal{Kind: events.SignalRowDrift, Table: pr.source, Topic: topicOf(t, pr.source)}
	for i, p := range parts {
		if p.value != "" {
			values = append(values, p.value)
		}
		switch p.v {
		case vUnknown:
			if failed == nil {
				failed = p.err
				if failed == nil {
					failed = errors.New("watch: row check unknown")
				}
			}
			continue
		case vClear:
			clear = true
		case vBreach:
			if first == nil {
				first = &parts[i]
			}
			labels = append(labels, p.label)
			details = append(details, p.detail)
		}
		sig.Sample += p.sample
		sig.Missing += p.missing
		sig.Mismatched += p.mismatched
	}
	r.value = strings.Join(values, "; ")
	switch {
	case first == nil && failed != nil:
		return reading{v: vUnknown, err: failed}
	case first == nil && clear:
		r.v = vClear
		return r
	case first == nil:
		r.v = vHold
		return r
	}
	r.v = vBreach
	r.sev = events.SignalSeverity(events.SignalRowDrift)
	r.label = "row drift " + pr.source + ": " + strings.Join(labels, "; ")
	sig.Severity = r.sev
	sig.Window = events.DriftWindow{From: first.from, To: first.to}
	sig.Detail = strings.Join(details, "; ")
	r.ev = driftEvidence(sig)
	return r
}
