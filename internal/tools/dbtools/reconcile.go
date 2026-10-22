package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

const (
	// maxReconcileWindow bounds since..until.
	maxReconcileWindow = 6 * time.Hour
	// reconcileBuckets is the most rows the comparison table has.
	reconcileBuckets = 12
	// reconcileTolerance is the share of a bucket's source rows the sink or the
	// topic may lack before the bucket counts as a gap (updates and late rows
	// make the three counts differ a little).
	reconcileTolerance = 0.02
	// defaultSettle is how long a source row may legitimately still be on its
	// way to the sink. The window is cut that long before now, so the rows
	// of a healthy pipeline that are still in flight are never counted as
	// lost; everything older is judged as before. maxSettle bounds the
	// optional "settle" input.
	defaultSettle = 2 * time.Minute
	maxSettle     = time.Hour
	// reconcileKeyCap bounds the distinct keys one side may hold in the
	// window before the key comparison is refused. Keys are the exact row
	// count (one key is one row, updates included), so the diff is only
	// reported when neither side can have more keys than were read; past the
	// cap the agent narrows the window instead of reading a truncated diff
	// as "no more missing rows" (the confident-wrong-number failure, #409).
	reconcileKeyCap = 2000
	// reconcileKeySample is the most keys a summary or Data lists per side.
	reconcileKeySample = 10
)

// ReconcileRow is one bucket of the comparison. Topic is -1 when topic rates
// were not available.
type ReconcileRow struct {
	Bucket time.Time `json:"bucket"`
	Source int64     `json:"source"`
	Topic  int64     `json:"topic"`
	Sink   int64     `json:"sink"`
}

// ReconcileData is the detail behind a reconcile result: the bucket table and,
// when both clients can list keys, the exact key diff.
type ReconcileData struct {
	Buckets []ReconcileRow `json:"buckets"`
	Keys    *ReconcileKeys `json:"keys,omitempty"`
}

// ReconcileKeys is the key-level comparison of source and sink rows over the
// same window. One key is one row, however often the row was updated, so
// Source and Sink are row counts, not change-event counts. Missing and Extra
// are exact (a window past reconcileKeyCap leaves the key section out with a
// note instead of reporting a truncated diff); MissingKeys and ExtraKeys list
// at most reconcileKeySample of them, and they are always primary-key values.
type ReconcileKeys struct {
	Column      string   `json:"column"`      // source key column
	SinkColumn  string   `json:"sink_column"` // sink key column
	Source      int64    `json:"source"`
	Sink        int64    `json:"sink"`
	Missing     int64    `json:"missing"` // in the source, not in the sink
	Extra       int64    `json:"extra"`   // in the sink, not in the source
	MissingKeys []string `json:"missing_keys,omitempty"`
	ExtraKeys   []string `json:"extra_keys,omitempty"`
}

func reconcileTool(d Deps) tools.Tool {
	return &tool{
		name: "reconcile",
		desc: "Compare row counts per time bucket (at most 12) between the source table, the Kafka topic and the warehouse sink table, " +
			"and say whether the topic is short of rows (connector side) or the side is unproven (confirm with warehouse_row before naming a writer). " +
			"Counts only, plus the compared key column's values; never another column value. Source and sink counts use the pipeline's sink_ts_column; the topic count is the number of change events, " +
			"which is not the same as rows: an update or a delete adds an event without adding a row, so a topic that carried at least as many events as the source had rows does not prove where the loss happened " +
			"(the verdict then says the side is unproven: confirm one missing key with warehouse_row before naming a writer). " +
			"Rows newer than the settle margin (default 2m) are treated as still in flight and not judged: the window is cut that long before now. If the consumer lags more than that, pass a larger settle or an earlier until. " +
			"When both tables report a single-column primary key, it also compares the key sets over the same window and lists the rows in the source but not in the sink's window and the sink rows the source does not have (at most 2000 keys per side; a larger window leaves the key section out with a note, so narrow it). " +
			"key_column, when given, must name the table's primary key; it can never read another column. The key sets are compared as text (id::text), so a sink that stores the key in another type or letter case can show the same row as missing and extra.",
		schema: objSchema(map[string]any{
			"table":      strProp("Source table, \"schema.table\", one of the room's source tables."),
			"since":      strProp("Start of the window, RFC 3339. The window is at most 6 hours."),
			"until":      strProp("End of the window, RFC 3339. Default: now minus settle. Never later than now minus settle."),
			"settle":     strProp("Optional margin for rows still in flight, a Go duration such as \"5m\" (0 to 1h). Default 2m."),
			"sink_table": strProp("Sink table, only needed when the pipeline has several sink tables and none has the source table's name."),
			"key_column": strProp("Primary key column to compare. Optional, and only accepted when it is the table's primary key; default: each table's own primary key."),
		}, "table", "since"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				Table     string `json:"table"`
				Since     string `json:"since"`
				Until     string `json:"until"`
				Settle    string `json:"settle"`
				SinkTable string `json:"sink_table"`
				KeyColumn string `json:"key_column"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if !validTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\"", tools.ErrBadInput)
			}
			if input.KeyColumn != "" && !validColumn(input.KeyColumn) {
				return tools.Result{}, fmt.Errorf("%w: invalid key_column", tools.ErrBadInput)
			}
			since, err := parseRFC3339(input.Since)
			if err != nil {
				return tools.Result{}, err
			}
			settle := defaultSettle
			if input.Settle != "" {
				if settle, err = time.ParseDuration(input.Settle); err != nil || settle < 0 || settle > maxSettle {
					return tools.Result{}, fmt.Errorf("%w: settle must be a duration from 0 to %s", tools.ErrBadInput, maxSettle)
				}
			}
			now := time.Now()
			until := now
			if input.Until != "" {
				if until, err = parseRFC3339(input.Until); err != nil {
					return tools.Result{}, err
				}
			}
			if !until.After(since) {
				return tools.Result{}, fmt.Errorf("%w: until must be after since", tools.ErrBadInput)
			}
			// Cut the window at now−settle. Only that slice goes unjudged; the
			// rest of the newest bucket is still compared.
			var note string
			if cut := now.Add(-settle); until.After(cut) {
				if cut.Sub(since) < time.Minute { // under one bucket of settled data
					return tools.Result{}, fmt.Errorf("%w: no verdict possible: the window ends %s before now (rows newer than that are still in flight) but less than a minute of it is after since; move since back or lower settle", tools.ErrBadInput, settle)
				}
				until = cut
				note = fmt.Sprintf("\nnote: rows newer than %s (the last %s) are in flight and not judged", topology.FormatTS(cut), settle)
			}
			if until.Sub(since) > maxReconcileWindow {
				return tools.Result{}, fmt.Errorf("%w: the window is %s, at most %s", tools.ErrBadInput, until.Sub(since).Round(time.Minute), maxReconcileWindow)
			}
			if input.SinkTable != "" && !d.validWarehouseTable(input.SinkTable) {
				return tools.Result{}, fmt.Errorf("%w: invalid sink_table", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			if sc.Unscoped() {
				return tools.Result{}, fmt.Errorf("%w: reconcile needs the room's pipeline", tools.ErrBadInput)
			}
			if err := sc.CheckSourceTable(input.Table); err != nil {
				return tools.Result{}, err
			}
			p := sc.Pipeline()
			if p.SinkTSColumn == "" {
				return tools.Result{}, fmt.Errorf("%w: no timestamp column configured (sink_ts_column) for pipeline %q", tools.ErrBadInput, p.ID)
			}
			sink, err := pickSinkTable(input.Table, input.SinkTable, p.SinkTables)
			if err != nil {
				return tools.Result{}, err
			}
			if err := sc.CheckSinkTable(sink); err != nil {
				return tools.Result{}, err
			}
			topic := p.TopicPrefix + "." + input.Table

			src, ok := d.Src.(clients.CountReader)
			if !ok {
				return tools.Result{Summary: "source counts are not supported by this client"}, nil
			}
			snk, ok := d.Wh.(clients.CountReader)
			if !ok {
				return tools.Result{Summary: "sink counts are not supported by this client"}, nil
			}
			ctx, cancel := toolCtx(ctx)
			defer cancel()

			width, n := reconcileGrid(since, until)
			// The bucket counts and the key diff read the same window from the
			// same two systems; run them together so one slow side does not
			// spend the other's deadline.
			var (
				wg        sync.WaitGroup
				rows      []ReconcileRow
				countsErr error
				keys      reconcileKeyDiff
			)
			wg.Add(2)
			go func() {
				defer wg.Done()
				rows, countsErr = d.reconcileCounts(ctx, src, snk, input.Table, sink, topic, p.SinkTSColumn, since, until, width, n)
			}()
			go func() {
				defer wg.Done()
				keys = d.reconcileKeyDiff(ctx, input.Table, sink, p.SinkTSColumn, input.KeyColumn, since, until)
			}()
			wg.Wait()
			if countsErr != nil {
				return tools.Result{}, countsErr
			}
			if keys.err != nil {
				// The only key-diff error is a refused key_column (ErrBadInput);
				// every other key problem is a note beside the bucket table.
				return tools.Result{}, keys.err
			}
			data := ReconcileData{Buckets: rows}
			if keys.have {
				data.Keys = &keys.keys
			}
			summary := reconcileSummary(input.Table, sink, topic, p.SinkTSColumn, width, rows) + keys.summary() + note
			summary, truncated := tools.Truncate(summary)
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
		},
	}
}

// pickSinkTable chooses the sink table for a source table: the explicit one,
// the only one, or the one with the same table name.
func pickSinkTable(source, explicit string, sinks []string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if len(sinks) == 1 {
		return sinks[0], nil
	}
	name := source[strings.LastIndex(source, ".")+1:]
	var match []string
	for _, s := range sinks {
		if s[strings.LastIndex(s, ".")+1:] == name {
			match = append(match, s)
		}
	}
	if len(match) == 1 {
		return match[0], nil
	}
	return "", fmt.Errorf("%w: cannot tell which of %s is the sink of %s: pass sink_table", tools.ErrBadInput, strings.Join(sinks, ", "), source)
}

// reconcileGrid returns the bucket width (whole minutes, at least one) and the
// number of buckets for the window: at most reconcileBuckets.
func reconcileGrid(since, until time.Time) (time.Duration, int) {
	w := until.Sub(since) / reconcileBuckets
	width := ((w + time.Minute - 1) / time.Minute) * time.Minute
	if width < time.Minute {
		width = time.Minute
	}
	n := int((until.Sub(since) + width - 1) / width)
	return width, n
}

// reconcileCounts reads the three counts in parallel. A topic that cannot be
// measured (no RateReader, or its read failed) gives Topic = -1 in every row.
func (d Deps) reconcileCounts(ctx context.Context, src, snk clients.CountReader, srcTable, sinkTable, topic, tsCol string, since, until time.Time, width time.Duration, n int) ([]ReconcileRow, error) {
	var (
		wg                        sync.WaitGroup
		srcCounts, snkCounts      []clients.MinuteCount
		srcErr, snkErr            error
		topicCounts               []int64
		topicErr                  error
		topicWanted, topicCapable bool
	)
	rr, topicCapable := d.Kafka.(clients.RateReader)
	topicWanted = d.Kafka != nil && topicCapable
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcCounts, srcErr = src.CountByBucket(ctx, srcTable, tsCol, since, until, width)
	}()
	go func() {
		defer wg.Done()
		snkCounts, snkErr = snk.CountByBucket(ctx, sinkTable, tsCol, since, until, width)
	}()
	if topicWanted {
		wg.Add(1)
		go func() { defer wg.Done(); topicCounts, topicErr = topicBuckets(ctx, rr, topic, since, until, width, n) }()
	}
	wg.Wait()
	if srcErr != nil {
		return nil, fmt.Errorf("dbtools: reconcile: source %s: %w", srcTable, srcErr)
	}
	if snkErr != nil {
		return nil, fmt.Errorf("dbtools: reconcile: sink %s: %w", sinkTable, snkErr)
	}
	rows := make([]ReconcileRow, n)
	for i := range rows {
		rows[i] = ReconcileRow{Bucket: since.Add(time.Duration(i) * width).UTC(), Topic: -1}
	}
	fill := func(counts []clients.MinuteCount, set func(*ReconcileRow, int64)) {
		for _, c := range counts {
			if i := int(c.Bucket.Sub(since) / width); i >= 0 && i < n {
				set(&rows[i], c.Count)
			}
		}
	}
	fill(srcCounts, func(r *ReconcileRow, v int64) { r.Source = v })
	fill(snkCounts, func(r *ReconcileRow, v int64) { r.Sink = v })
	if topicWanted && topicErr == nil {
		for i := range rows {
			rows[i].Topic = topicCounts[i]
		}
	}
	return rows, nil
}

// topicBuckets counts the records written to topic in each bucket from offsets
// alone: records since each bucket boundary, differenced. n+1 offset reads.
func topicBuckets(ctx context.Context, rr clients.RateReader, topic string, since, until time.Time, width time.Duration, n int) ([]int64, error) {
	since0 := make([]int64, n+1)
	errs := make([]error, n+1)
	var wg sync.WaitGroup
	for i := 0; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			at := since.Add(time.Duration(i) * width)
			if at.After(until) {
				at = until
			}
			rates, err := rr.PartitionRates(ctx, at, topic)
			if err != nil {
				errs[i] = err
				return
			}
			for _, r := range rates {
				since0[i] += r.Records
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			if errors.Is(err, clients.ErrNotSupported) {
				return nil, err
			}
			return nil, fmt.Errorf("topic rates: %w", err)
		}
	}
	out := make([]int64, n)
	for i := range out {
		if v := since0[i] - since0[i+1]; v > 0 {
			out[i] = v
		}
	}
	return out, nil
}

// reconcileSummary renders the table and the verdict.
func reconcileSummary(source, sink, topic, tsCol string, width time.Duration, rows []ReconcileRow) string {
	var sb strings.Builder
	haveTopic := rows[0].Topic >= 0
	fmt.Fprintf(&sb, "reconcile %s → %s → %s by %s, buckets of %s from %s\n", source, topic, sink, tsCol, width, topology.FormatTS(rows[0].Bucket))
	sb.WriteString("bucket | source rows | topic events | sink rows | sink−source\n")
	for _, r := range rows {
		t := "n/a"
		if r.Topic >= 0 {
			t = fmt.Sprint(r.Topic)
		}
		fmt.Fprintf(&sb, "%s | %d | %s | %d | %+d\n", r.Bucket.Format("15:04"), r.Source, t, r.Sink, r.Sink-r.Source)
	}
	first, last := -1, -1
	var missing, srcSum, topicSum int64
	for i, r := range rows {
		gap := r.Source - r.Sink
		if gap >= 1 && float64(gap) > reconcileTolerance*float64(r.Source) {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		sb.WriteString("verdict: the sink matches the source in every bucket (within 2%)")
		return sb.String()
	}
	for _, r := range rows[first : last+1] {
		missing += r.Source - r.Sink
		srcSum += r.Source
		topicSum += r.Topic
	}
	end := rows[last].Bucket.Add(width)
	span := fmt.Sprintf("%s–%s", rows[first].Bucket.Format("15:04"), end.Format("15:04"))
	// The topic column counts change events, not rows: an update or a delete
	// adds an event without adding a row, so events >= source rows is the
	// normal state of a healthy pipeline that updates anything. Only a topic
	// that carried FEWER events than the source had rows is evidence of the
	// connector losing them; the other direction bounds the loss but does not
	// prove where it happened, so the side is left open and the next check is
	// named (#482, KAN-482).
	switch {
	case !haveTopic:
		fmt.Fprintf(&sb, "verdict: sink is missing %d rows in %s; topic counts are not available, so the side is unknown", missing, span)
	case float64(topicSum) < (1-reconcileTolerance)*float64(srcSum):
		fmt.Fprintf(&sb, "verdict: sink is missing %d rows in %s; the topic carried only %d change events for %d source rows (connector side)", missing, span, topicSum, srcSum)
	default:
		fmt.Fprintf(&sb, "verdict: sink is missing %d rows in %s; the topic carried %d change events for %d source rows, so the rows may be there, but events include updates and deletes: the side is unproven, confirm one missing key with warehouse_row before naming a writer", missing, span, topicSum, srcSum)
	}
	return sb.String()
}
