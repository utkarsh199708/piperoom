package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// freshnessTolerance is how far the sink may trail the source before a
// warehouse_freshness call calls it behind. The watcher's per-pipeline
// watch.freshness_tolerance (owners.yaml, default 2m) is the planned config;
// until owners exposes it, the tool uses the same default here.
const freshnessTolerance = 2 * time.Minute

// sourceFreshnessReader is the optional source-side read warehouse_freshness
// compares the sink against (internal/clients/postgres implements it). It is a
// local adapter until clients.SourceReader gains Freshness: see the contract
// change request in the PR.
type sourceFreshnessReader interface {
	Freshness(ctx context.Context, table, tsColumn string) (time.Time, error)
}

func warehouseFreshnessTool(d Deps) tools.Tool {
	return &tool{
		name: "warehouse_freshness",
		desc: "Compare the newest timestamp column of a warehouse (sink) table with the newest timestamp of the pipeline's matching source table. " +
			"Verdicts: in sync (the sink trails the source by at most 2m), behind by X (the sink is stale: the source has newer rows), " +
			"idle (the source itself has no new writes and the sink matches it, so a quiet pipeline is not stale), " +
			"unknown (the source could not be read; only the sink's age is shown).",
		schema: objSchema(map[string]any{
			"table":     strProp("\"schema.table\" (or \"db.schema.table\" on Snowflake, Databricks or BigQuery) to inspect."),
			"ts_column": strProp("The pipeline's sink timestamp column (sink_ts_column in owners.yaml, shown as ts_column on the pipeline map); no other column is accepted."),
		}, "table", "ts_column"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				Table    string `json:"table"`
				TSColumn string `json:"ts_column"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if !d.validWarehouseTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\" (or \"db.schema.table\" on Snowflake, Databricks or BigQuery)", tools.ErrBadInput)
			}
			if !d.validWarehouseColumn(input.TSColumn) {
				return tools.Result{}, fmt.Errorf("%w: invalid ts_column", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			if err := sc.CheckSinkTable(input.Table); err != nil {
				return tools.Result{}, err
			}
			// Only the pipeline's sink_ts_column, as pg_row/warehouse_row allow
			// only the key: max() of any other column (an email, a card
			// number) reads personal data out of the sink table (#564).
			if !sc.Unscoped() {
				want := sc.Pipeline().SinkTSColumn
				if want == "" {
					return tools.Result{}, fmt.Errorf("%w: pipeline %q sets no sink_ts_column in owners.yaml, so warehouse_freshness has no column to read", tools.ErrBadInput, sc.Pipeline().ID)
				}
				if input.TSColumn != want && !(d.isSnowflake() && strings.EqualFold(input.TSColumn, want)) {
					return tools.Result{}, fmt.Errorf("%w: ts_column must be the pipeline's sink_ts_column %q", tools.ErrBadInput, want)
				}
				input.TSColumn = want
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			ts, err := d.Wh.Freshness(ctx, input.Table, input.TSColumn)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: warehouse_freshness: %w", err)
			}

			data := map[string]any{"table": input.Table, "ts_column": input.TSColumn}
			var summary string
			ahead := time.Until(ts)
			switch {
			case ts.IsZero():
				summary = "no rows (empty table)"
				data["newest"] = nil
				data["verdict"] = "unknown"
			case ts.Year() >= 9999:
				// The warehouse client's stand-in for a Postgres 'infinity':
				// no row holds this time, so neither a time nor an age is shown.
				summary = fmt.Sprintf("newest=infinity (a sentinel or bad value in %s): staleness cannot be judged", input.TSColumn)
				data["newest"] = "infinity"
				data["in_future"] = true
				data["verdict"] = "unknown"
			case ahead > topology.FutureSkew:
				// Clamped to "stale for 0s" this would read as fresh while
				// loading may have stopped: the column is not comparable with
				// this clock (skew, another time zone) or holds a bad value.
				summary = fmt.Sprintf("newest=%s is IN THE FUTURE by %s (clock skew, time zone or bad data): staleness cannot be judged from %s",
					topology.FormatTS(ts), topology.HumanAge(ahead), input.TSColumn)
				data["newest"] = topology.FormatTS(ts)
				data["in_future"] = true
				data["ahead_by"] = topology.HumanAge(ahead)
				data["verdict"] = "unknown"
			case ahead > 0:
				// The sink's clock is ahead of ours by less than FutureSkew.
				// Comparing it with the source would make the lag look
				// negative for a reason that is not pipeline health, so keep
				// the age-only answer (the #422 behaviour).
				summary = fmt.Sprintf("newest=%s (stale for %s)", topology.FormatTS(ts), topology.HumanAge(time.Since(ts)))
				data["newest"] = topology.FormatTS(ts)
				data["stale_for"] = topology.HumanAge(time.Since(ts))
				data["verdict"] = "unknown"
			default:
				summary = d.freshnessVerdict(ctx, sc, input.Table, input.TSColumn, ts, data)
			}
			summary, truncated := tools.Truncate(summary)
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
		},
	}
}

// freshnessVerdict compares the sink's newest timestamp with the source's and
// sets data["verdict"]. It never returns an error: a source that cannot be read
// falls back to the old age-only answer, labelled as such (#KAN-403, TC4),
// because the sink's age alone still tells the operator something.
func (d Deps) freshnessVerdict(ctx context.Context, sc tools.Scope, sinkTable, tsColumn string, sink time.Time, data map[string]any) string {
	sinkAge := time.Since(sink)
	data["newest"] = topology.FormatTS(sink)
	data["stale_for"] = topology.HumanAge(sinkAge)
	data["tolerance"] = topology.HumanAge(freshnessTolerance)
	fallback := func(label, reason string) string {
		data["verdict"] = "unknown"
		data["source_error"] = reason
		return fmt.Sprintf("newest=%s (stale for %s); %s, age only: %s",
			topology.FormatTS(sink), topology.HumanAge(sinkAge), label, reason)
	}
	if sc.Unscoped() {
		return fallback("source unknown", "no pipeline scope, so no source table to compare")
	}
	srcTable, err := sourceTableForSink(sc.Pipeline(), sinkTable)
	if err != nil {
		return fallback("source unknown", err.Error())
	}
	data["source_table"] = srcTable
	sr, ok := d.Src.(sourceFreshnessReader)
	if !ok {
		// The capability is optional (a fake, the eval replay, another source
		// kind): the sink's age is still shown, the verdict is not guessed.
		return fallback("source unknown", "the source client cannot read a newest timestamp")
	}
	src, err := sr.Freshness(ctx, srcTable, tsColumn)
	if err != nil {
		return fallback("source unreadable", err.Error())
	}
	return freshnessCompare(sink, src, sinkAge, data)
}

// freshnessCompare renders the verdict once both timestamps are known. Lag is
// source newest minus sink newest, so a negative lag means the sink is ahead
// (clock skew or a backfill), never "behind by -X".
func freshnessCompare(sink, src time.Time, sinkAge time.Duration, data map[string]any) string {
	if src.IsZero() {
		// The source table has no rows at all: no source activity to miss,
		// so the sink's age alone must not read as stale.
		data["verdict"] = "idle"
		data["idle"] = true
		data["source_newest"] = nil
		return fmt.Sprintf("newest=%s (idle: the source table has no rows, so the sink is not stale on its own)", topology.FormatTS(sink))
	}
	if src.Year() >= 9999 {
		data["verdict"] = "unknown"
		data["source_newest"] = "infinity"
		data["source_error"] = "source newest is infinity (a sentinel or bad value)"
		return fmt.Sprintf("newest=%s (stale for %s); source unknown, age only: %s",
			topology.FormatTS(sink), topology.HumanAge(sinkAge), data["source_error"])
	}
	if ahead := time.Until(src); ahead > topology.FutureSkew {
		data["verdict"] = "unknown"
		data["source_newest"] = topology.FormatTS(src)
		data["source_error"] = fmt.Sprintf("source newest is IN THE FUTURE by %s (clock skew, time zone or bad data)", topology.HumanAge(ahead))
		return fmt.Sprintf("newest=%s (stale for %s); source unreadable, age only: %s",
			topology.FormatTS(sink), topology.HumanAge(sinkAge), data["source_error"])
	}
	lag := src.Sub(sink)
	data["source_newest"] = topology.FormatTS(src)
	data["source_age"] = topology.HumanAge(time.Since(src))
	data["lag_seconds"] = int64(lag / time.Second)
	switch {
	case lag > freshnessTolerance:
		// The source has rows the sink lacks: a real freshness failure.
		data["verdict"] = "behind"
		return fmt.Sprintf("newest=%s (stale: behind by %s; source newest=%s)",
			topology.FormatTS(sink), topology.HumanAge(lag), topology.FormatTS(src))
	case lag < 0:
		data["verdict"] = "in sync"
		return fmt.Sprintf("newest=%s (in sync (sink ahead by %s); source newest=%s)",
			topology.FormatTS(sink), topology.HumanAge(-lag), topology.FormatTS(src))
	case time.Since(src) > freshnessTolerance:
		// The source itself is quiet and the sink matches it: idle, not stale.
		data["verdict"] = "idle"
		data["idle"] = true
		return fmt.Sprintf("newest=%s (idle: no writes at the source for %s; sink in sync; source newest=%s)",
			topology.FormatTS(sink), topology.HumanAge(time.Since(src)), topology.FormatTS(src))
	default:
		data["verdict"] = "in sync"
		return fmt.Sprintf("newest=%s (in sync: lag %s; source newest=%s)",
			topology.FormatTS(sink), topology.HumanAge(lag), topology.FormatTS(src))
	}
}

// sourceTableForSink maps the sink table a room asked about to the pipeline's
// source table. With several source tables, the one whose table name matches
// the sink's is used; an ambiguous or empty mapping makes the verdict unknown
// rather than guessing a table (and reading someone else's rows).
func sourceTableForSink(p owners.Pipeline, sink string) (string, error) {
	if len(p.SourceTables) == 1 {
		return p.SourceTables[0], nil
	}
	if len(p.SourceTables) == 0 {
		return "", fmt.Errorf("pipeline %q lists no source_tables, so the sink has no source to compare with", p.ID)
	}
	name := sink[strings.LastIndex(sink, ".")+1:]
	var match []string
	for _, s := range p.SourceTables {
		if s[strings.LastIndex(s, ".")+1:] == name {
			match = append(match, s)
		}
	}
	if len(match) == 1 {
		return match[0], nil
	}
	return "", fmt.Errorf("cannot tell which of the pipeline's source_tables is the source of %s (none or several share its table name)", sink)
}

func warehouseRowTool(d Deps) tools.Tool {
	return &tool{
		name: "warehouse_row",
		desc: "Read one row by primary key from the warehouse. Pipeline-scoped: pipeline_id is required and table must be one of the pipeline's sink tables. Returns only timestamp column values.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
			"table":       strProp("\"schema.table\" (or \"db.schema.table\" on Snowflake, Databricks or BigQuery) to read."),
			"pk_column":   strProp("Primary key column name."),
			"key":         strProp("Primary key value."),
		}, "pipeline_id", "table", "pk_column", "key"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
				Table      string `json:"table"`
				PKColumn   string `json:"pk_column"`
				Key        string `json:"key"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			// Format checks come first, so bad input stays ErrBadInput whatever
			// the scope is (same order as warehouse_freshness).
			if !d.validWarehouseTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\" (or \"db.schema.table\" on Snowflake, Databricks or BigQuery)", tools.ErrBadInput)
			}
			if !d.validWarehouseColumn(input.PKColumn) {
				return tools.Result{}, fmt.Errorf("%w: invalid pk_column", tools.ErrBadInput)
			}
			if input.Key == "" {
				return tools.Result{}, fmt.Errorf("%w: key is required", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			if err := scopedTable(input.PipelineID, input.Table, p.SinkTables); err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			// Only the primary key, as in pg_row: any other column (an email, a
			// tax id) makes the tool an existence oracle for personal data.
			// Checked when the client reports the key (the Postgres warehouse
			// client does).
			if pc, ok := d.Wh.(pkColumner); ok {
				pk, err := pc.PrimaryKeyColumn(ctx, input.Table)
				if errors.Is(err, clients.ErrNotSupported) {
					// A composite key: one key value is not one row (#483).
					return tools.Result{}, fmt.Errorf("%w: %s has a composite primary key; warehouse_row looks rows up by a single-column key only", tools.ErrBadInput, input.Table)
				}
				if err != nil {
					return tools.Result{}, fmt.Errorf("dbtools: warehouse_row: %w", err)
				}
				// Snowflake folds unquoted names to upper case and Databricks to
				// lower case; both report the key in their own case, so "ID" and
				// "id" are the same column there. Postgres is case-sensitive. Row
				// gets the canonical pk.
				if input.PKColumn != pk && !((d.isSnowflake() || d.isDatabricks()) && strings.EqualFold(input.PKColumn, pk)) {
					return tools.Result{}, fmt.Errorf("%w: pk_column must be the primary key column of %s (%q)", tools.ErrBadInput, input.Table, pk)
				}
				input.PKColumn = pk
			}
			row, found, err := d.Wh.Row(ctx, input.Table, input.PKColumn, input.Key)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: warehouse_row: %w", err)
			}
			if !found {
				return tools.Result{Summary: "not found", Data: map[string]any{"found": false}}, nil
			}
			summary, truncated := tools.Truncate(whRowSummary(row))
			return tools.Result{Summary: summary, Data: redactWhRow(row), Truncated: truncated}, nil
		},
	}
}

// redactWhRow mirrors whRowSummary: only the timestamp column values are kept,
// every other column is dropped, so the full warehouse row never leaves the
// tool (finding F3).
func redactWhRow(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for name, v := range row {
		if isTimeColumn(name, v) {
			out[name] = v
		}
	}
	return out
}

// whRowSummary shows only found/timestamps, never full row values.
func whRowSummary(row map[string]any) string {
	var sb strings.Builder
	sb.WriteString("found: yes")
	for _, name := range sortedKeys(row) {
		if isTimeColumn(name, row[name]) {
			sb.WriteString(", " + name + "=" + fmt.Sprintf("%v", row[name]))
		}
	}
	return sb.String()
}
