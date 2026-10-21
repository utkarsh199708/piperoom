package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

func warehouseFreshnessTool(d Deps) tools.Tool {
	return &tool{
		name: "warehouse_freshness",
		desc: "Report the newest timestamp column of a warehouse table and how stale it is.",
		schema: objSchema(map[string]any{
			"table":     strProp("\"schema.table\" (or \"db.schema.table\" on Snowflake) to inspect."),
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
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\" (or \"db.schema.table\" on Snowflake)", tools.ErrBadInput)
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
			if ts.IsZero() {
				summary = "no rows (empty table)"
				data["newest"] = nil
			} else if ts.Year() >= 9999 {
				// The warehouse client's stand-in for a Postgres 'infinity':
				// no row holds this time, so neither a time nor an age is shown.
				summary = fmt.Sprintf("newest=infinity (a sentinel or bad value in %s): staleness cannot be judged", input.TSColumn)
				data["newest"] = "infinity"
				data["in_future"] = true
			} else if ahead := time.Until(ts); ahead > topology.FutureSkew {
				// Clamped to "stale for 0s" this would read as fresh while
				// loading may have stopped: the column is not comparable with
				// this clock (skew, another time zone) or holds a bad value.
				summary = fmt.Sprintf("newest=%s is IN THE FUTURE by %s (clock skew, time zone or bad data): staleness cannot be judged from %s",
					topology.FormatTS(ts), topology.HumanAge(ahead), input.TSColumn)
				data["newest"] = topology.FormatTS(ts)
				data["in_future"] = true
				data["ahead_by"] = topology.HumanAge(ahead)
			} else {
				stale := time.Since(ts)
				summary = fmt.Sprintf("newest=%s (stale for %s)", topology.FormatTS(ts), topology.HumanAge(stale))
				data["newest"] = topology.FormatTS(ts)
				data["stale_for"] = topology.HumanAge(stale)
			}
			summary, truncated := tools.Truncate(summary)
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
		},
	}
}

func warehouseRowTool(d Deps) tools.Tool {
	return &tool{
		name: "warehouse_row",
		desc: "Read one row by primary key from the warehouse. Pipeline-scoped: pipeline_id is required and table must be one of the pipeline's sink tables. Returns only timestamp column values.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
			"table":       strProp("\"schema.table\" (or \"db.schema.table\" on Snowflake) to read."),
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
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\" (or \"db.schema.table\" on Snowflake)", tools.ErrBadInput)
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
