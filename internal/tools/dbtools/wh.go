package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

func warehouseFreshnessTool(d Deps) tools.Tool {
	return &tool{
		name: "warehouse_freshness",
		desc: "Report the newest timestamp column of a warehouse table and how stale it is.",
		schema: objSchema(map[string]any{
			"table":     strProp("\"schema.table\" to inspect."),
			"ts_column": strProp("Timestamp column whose max indicates freshness."),
		}, "table", "ts_column"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				Table    string `json:"table"`
				TSColumn string `json:"ts_column"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if !validTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\"", tools.ErrBadInput)
			}
			if !validColumn(input.TSColumn) {
				return tools.Result{}, fmt.Errorf("%w: invalid ts_column", tools.ErrBadInput)
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
		desc: "Read one row by primary key from the warehouse.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
			"table":       strProp("\"schema.table\" to read."),
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
			p, err := d.pipeline(input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			if !validTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\"", tools.ErrBadInput)
			}
			if !validColumn(input.PKColumn) {
				return tools.Result{}, fmt.Errorf("%w: invalid pk_column", tools.ErrBadInput)
			}
			if input.Key == "" {
				return tools.Result{}, fmt.Errorf("%w: key is required", tools.ErrBadInput)
			}
			if err := scopedTable(input.PipelineID, input.Table, p.SinkTables); err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
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
		if isTimestampColumn(name) {
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
		if isTimestampColumn(name) {
			sb.WriteString(", " + name + "=" + fmt.Sprintf("%v", row[name]))
		}
	}
	return sb.String()
}
