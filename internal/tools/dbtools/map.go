package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func pipelineMapTool(d Deps) tools.Tool {
	return &tool{
		name: "pipeline_map",
		desc: "Render the pipeline map: tables, connectors, topics, consumer groups, sink tables with owners and health.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			g, err := d.Builder.Build(ctx, input.PipelineID)
			if err != nil {
				return tools.Result{}, wrapErr("pipeline_map", err)
			}
			summary, truncated := tools.Truncate(d.Builder.Render(g))
			return tools.Result{Summary: summary, Data: g, Truncated: truncated}, nil
		},
	}
}

func traceRecordTool(d Deps) tools.Tool {
	return &tool{
		name: "trace_record",
		desc: "Follow one source row by primary key through every pipeline stage and report where it stopped.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
			"table":       strProp("\"schema.table\" of the source row."),
			"key":         strProp("Primary key value."),
		}, "pipeline_id", "table", "key"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
				Table      string `json:"table"`
				Key        string `json:"key"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			if !validTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\"", tools.ErrBadInput)
			}
			if input.Key == "" {
				return tools.Result{}, fmt.Errorf("%w: key is required", tools.ErrBadInput)
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			stages, err := d.Builder.Trace(ctx, input.PipelineID, input.Table, input.Key)
			if err != nil {
				return tools.Result{}, wrapErr("trace_record", err)
			}

			summary, truncated := tools.Truncate(renderTraceSummary(stages))
			return tools.Result{Summary: summary, Data: stages, Truncated: truncated}, nil
		},
	}
}

// renderTraceSummary renders one line per stage and names the stuck stage.
func renderTraceSummary(stages []events.TraceStage) string {
	var sb strings.Builder
	stopped := ""
	for _, s := range stages {
		sb.WriteString(s.Layer + " " + s.Name + ": " + s.Status)
		keys := make([]string, 0, len(s.Facts))
		for k := range s.Facts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			sb.WriteString(" " + k + "=" + s.Facts[k])
		}
		sb.WriteByte('\n')
		if s.Status == "stopped_here" {
			stopped = s.Layer + " " + s.Name
		}
	}
	if stopped != "" {
		sb.WriteString("stopped at: " + stopped + "\n")
	}
	return sb.String()
}
