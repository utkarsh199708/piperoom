package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

func pgReplicationSlotsTool(d Deps) tools.Tool {
	return &tool{
		name:   "pg_replication_slots",
		desc:   "List logical replication slots on the source Postgres: active state, retained WAL size and wal_status.",
		schema: objSchema(map[string]any{}),
		run: func(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
			ctx, cancel := toolCtx(ctx)
			defer cancel()
			slots, err := d.Src.ReplicationSlots(ctx)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_replication_slots: %w", err)
			}
			var sb strings.Builder
			for _, s := range slots {
				fmt.Fprintf(&sb, "slot %s: active=%t retained=%s wal_status=%s\n",
					s.Name, s.Active, topology.HumanBytes(s.RetainedWALBytes), s.WALStatus)
			}
			if len(slots) == 0 {
				sb.WriteString("(no replication slots)\n")
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: slots, Truncated: truncated}, nil
		},
	}
}

func pgRecentDDLTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_recent_ddl",
		desc: "Show recent DDL changes on the source Postgres, newest first.",
		schema: objSchema(map[string]any{
			"since": strProp("Only changes at or after this RFC 3339 timestamp."),
			"table": strProp("Optional \"schema.table\" filter."),
		}, "since"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				Since string `json:"since"`
				Table string `json:"table"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			since, err := parseRFC3339(input.Since)
			if err != nil {
				return tools.Result{}, err
			}
			if input.Table != "" && !validTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\"", tools.ErrBadInput)
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()

			var changes []clients.DDLChange
			if input.Table != "" {
				if f, ok := d.Src.(tableDDLFilterer); ok {
					changes, err = f.RecentDDLTable(ctx, since, input.Table, 100)
				} else {
					changes, err = d.Src.RecentDDL(ctx, since, 100)
					if err == nil {
						changes = filterDDL(changes, input.Table)
					}
				}
			} else {
				changes, err = d.Src.RecentDDL(ctx, since, 100)
			}
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_recent_ddl: %w", err)
			}

			var sb strings.Builder
			for _, c := range changes {
				fmt.Fprintf(&sb, "%s %s %s | %s | %s\n",
					topology.FormatTS(c.At), c.CommandTag, c.ObjectIdentity, cutShort(c.Query, 300), c.Username)
			}
			if len(changes) == 0 {
				sb.WriteString("(no DDL since " + topology.FormatTS(since) + ")\n")
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: changes, Truncated: truncated}, nil
		},
	}
}

func pgLongTransactionsTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_long_transactions",
		desc: "List transactions on the source Postgres that have been open longer than a threshold.",
		schema: objSchema(map[string]any{
			"older_than_minutes": intProp("Minimum transaction age in minutes. Default 5."),
		}),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				OlderThanMinutes int `json:"older_than_minutes"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.OlderThanMinutes == 0 {
				input.OlderThanMinutes = 5
			}
			if input.OlderThanMinutes < 0 {
				return tools.Result{}, fmt.Errorf("%w: older_than_minutes must be >= 0", tools.ErrBadInput)
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			txns, err := d.Src.LongTransactions(ctx, time.Duration(input.OlderThanMinutes)*time.Minute)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_long_transactions: %w", err)
			}

			var sb strings.Builder
			for _, t := range txns {
				fmt.Fprintf(&sb, "pid=%d user=%s state=%s age=%s query=%s\n",
					t.PID, t.Username, t.State, topology.HumanAge(t.Age), cutShort(t.Query, 200))
			}
			if len(txns) == 0 {
				sb.WriteString("(no long transactions)\n")
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: txns, Truncated: truncated}, nil
		},
	}
}

func pgRowTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_row",
		desc: "Read one row by primary key from the source Postgres. Pipeline-scoped: pipeline_id is required and table must be one of the pipeline's source tables. Returns only the key and timestamp column values.",
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
			if err := scopedTable(input.PipelineID, input.Table, p.SourceTables); err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			row, found, err := d.Src.Row(ctx, input.Table, input.PKColumn, input.Key)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_row: %w", err)
			}
			if !found {
				return tools.Result{Summary: "not found", Data: map[string]any{"found": false}}, nil
			}
			summary, truncated := tools.Truncate(rowSummary(row, input.PKColumn))
			return tools.Result{Summary: summary, Data: redactRow(row, input.PKColumn), Truncated: truncated}, nil
		},
	}
}

// redactRow returns the same values rowSummary exposes: the primary-key and
// timestamp column values, with every other column kept as a name only (nil
// value), so the full row never leaves the tool (finding F3).
func redactRow(row map[string]any, pkColumn string) map[string]any {
	out := make(map[string]any, len(row))
	for name, v := range row {
		if name == pkColumn || isTimestampColumn(name) {
			out[name] = v
		} else {
			out[name] = nil
		}
	}
	return out
}

// rowSummary lists column names, with values only for the key column and
// timestamp columns (never full row values).
func rowSummary(row map[string]any, pkColumn string) string {
	var sb strings.Builder
	sb.WriteString("found: yes")
	if ts := tsValue(row, "updated_at"); ts != "" {
		sb.WriteString(", updated_at=" + ts)
	}
	parts := make([]string, 0, len(row))
	for _, name := range sortedKeys(row) {
		if name == pkColumn || isTimestampColumn(name) {
			parts = append(parts, name+"="+fmt.Sprintf("%v", row[name]))
		} else {
			parts = append(parts, name)
		}
	}
	sb.WriteString(", columns=[" + strings.Join(parts, ", ") + "]")
	return sb.String()
}

func tsValue(row map[string]any, name string) string {
	v, ok := row[name]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// tableDDLFilterer is implemented by SourceDB implementations that can filter
// DDL by table inside SQL (exact object_identity match, before LIMIT).
type tableDDLFilterer interface {
	RecentDDLTable(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error)
}

// filterDDL is the exact-match fallback for SourceDB implementations without a
// SQL table filter.
func filterDDL(changes []clients.DDLChange, table string) []clients.DDLChange {
	out := changes[:0]
	for _, c := range changes {
		if c.ObjectIdentity == table {
			out = append(out, c)
		}
	}
	return out
}
