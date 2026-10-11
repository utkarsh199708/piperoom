package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/tools"
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
					s.Name, s.Active, humanBytes(s.RetainedWALBytes), s.WALStatus)
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
			changes, err := d.Src.RecentDDL(ctx, since, 100)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_recent_ddl: %w", err)
			}

			filtered := changes
			if input.Table != "" {
				filtered = filtered[:0]
				for _, c := range changes {
					if strings.Contains(c.ObjectIdentity, input.Table) {
						filtered = append(filtered, c)
					}
				}
			}

			var sb strings.Builder
			for _, c := range filtered {
				fmt.Fprintf(&sb, "%s %s %s | %s | %s\n",
					formatTS(c.At), c.CommandTag, c.ObjectIdentity, cutShort(c.Query, 300), c.Username)
			}
			if len(filtered) == 0 {
				sb.WriteString("(no DDL since " + formatTS(since) + ")\n")
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: filtered, Truncated: truncated}, nil
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
					t.PID, t.Username, t.State, humanAge(t.Age), cutShort(t.Query, 200))
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
		desc: "Read one row by primary key from the source Postgres.",
		schema: objSchema(map[string]any{
			"table":     strProp("\"schema.table\" to read."),
			"pk_column": strProp("Primary key column name."),
			"key":       strProp("Primary key value."),
		}, "table", "pk_column", "key"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				Table    string `json:"table"`
				PKColumn string `json:"pk_column"`
				Key      string `json:"key"`
			}
			if err := decode(in, &input); err != nil {
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
			return tools.Result{Summary: summary, Data: row, Truncated: truncated}, nil
		},
	}
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
