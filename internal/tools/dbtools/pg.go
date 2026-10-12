package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
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
			// Slots are database-wide and owners.yaml names none, so this tool
			// is not filtered; it still needs a room scope (or the opt-out).
			if _, err := d.scope(ctx); err != nil {
				return tools.Result{}, err
			}
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
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			if input.Table != "" {
				if err := sc.CheckSourceTable(input.Table); err != nil {
					return tools.Result{}, err
				}
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()

			var changes []clients.DDLChange
			switch {
			case input.Table != "":
				changes, err = d.recentDDLTable(ctx, since, input.Table)
			case sc.Unscoped():
				changes, err = d.Src.RecentDDL(ctx, since, 100)
			default:
				// No table given: only DDL on the pipeline's own source tables.
				changes, err = d.recentDDLTables(ctx, since, sc.Pipeline().SourceTables)
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
		desc: "List transactions on the source Postgres that have been open longer than a threshold (pid, user, application, state, age, wait event, statement verb; no query text).",
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
			// Open transactions are database-wide (any long transaction holds the
			// WAL horizon) and cannot be tied to a pipeline, so the tool is not
			// filtered; it needs a room scope (or the opt-out) and returns no
			// query text.
			if _, err := d.scope(ctx); err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			txns, err := d.Src.LongTransactions(ctx, time.Duration(input.OlderThanMinutes)*time.Minute)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_long_transactions: %w", err)
			}

			// Query text is never returned: it often holds literal row values and
			// the sessions belong to any pipeline. Keep the diagnostics and at
			// most the statement verb.
			rows := make([]longTxn, 0, len(txns))
			var sb strings.Builder
			for _, t := range txns {
				r := longTxn{
					PID: t.PID, User: t.Username, ApplicationName: t.ApplicationName, State: t.State,
					Age: topology.HumanAge(t.Age), AgeSeconds: int64(t.Age / time.Second),
					WaitEvent: t.WaitEvent, Verb: statementVerb(t.Query),
				}
				rows = append(rows, r)
				fmt.Fprintf(&sb, "pid=%d user=%s app=%s state=%s age=%s wait=%s verb=%s\n",
					r.PID, r.User, r.ApplicationName, r.State, r.Age, orDash(r.WaitEvent), orDash(r.Verb))
			}
			if len(txns) == 0 {
				sb.WriteString("(no long transactions)\n")
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: rows, Truncated: truncated}, nil
		},
	}
}

// longTxn is one open transaction as the tool reports it: no query text.
type longTxn struct {
	PID             int    `json:"pid"`
	User            string `json:"user"`
	ApplicationName string `json:"application_name"`
	State           string `json:"state"`
	Age             string `json:"age"`
	AgeSeconds      int64  `json:"age_seconds"`
	WaitEvent       string `json:"wait_event"`
	Verb            string `json:"verb"`
}

// statementVerb returns the first keyword of a statement in upper case (at most
// 16 letters), or "" when it does not start with letters. It is applied even
// when the client already sends only a verb, so a client that returns the full
// statement text still cannot leak it.
func statementVerb(q string) string {
	q = strings.TrimSpace(q)
	end := 0
	for end < len(q) && end < 16 && (q[end] >= 'a' && q[end] <= 'z' || q[end] >= 'A' && q[end] <= 'Z') {
		end++
	}
	return strings.ToUpper(q[:end])
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
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
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
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

// recentDDLTable returns the recent DDL of one table.
func (d Deps) recentDDLTable(ctx context.Context, since time.Time, table string) ([]clients.DDLChange, error) {
	if f, ok := d.Src.(tableDDLFilterer); ok {
		return f.RecentDDLTable(ctx, since, table, 100)
	}
	changes, err := d.Src.RecentDDL(ctx, since, 100)
	if err != nil {
		return nil, err
	}
	return filterDDL(changes, table), nil
}

// recentDDLTables returns the recent DDL of the given tables only, newest first
// and at most 100 changes. With a SQL table filter each table is read on its
// own (so one busy table cannot push the others past the limit); without one
// the unfiltered read is filtered in memory.
func (d Deps) recentDDLTables(ctx context.Context, since time.Time, tables []string) ([]clients.DDLChange, error) {
	var out []clients.DDLChange
	if f, ok := d.Src.(tableDDLFilterer); ok {
		for _, t := range tables {
			part, err := f.RecentDDLTable(ctx, since, t, 100)
			if err != nil {
				return nil, err
			}
			out = append(out, part...)
		}
	} else {
		all, err := d.Src.RecentDDL(ctx, since, 100)
		if err != nil {
			return nil, err
		}
		for _, c := range all {
			if slices.Contains(tables, c.ObjectIdentity) {
				out = append(out, c)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	if len(out) > 100 {
		out = out[:100]
	}
	return out, nil
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
