package dbtools

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

func pgReplicationSlotsTool(d Deps) tools.Tool {
	return &tool{
		name:   "pg_replication_slots",
		desc:   "List the room pipeline's replication slots on the source Postgres (owners.yaml watch.slot, else slot.name from its Debezium connector config): active state, retained WAL size and wal_status. Other slots are only counted.",
		schema: objSchema(map[string]any{}),
		run: func(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			ctx, cancel := toolCtx(ctx)
			defer cancel()
			if sc.Unscoped() {
				slots, err := d.Src.ReplicationSlots(ctx)
				if err != nil {
					return tools.Result{}, fmt.Errorf("dbtools: pg_replication_slots: %w", err)
				}
				return slotsResult(slots, ""), nil
			}

			// Scoped: only the pipeline's own slots. A slot name that cannot be
			// resolved fails the call; it never falls back to every slot.
			// A nil Connect stays a nil reader: Slots then says Connect is missing.
			names, err := sc.Slots(ctx, d.Connect)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_replication_slots: no slot is shown: %w", err)
			}
			slots, other, err := d.namedSlots(ctx, names)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_replication_slots: %w", err)
			}
			var head strings.Builder
			if len(names) == 0 {
				fmt.Fprintf(&head, "(pipeline %s has no Debezium Postgres source connector, so no replication slot)\n", sc.Pipeline().ID)
			}
			for _, n := range names {
				if !slices.ContainsFunc(slots, func(s clients.ReplicationSlot) bool { return s.Name == n }) {
					fmt.Fprintf(&head, "slot %s: not found in pg_replication_slots\n", n)
				}
			}
			if line := tools.NotShown(other, "replication slot", "replication slots"); line != "" {
				head.WriteString(line + "\n")
			}
			return slotsResult(slots, head.String()), nil
		},
	}
}

// slotsResult renders one line per slot, then extra (lines that are not about
// a listed slot, already newline-terminated). Data holds exactly the listed
// slots.
func slotsResult(slots []clients.ReplicationSlot, extra string) tools.Result {
	var sb strings.Builder
	for _, s := range slots {
		fmt.Fprintf(&sb, "slot %s: active=%t retained=%s wal_status=%s\n",
			s.Name, s.Active, topology.HumanBytes(s.RetainedWALBytes), s.WALStatus)
	}
	if len(slots) == 0 && extra == "" {
		sb.WriteString("(no replication slots)\n")
	}
	sb.WriteString(extra)
	summary, truncated := tools.Truncate(sb.String())
	return tools.Result{Summary: summary, Data: slots, Truncated: truncated}
}

// namedSlotLister is implemented by SourceDB implementations that filter slots
// by name in SQL and only count the others (internal/clients/postgres).
type namedSlotLister interface {
	ReplicationSlotsNamed(ctx context.Context, names []string) ([]clients.ReplicationSlot, int, error)
}

// namedSlots returns the slots called names and the number of other slots.
// Without the SQL filter it filters the full list in memory, so the result
// is the same either way.
func (d Deps) namedSlots(ctx context.Context, names []string) ([]clients.ReplicationSlot, int, error) {
	if l, ok := d.Src.(namedSlotLister); ok {
		return l.ReplicationSlotsNamed(ctx, names)
	}
	all, err := d.Src.ReplicationSlots(ctx)
	if err != nil {
		return nil, 0, err
	}
	var out []clients.ReplicationSlot
	for _, s := range all {
		if slices.Contains(names, s.Name) {
			out = append(out, s)
		}
	}
	return out, len(all) - len(out), nil
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

const (
	longTxnDefaultMinutes = 5
	// longTxnMaxMinutes (30 days) is far past any transaction worth asking
	// about. The bound also keeps minutes*time.Minute inside int64: an
	// unclamped value above about 1.5e8 wraps the Duration negative, which
	// would turn "older than N" into "every transaction".
	longTxnMaxMinutes = 30 * 24 * 60
)

// olderThanMinutes applies the older_than_minutes rules: missing is the
// default, below 1 is refused rather than quietly replaced (the model would
// read the answer as covering the age it asked for), above the maximum is
// clamped.
func olderThanMinutes(v *int) (int, error) {
	if v == nil {
		return longTxnDefaultMinutes, nil
	}
	if *v < 1 {
		return 0, fmt.Errorf("%w: older_than_minutes must be between 1 and %d", tools.ErrBadInput, longTxnMaxMinutes)
	}
	return min(*v, longTxnMaxMinutes), nil
}

func pgLongTransactionsTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_long_transactions",
		desc: "List transactions on the source Postgres that have been open longer than a threshold (pid, user, application, state, age, wait event, statement verb; no query text). In a room, only transactions that hold or wait for a lock on the pipeline's source tables are listed; the others are counted, and the oldest writers among them (a writer on any table or database holds back every logical slot) are shown with pid, same_database, state, age, xid_age and wait type only. has_xid marks writers that hold the WAL horizon; long read-only transactions are listed too because they block DDL and VACUUM.",
		schema: objSchema(map[string]any{
			"older_than_minutes": intProp("Minimum transaction age in minutes, 1-43200. Default 5."),
		}),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				OlderThanMinutes *int `json:"older_than_minutes"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			minutes, err := olderThanMinutes(input.OlderThanMinutes)
			if err != nil {
				return tools.Result{}, err
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			olderThan := time.Duration(minutes) * time.Minute

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			var txns []clients.Transaction
			var tt postgres.TableTransactions
			if sc.Unscoped() {
				txns, err = d.Src.LongTransactions(ctx, olderThan)
			} else {
				// Scoped: only sessions of the source database that lock one of
				// the pipeline's tables are listed. Other sessions belong to
				// other pipelines, users or databases: they are counted, and
				// the oldest writers among them are shown anonymised. A client
				// that cannot filter is refused, never read in full.
				l, ok := d.Src.(tableTxnLister)
				if !ok {
					return tools.Result{}, errors.New("dbtools: pg_long_transactions: the source client cannot filter transactions by table, so none are shown")
				}
				tt, err = l.LongTransactionsOnTables(ctx, olderThan, sc.Pipeline().SourceTables)
				txns = tt.Transactions
			}
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_long_transactions: %w", err)
			}

			// Query text is never returned: it often holds literal row values.
			// Keep the diagnostics and at most the statement verb.
			rows := make([]longTxn, 0, len(txns))
			for _, t := range txns {
				rows = append(rows, longTxn{
					PID: t.PID, User: t.Username, ApplicationName: t.ApplicationName, State: t.State,
					Age: topology.HumanAge(t.Age), AgeSeconds: int64(t.Age / time.Second),
					WaitEvent: t.WaitEvent, Verb: statementVerb(t.Query), HasXID: t.HasXID,
				})
			}
			if sc.Unscoped() {
				var sb strings.Builder
				for _, r := range rows {
					writeTxnLine(&sb, r)
				}
				if len(txns) == 0 {
					sb.WriteString("(no long transactions)\n")
				}
				summary, truncated := tools.Truncate(sb.String())
				return tools.Result{Summary: summary, Data: rows, Truncated: truncated}, nil
			}

			// Scoped. The bounded block about the other sessions comes first:
			// the count line, at most MaxOtherWriters writer lines and the
			// "more" line (well under 2 KB). The pipeline's own rows follow,
			// capped, so truncation can never cut the other writers, which are
			// the classic cause of a slot that keeps WAL.
			var sb strings.Builder
			data := scopedTxns{Transactions: rows, OtherNotShown: tt.Other, OtherWriters: tt.OtherWriters}
			if line := tools.NotShown(tt.Other, "long transaction", "long transactions"); line != "" {
				fmt.Fprintf(&sb, "%s; %d of them %s (has_xid)\n", line, tt.OtherWriters, tools.Plural(tt.OtherWriters, "is a writer", "are writers"))
			}
			// A writer on any table, in any database, holds back the
			// restart_lsn of every logical slot. Show the oldest ones, with the
			// allow-listed fields only, so the agent can name the cause and a
			// DBA can act.
			for i, w := range tt.OldestOtherWriters {
				if i == postgres.MaxOtherWriters {
					break
				}
				ow := anonWriter(w)
				data.OldestOtherWriters = append(data.OldestOtherWriters, ow)
				if i == 0 {
					sb.WriteString("oldest writers outside this pipeline's tables (they hold back every logical slot's restart_lsn; no user, application, database or query shown):\n")
				}
				fmt.Fprintf(&sb, "  pid=%d same_database=%s state=%s age=%s xid_age=%d wait_type=%s\n",
					ow.PID, yesNo(ow.SameDatabase), orDash(ow.State), ow.Age, ow.XIDAge, orDash(ow.WaitEventType))
			}
			if more := tt.OtherWriters - len(data.OldestOtherWriters); more > 0 && len(data.OldestOtherWriters) > 0 {
				fmt.Fprintf(&sb, "  (%d more %s not listed)\n", more, tools.Plural(more, "writer", "writers"))
			}

			if len(rows) == 0 {
				sb.WriteString("(no long transactions on this pipeline's tables)\n")
			}
			// The oldest maxPipelineTxnLines rows go to Summary; Data has all.
			shown := slices.Clone(rows)
			slices.SortStableFunc(shown, func(a, b longTxn) int { return cmp.Compare(b.AgeSeconds, a.AgeSeconds) })
			if len(shown) > maxPipelineTxnLines {
				shown = shown[:maxPipelineTxnLines]
			}
			for _, r := range shown {
				writeTxnLine(&sb, r)
			}
			if more := len(rows) - len(shown); more > 0 {
				fmt.Fprintf(&sb, "(%d more %s on this pipeline's tables, in Data)\n", more, tools.Plural(more, "session", "sessions"))
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
		},
	}
}

// maxPipelineTxnLines caps the pipeline's own long transactions in a scoped
// Summary (the oldest ones); Data keeps every row.
const maxPipelineTxnLines = 30

// writeTxnLine renders one long transaction of the pipeline (or of any
// session, unscoped).
func writeTxnLine(sb *strings.Builder, r longTxn) {
	writer := "no"
	if r.HasXID {
		writer = "yes"
	}
	fmt.Fprintf(sb, "pid=%d user=%s app=%s state=%s age=%s wait=%s verb=%s writer=%s\n",
		r.PID, r.User, r.ApplicationName, r.State, r.Age, orDash(r.WaitEvent), orDash(r.Verb), writer)
}

// scopedTxns is Data of a scoped pg_long_transactions call. Unscoped calls
// keep the plain []longTxn.
type scopedTxns struct {
	// Transactions are the long transactions on the pipeline's tables.
	Transactions []longTxn `json:"transactions"`
	// OtherNotShown counts every other long transaction, in any database;
	// OtherWriters the writers (has_xid) among them.
	OtherNotShown int `json:"other_not_shown"`
	OtherWriters  int `json:"other_writers"`
	// OldestOtherWriters are the oldest of those writers (at most
	// postgres.MaxOtherWriters), anonymised.
	OldestOtherWriters []otherWriter `json:"oldest_other_writers,omitempty"`
}

// otherWriter is the whole allow-list for a session outside the pipeline's
// tables: what tells whether it pins a slot and for how long. It has no field
// for the user, application, client address, database name, query or verb, so
// none of them can reach Summary or Data.
type otherWriter struct {
	PID           int    `json:"pid"`
	SameDatabase  bool   `json:"same_database"`
	State         string `json:"state"`
	Age           string `json:"age"`
	AgeSeconds    int64  `json:"age_seconds"`
	XIDAge        int64  `json:"xid_age"`
	WaitEventType string `json:"wait_event_type"`
}

// backendStates are the values of pg_stat_activity.state.
var backendStates = map[string]bool{
	"active": true, "idle": true, "idle in transaction": true, "idle in transaction (aborted)": true,
	"fastpath function call": true, "disabled": true,
}

// waitEventTypes are the values of pg_stat_activity.wait_event_type
// (PostgreSQL 13-17, "Wait Event Types" in the monitoring docs).
var waitEventTypes = map[string]bool{
	"Activity": true, "BufferPin": true, "Client": true, "Extension": true, "InjectionPoint": true,
	"IO": true, "IPC": true, "Lock": true, "LWLock": true, "Timeout": true,
}

// anonWriter maps a client row onto the allow-list. State and wait type are
// fixed vocabularies in Postgres; anything else (a client that is not the real
// one) becomes "?", so no free text gets through.
func anonWriter(w postgres.OtherWriter) otherWriter {
	state := w.State
	if !backendStates[state] {
		state = "?"
	}
	wait := w.WaitEventType
	if wait != "" && !waitEventTypes[wait] {
		wait = "?"
	}
	age := time.Duration(w.AgeSeconds) * time.Second
	return otherWriter{
		PID: w.PID, SameDatabase: w.SameDatabase, State: state,
		Age: topology.HumanAge(age), AgeSeconds: w.AgeSeconds, XIDAge: w.XIDAge, WaitEventType: wait,
	}
}

// tableTxnLister is implemented by SourceDB implementations that can tell, in
// SQL, which long transactions lock a set of tables (internal/clients/postgres).
type tableTxnLister interface {
	LongTransactionsOnTables(ctx context.Context, olderThan time.Duration, tables []string) (postgres.TableTransactions, error)
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
	// HasXID marks a writer: only a transaction with an xid holds the WAL
	// horizon for a replication slot.
	HasXID bool `json:"has_xid"`
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
			// Only the primary key: a lookup by any other column (an email,
			// a tax id, ctid) makes the tool an existence oracle for personal
			// data and puts the probed value in the room log.
			if pc, ok := d.Src.(pkColumner); ok {
				pk, err := pc.PrimaryKeyColumn(ctx, input.Table)
				if err != nil {
					return tools.Result{}, fmt.Errorf("dbtools: pg_row: %w", err)
				}
				if input.PKColumn != pk {
					return tools.Result{}, fmt.Errorf("%w: pk_column must be the primary key column of %s (%q)", tools.ErrBadInput, input.Table, pk)
				}
			}
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

// pkColumner is implemented by SourceDB implementations that report a
// table's primary key column (the postgres client does).
type pkColumner interface {
	PrimaryKeyColumn(ctx context.Context, table string) (string, error)
}

// redactRow returns the same values rowSummary exposes: the primary-key and
// timestamp column values, with every other column kept as a name only (nil
// value), so the full row never leaves the tool (finding F3).
func redactRow(row map[string]any, pkColumn string) map[string]any {
	out := make(map[string]any, len(row))
	for name, v := range row {
		if name == pkColumn || isTimeColumn(name, v) {
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
	if ts := tsValue(row, "updated_at"); ts != "" && isTimeColumn("updated_at", row["updated_at"]) {
		sb.WriteString(", updated_at=" + ts)
	}
	parts := make([]string, 0, len(row))
	for _, name := range sortedKeys(row) {
		if name == pkColumn || isTimeColumn(name, row[name]) {
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
