package dbtools

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/clients/postgres/srcneutral"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// The source-neutral tools on a MySQL source (Q8n, docs/design/mysql-source.md
// §3, §7.1 and §7.4).
//
// The four Q2a source_* tools read the MySQL client through the same item 2
// interfaces as Postgres (both clients alias the clients contract's DTOs);
// their per-source wording comes from the setting
// names and TableReplication.Engine. source_row and source_long_transactions
// are pg_row and pg_long_transactions under neutral names, registered only
// for a MySQL source in this wave (design D5): a Postgres set's tool list,
// and so every Postgres prompt and eval label, is unchanged.

// pgOnlyTools are the tools a MySQL source never offers (design §3): they
// read Postgres-only objects (slots, WAL, publications, TOAST, the DDL audit
// trigger) or keep their pg_ name for Postgres rooms.
var pgOnlyTools = []string{
	"pg_replication_slots",
	"pg_streaming_liveness",
	"pg_recent_ddl",
	"pg_long_transactions",
	"pg_row",
	"pg_publication_tables",
	"pg_connector_scope_gap",
	"pg_toast_risk",
}

// mysqlOnlyTools are the tools only a MySQL source offers in this wave.
var mysqlOnlyTools = []string{"source_long_transactions", "source_row"}

// PostgresOnlyTools returns the names Register leaves out for a MySQL
// source; MySQLOnlyTools the names it adds. New slices per call; wiring
// (Q8g) and its tests read them instead of copying the lists.
func PostgresOnlyTools() []string { return slices.Clone(pgOnlyTools) }

// MySQLOnlyTools returns the tool names Register adds for a MySQL source.
func MySQLOnlyTools() []string { return slices.Clone(mysqlOnlyTools) }

// sourceKind is the source kind a call reads: the room pipeline's
// source_kind, else Deps.SourceKind, else the client's (srcneutral.KindOf),
// else postgres.
func (d Deps) sourceKind(sc tools.Scope) string {
	if !sc.Unscoped() {
		pl := sc.Pipeline()
		return pl.SourceKindOrDefault()
	}
	if d.SourceKind != "" {
		return d.SourceKind
	}
	if k := srcneutral.KindOf(d.Src); k != "" {
		return k
	}
	return owners.SourceKindPostgres
}

// mysqlTools is the MySQL tool list in registration order: the neutral
// source tools with MySQL descriptions, the two neutral names, then the
// source-independent tools in their usual order.
func mysqlTools(d Deps) []tools.Tool {
	return []tools.Tool{
		withDesc(sourceSettingsTool(d), mysqlSettingsDesc),
		withDesc(sourceTableReplicationTool(d), mysqlTableReplicationDesc),
		withDesc(sourceRoleGrantsTool(d), mysqlRoleGrantsDesc),
		withDesc(sourceBlockingSessionsTool(d), mysqlBlockingSessionsDesc),
		sourceLongTransactionsTool(d),
		sourceRowTool(d),
		warehouseFreshnessTool(d),
		warehouseRowTool(d),
		warehouseLoadErrorsTool(d),
		reconcileTool(d),
		pipelineMapTool(d),
		traceRecordTool(d),
	}
}

// withDesc replaces a dbtools tool's description.
func withDesc(t tools.Tool, desc string) tools.Tool {
	if tt, ok := t.(*tool); ok {
		c := *tt
		c.desc = desc
		return &c
	}
	return t
}

const (
	mysqlSettingsDesc         = "Read the CDC-relevant settings of the source MySQL: version, log_bin, binlog_format, binlog_row_image, gtid_mode, enforce_gtid_consistency, server_id, binlog_expire_logs_seconds (and binlog_expire_logs_auto_purge), lower_case_table_names, and the binlog retention hours on RDS/Aurora, each with pending_restart (a persisted value that is not the running one) and what Debezium needs. Read it before proposing a configuration change."
	mysqlTableReplicationDesc = "For each source table of the pipeline, show whether it has a primary key and its storage engine. Without a primary key Debezium emits the table's change events with no key unless message.key.columns names one; a non-transactional engine (not InnoDB) cannot give a consistent snapshot."
	mysqlRoleGrantsDesc       = "For the pipeline's Debezium connector user (database.user from its Connect config), show its accounts (user@host), whether it can log in, whether it has REPLICATION SLAVE and REPLICATION CLIENT, the other global privileges the connector needs (RELOAD, LOCK TABLES, SHOW DATABASES) and whether it can SELECT each source table. Use for connector authentication, binlog and permission failures."
	mysqlBlockingSessionsDesc = "List the source MySQL sessions that hold the connector back: an InnoDB transaction open longer than older_than_minutes, or a session holding a metadata lock another session waits for (a snapshot's FLUSH TABLES WITH READ LOCK or LOCK TABLES waits behind it). Each row has the processlist id as pid, the transaction start (started_at; zero without one), user, program_name, command and state, whether it holds a waited-for lock, and its statement redacted and cut to 200 bytes as untrusted text; a session outside the pipeline's tables is listed with identity hidden. Other users' threads need PROCESS. Ending a session is an action, not this tool."
)

func sourceLongTransactionsTool(d Deps) tools.Tool {
	return longTransactionsTool(d, "source_long_transactions",
		"List transactions on the source database that have been open longer than a threshold (pid, user, application, state, age, statement verb; no query text). On MySQL pid is the processlist id and the age is the InnoDB transaction's. In a room, only transactions that hold or wait for a lock on the pipeline's source tables are listed with identity; the others are counted. Long transactions stall a snapshot's locking and DDL, and a large uncommitted write reaches the binlog only at commit.", true)
}

func sourceRowTool(d Deps) tools.Tool {
	return rowTool(d, "source_row",
		"Read one row by primary key from the source database. Pipeline-scoped: pipeline_id is required and table must be one of the pipeline's source tables (\"db.table\" on MySQL). pk_column must be the table's single-column primary key. Returns only the key and timestamp column values.",
		"\"db.table\" (MySQL) or \"schema.table\" (Postgres) to read.", true)
}

// scopedLongFromBlockers answers a scoped long-transactions call from the
// scoped blocking-session read, for a client without a table-filtered
// LongTransactions (MySQL): the sessions whose transaction is older than the
// threshold by the server's clock (OldXact) and that hold or wait for a lock
// on the pipeline's tables are listed; the others are counted, never shown.
// The read cannot tell writers apart, so no line claims one.
func scopedLongFromBlockers(ctx context.Context, name string, c scopedBlockersClient, olderThan time.Duration, pl owners.Pipeline) (tools.Result, error) {
	tables, err := pipelineTables(pl.ID, pl.SourceTables)
	if err != nil {
		return tools.Result{}, err
	}
	sessions, err := c.BlockersOnTables(ctx, olderThan, tables)
	if err != nil {
		return tools.Result{}, fmt.Errorf("dbtools: %s: %w", name, err)
	}
	now := time.Now()
	rows := make([]longTxn, 0, len(sessions))
	other := 0
	for _, s := range sessions {
		if !s.OldXact || s.XactStart.IsZero() {
			// A lock holder without a long transaction is
			// source_blocking_sessions' row, not a long transaction.
			continue
		}
		if !s.InScope {
			other++
			continue
		}
		age := now.Sub(s.XactStart)
		rows = append(rows, longTxn{
			PID: int(s.PID), User: oneLineShort(s.User, 64), ApplicationName: oneLineShort(s.App, 64), State: oneLineShort(s.State, 64),
			Age: topology.HumanAge(age), AgeSeconds: int64(age / time.Second), Verb: statementVerb(s.Query),
		})
	}
	var sb strings.Builder
	if line := tools.NotShown(other, "long transaction", "long transactions"); line != "" {
		sb.WriteString(line + "\n")
	}
	if len(rows) == 0 {
		sb.WriteString("(no long transactions on this pipeline's tables)\n")
	}
	shown := slices.Clone(rows)
	slices.SortStableFunc(shown, func(a, b longTxn) int { return cmp.Compare(b.AgeSeconds, a.AgeSeconds) })
	if len(shown) > maxPipelineTxnLines {
		shown = shown[:maxPipelineTxnLines]
	}
	for _, r := range shown {
		fmt.Fprintf(&sb, "pid=%d user=%s app=%s state=%s age=%s verb=%s\n", r.PID, orDash(r.User), orDash(r.ApplicationName), orDash(r.State), r.Age, orDash(r.Verb))
	}
	if more := len(rows) - len(shown); more > 0 {
		fmt.Fprintf(&sb, "(%d more %s on this pipeline's tables, in Data)\n", more, tools.Plural(more, "session", "sessions"))
	}
	summary, truncated := tools.Truncate(sb.String())
	return tools.Result{Summary: summary, Data: scopedTxns{Transactions: rows, OtherNotShown: other}, Truncated: truncated}, nil
}
