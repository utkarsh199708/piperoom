package sourceactions

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/clients/postgres/srcneutral"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// The source kind of a step (Q8n, docs/design/mysql-source.md §8.1).
//
// source_grant_select, source_create_heartbeat_table and source_end_session
// run on a Postgres or a MySQL pipeline over the same reader and writer
// interfaces; pg_set_replica_identity is Postgres-only and refuses a MySQL
// pipeline. The approving layer of a source-neutral step is the pipeline's
// source layer (postgres or mysql): the three actions implement the
// contract's actions.SourceLayered marker (Q0 item 7, design §5.3), so
// actions.LayerFor and actions.StepLayer give the mysql layer on a MySQL
// pipeline.

// SourceDescribed is implemented by an action whose description depends on
// the source kind: the agent's action list of a MySQL room (Q8g) shows
// DescribeFor(mysql), and Describe() stays the Postgres text, so no Postgres
// prompt changes.
type SourceDescribed interface {
	DescribeFor(sourceKind string) string
}

var (
	_ actions.SourceLayered = (*grantSelect)(nil)
	_ actions.SourceLayered = (*createHeartbeatTable)(nil)
	_ actions.SourceLayered = (*endSession)(nil)

	_ SourceDescribed = (*createHeartbeatTable)(nil)
	_ SourceDescribed = (*endSession)(nil)
)

func (a *grantSelect) SourceLayered()          {}
func (a *createHeartbeatTable) SourceLayered() {}
func (a *endSession) SourceLayered()           {}

// stepKind returns the source kind a step works on: the room pipeline's
// source_kind when ctx carries it, else the kind of the source client
// (srcneutral.KindOf), else postgres. The pipeline wins: a client of the
// other kind is a wiring fault that Q8g refuses at Open, and the eval replay
// serves both kinds through one postgres-typed reader.
func stepKind(ctx context.Context, src any) string {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return pl.SourceKindOrDefault()
	}
	if k := srcneutral.KindOf(src); k != "" {
		return k
	}
	return owners.SourceKindPostgres
}

// refusePostgresOnly is the refusal of a Postgres-only action on a MySQL
// pipeline: CheckScope's, so the step never reaches an approver.
func refusePostgresOnly(name string, pl owners.Pipeline) error {
	if pl.IsMySQL() {
		return fmt.Errorf("sourceactions: %s: %w: pipeline %q has source_kind mysql and %s is a Postgres-only action (MySQL has no replica identity)", name, tools.ErrOutOfScope, pl.ID, name)
	}
	return nil
}

// isMySQL reports whether kind is the MySQL source kind.
func isMySQL(kind string) bool { return kind == owners.SourceKindMySQL }

// The MySQL renderings of design §5.6, for preview and result text only; no
// statement is built from them here. The names passed identRe, which admits
// no backtick; a backtick is still doubled, the MySQL way, so a copied
// statement stays safe.
func quoteMySQLIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func quoteMySQLTable(t string) string {
	s, n, _ := strings.Cut(t, ".")
	return quoteMySQLIdent(s) + "." + quoteMySQLIdent(n)
}

// quoteTableFor renders a "schema.table" (Postgres) or "db.table" (MySQL)
// name for kind.
func quoteTableFor(kind, t string) string {
	if isMySQL(kind) {
		return quoteMySQLTable(t)
	}
	return quoteTable(t)
}

func quoteTablesFor(kind string, ts []string) string {
	q := make([]string, len(ts))
	for i, t := range ts {
		q[i] = quoteTableFor(kind, t)
	}
	return strings.Join(q, ", ")
}

// mysqlAccount renders a "user@host" account from RoleInfo.Accounts as
// 'user'@'host'. The host comes from mysql.user, so it is folded to one line
// and its quotes doubled; it is display text only.
func mysqlAccount(account string) string {
	i := strings.LastIndexByte(account, '@')
	if i < 0 {
		return "'" + strings.ReplaceAll(oneLine(account, 96), "'", "''") + "'"
	}
	user, host := account[:i], account[i+1:]
	q := func(s string) string { return "'" + strings.ReplaceAll(oneLine(s, 96), "'", "''") + "'" }
	return q(user) + "@" + q(host)
}
