package connprobe

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/owners"
)

// sourceDBChecks are the ids of the source database probe, in order.
var sourceDBChecks = []string{
	"config", "reach", "login", "read_only", "statement_timeout",
	"read_slots", "read_publications", "select_tables", "can_write",
}

var sourceNames = pgNames{envVar: "SOURCE_DB_URL", pipeRe: "connections.source_db_url", what: "source database"}

// probeSourceDB tests the CDC source Postgres the way Piperoom reads it:
// reach, login, the read-only guards of the role, the catalog views the tools
// read, SELECT on every source table, and a warning if the role can write
// (docs/pilot/permissions.md, "Source Postgres"). It only reads.
func probeSourceDB(ctx context.Context, t Target, g *Guard) Result {
	return probeSourceDBWith(ctx, t, g, connectPG)
}

func probeSourceDBWith(ctx context.Context, t Target, g *Guard, connect pgConnector) Result {
	res := Result{Kind: KindSourceDB}
	o := openPG(ctx, t.Config.SourceDBURL, sourceNames, g, connect)
	res.Checks = o.checks
	if !o.ok {
		res.Checks = skipRest(res.Checks, "not run: "+failedStep(o.checks)+" failed", sourceDBChecks[len(o.checks):]...)
		return res
	}
	defer o.sess.close()
	role := o.cfg.User
	s := o.sess

	res.Checks = append(res.Checks, readOnlyCheck(ctx, s, role))
	res.Checks = append(res.Checks, statementTimeoutCheck(ctx, s, role))
	res.Checks = append(res.Checks,
		catalogReadCheck(ctx, s, "read_slots", "pg_replication_slots", sqlSlots, "replication slot", "replication slots", role))
	res.Checks = append(res.Checks,
		catalogReadCheck(ctx, s, "read_publications", "pg_publication_tables", sqlPubs, "publication table", "publication tables", role))

	tables := sourceTables(t.Pipelines)
	items := make([]selectItem, len(tables))
	for i, n := range tables {
		items[i] = selectItem{name: n}
	}
	res.Checks = append(res.Checks, selectCheck(ctx, s, "select_tables", "source", role, items))
	res.Checks = append(res.Checks, canWriteCheck(ctx, s, role, tables))
	return res
}

func failedStep(checks []Check) string {
	if len(checks) == 0 {
		return "an earlier step"
	}
	return checks[len(checks)-1].ID
}

// sourceTables lists the distinct source tables of all pipelines in order.
func sourceTables(ps []owners.Pipeline) []string {
	return distinct(ps, func(p owners.Pipeline) []string { return p.SourceTables })
}

func distinct(ps []owners.Pipeline, f func(owners.Pipeline) []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range ps {
		for _, n := range f(p) {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	return out
}

func statementTimeoutCheck(ctx context.Context, s pgSession, role string) Check {
	return step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		var shown string
		var ms int64
		if err := s.queryRow(sctx, sqlTimeout, nil, &shown, &ms); err != nil {
			return Check{ID: "statement_timeout", Status: StatusFail,
				Problem: "cannot read the role's statement_timeout setting",
				Fix:     "Check that the database accepts a plain SELECT from this role."}
		}
		if ms > 0 {
			return Check{ID: "statement_timeout", Status: StatusOK, Detail: "statement_timeout = " + safeName(shown)}
		}
		return Check{ID: "statement_timeout", Status: StatusWarn,
			Problem: fmt.Sprintf("role %s has no statement_timeout, so one slow query can run until the server stops it", safeName(role)),
			Fix:     fmt.Sprintf("ALTER ROLE %s SET statement_timeout = '10s';", roleSQL(role))}
	})
}

// catalogReadCheck counts the rows of a catalog view the tools read. Both
// views are readable by any role unless a hardened database revoked that, so
// a failure here is rare and the fix is a plain GRANT on the view.
func catalogReadCheck(ctx context.Context, s pgSession, id, view, sql, one, many, role string) Check {
	return step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		var n int64
		if err := s.queryRow(sctx, sql, nil, &n); err != nil {
			if classify(err) == classTimeout {
				return Check{ID: id, Status: StatusFail,
					Problem: fmt.Sprintf("reading %s timed out", view),
					Fix:     "Check that the database answers queries from this role."}
			}
			return Check{ID: id, Status: StatusFail,
				Problem: fmt.Sprintf("role %s cannot read %s", safeName(role), view),
				Fix:     fmt.Sprintf("GRANT SELECT ON pg_catalog.%s TO %s;", view, roleSQL(role))}
		}
		return Check{ID: id, Status: StatusOK, Detail: fmt.Sprintf("%s visible in %s", plural(int(n), one, many), view)}
	})
}

// canWriteCheck warns when the role is a superuser or can change a source
// table: Piperoom promises to be read-only, and the credential is the real
// boundary (docs/pilot/permissions.md).
func canWriteCheck(ctx context.Context, s pgSession, role string, tables []string) Check {
	return step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		r := roleSQL(role)

		var super bool
		if err := s.queryRow(sctx, sqlSuper, nil, &super); err != nil {
			return Check{ID: "can_write", Status: StatusFail,
				Problem: "cannot read the role's attributes from pg_roles",
				Fix:     "Check that the database accepts a plain SELECT from this role."}
		}
		if super {
			return Check{ID: "can_write", Status: StatusWarn,
				Problem: fmt.Sprintf("role %s is a superuser, so it can write everywhere", safeName(role)),
				Fix:     fmt.Sprintf("Use a dedicated role as in docs/pilot/permissions.md (CREATE ROLE ... NOSUPERUSER), or run ALTER ROLE %s NOSUPERUSER;", r)}
		}

		var writable, fixes []string
		unchecked := 0
		for _, name := range tables {
			schema, table, ok := splitQualified(name)
			if !ok {
				unchecked++
				continue
			}
			var w bool
			if err := s.queryRow(sctx, sqlTableWr, []any{quoteQualified(schema, table)}, &w); err != nil {
				// A missing or invisible table is select_tables' finding, not a write.
				unchecked++
				continue
			}
			if w {
				writable = append(writable, name)
				fixes = append(fixes, fmt.Sprintf("REVOKE INSERT, UPDATE, DELETE ON %s FROM %s;", name, r))
			}
		}
		if len(writable) > 0 {
			return Check{ID: "can_write", Status: StatusWarn,
				Problem: fmt.Sprintf("role %s can change %s", safeName(role), listNames(writable, 5)),
				Fix:     dedupeJoin(fixes, 6)}
		}
		if len(tables) == 0 {
			return Check{ID: "can_write", Status: StatusOK, Detail: "not a superuser; no source tables are configured"}
		}
		d := fmt.Sprintf("not a superuser; no write rights on %s", plural(len(tables)-unchecked, "source table", "source tables"))
		if unchecked > 0 {
			d += fmt.Sprintf(" (%s not checked)", plural(unchecked, "table", "tables"))
		}
		return Check{ID: "can_write", Status: StatusOK, Detail: strings.TrimSpace(d)}
	})
}

// cfgKeys names the owners.yaml keys behind the objects a select check reads.
func cfgKeys(noun string) string {
	if noun == "sink" {
		return "sink_tables and sink_ts_column"
	}
	return "source_tables"
}

// quoteQualified renders a validated schema and table as one quoted name, the
// form has_table_privilege reads as a table name.
func quoteQualified(schema, table string) string {
	return pgx.Identifier{schema, table}.Sanitize()
}
