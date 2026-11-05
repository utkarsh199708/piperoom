package connprobe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// This file holds what the source database probe and the Postgres warehouse
// probe share: opening a guarded connection, the config/reach/login steps and
// the SELECT-privilege checks. SQL runs behind pgSession so the check logic is
// unit-testable without a database.

// pgSession is the part of a Postgres connection the checks use.
type pgSession interface {
	// queryRow runs sql in its own READ ONLY transaction (rolled back
	// afterwards) and scans the single result row into dest. One transaction
	// per query, because a failed statement (permission denied) aborts its
	// transaction and the next check must not inherit that.
	queryRow(ctx context.Context, sql string, args []any, dest ...any) error
	// close ends the connection.
	close()
}

// pgConnector opens a session for cfg. The real one is connectPG.
type pgConnector func(ctx context.Context, cfg *pgx.ConnConfig) (pgSession, error)

// pgxSession is the real pgSession over one pgx connection.
type pgxSession struct{ conn *pgx.Conn }

func connectPG(ctx context.Context, cfg *pgx.ConnConfig) (pgSession, error) {
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connprobe: connect to postgres: %w", err)
	}
	return &pgxSession{conn: conn}, nil
}

func (s *pgxSession) queryRow(ctx context.Context, sql string, args []any, dest ...any) error {
	tx, err := s.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("connprobe: begin read-only transaction: %w", err)
	}
	defer func() {
		// Roll back even when ctx has ended, or the connection keeps an open
		// transaction until Close.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rctx)
	}()
	if err := tx.QueryRow(ctx, sql, args...).Scan(dest...); err != nil {
		return fmt.Errorf("connprobe: query: %w", err)
	}
	return nil
}

func (s *pgxSession) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.conn.Close(ctx)
}

// identRE is the identifier rule of AGENTS.md section 4, rule 6.
var identRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// splitQualified splits "schema.table" and checks both parts.
func splitQualified(name string) (schema, table string, ok bool) {
	parts := strings.Split(name, ".")
	if len(parts) != 2 || !identRE.MatchString(parts[0]) || !identRE.MatchString(parts[1]) {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// safeName makes a role or database name from a connection string fit for a
// sentence: printable ASCII only, at most 63 bytes (Postgres' own limit).
func safeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 0x20 || r > 0x7e {
			r = '?'
		}
		b.WriteRune(r)
		if b.Len() >= 63 {
			break
		}
	}
	return b.String()
}

var plainRoleRE = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// roleSQL renders a role name for a fix statement: bare when it needs no
// quoting, quoted otherwise.
func roleSQL(role string) string {
	if plainRoleRE.MatchString(role) {
		return role
	}
	return pgx.Identifier{safeName(role)}.Sanitize()
}

// listNames joins up to max names and says how many were left out.
func listNames(names []string, max int) string {
	if len(names) <= max {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:max], ", "), len(names)-max)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// errClass groups server errors the checks tell apart. Only the class is used
// to pick fixed text; the server's own message never reaches a Check.
type errClass int

const (
	classOther errClass = iota
	classTimeout
	classPermission // SQLSTATE 42501
	classMissing    // 42P01 table, 3F000 schema, 42703 column
)

func classify(err error) errClass {
	if errors.Is(err, context.DeadlineExceeded) {
		return classTimeout
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.SQLState() {
		case "42501":
			return classPermission
		case "42P01", "3F000", "42703":
			return classMissing
		}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return classTimeout
	}
	return classOther
}

// pgOpened is what the leading config, reach and login steps produce.
type pgOpened struct {
	cfg    *pgx.ConnConfig
	sess   pgSession
	checks []Check
	ok     bool // all three passed; sess is open and must be closed
}

// pgNames are the setting names a fix refers to.
type pgNames struct {
	envVar string // "SOURCE_DB_URL"
	pipeRe string // the pipeline reference, "connections.source_db_url"
	what   string // "source database" / "warehouse"
}

// openPG runs the config, reach and login steps for dsn and returns the open
// session when all three pass. On a failure the later steps of the caller are
// reported as skipped, so the checks hold the failed step only.
func openPG(ctx context.Context, dsn string, n pgNames, g *Guard, connect pgConnector) pgOpened {
	var out pgOpened
	var cfg *pgx.ConnConfig

	cfgCheck := step(func() Check {
		c, chk := parsePGConfig(dsn, n, g)
		cfg = c
		return chk
	})
	out.checks = append(out.checks, cfgCheck)
	if cfgCheck.Status == StatusFail {
		return out
	}
	out.cfg = cfg

	reach := step(func() Check { return reachPG(ctx, cfg, n, g) })
	out.checks = append(out.checks, reach)
	if reach.Status == StatusFail {
		return out
	}

	var sess pgSession
	login := step(func() Check {
		lctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		s, err := connect(lctx, cfg)
		if err != nil {
			return loginFailure(err, cfg, n)
		}
		sess = s
		return Check{ID: "login", Status: StatusOK, Detail: fmt.Sprintf("logged in as role %s to database %s", safeName(cfg.User), safeName(cfg.Database))}
	})
	out.checks = append(out.checks, login)
	if login.Status == StatusFail {
		return out
	}
	out.sess = sess
	out.ok = true
	return out
}

// parsePGConfig parses dsn and points the connection's dial at the guard.
// pgx would otherwise look the host up itself and dial the answer, and would
// try Unix sockets when the string names no host.
func parsePGConfig(dsn string, n pgNames, g *Guard) (*pgx.ConnConfig, Check) {
	if strings.TrimSpace(dsn) == "" {
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: fmt.Sprintf("no connection string is set for the %s", n.what),
			Fix:     fmt.Sprintf("Set %s (or the pipeline's %s) to a postgres:// connection string of a read-only role.", n.envVar, n.pipeRe)}
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		// The error text can quote parts of the string; say nothing of it.
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: fmt.Sprintf("the connection string in %s cannot be parsed", n.envVar),
			Fix:     "Use postgres://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require and URL-encode special characters in the password."}
	}
	hosts := append([]string{cfg.Host}, fallbackHosts(cfg)...)
	for _, h := range hosts {
		if h == "" || strings.HasPrefix(h, "/") {
			return nil, Check{ID: "config", Status: StatusFail,
				Problem: fmt.Sprintf("the connection string in %s does not name a TCP host", n.envVar),
				Fix:     "Name the database host and port in the connection string; Unix sockets are not tested."}
		}
	}
	// The read_only check asks the server what the role's own default is, so
	// a value forced from the connection string must not hide it.
	delete(cfg.RuntimeParams, "default_transaction_read_only")
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.ConnectTimeout = DefaultStepTimeout
	cfg.DialFunc = g.DialContext
	// Hand the host name to DialFunc unresolved: the guard resolves it once,
	// checks every address and dials the address it checked.
	cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
	return cfg, Check{ID: "config", Status: StatusOK,
		Detail: fmt.Sprintf("connection string parsed (role %s, database %s)", safeName(cfg.User), safeName(cfg.Database))}
}

func fallbackHosts(cfg *pgx.ConnConfig) []string {
	var hs []string
	for _, f := range cfg.Fallbacks {
		hs = append(hs, f.Host)
	}
	return hs
}

// reachPG opens a TCP connection to every distinct host:port of the
// connection string through the guard, then closes it.
func reachPG(ctx context.Context, cfg *pgx.ConnConfig, n pgNames, g *Guard) Check {
	type hp struct {
		host string
		port uint16
	}
	seen := map[hp]bool{}
	targets := []hp{{cfg.Host, cfg.Port}}
	for _, f := range cfg.Fallbacks {
		targets = append(targets, hp{f.Host, f.Port})
	}
	var reached []string
	for _, t := range targets {
		if seen[t] {
			continue
		}
		seen[t] = true
		addr := net.JoinHostPort(t.host, fmt.Sprint(t.port))
		dctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		conn, err := g.DialContext(dctx, "tcp", addr)
		cancel()
		if err != nil {
			return reachFailure(err, addr, n)
		}
		_ = conn.Close()
		reached = append(reached, addr)
	}
	return Check{ID: "reach", Status: StatusOK, Detail: "TCP connection to " + strings.Join(reached, ", ")}
}

func reachFailure(err error, addr string, n pgNames) Check {
	switch {
	case errors.Is(err, ErrBlockedAddress):
		return Check{ID: "reach", Status: StatusFail,
			Problem: fmt.Sprintf("the address of %s is not allowed (link-local, cloud-metadata and reserved addresses are always refused; loopback and private ones only when the server allows them)", addr),
			Fix:     fmt.Sprintf("Use the database's normal host name or address in %s.", n.envVar)}
	case classify(err) == classTimeout:
		return Check{ID: "reach", Status: StatusFail,
			Problem: fmt.Sprintf("connecting to %s timed out", addr),
			Fix:     "Check the host and port, and that a firewall or security group allows this server to reach the database."}
	}
	return Check{ID: "reach", Status: StatusFail,
		Problem: fmt.Sprintf("cannot open a TCP connection to %s", addr),
		Fix:     "Check the host name and port, that the database is listening on it, and that a firewall or security group allows this server."}
}

func loginFailure(err error, cfg *pgx.ConnConfig, n pgNames) Check {
	role, db := safeName(cfg.User), safeName(cfg.Database)
	if errors.Is(err, ErrBlockedAddress) {
		return Check{ID: "login", Status: StatusFail,
			Problem: "the database address is not allowed",
			Fix:     fmt.Sprintf("Use the database's normal host name or address in %s.", n.envVar)}
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.SQLState() {
		case "28P01":
			return Check{ID: "login", Status: StatusFail,
				Problem: fmt.Sprintf("password authentication failed for role %s", role),
				Fix:     fmt.Sprintf("Check the password of role %s in %s (URL-encode special characters).", role, n.envVar)}
		case "28000":
			return Check{ID: "login", Status: StatusFail,
				Problem: fmt.Sprintf("role %s is not allowed to log in from this server", role),
				Fix:     fmt.Sprintf("Check that role %s exists with LOGIN and that pg_hba.conf has a rule for this server's address.", role)}
		case "3D000":
			return Check{ID: "login", Status: StatusFail,
				Problem: fmt.Sprintf("database %s does not exist", db),
				Fix:     fmt.Sprintf("Check the database name in %s.", n.envVar)}
		case "53300":
			return Check{ID: "login", Status: StatusFail,
				Problem: fmt.Sprintf("role %s has no free connection slots", role),
				Fix:     fmt.Sprintf("Raise the CONNECTION LIMIT of role %s or close idle sessions.", role)}
		}
	}
	if classify(err) == classTimeout {
		return Check{ID: "login", Status: StatusFail,
			Problem: "logging in timed out",
			Fix:     "Check that the database accepts connections and that sslmode matches what the server supports."}
	}
	return Check{ID: "login", Status: StatusFail,
		Problem: fmt.Sprintf("cannot log in as role %s to database %s", role, db),
		Fix:     fmt.Sprintf("Check the user, password, database name and sslmode in %s.", n.envVar)}
}

// readOnlyCheck reports the role's default_transaction_read_only. It is a
// second layer: Piperoom also forces it on its own connections, and the role
// itself must not be able to write (docs/pilot/permissions.md).
func readOnlyCheck(ctx context.Context, s pgSession, role string) Check {
	return step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		var v string
		if err := s.queryRow(sctx, sqlReadOnly, nil, &v); err != nil {
			return Check{ID: "read_only", Status: StatusFail,
				Problem: "cannot read the role's default_transaction_read_only setting",
				Fix:     "Check that the database accepts a plain SELECT from this role."}
		}
		if v == "on" {
			return Check{ID: "read_only", Status: StatusOK, Detail: "default_transaction_read_only = on"}
		}
		return Check{ID: "read_only", Status: StatusWarn,
			Problem: fmt.Sprintf("role %s starts read-write transactions (default_transaction_read_only = %s)", safeName(role), safeName(v)),
			Fix:     fmt.Sprintf("ALTER ROLE %s SET default_transaction_read_only = on;", roleSQL(role))}
	})
}

// SQL used by the checks. Fixed text: object names only ever travel as
// parameters, already validated and quoted.
const (
	sqlReadOnly = `SELECT current_setting('default_transaction_read_only')`
	sqlTimeout  = `SELECT current_setting('statement_timeout'), (SELECT setting::bigint FROM pg_settings WHERE name = 'statement_timeout')`
	sqlSlots    = `SELECT count(*) FROM pg_replication_slots`
	sqlPubs     = `SELECT count(*) FROM pg_publication_tables`
	sqlSuper    = `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`
	sqlTableSel = `SELECT has_schema_privilege(current_user, $1::text, 'USAGE'), has_table_privilege(current_user, $2::text, 'SELECT')`
	sqlColSel   = `SELECT has_column_privilege(current_user, $1::text, $2::text, 'SELECT')`
	sqlTableWr  = `SELECT has_table_privilege(current_user, $1::text, 'INSERT,UPDATE,DELETE')`
)

// selectItem is one table whose SELECT privilege (and, for a sink, timestamp
// column) is checked.
type selectItem struct {
	name  string // "schema.table" as written in owners.yaml
	tsCol string // timestamp column; "" = none to check
}

// selectCheck checks USAGE on the schema and SELECT on every item for the
// current role. noun is "source" or "sink". The problem lists object names
// only; the fix quotes the grants of docs/pilot/permissions.md.
func selectCheck(ctx context.Context, s pgSession, id, noun, role string, items []selectItem) Check {
	if len(items) == 0 {
		return Check{ID: id, Status: StatusSkip, Detail: fmt.Sprintf("no %s tables are configured for this connection", noun)}
	}
	return step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()

		var invalid, missing, denied, badCol, deniedCol []string
		var fixes []string
		cols := 0
		for _, it := range items {
			schema, table, ok := splitQualified(it.name)
			if !ok {
				invalid = append(invalid, "a "+noun+" table name")
				continue
			}
			full := pgx.Identifier{schema, table}.Sanitize()
			// has_schema_privilege takes the schema's plain name (validated
			// above), has_table_privilege a name it parses like a regclass, so
			// the table is passed quoted to keep its exact case.
			var usage, sel bool
			err := s.queryRow(sctx, sqlTableSel, []any{schema, full}, &usage, &sel)
			switch {
			case err == nil && usage && sel:
				// fine
			case err == nil, classify(err) == classPermission:
				denied = append(denied, it.name)
				fixes = append(fixes,
					fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;", schema, roleSQL(role)),
					fmt.Sprintf("GRANT SELECT ON %s TO %s;", it.name, roleSQL(role)))
				continue
			case classify(err) == classMissing:
				missing = append(missing, it.name)
				continue
			case classify(err) == classTimeout:
				return Check{ID: id, Status: StatusFail,
					Problem: fmt.Sprintf("checking SELECT on the %s tables timed out", noun),
					Fix:     "Check that the database answers queries from this role."}
			default:
				return Check{ID: id, Status: StatusFail,
					Problem: fmt.Sprintf("cannot check SELECT on %s", it.name),
					Fix:     "Check that the database accepts a plain SELECT from this role."}
			}
			if it.tsCol == "" {
				continue
			}
			if !identRE.MatchString(it.tsCol) {
				badCol = append(badCol, "the timestamp column of "+it.name)
				continue
			}
			var colOK bool
			err = s.queryRow(sctx, sqlColSel, []any{full, it.tsCol}, &colOK)
			switch {
			case err == nil && colOK:
				cols++
			case err == nil, classify(err) == classPermission:
				deniedCol = append(deniedCol, it.tsCol+" of "+it.name)
				fixes = append(fixes, fmt.Sprintf("GRANT SELECT (%s) ON %s TO %s;", pgx.Identifier{it.tsCol}.Sanitize(), it.name, roleSQL(role)))
			case classify(err) == classMissing:
				badCol = append(badCol, it.tsCol+" of "+it.name)
			default:
				return Check{ID: id, Status: StatusFail,
					Problem: fmt.Sprintf("cannot check SELECT on column %s of %s", it.tsCol, it.name),
					Fix:     "Check that the database accepts a plain SELECT from this role."}
			}
		}

		var problems []string
		if len(invalid) > 0 {
			problems = append(problems, fmt.Sprintf("%s is not written schema.table", listNames(invalid, 3)))
		}
		if len(missing) > 0 {
			problems = append(problems, fmt.Sprintf("%s does not exist or is not visible to the role", listNames(missing, 5)))
		}
		if len(denied) > 0 {
			problems = append(problems, fmt.Sprintf("role %s cannot SELECT %s", safeName(role), listNames(denied, 5)))
		}
		if len(badCol) > 0 {
			problems = append(problems, fmt.Sprintf("no such column: %s", listNames(badCol, 5)))
		}
		if len(deniedCol) > 0 {
			problems = append(problems, fmt.Sprintf("role %s cannot SELECT column %s", safeName(role), listNames(deniedCol, 5)))
		}
		if len(problems) == 0 {
			d := fmt.Sprintf("SELECT on %s", plural(len(items), noun+" table", noun+" tables"))
			if cols > 0 {
				d += fmt.Sprintf(" and %s", plural(cols, "timestamp column", "timestamp columns"))
			}
			return Check{ID: id, Status: StatusOK, Detail: d}
		}
		fix := dedupeJoin(fixes, 6)
		if len(missing)+len(invalid)+len(badCol) > 0 {
			extra := "Fix " + cfgKeys(noun) + " in owners.yaml so they name objects in this database."
			if fix != "" {
				fix += " "
			}
			fix += extra
		}
		return Check{ID: id, Status: StatusFail, Problem: strings.Join(problems, "; ") + ".", Fix: fix}
	})
}

// dedupeJoin joins fix statements, each once, at most max of them.
func dedupeJoin(fixes []string, max int) string {
	seen := map[string]bool{}
	var out []string
	for _, f := range fixes {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	if len(out) > max {
		out = append(out[:max], fmt.Sprintf("(and %d more grants)", len(out)-max))
	}
	return strings.Join(out, " ")
}

// skipRest appends a skip check for each id in order.
func skipRest(checks []Check, why string, ids ...string) []Check {
	for _, id := range ids {
		checks = append(checks, skipped(id, why))
	}
	return checks
}
