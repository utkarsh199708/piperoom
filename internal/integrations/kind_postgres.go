package integrations

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/integrations/spec"
)

// Postgres kind ids.
const (
	KindPostgresSource    = "postgres-source"
	KindWarehousePostgres = "warehouse-postgres"
)

// TLS modes offered for Postgres. There is no "require" (encrypt without
// verifying): like netsec, every TLS mode verifies the server.
const (
	sslVerifyFull = "verify-full"
	sslVerifyCA   = "verify-ca"
	sslDisable    = "disable"
)

const minPGVersion = 130000

var (
	identRe      = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	plainRoleRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	hostPattern  = `^[A-Za-z0-9._:\[\]-]+$`
	portPattern  = `^[0-9]{1,5}$`
	tablePattern = `^\s*([A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*(\s*,\s*([A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*)*\s*$`
)

// pgSession is the part of a Postgres connection the checks use.
type pgSession interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	// TLS reports whether the connection is encrypted.
	TLS() bool
	Close(ctx context.Context) error
}

// pgTarget is a parsed Postgres form.
type pgTarget struct {
	Host     string
	Port     uint16
	Database string
	User     string
	Password netsec.Secret
	SSLMode  string
	TLS      tlsMaterial
	Tables   []string // schema.table, validated
}

type pgConnector func(ctx context.Context, t pgTarget) (pgSession, error)

func pgFields(tablesHelp string) []spec.Field {
	return []spec.Field{
		{Name: "host", Label: "Host", Help: "Database host name or IP address", Type: spec.FieldText, Required: true, Pattern: hostPattern},
		{Name: "port", Label: "Port", Type: spec.FieldNumber, Default: "5432", Pattern: portPattern},
		{Name: "database", Label: "Database", Type: spec.FieldText, Required: true, Pattern: `^[A-Za-z0-9_$-]+$`},
		// AllowRef: a Vault database-engine lease issues a user name per
		// lease, so it is referenced from the same secret as the password
		// (secret://vault/database/creds/<role>#username and #password read
		// one cached lease).
		{Name: "user", Label: "User", Help: "A dedicated read-only role; the setup script creates one. For short-lived logins, a secret:// reference such as secret://vault/database/creds/ro#username", Type: spec.FieldText, Required: true, Pattern: `^[A-Za-z0-9_.@-]+$`, AllowRef: true},
		{Name: "password", Label: "Password", Type: spec.FieldSecret, Required: true},
		{Name: "sslmode", Label: "TLS mode", Help: "verify-full checks the certificate and the host name", Type: spec.FieldSelect, Default: sslVerifyFull, Options: []string{sslVerifyFull, sslVerifyCA, sslDisable}},
		{Name: "ca_cert", Label: "CA certificate", Help: "PEM; empty = the system's trusted CAs", Type: spec.FieldPEM},
		{Name: "tables", Label: "Tables", Help: tablesHelp, Type: spec.FieldText, Pattern: tablePattern},
	}
}

// parsePG reads a Postgres form. Errors name the field, never a value.
func parsePG(v spec.Values) (pgTarget, error) {
	t := pgTarget{
		Host: strings.Trim(strings.TrimSpace(v.Get("host")), "[]"), Database: strings.TrimSpace(v.Get("database")),
		User: strings.TrimSpace(v.Get("user")), Password: v.Secret("password"),
		SSLMode: v.Get("sslmode"), TLS: tlsMaterial{CA: v.Secret("ca_cert")},
	}
	if t.Host == "" {
		return t, &spec.ValidationError{Field: "host", Message: "required"}
	}
	port := v.Get("port")
	if port == "" {
		port = "5432"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return t, &spec.ValidationError{Field: "port", Message: "must be 1-65535"}
	}
	t.Port = uint16(n)
	if t.SSLMode == "" {
		t.SSLMode = sslVerifyFull
	}
	switch t.SSLMode {
	case sslVerifyFull, sslVerifyCA, sslDisable:
	default:
		return t, &spec.ValidationError{Field: "sslmode", Message: "must be verify-full, verify-ca or disable"}
	}
	if t.SSLMode == sslDisable && t.TLS.CA != "" {
		return t, &spec.ValidationError{Field: "ca_cert", Message: "a CA certificate is set but TLS is disabled"}
	}
	tables, err := parseTables(v.Get("tables"))
	if err != nil {
		return t, err
	}
	t.Tables = tables
	return t, nil
}

// parseTables splits "public.orders, customers" into validated
// schema-qualified names ("public.customers").
func parseTables(s string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, raw := range strings.Split(s, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		parts := strings.Split(name, ".")
		if len(parts) == 1 {
			parts = []string{"public", parts[0]}
		}
		if len(parts) != 2 || !identRe.MatchString(parts[0]) || !identRe.MatchString(parts[1]) {
			return nil, &spec.ValidationError{Field: "tables", Message: "each table must be schema.table or table (letters, digits, _)"}
		}
		q := parts[0] + "." + parts[1]
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	return out, nil
}

// roleName is the role as it should appear in a fix: bare when it is a
// plain lower-case identifier, quoted otherwise.
// sqlIdent writes an identifier the way a DBA would paste it: plain lower
// case as-is, anything else quoted, so the fix targets the same object the
// check looked at (Postgres folds unquoted names to lower case).
func sqlIdent(name string) string {
	if plainRoleRe.MatchString(name) {
		return name
	}
	return pgx.Identifier{name}.Sanitize()
}

// sqlTable writes schema.table for fix text and setup scripts.
func sqlTable(tbl string) string {
	parts := strings.SplitN(tbl, ".", 2)
	if len(parts) != 2 {
		return sqlIdent(tbl)
	}
	return sqlIdent(parts[0]) + "." + sqlIdent(parts[1])
}

func roleName(user string) string {
	if user == "" {
		return "<role>"
	}
	if plainRoleRe.MatchString(user) {
		return user
	}
	return pgx.Identifier{user}.Sanitize()
}

// tlsConfig builds the TLS settings of sslmode. verify-ca checks the chain
// but not the host name, done by hand because crypto/tls has no such mode.
func (t pgTarget) tlsConfig() (*tls.Config, error) {
	if t.SSLMode == sslDisable {
		return nil, nil
	}
	cfg, err := t.TLS.config()
	if err != nil {
		return nil, err
	}
	cfg.ServerName = t.Host
	if t.SSLMode == sslVerifyCA {
		roots := cfg.RootCAs
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server sent no certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter})
			return err
		}
	}
	return cfg, nil
}

// realPGConnect opens one connection through the guard. The session does not
// set default_transaction_read_only, so the checks see the role's own setting.
func realPGConnect(g *Guard) pgConnector {
	return func(ctx context.Context, t pgTarget) (pgSession, error) {
		cfg, err := pgx.ParseConfig("sslmode=disable")
		if err != nil {
			return nil, fmt.Errorf("integrations: postgres config: %w", err)
		}
		tc, err := t.tlsConfig()
		if err != nil {
			return nil, err
		}
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password = t.Host, t.Port, t.Database, t.User, t.Password.Reveal()
		cfg.TLSConfig = tc
		cfg.Fallbacks = nil
		cfg.ConnectTimeout = 0
		// Hand the host name to the guard's dialer, which resolves and checks it.
		cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
		cfg.DialFunc = g.DialContext
		cfg.RuntimeParams = map[string]string{"application_name": "piperoom-test"}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return pgxSession{conn}, nil
	}
}

type pgxSession struct{ c *pgx.Conn }

func (s pgxSession) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return s.c.QueryRow(ctx, sql, args...)
}

func (s pgxSession) TLS() bool {
	_, ok := s.c.PgConn().Conn().(*tls.Conn)
	return ok
}

func (s pgxSession) Close(ctx context.Context) error { return s.c.Close(ctx) }

// sslRequestCode is the SSLRequest message code of the Postgres protocol.
const sslRequestCode = 80877103

// sslRefused reports whether the server at host:port answers an SSLRequest
// with 'N' (no TLS). pgconn turns that answer into an untyped error, so the
// login error cannot tell it apart; asking again on a fresh connection can.
// false when the probe cannot tell (no answer, dial error).
func (d Deps) sslRefused(ctx context.Context, host, port string) bool {
	if d.pgSSLProbe != nil {
		return d.pgSSLProbe(ctx, host, port)
	}
	ctx, cancel := context.WithTimeout(ctx, d.checkTimeout())
	defer cancel()
	c, err := d.dial()(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return false
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	msg := make([]byte, 8)
	binary.BigEndian.PutUint32(msg[0:4], 8)
	binary.BigEndian.PutUint32(msg[4:8], sslRequestCode)
	if _, err := c.Write(msg); err != nil {
		return false
	}
	reply := make([]byte, 1)
	if _, err := io.ReadFull(c, reply); err != nil {
		return false
	}
	return reply[0] == 'N'
}

// pgCode returns the SQLSTATE of err, or "".
func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// pgKindOpts differ between the source and the warehouse card.
type pgKindOpts struct {
	source bool // version, recovery and slot checks
}

func (d Deps) postgresTest(o pgKindOpts) func(ctx context.Context, v spec.Values) []spec.Check {
	return func(ctx context.Context, v spec.Values) []spec.Check {
		ctx, cancel := testCtx(ctx, d.TestTimeout)
		defer cancel()
		cl := newChecklist(ctx, d.CheckTimeout, v)
		t, err := parsePG(v)
		if err != nil {
			cl.add("input", "Form values are valid", fail(err.Error(), ""))
			return cl.result()
		}
		role := roleName(t.User)
		port := strconv.Itoa(int(t.Port))

		if cl.run("reach", "Can reach "+net.JoinHostPort(t.Host, port), reachCheck(d.dial(), t.Host, port)) != spec.CheckPass {
			cl.skip("login", "Login works", "skipped: the host is not reachable")
			return cl.result()
		}

		// The connection outlives this deadline: pgx uses ctx only to connect.
		lctx, lcancel := context.WithTimeout(ctx, d.checkTimeout())
		sess, loginErr := d.pgConnect(lctx, t)
		timedOut := errors.Is(lctx.Err(), context.DeadlineExceeded)
		lcancel()

		// TLS line.
		switch {
		case t.SSLMode == sslDisable:
			cl.add("tls", "Connection is encrypted (TLS)", warn("TLS is disabled: the password and query results cross the network in clear text",
				"Set TLS mode to verify-full (and paste the server's CA if it is private)."))
		case loginErr != nil && pgCode(loginErr) == "" && !isTLSError(loginErr) && d.sslRefused(ctx, t.Host, port):
			// pgconn reports a refused SSLRequest as plain text, so the
			// server is asked again on a fresh connection instead.
			cl.add("tls", "TLS handshake", fail("the server did not use TLS: it refused the TLS request", "Enable ssl on the server, or set TLS mode to disable for a local test only."))
			cl.skip("login", "Login works", "skipped: the server does not accept TLS")
			return cl.result()
		case loginErr != nil && isTLSError(loginErr):
			cl.add("tls", "TLS handshake", fail("TLS handshake failed: "+errText(loginErr),
				"Paste the CA certificate that signs the database's certificate (for RDS: the region's RDS CA bundle), or use the host name on the certificate."))
		case loginErr != nil && pgCode(loginErr) == "":
			cl.skip("tls", "TLS handshake", "skipped: the connection failed before TLS could be checked")
		case loginErr == nil && !sess.TLS():
			cl.add("tls", "TLS handshake", fail("the server did not use TLS", "Enable ssl on the server, or set TLS mode to disable for a local test only."))
		default:
			cl.add("tls", "TLS handshake", pass(""))
		}

		if loginErr != nil {
			cl.add("login", "Login works", pgLoginFailure(loginErr, timedOut, t, role))
			return cl.result()
		}
		cl.add("login", "Login works as "+t.User, pass(""))
		defer func() {
			cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), d.checkTimeout())
			defer ccancel()
			_ = sess.Close(cctx)
		}()

		if d.pgCAUnsupported(t) {
			cl.add("tls_ca", "The live client can use the pasted CA", fail(
				"the test used the pasted CA, but the server's Postgres client takes TLS files only (netsec PEM support, integrations PR 2 is not merged)",
				"Leave the CA empty and use a certificate signed by a trusted CA, or set the source with SOURCE_DB_URL and sslrootcert until PR 2 lands."))
		}

		if o.source {
			cl.run("version", "Postgres 13 or newer", func(ctx context.Context) outcome {
				var num int
				if err := sess.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&num); err != nil {
					return fail("cannot read server_version_num: "+errText(err), "")
				}
				if num < minPGVersion {
					return fail(fmt.Sprintf("server version %s is older than 13", pgVersionText(num)),
						"Upgrade the database to Postgres 13 or newer; the watcher reads safe_wal_size, added in 13.")
				}
				return pass(fmt.Sprintf("version %d", num/10000))
			})
			cl.run("primary", "Connected to the primary (not a replica)", func(ctx context.Context) outcome {
				var rec bool
				if err := sess.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&rec); err != nil {
					return fail("cannot call pg_is_in_recovery(): "+errText(err), "")
				}
				if rec {
					return fail("this server is a read replica (pg_is_in_recovery() is true)",
						"Point Piperoom at the primary: replication slots and their WAL live there.")
				}
				return pass("")
			})
			cl.run("slots_visible", "Can read pg_replication_slots", func(ctx context.Context) outcome {
				var n int
				if err := sess.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots").Scan(&n); err != nil {
					return fail(fmt.Sprintf("%s cannot read pg_replication_slots: %s", role, errText(err)),
						// pg_monitor carries no SELECT on this view; the default
						// grant is to PUBLIC, so a revoked one is granted back.
						fmt.Sprintf("GRANT SELECT ON pg_catalog.pg_replication_slots TO %s;", role))
				}
				if n == 0 {
					return warn("no replication slots exist; Debezium creates one when its connector starts", "")
				}
				return pass(fmt.Sprintf("%d slot(s)", n))
			})
		}

		var super bool
		cl.run("not_superuser", "Role is not a superuser", func(ctx context.Context) outcome {
			if err := sess.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil {
				return fail("cannot read pg_roles: "+errText(err), "")
			}
			if super {
				return fail(role+" is a superuser, so it can write to every table",
					"Create a dedicated read-only role with the setup script and use it here.")
			}
			return pass("")
		})

		if len(t.Tables) == 0 {
			cl.skip("tables", "Table privileges", "No tables listed: add the pipeline's tables to check that the role can read them and cannot write them.")
		}
		for _, tbl := range t.Tables {
			pgTableChecks(cl, sess, tbl, role, super)
		}

		cl.run("timeout", "Role has a statement_timeout or is read-only by default", func(ctx context.Context) outcome {
			var timeout, ro string
			if err := sess.QueryRow(ctx, "SELECT current_setting('statement_timeout'), current_setting('default_transaction_read_only')").Scan(&timeout, &ro); err != nil {
				return warn("cannot read the role's settings: "+errText(err), "")
			}
			var missing []string
			if timeout == "0" {
				missing = append(missing, fmt.Sprintf("ALTER ROLE %s SET statement_timeout = '10s';", role))
			}
			if ro != "on" {
				missing = append(missing, fmt.Sprintf("ALTER ROLE %s SET default_transaction_read_only = on;", role))
			}
			if timeout == "0" && ro != "on" {
				return warn("statement_timeout is 0 and default_transaction_read_only is off for this role", strings.Join(missing, " "))
			}
			if len(missing) > 0 {
				return pass("recommended as well: " + strings.Join(missing, " "))
			}
			return pass("")
		})
		return cl.result()
	}
}

func pgLoginFailure(err error, timedOut bool, t pgTarget, role string) outcome {
	switch code := pgCode(err); {
	case timedOut:
		return fail("login timed out", "Check that the server accepts connections from the Piperoom host (pg_hba.conf, security group).")
	case code == "28P01":
		return fail("password authentication failed for "+t.User, "Check the user and password, or reset it: ALTER ROLE "+role+" PASSWORD '<new password>';")
	case code == "28000":
		return fail("the server refused this login (pg_hba.conf or a missing role)",
			"Add a pg_hba.conf line for "+t.User+" from the Piperoom host, or create the role with the setup script.")
	case code == "3D000":
		return fail("database "+t.Database+" does not exist", "Check the database name.")
	case code == "42501":
		return fail(t.User+" may not connect to "+t.Database, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s;", pgx.Identifier{t.Database}.Sanitize(), role))
	case isTLSError(err):
		return skippedOutcome("skipped: the TLS handshake failed")
	default:
		return fail("cannot log in: "+errText(err), "Check the host, port, user and password.")
	}
}

// pgWritePrivs are the table privileges that let a role change a table or
// its data. Column grants (INSERT/UPDATE on some columns) are no table
// privilege, so they are checked with has_any_column_privilege.
var pgWritePrivs = []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}

// pgTableChecks adds the SELECT and read-only checks for one table. super is
// the not_superuser check's finding: grants do not bind a superuser, so a
// REVOKE fix would do nothing.
func pgTableChecks(cl *checklist, sess pgSession, tbl, role string, super bool) {
	parts := strings.SplitN(tbl, ".", 2)
	quoted := pgx.Identifier{parts[0], parts[1]}.Sanitize()
	var sel, colIns, colUpd bool
	table := make([]bool, len(pgWritePrivs))
	var qerr error
	cl.run("select:"+tbl, "Role can SELECT from "+tbl, func(ctx context.Context) outcome {
		dest := []any{&sel}
		q := "SELECT has_table_privilege($1::text, 'SELECT')"
		for i, p := range pgWritePrivs {
			q += ", has_table_privilege($1::text, '" + p + "')"
			dest = append(dest, &table[i])
		}
		q += ", has_any_column_privilege($1::text, 'INSERT'), has_any_column_privilege($1::text, 'UPDATE')"
		dest = append(dest, &colIns, &colUpd)
		qerr = sess.QueryRow(ctx, q, quoted).Scan(dest...)
		switch {
		case qerr != nil && pgCode(qerr) == "42P01":
			return fail("table "+tbl+" does not exist", "Check the table name; list schema.table as it is in the database.")
		case qerr != nil:
			return fail("cannot check privileges on "+tbl+": "+errText(qerr), "")
		case !sel:
			return fail(role+" cannot SELECT from "+tbl, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s; GRANT SELECT ON %s TO %s;", sqlIdent(parts[0]), role, sqlTable(tbl), role))
		}
		return pass("")
	})
	if super {
		cl.add("read_only:"+tbl, "Role cannot write to "+tbl, fail("superuser: table grants do not apply",
			"Create a dedicated read-only role with the setup script and use it here."))
		return
	}
	if qerr != nil {
		cl.skip("read_only:"+tbl, "Role cannot write to "+tbl, "skipped: the privilege check failed")
		return
	}
	cl.run("read_only:"+tbl, "Role cannot INSERT, UPDATE, DELETE or TRUNCATE "+tbl, func(ctx context.Context) outcome {
		var can, fixes []string
		var tablePrivs []string
		for i, p := range pgWritePrivs {
			if table[i] {
				can = append(can, p)
				tablePrivs = append(tablePrivs, p)
			}
		}
		if len(tablePrivs) > 0 {
			fixes = append(fixes, fmt.Sprintf("REVOKE %s ON %s FROM %s;", strings.Join(tablePrivs, ", "), sqlTable(tbl), role))
		}
		// A column grant shows only when the table grant of the same
		// privilege is absent (has_any_column_privilege includes it).
		for _, c := range []struct {
			priv       string
			col, whole bool
		}{{"INSERT", colIns, table[0]}, {"UPDATE", colUpd, table[1]}} {
			if !c.col || c.whole {
				continue
			}
			can = append(can, c.priv+" (some columns)")
			cols := pgGrantedColumns(ctx, sess, quoted, c.priv)
			if cols == "" {
				cols = "<columns>"
			}
			fixes = append(fixes, fmt.Sprintf("REVOKE %s (%s) ON %s FROM %s;", c.priv, cols, sqlTable(tbl), role))
		}
		if len(can) > 0 {
			return fail(role+" can "+strings.Join(can, ", ")+" on "+tbl, strings.Join(fixes, " "))
		}
		return pass("")
	})
}

// pgGrantedColumns lists the columns of quoted on which the current role
// holds priv, quoted for a REVOKE, or "" when they cannot be read.
func pgGrantedColumns(ctx context.Context, sess pgSession, quoted, priv string) string {
	var cols string
	err := sess.QueryRow(ctx,
		"SELECT coalesce(string_agg(quote_ident(attname), ', ' ORDER BY attnum), '') FROM pg_attribute "+
			"WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped AND has_column_privilege(attrelid, attnum, $2::text)",
		quoted, priv).Scan(&cols)
	if err != nil {
		return ""
	}
	return cols
}

// pgVersionText formats server_version_num: since Postgres 10 it is
// major*10000+minor; before that major*10000+minor*100+patch.
func pgVersionText(num int) string {
	if num >= 100000 {
		return fmt.Sprintf("%d.%d", num/10000, num%10000)
	}
	return fmt.Sprintf("%d.%d.%d", num/10000, num%10000/100, num%100)
}

// pgURL builds the connection URL for postgres.New / warehouse.New. It holds
// the password: it goes straight into the constructor and is never logged.
func pgURL(t pgTarget) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(t.User, t.Password.Reveal()),
		Host:   net.JoinHostPort(t.Host, strconv.Itoa(int(t.Port))),
		Path:   "/" + t.Database,
	}
	q := url.Values{}
	q.Set("sslmode", t.SSLMode)
	q.Set("application_name", "piperoom")
	u.RawQuery = q.Encode()
	return u.String()
}

// pgCAUnsupported reports a pasted CA that postgres.New cannot take yet: it
// reads TLS settings from the URL, and sslrootcert is a file path.
func (d Deps) pgCAUnsupported(t pgTarget) bool { return t.TLS.CA != "" }

func (d Deps) postgresBuild(source bool) func(ctx context.Context, v spec.Values) (any, func(), error) {
	return func(ctx context.Context, v spec.Values) (any, func(), error) {
		t, err := parsePG(v)
		if err != nil {
			return nil, nil, err
		}
		if d.pgCAUnsupported(t) {
			return nil, nil, fmt.Errorf("%w: a pasted Postgres CA needs netsec PEM support (integrations PR 2)", ErrUnsupported)
		}
		if err := d.guard().CheckHost(ctx, t.Host); err != nil {
			return nil, nil, err
		}
		if source {
			db, err := postgres.New(ctx, pgURL(t))
			if err != nil {
				return nil, nil, scrubErr(fmt.Errorf("integrations: source postgres: %w", err), t.Password)
			}
			return db, db.Close, nil
		}
		db, err := warehouse.New(ctx, pgURL(t))
		if err != nil {
			return nil, nil, scrubErr(fmt.Errorf("integrations: warehouse postgres: %w", err), t.Password)
		}
		return db, db.Close, nil
	}
}

// pgSetupScript is the read-only role script from docs/pilot/permissions.md,
// filled with the form's role, database and tables. Unsafe names become
// placeholders, so the script never carries injected SQL.
func pgSetupScript(source bool) func(v spec.Values) string {
	return func(v spec.Values) string {
		role := strings.TrimSpace(v.Get("user"))
		if !plainRoleRe.MatchString(role) {
			role = "piperoom_ro"
		}
		db := strings.TrimSpace(v.Get("database"))
		if !identRe.MatchString(db) {
			db = "<database>"
		}
		tables, err := parseTables(v.Get("tables"))
		if err != nil {
			tables = nil
		}
		var b strings.Builder
		fmt.Fprintf(&b, "-- Read-only role for Piperoom. Run as an admin of %s.\n", db)
		// No password literal: a script run unedited must not leave a login
		// with a known (published) password. The admin sets it afterwards.
		fmt.Fprintf(&b, "CREATE ROLE %s LOGIN\n", role)
		b.WriteString("  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS\n  CONNECTION LIMIT 5;\n")
		fmt.Fprintf(&b, "ALTER ROLE %s SET default_transaction_read_only = on;\n", role)
		fmt.Fprintf(&b, "ALTER ROLE %s SET statement_timeout = '10s';\n", role)
		fmt.Fprintf(&b, "-- Then set its password from your secret store, in psql: \\password %s\n\n", role)
		fmt.Fprintf(&b, "GRANT CONNECT ON DATABASE %s TO %s;\n", db, role)
		if len(tables) == 0 {
			fmt.Fprintf(&b, "\n-- Only the pipeline's tables (list them in the form to fill this in).\n")
			fmt.Fprintf(&b, "GRANT USAGE ON SCHEMA public TO %s;\nGRANT SELECT ON public.<table> TO %s;\n", role, role)
		} else {
			b.WriteString("\n-- Only the pipeline's tables. Never GRANT ... ON ALL TABLES.\n")
			schemas := map[string]bool{}
			for _, t := range tables {
				s := strings.SplitN(t, ".", 2)[0]
				if !schemas[s] {
					schemas[s] = true
					fmt.Fprintf(&b, "GRANT USAGE ON SCHEMA %s TO %s;\n", sqlIdent(s), role)
				}
			}
			for _, t := range tables {
				fmt.Fprintf(&b, "GRANT SELECT ON %s TO %s;\n", sqlTable(t), role)
			}
		}
		if source {
			// Optional, so it must not fail the script (psql -1 rolls back
			// everything on the first error) where the schema is absent.
			b.WriteString("\n-- Optional DDL audit log (pg_recent_ddl); see docs/pilot/permissions.md.\n")
			b.WriteString("-- Granted only when the piperoom_audit schema exists.\n")
			b.WriteString("DO $$\nBEGIN\n")
			b.WriteString("  IF to_regclass('piperoom_audit.ddl_log') IS NOT NULL THEN\n")
			fmt.Fprintf(&b, "    GRANT USAGE ON SCHEMA piperoom_audit TO %s;\n", role)
			fmt.Fprintf(&b, "    GRANT SELECT ON piperoom_audit.ddl_log TO %s;\n", role)
			b.WriteString("  END IF;\nEND $$;\n")
		}
		return b.String()
	}
}

func (d Deps) postgresKinds() []spec.Kind {
	return []spec.Kind{
		{
			ID: KindPostgresSource, Category: spec.CategorySourceDB, Title: "PostgreSQL",
			Summary: "The CDC source database (self-hosted, RDS, Aurora, Cloud SQL)", Logo: "postgres",
			Reads:    []string{"replication slot state and retained WAL", "recent DDL from piperoom_audit.ddl_log", "long-running transaction ages (not their SQL)", "single rows of the pipeline's tables, by primary key, during an investigation", "table privileges of its own role"},
			Never:    []string{"writes to any table (sessions are read-only)", "reads tables outside the pipeline"},
			Fields:   pgFields("Comma list of the pipeline's source tables, e.g. public.orders, public.customers"),
			Required: true, SetupScript: pgSetupScript(true),
			Test: d.postgresTest(pgKindOpts{source: true}), Build: d.postgresBuild(true),
		},
		{
			ID: KindWarehousePostgres, Category: spec.CategoryWarehouse, Title: "PostgreSQL / Redshift warehouse",
			Summary: "The sink the pipeline writes to", Logo: "postgres",
			Reads:    []string{"the newest timestamp of each sink table (freshness)", "single rows by primary key during an investigation", "table privileges of its own role"},
			Never:    []string{"writes to any table (sessions are read-only)"},
			Fields:   pgFields("Comma list of the pipeline's sink tables, e.g. analytics.orders"),
			Required: true, SetupScript: pgSetupScript(false),
			Test: d.postgresTest(pgKindOpts{}), Build: d.postgresBuild(false),
		},
	}
}
