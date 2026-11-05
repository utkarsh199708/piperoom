package sqlserver

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// SetupWriter is the SQL Server write client, clients.SetupSourceWriter
// (U4a-2). It has a pool of its own on the write DSN (a separate role) and
// never shares a connection with the read-only Client. Only wiring, with write
// mode on, may build one.
//
// Every method validates identifiers against ^[a-zA-Z_][a-zA-Z0-9_]*$ and
// []-quotes the ones it must interpolate (CREATE TABLE, CREATE LOGIN, CREATE
// USER, GRANT); values (schema/table names to the stored procedures, the
// canary nonce) are passed as @pN parameters. The generated read-role password
// is interpolated as a quoted string literal, not bound, because CREATE LOGIN's
// DDL grammar has no parameter position for PASSWORD (see CreateReadRole).
// Each call is bounded by the write timeout (10 s by default) whatever ctx
// says.
//
// The pipeline scope fails closed, like tools.ScopeFor: every method needs the
// room's whole pipeline in ctx (tools.WithPipeline) or the explicit
// tools.WithUnscoped opt-out of CLI, tests and the runner. Anything else is
// tools.ErrNoScope before any I/O; a source table outside the pipeline is
// tools.ErrOutOfScope.
type SetupWriter struct {
	db      writeDB
	timeout time.Duration
	secret  SecretStore // nil = CreateReadRole refuses rather than drop a password
}

var _ clients.SetupSourceWriter = (*SetupWriter)(nil)

// ErrNoWriter is returned by NewSetupWriter when the write DSN is empty, and by
// every method of a nil or zero SetupWriter: the caller has no write
// credentials. Its message never carries a DSN or password.
var ErrNoWriter = errors.New("sqlserver: no write credentials (the SQL Server write DSN is not set)")

// errCaptureExists is returned by CreatePublication when the capture instance
// it would create already exists; nothing is altered.
var errCaptureExists = errors.New("sqlserver: capture instance already exists")

// errRoleExists is returned by CreateReadRole when the login already exists;
// the login is never re-passworded.
var errRoleExists = errors.New("sqlserver: login already exists")

// errCreateLogin is returned by CreateReadRole when CREATE LOGIN fails. It is
// deliberately generic: go-mssqldb echoes the statement text on failure, and
// that text now embeds the generated password literal, so the driver error is
// not wrapped.
var errCreateLogin = errors.New("CREATE LOGIN failed")

// writeDB is the part of *sql.DB the setup writer uses; tests substitute a
// fake. It extends the read interface with Exec for the writes.
type writeDB interface {
	db
	Exec(ctx context.Context, query string, args ...any) error
}

// Exec runs a statement that writes and keeps only its error; the writer does
// not need the rows affected.
func (d sqlDB) Exec(ctx context.Context, query string, args ...any) error {
	_, err := d.DB.ExecContext(ctx, query, args...)
	return err
}

// SecretStore stores one generated credential under a name. The writer
// generates the read-role password itself and never reads it back; the
// implementation is the runner's secret store (docs/design/setup-agent.md
// §3.7), whose values leave the runner only after the person's terminal
// confirmation.
type SecretStore interface {
	Put(ctx context.Context, name, value string) error
}

// SetupWriterOption changes a NewSetupWriter default.
type SetupWriterOption func(*setupWriterOptions)

type setupWriterOptions struct {
	timeout time.Duration
	secret  SecretStore
}

// WithSetupTimeout sets the deadline of each call (connect included). d <= 0
// keeps 10 s. Tests use it to keep the deadline short.
func WithSetupTimeout(d time.Duration) SetupWriterOption {
	return func(o *setupWriterOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithSecretStore injects the store that keeps the generated read-role
// password. Without one, CreateReadRole refuses rather than put the password in
// any return value.
func WithSecretStore(s SecretStore) SetupWriterOption {
	return func(o *setupWriterOptions) { o.secret = s }
}

func newSetupWriterOptions(opts []SetupWriterOption) setupWriterOptions {
	o := setupWriterOptions{timeout: callTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// NewSetupWriter opens the write pool on dsn and verifies it with a ping. An
// empty dsn returns ErrNoWriter and opens nothing. Errors never carry the DSN
// or its password.
func NewSetupWriter(ctx context.Context, dsn string, opts ...SetupWriterOption) (*SetupWriter, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, ErrNoWriter
	}
	o := newSetupWriterOptions(opts)
	d, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: setup writer: open: %w", redactDSN(err, dsn))
	}
	d.SetMaxOpenConns(2)
	d.SetMaxIdleConns(2)
	d.SetConnMaxLifetime(5 * time.Minute)
	s := sqlDB{d}
	pctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	if err := s.Ping(pctx); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("sqlserver: setup writer: connect: %w", redactDSN(err, dsn))
	}
	return &SetupWriter{db: s, timeout: o.timeout, secret: o.secret}, nil
}

// Close releases the write pool.
func (w *SetupWriter) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	return w.db.Close()
}

// enableDatabaseSQL turns CDC on for the current database. It is idempotent:
// when CDC is already on it returns 0 without an error.
const enableDatabaseSQL = `EXEC sys.sp_cdc_enable_db`

// enableTableSQL turns CDC on for one table. The schema, table and capture
// instance names are values (@pN); role_name is NULL so no gating role is used
// (access to the change data is granted to the read role by CreateReadRole
// instead).
const enableTableSQL = `EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = @p2, @role_name = NULL, @capture_instance = @p3`

// CreatePublication enables CDC on the database and on each of tables. The
// publication name becomes the capture instance: for a single table the
// capture instance is name; for several, table "schema.table" gets
// name_schema_table, so each capture instance stays unique (sp_cdc_enable_table
// requires it). A capture instance of that name already existing is an error
// and nothing is altered.
func (w *SetupWriter) CreatePublication(ctx context.Context, name string, tables []string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	if _, err := validateIdent(name); err != nil {
		return err
	}
	pairs, err := splitTables(tables)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return errors.New("sqlserver: CreatePublication: no tables given")
	}
	// The capture instance names are derived, and bounded by
	// MaxCaptureInstanceChars, before any I/O by the same function the setup
	// driver validates against (CaptureInstances).
	instances, err := CaptureInstances(name, tables)
	if err != nil {
		return err
	}
	if err := checkSetupScope(ctx, pairs); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	if err := w.db.Exec(ctx, enableDatabaseSQL); err != nil {
		return fmt.Errorf("sqlserver: enable CDC on the database: %w", err)
	}
	// Check every capture instance first, then enable in a second loop: if any
	// instance already exists the call must alter nothing, so no table may be
	// enabled before all the checks have passed.
	for _, inst := range instances {
		var n int64
		if err := w.db.QueryRow(ctx, `SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance = @p1`, inst).Scan(&n); err != nil {
			return fmt.Errorf("sqlserver: read capture instance %q: %w", inst, err)
		}
		if n > 0 {
			return fmt.Errorf("sqlserver: %w: capture instance %q already exists", errCaptureExists, inst)
		}
	}
	for i, p := range pairs {
		if err := w.db.Exec(ctx, enableTableSQL, p[0], p[1], instances[i]); err != nil {
			return fmt.Errorf("sqlserver: enable CDC on table %s.%s: %w", p[0], p[1], err)
		}
	}
	return nil
}

// captureInstance names the capture instance sp_cdc_enable_table creates for
// one table (see CreatePublication).
func captureInstance(name string, n int, schema, table string) string {
	if n == 1 {
		return name
	}
	return name + "_" + schema + "_" + table
}

// CreateCanaryTable creates the ladder's canary table ("schema.table": an id, a
// nonce column and a timestamp) with its primary key on nonce, so the connector
// keys the canary change event by the nonce, and enables CDC on it. It is
// idempotent: the table and its capture instance exist afterwards whether or
// not they did before.
func (w *SetupWriter) CreateCanaryTable(ctx context.Context, table string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	schema, tbl, err := splitTable(table)
	if err != nil {
		return err
	}
	if err := checkSetupScope(ctx, [][2]string{{schema, tbl}}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	qs, err := quoteIdent(schema)
	if err != nil {
		return err
	}
	qt, err := quoteIdent(tbl)
	if err != nil {
		return err
	}
	full := qs + "." + qt
	createSQL := fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (id BIGINT NOT NULL IDENTITY(1,1), nonce NVARCHAR(64) NOT NULL PRIMARY KEY, ts DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME())", full, full)
	if err := w.db.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("sqlserver: create canary table %s: %w", table, err)
	}
	var tracked bool
	if err := w.db.QueryRow(ctx, `SELECT is_tracked_by_cdc FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = @p1 AND t.name = @p2`, schema, tbl).Scan(&tracked); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlserver: read CDC state of canary table %s: %w", table, err)
		}
	}
	if !tracked {
		inst := schema + "_" + tbl
		if err := w.db.Exec(ctx, enableTableSQL, schema, tbl, inst); err != nil {
			return fmt.Errorf("sqlserver: enable CDC on canary table %s: %w", table, err)
		}
	}
	return nil
}

// InsertCanary inserts one canary row. The nonce is a value (@p1), never
// interpolated; id and ts are generated by the server (IDENTITY and the column
// default).
func (w *SetupWriter) InsertCanary(ctx context.Context, table, nonce string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	schema, tbl, err := splitTable(table)
	if err != nil {
		return err
	}
	if err := checkSetupScope(ctx, [][2]string{{schema, tbl}}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	qs, err := quoteIdent(schema)
	if err != nil {
		return err
	}
	qt, err := quoteIdent(tbl)
	if err != nil {
		return err
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s (nonce) VALUES (@p1)", qs, qt)
	if err := w.db.Exec(ctx, insertSQL, nonce); err != nil {
		return fmt.Errorf("sqlserver: insert canary row into %s: %w", table, err)
	}
	return nil
}

// CreateReadRole creates a login and a database user named role with the grants
// the Debezium SQL Server connector needs on tables (SELECT on each table,
// SELECT on the cdc schema for the change tables and functions, and VIEW
// DATABASE STATE for the LSN), with a password the writer generates and stores
// in the secret store. It returns only the store name; the password appears in
// no return value, log, event or prompt. An existing login of that name is an
// error, never re-passworded.
func (w *SetupWriter) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if w == nil || w.db == nil {
		return "", ErrNoWriter
	}
	login, err := validateIdent(role)
	if err != nil {
		return "", err
	}
	pairs, err := splitTables(tables)
	if err != nil {
		return "", err
	}
	if len(pairs) == 0 {
		return "", errors.New("sqlserver: CreateReadRole: no tables given")
	}
	if err := checkSetupScope(ctx, pairs); err != nil {
		return "", err
	}
	if w.secret == nil {
		return "", errors.New("sqlserver: CreateReadRole: no secret store to keep the generated password (pass WithSecretStore)")
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	var n int64
	if err := w.db.QueryRow(ctx, `SELECT COUNT(*) FROM sys.server_principals WHERE name = @p1 AND type IN ('S','U','G')`, login).Scan(&n); err != nil {
		return "", fmt.Errorf("sqlserver: read login %q: %w", login, err)
	}
	if n > 0 {
		return "", fmt.Errorf("sqlserver: %w: login %q already exists", errRoleExists, login)
	}

	password, err := randomPassword()
	if err != nil {
		return "", err
	}
	qlogin, err := quoteIdent(login)
	if err != nil {
		return "", err
	}

	// CREATE LOGIN ... WITH PASSWORD takes a string literal (or a hashed
	// password), not a TDS parameter: the DDL grammar has no parameter position
	// in this clause, so the password cannot be bound as @p1. The generated
	// password is hex (randomPassword) and quotePassword doubles any single
	// quote, so interpolating it as a quoted literal is safe; on failure the
	// driver error is not wrapped below because it can echo the statement.
	createLogin := fmt.Sprintf("CREATE LOGIN %s WITH PASSWORD = %s, CHECK_POLICY = OFF", qlogin, quotePassword(password))
	if err := w.db.Exec(ctx, createLogin); err != nil {
		return "", fmt.Errorf("sqlserver: create login %q: %w", login, errCreateLogin)
	}
	if err := w.db.Exec(ctx, fmt.Sprintf("CREATE USER %s FOR LOGIN %s", qlogin, qlogin)); err != nil {
		return "", fmt.Errorf("sqlserver: create user for login %q: %w", login, err)
	}
	for _, p := range pairs {
		qs, qerr := quoteIdent(p[0])
		if qerr != nil {
			return "", qerr
		}
		qt, qerr := quoteIdent(p[1])
		if qerr != nil {
			return "", qerr
		}
		if err := w.db.Exec(ctx, fmt.Sprintf("GRANT SELECT ON %s.%s TO %s", qs, qt, qlogin)); err != nil {
			return "", fmt.Errorf("sqlserver: grant SELECT on %s.%s to %q: %w", p[0], p[1], login, err)
		}
	}
	if err := w.db.Exec(ctx, "GRANT SELECT ON SCHEMA::cdc TO "+qlogin); err != nil {
		return "", fmt.Errorf("sqlserver: grant SELECT on schema cdc to %q: %w", login, err)
	}
	if err := w.db.Exec(ctx, "GRANT VIEW DATABASE STATE TO "+qlogin); err != nil {
		return "", fmt.Errorf("sqlserver: grant VIEW DATABASE STATE to %q: %w", login, err)
	}

	name := "mssql_" + login
	if err := w.secret.Put(ctx, name, password); err != nil {
		putErr := fmt.Errorf("sqlserver: store read-role password: %w", err)
		// The password is lost, so a retry would hit errRoleExists on the login
		// this call just created. Roll it back: drop the database user first (it
		// depends on the login), then the login, best effort, and report the Put
		// error joined with any drop errors. login is a validated identifier and
		// qlogin is []-quoted, so interpolating it into the DROP statements is safe.
		dropErrs := make([]error, 0, 2)
		if derr := w.db.Exec(ctx, "DROP USER "+qlogin); derr != nil {
			dropErrs = append(dropErrs, fmt.Errorf("sqlserver: drop user %q after secret-store failure: %w", login, derr))
		}
		if derr := w.db.Exec(ctx, "DROP LOGIN "+qlogin); derr != nil {
			dropErrs = append(dropErrs, fmt.Errorf("sqlserver: drop login %q after secret-store failure: %w", login, derr))
		}
		return "", errors.Join(append([]error{putErr}, dropErrs...)...)
	}
	return name, nil
}

// AlterSystem has no SQL Server equivalent in this wave (SQL Server changes
// settings with sp_configure, not ALTER SYSTEM), so it answers
// clients.ErrNotSupported without any I/O.
func (w *SetupWriter) AlterSystem(context.Context, string, string) (bool, error) {
	return false, fmt.Errorf("sqlserver: ALTER SYSTEM: %w", clients.ErrNotSupported)
}

// randomPassword returns a fresh hex password (24 random bytes) for a login.
func randomPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("sqlserver: generate read-role password: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// quotePassword quotes password as a SQL Server string literal. The generated
// password is hex, so it holds only [0-9a-f] and never a quote; the doubling of
// any single quote is defensive, in case the literal form is ever reused for a
// caller-supplied value.
func quotePassword(password string) string {
	return "'" + strings.ReplaceAll(password, "'", "''") + "'"
}

// checkSetupScope is the writer's scope gate, in the order of tools.ScopeFor:
// with the room's whole pipeline in ctx a table outside it is
// tools.ErrOutOfScope; tools.WithUnscoped passes; anything else (a context that
// names the pipeline only by id included) is tools.ErrNoScope before any I/O.
// Every table a call will touch must be passed, so no write can ever reach a
// table the pipeline does not authorize (the canary target included).
func checkSetupScope(ctx context.Context, tables [][2]string) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("sqlserver: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	if sc.Unscoped() {
		return nil
	}
	p := sc.Pipeline()
	for _, t := range tables {
		name := t[0] + "." + t[1]
		if !slices.Contains(p.SourceTables, name) {
			return fmt.Errorf("sqlserver: %w: table %s is not a source table of pipeline %q", tools.ErrOutOfScope, name, p.ID)
		}
	}
	return nil
}

// redactDSN scrubs dsn, and the password inside a go-mssqldb URL, from err's
// text, so a connect or parse error never carries credentials into a log, event
// or prompt.
func redactDSN(err error, dsn string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if dsn != "" {
		msg = strings.ReplaceAll(msg, dsn, "REDACTED")
	}
	if u, perr := url.Parse(dsn); perr == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" {
			msg = strings.ReplaceAll(msg, pw, "REDACTED")
		}
	}
	return errors.New(msg)
}
