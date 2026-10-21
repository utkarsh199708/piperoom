package oracle

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

	go_ora "github.com/sijms/go-ora/v2"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Writer is the write half of the Oracle client: clients.SetupSourceWriter.
// It has its own pool on a write-capable DSN, never the read-only Client pool,
// and only the setup runner (write mode on) builds one. Every method validates
// its identifiers with the rule 6 check of identifier.go and quotes them, so
// no statement is built from an untrusted fragment; values are bound with :1
// positional parameters. Each method is bounded by the per-call deadline
// (queryTimeout, 10 s) whatever ctx says, and fails closed on the room's
// pipeline scope exactly like the Postgres SourceWriter: the room's whole
// pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped
// opt-out, otherwise tools.ErrNoScope before any I/O.
type Writer struct {
	db      *sql.DB     // nil in tests; closed by Close
	dbx     writeDB     // the db; a fake in tests
	target  string      // receipt target: oracle host:port/service, never credentials
	secrets SecretStore // nil makes CreateReadRole refuse before creating anything
}

var _ clients.SetupSourceWriter = (*Writer)(nil)

// ErrNoWriter is returned by NewWriter when dsn is empty, and by every method
// of a nil or zero Writer. An empty DSN is the write-mode-off signal for the
// Oracle source (the write pool has no credentials), so wiring hands the setup
// stack no Writer and the setup actions refuse at Validate.
var ErrNoWriter = errors.New("oracle: no write credentials (a write DSN is required)")

// errNoPassword refuses a write DSN without a password. The write pool must
// authenticate as its own role; an empty password almost always means the
// read-only DSN was reused by mistake.
var errNoPassword = errors.New("oracle: the DSN has no password; the write pool needs its own credentials")

// SecretStore stores one named credential and is how CreateReadRole deposits
// the password it generated without ever returning or logging it. The setup
// runner's secret store implements it; a nil store makes CreateReadRole refuse
// before it creates the role, so a generated password is never lost.
type SecretStore interface {
	Store(ctx context.Context, name, value string) error
}

// readPasswordSecretName is the secret-store name CreateReadRole stores the
// generated password under and returns. It is fixed (one source, one read
// role), so the source driver references it as ${secret:source_read_password}.
const readPasswordSecretName = "source_read_password"

// WriterOption changes a NewWriter default.
type WriterOption func(*writerOptions)

type writerOptions struct {
	secrets SecretStore
}

// WithSecretStore sets the store CreateReadRole writes the generated password
// to. A nil store (the default) makes CreateReadRole refuse before it creates
// the role: a generated password must never be lost.
func WithSecretStore(s SecretStore) WriterOption {
	return func(o *writerOptions) { o.secrets = s }
}

func newWriterOptions(opts []WriterOption) writerOptions {
	var o writerOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// NewWriter opens the write pool on dsn and verifies it with a ping under the
// per-call deadline. dsn is a go-ora URL with its own write role:
// oracle://user:password@host:port/service. An empty dsn returns ErrNoWriter
// and opens nothing; a DSN without a password is refused. Errors never carry
// the DSN or its password.
func NewWriter(ctx context.Context, dsn string, opts ...WriterOption) (*Writer, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, ErrNoWriter
	}
	cfg, err := go_ora.ParseConfig(dsn)
	if err != nil {
		// go-ora returns url.Parse's error unchanged; *url.Error prints the
		// full URL (including the password). Report only the inner cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			return nil, fmt.Errorf("oracle: writer: parse config: %w", ue.Err)
		}
		return nil, fmt.Errorf("oracle: writer: parse config: %w", err)
	}
	if cfg.Password == "" {
		return nil, errNoPassword
	}

	o := newWriterOptions(opts)
	db := sql.OpenDB(go_ora.NewConnector(dsn))
	// Setup writes are rare and sequential; one session is enough and keeps
	// the write role from opening extra sessions on the production source.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(queryTimeout)

	pingCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("oracle: writer: ping: %w", err)
	}

	return &Writer{db: db, dbx: dbWriter{db}, target: targetOf(dsn), secrets: o.secrets}, nil
}

// Close releases the write pool.
func (w *Writer) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	return w.db.Close()
}

// writeDB runs the statements a setup write needs: reads (existence checks)
// and writes (DDL/DML). dbWriter adapts *sql.DB to it; a fake replaces it in
// tests. The QueryContext signature returns sqlRows (not *sql.Rows) so tests
// can supply deterministic results, the same reason the read client has its
// own querier adapter.
type writeDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error)
}

// dbWriter adapts *sql.DB to writeDB.
type dbWriter struct {
	db *sql.DB
}

func (q dbWriter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.db.ExecContext(ctx, query, args...)
}

func (q dbWriter) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	return q.db.QueryContext(ctx, query, args...)
}

// exec runs one write statement under the caller's (already deadline-scoped)
// context.
func (w *Writer) exec(ctx context.Context, stmt string, args ...any) error {
	if _, err := w.dbx.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("oracle: exec: %w", err)
	}
	return nil
}

// exists reports whether a query returns at least one row.
func (w *Writer) exists(ctx context.Context, q string, args ...any) (bool, error) {
	rows, err := w.dbx.QueryContext(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("oracle: query: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return true, nil
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("oracle: read: %w", err)
	}
	return false, nil
}

// writeTables validates tables ("owner.table" each) and drops repeats,
// keeping the first order.
func writeTables(tables []string) ([]string, error) {
	if len(tables) == 0 {
		return nil, errors.New("oracle: no tables given")
	}
	out := make([]string, 0, len(tables))
	seen := make(map[string]bool, len(tables))
	for _, t := range tables {
		if _, _, err := tableParts(t); err != nil {
			return nil, err
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out, nil
}

// writeScope resolves the scope gate exactly like tools.ScopeFor. It fails
// closed: the room's whole pipeline, or the explicit tools.WithUnscoped
// opt-out, or tools.ErrNoScope before any I/O.
func (w *Writer) writeScope(ctx context.Context) (tools.Scope, error) {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return tools.Scope{}, fmt.Errorf("oracle: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	return sc, nil
}

// checkScope requires the scope gate to pass, without checking any table.
func (w *Writer) checkScope(ctx context.Context) error {
	_, err := w.writeScope(ctx)
	return err
}

// checkTables requires the scope gate and that every table is one of the
// pipeline's source tables.
func (w *Writer) checkTables(ctx context.Context, tables []string) error {
	sc, err := w.writeScope(ctx)
	if err != nil {
		return err
	}
	if sc.Unscoped() {
		return nil
	}
	p := sc.Pipeline()
	for _, t := range tables {
		if !slices.Contains(p.SourceTables, t) {
			return fmt.Errorf("oracle: %w: table %q is not a source table of pipeline %q", tools.ErrOutOfScope, t, p.ID)
		}
	}
	return nil
}

// supplementalLoggingSQL is the statement that creates the all-column
// supplemental log group the Debezium Oracle connector requires. quotedTable
// is a "owner"."table" pair produced by quoteTable.
func supplementalLoggingSQL(quotedTable string) string {
	return "ALTER TABLE " + quotedTable + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
}

// CreatePublication enables supplemental logging for each table, which is
// Oracle's equivalent of a Postgres publication: one ALTER TABLE ... ADD
// SUPPLEMENTAL LOG DATA (ALL) COLUMNS per table. Oracle has no publication
// object, so name is ignored and never reaches a statement. Every table must
// be one of the room's pipeline source tables.
func (w *Writer) CreatePublication(ctx context.Context, name string, tables []string) error {
	if w == nil || w.dbx == nil {
		return ErrNoWriter
	}
	list, err := writeTables(tables)
	if err != nil {
		return err
	}
	if err := w.checkTables(ctx, list); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	for _, t := range list {
		quoted, err := quoteTable(t)
		if err != nil {
			return err
		}
		if err := w.exec(ctx, supplementalLoggingSQL(quoted)); err != nil {
			return fmt.Errorf("oracle: supplemental logging for table %q: %w", t, err)
		}
	}
	return nil
}

// tableExistsQuery asks ALL_TABLES whether one table exists, with the owner
// and name bound as values.
const tableExistsQuery = `SELECT 1 FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`

// canaryCreateSQL is the ladder's canary table: an id, a nonce column (the
// primary key, so Debezium uses it as the message key) and a timestamp.
const canaryCreateSQL = `("ID" NUMBER GENERATED BY DEFAULT AS IDENTITY NOT NULL, "NONCE" VARCHAR2(64) NOT NULL, "TS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("NONCE"))`

// CreateCanaryTable creates the ladder's canary table ("owner.table") with
// the id, nonce and timestamp columns and the supplemental logging the
// connector needs. It is idempotent: a table that already exists is left as
// it is, so the table exists afterwards whether or not it did before.
func (w *Writer) CreateCanaryTable(ctx context.Context, table string) error {
	if w == nil || w.dbx == nil {
		return ErrNoWriter
	}
	owner, name, err := tableParts(table)
	if err != nil {
		return err
	}
	if err := w.checkScope(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	exists, err := w.exists(ctx, tableExistsQuery, owner, name)
	if err != nil {
		return fmt.Errorf("oracle: check canary table %q: %w", table, err)
	}
	if exists {
		return nil
	}
	quoted, err := quoteTable(table)
	if err != nil {
		return err
	}
	if err := w.exec(ctx, "CREATE TABLE "+quoted+" "+canaryCreateSQL); err != nil {
		return fmt.Errorf("oracle: create canary table %q: %w", table, err)
	}
	if err := w.exec(ctx, supplementalLoggingSQL(quoted)); err != nil {
		return fmt.Errorf("oracle: canary supplemental logging %q: %w", table, err)
	}
	return nil
}

// InsertCanary inserts one row into the canary table with nonce as its
// (primary-key) nonce value, bound as a value so it is never spliced into the
// statement. The nonce comes from the ladder runner, never from the model.
func (w *Writer) InsertCanary(ctx context.Context, table, nonce string) error {
	if w == nil || w.dbx == nil {
		return ErrNoWriter
	}
	if _, _, err := tableParts(table); err != nil {
		return err
	}
	if err := w.checkScope(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(nonce) == "" {
		return errors.New("oracle: insert canary: no nonce")
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	quoted, err := quoteTable(table)
	if err != nil {
		return err
	}
	if err := w.exec(ctx, `INSERT INTO `+quoted+` ("NONCE") VALUES (:1)`, nonce); err != nil {
		return fmt.Errorf("oracle: insert canary into %q: %w", table, err)
	}
	return nil
}

// userExistsQuery asks ALL_USERS whether a user exists, with the name bound.
const userExistsQuery = `SELECT 1 FROM ALL_USERS WHERE USERNAME = :1`

// logminerSysPrivs are the system privileges the Debezium Oracle connector
// docs grant the LogMiner user. Fixed, never user input.
var logminerSysPrivs = []string{
	"CREATE SESSION",
	"SET CONTAINER",
	"FLASHBACK ANY TABLE",
	"SELECT ANY TRANSACTION",
	"LOGMINING",
}

// logminerObjects are the dictionary objects the Debezium Oracle connector
// docs grant the LogMiner user SELECT on. Fixed, never user input.
var logminerObjects = []string{
	"V_$DATABASE",
	"V_$ARCHIVED_LOG",
	"V_$LOGMNR_CONTENTS",
	"V_$LOGMNR_LOGS",
	"V_$LOG_HISTORY",
	"V_$LOG",
	"V_$THREAD",
	"DBA_TABLES",
	"DBA_TAB_COLUMNS",
	"DBA_INDEXES",
	"DBA_TRIGGERS",
	"DBA_CONSTRAINTS",
	"DBA_CONS_COLUMNS",
	"DBA_LOG_GROUPS",
	"DBA_REGISTRY",
}

// logminerRoles are the roles the Debezium Oracle connector docs grant the
// LogMiner user. Fixed, never user input.
var logminerRoles = []string{"SELECT_CATALOG_ROLE"}

// quotePassword quotes the generated password in the CREATE USER statement.
// The password is always hex ([0-9a-f]), so there is nothing to escape, but
// quoting keeps Oracle from folding its case.
func quotePassword(pw string) string {
	return `"` + strings.ReplaceAll(pw, `"`, `""`) + `"`
}

// randomHex returns n random bytes hex-encoded (2n lowercase hex characters).
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("oracle: random: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// CreateReadRole creates role with the grants of the Debezium Oracle docs (the
// LogMiner system privileges, the dictionary SELECTs and SELECT_CATALOG_ROLE)
// plus SELECT on each of tables, and a password this method generates. The
// password is stored in the secret store under readPasswordSecretName and is
// in no return value, log or error; the method returns only that name. An
// existing role is an error, never re-passworded. The password is stored
// before the role is created, so a store that cannot store it refuses with no
// role created; if the CREATE USER then fails, a random password for a role
// that does not exist is left in the store (harmless) rather than a role whose
// password was lost.
func (w *Writer) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if w == nil || w.dbx == nil {
		return "", ErrNoWriter
	}
	quotedRole, err := quoteIdent(role)
	if err != nil {
		return "", err
	}
	list, err := writeTables(tables)
	if err != nil {
		return "", err
	}
	if err := w.checkTables(ctx, list); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	exists, err := w.exists(ctx, userExistsQuery, role)
	if err != nil {
		return "", fmt.Errorf("oracle: check read role %q: %w", role, err)
	}
	if exists {
		return "", fmt.Errorf("oracle: read role %q already exists; it is never re-passworded", role)
	}

	if w.secrets == nil {
		return "", errors.New("oracle: no secret store: refusing to create a read role whose password could not be stored")
	}
	password, err := randomHex(16)
	if err != nil {
		return "", err
	}
	if err := w.secrets.Store(ctx, readPasswordSecretName, password); err != nil {
		return "", fmt.Errorf("oracle: store read role password: %w", err)
	}

	// CREATE USER carries the generated password; neither the statement nor
	// the password is logged or put into an error.
	if err := w.exec(ctx, "CREATE USER "+quotedRole+" IDENTIFIED BY "+quotePassword(password)); err != nil {
		return "", fmt.Errorf("oracle: create read role %q: %w", role, err)
	}
	for _, p := range logminerSysPrivs {
		if err := w.exec(ctx, "GRANT "+p+" TO "+quotedRole); err != nil {
			return "", fmt.Errorf("oracle: grant %s to role %q: %w", p, role, err)
		}
	}
	for _, o := range logminerObjects {
		if err := w.exec(ctx, "GRANT SELECT ON "+o+" TO "+quotedRole); err != nil {
			return "", fmt.Errorf("oracle: grant SELECT on %s to role %q: %w", o, role, err)
		}
	}
	for _, r := range logminerRoles {
		if err := w.exec(ctx, "GRANT "+r+" TO "+quotedRole); err != nil {
			return "", fmt.Errorf("oracle: grant %s to role %q: %w", r, role, err)
		}
	}
	for _, t := range list {
		quoted, err := quoteTable(t)
		if err != nil {
			return "", err
		}
		if err := w.exec(ctx, "GRANT SELECT ON "+quoted+" TO "+quotedRole); err != nil {
			return "", fmt.Errorf("oracle: grant SELECT on %q to role %q: %w", t, role, err)
		}
	}
	return readPasswordSecretName, nil
}

// alterSystemAllow are the Oracle-equivalent setting names AlterSystem may
// change. Oracle's only setup setting is enabling archive log mode (the
// design §2.5 "source's equivalent" of wal_level); every other name is refused
// before any I/O.
var alterSystemAllow = map[string]bool{
	"archive_log": true,
}

// AlterSystem sets one server setting. For Oracle the only allow-listed name
// is archive_log, which issues ALTER DATABASE ARCHIVELOG (value is accepted
// but unused: enabling archive log mode has no value to set, and the only form
// offered turns it on, never off) and reports pendingRestart true, because the
// mode takes effect only after a restart. It is a T3 op: the room service
// creates and executes it only inside a maintenance window.
func (w *Writer) AlterSystem(ctx context.Context, name, value string) (bool, error) {
	if w == nil || w.dbx == nil {
		return false, ErrNoWriter
	}
	if !alterSystemAllow[name] {
		return false, fmt.Errorf("oracle: alter system: %q is not an allow-listed setting (only archive_log)", name)
	}
	if err := w.checkScope(ctx); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if err := w.exec(ctx, "ALTER DATABASE ARCHIVELOG"); err != nil {
		return false, fmt.Errorf("oracle: alter system archive_log: %w", err)
	}
	return true, nil
}
