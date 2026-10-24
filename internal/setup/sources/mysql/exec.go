package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
	mysqlclient "piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// SetupWriter is the executing MySQL clients.SetupSourceWriter: the runner's
// local implementation of the setup-only writes (U2a). It runs on a write pool
// of its own (SOURCE_DB_WRITE_URL in production; the dev profile's write user in
// tests), never the read-only pool, and runs the SQL of this package's builders
// (CanaryTableSQL, InsertCanarySQL, CreateUserSQL, ReadUserGrants) with a
// deadline and wrapped errors. A room whose writes go through the paired
// runner's op protocol uses remoteclients.Source instead; both satisfy the same
// contract.
//
// Every identifier is checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ and
// backtick-quoted before it becomes SQL text; the canary nonce is a ? parameter.
// The generated read-user password is the one value placed in statement text:
// CREATE USER has no parameter position that every supported server accepts
// (see CreateReadRole). No method returns the password, logs it, or reads it
// back from the store.
//
// The fail-closed scope rule of the contract applies to every method: the room's
// whole MySQL pipeline in ctx (tools.WithPipeline) or the explicit
// tools.WithUnscoped opt-out, tools.ErrNoScope / tools.ErrOutOfScope before any
// I/O. Each call is bounded by the write timeout (10 s by default) whatever ctx
// says.
type SetupWriter struct {
	db      sqlDB
	closer  func() error
	timeout time.Duration
	secret  SecretStore // nil = CreateReadRole refuses rather than drop a password
}

var _ clients.SetupSourceWriter = (*SetupWriter)(nil)

// sqlDB is the part of *sql.DB the setup writer uses; tests substitute a fake
// database/sql driver under a real *sql.DB.
type sqlDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// SecretStore stores one generated credential under a name. The writer generates
// the read-user password itself and never reads it back; the implementation is
// the runner's secret store (docs/design/setup-agent.md §3.7), whose values
// leave the runner only after the person's terminal confirmation.
type SecretStore interface {
	Store(ctx context.Context, name, value string) error
}

// readPasswordSecretName is the store name CreateReadRole keeps the generated
// password under and returns, so the connector config references it as
// ${secret:source_read_password}.
const readPasswordSecretName = "source_read_password"

// setupWriteTimeout bounds each call; a whole setup write is a few short
// statements.
const setupWriteTimeout = 10 * time.Second

// MySQL error numbers the writer maps.
const (
	errBadDB             = 1049 // ER_BAD_DB_ERROR
	errNoSuchTable       = 1146 // ER_NO_SUCH_TABLE
	errCannotUser        = 1396 // ER_CANNOT_USER: CREATE USER failed (it exists)
	errDBAccessDenied    = 1044 // ER_DBACCESS_DENIED_ERROR
	errTableDenied       = 1142 // ER_TABLEACCESS_DENIED_ERROR
	errSpecificDenied    = 1227 // ER_SPECIFIC_ACCESS_DENIED_ERROR
	errNonexistingGrant  = 1141 // ER_NONEXISTING_GRANT
	errNoMatchingUserRow = 1133 // ER_PASSWORD_NO_MATCH
)

// SetupWriterOption changes a NewSetupWriter default.
type SetupWriterOption func(*setupWriterOptions)

type setupWriterOptions struct {
	timeout time.Duration
	secret  SecretStore
}

// WithSetupTimeout sets the deadline of each call. d <= 0 keeps 10 s.
func WithSetupTimeout(d time.Duration) SetupWriterOption {
	return func(o *setupWriterOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithSetupSecretStore injects the store that keeps the generated read-user
// password. Without one, CreateReadRole refuses before it creates anything
// rather than put the password in a return value.
func WithSetupSecretStore(s SecretStore) SetupWriterOption {
	return func(o *setupWriterOptions) { o.secret = s }
}

func newSetupWriterOptions(opts []SetupWriterOption) setupWriterOptions {
	o := setupWriterOptions{timeout: setupWriteTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// OpenSetupWriter opens the write pool on rawURL (a mysql:// URL, the Q8 client's
// rules: a parameter allow-list, TLS on any non-loopback host) and checks that
// its sessions are writable. An empty rawURL returns ErrNoWriter and opens
// nothing. Errors never carry the URL or its password.
func OpenSetupWriter(ctx context.Context, rawURL string, opts ...SetupWriterOption) (*SetupWriter, error) {
	if strings.TrimSpace(rawURL) == "" {
		return nil, ErrNoWriter
	}
	pool, err := mysqlclient.OpenSetupPool(ctx, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql setup writer: %w", err)
	}
	w, err := NewSetupWriter(ctx, pool, opts...)
	if err != nil {
		_ = pool.Close()
		return nil, err
	}
	w.closer = pool.Close
	return w, nil
}

// NewSetupWriter wraps an open write pool and checks that its sessions are
// writable. A nil pool is ErrNoWriter. The caller closes the pool.
func NewSetupWriter(ctx context.Context, db *sql.DB, opts ...SetupWriterOption) (*SetupWriter, error) {
	if db == nil {
		return nil, ErrNoWriter
	}
	o := newSetupWriterOptions(opts)
	pctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	if err := checkSetupWritable(pctx, db); err != nil {
		return nil, err
	}
	return &SetupWriter{db: db, timeout: o.timeout, secret: o.secret}, nil
}

// Close releases the pool when OpenSetupWriter opened it.
func (w *SetupWriter) Close() error {
	if w == nil || w.closer == nil {
		return nil
	}
	return w.closer()
}

// setupWritableSQL reads what makes every write of the pool fail.
const setupWritableSQL = `SELECT @@session.transaction_read_only, @@global.read_only, @@global.super_read_only`

// checkSetupWritable connects (the first query opens the first connection) and
// refuses a pool whose sessions start read-only (most likely the read-only user
// given by mistake) and a read-only server, which is a replica where every
// write fails.
func checkSetupWritable(ctx context.Context, db sqlDB) error {
	var sessionRO, readOnly, superRO int64
	if err := db.QueryRowContext(ctx, setupWritableSQL).Scan(&sessionRO, &readOnly, &superRO); err != nil {
		return fmt.Errorf("mysql setup writer: connect: %w", wrapTimeout(err))
	}
	if sessionRO == 1 {
		return errors.New("mysql setup writer: the write session has transaction_read_only = 1; the setup needs its own write user, never the read-only SOURCE_DB_URL user")
	}
	if readOnly == 1 || superRO == 1 {
		return errors.New("mysql setup writer: the server is read-only (read_only or super_read_only is on): the write URL points at a replica; the setup needs the primary")
	}
	return nil
}

// wrapTimeout names a deadline in the message and keeps the cause.
func wrapTimeout(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("timed out: %w", err)
	}
	return err
}

// Errors of the executing setup writer. Compare with errors.Is.
var (
	// errUserExists: CREATE USER found a user of that name. The contract says
	// an existing user is an error, never re-passworded.
	errUserExists = errors.New("mysql setup writer: user already exists (an existing user is never re-passworded)")
	// errCreateUser is returned when CREATE USER fails for any other reason. It
	// is deliberately generic: the statement carries the generated password
	// literal, so the server/driver error text is not wrapped.
	errCreateUser = errors.New("mysql setup writer: CREATE USER failed")
)

// CreatePublication answers clients.ErrNotSupported before any I/O: MySQL has no
// publications.
func (w *SetupWriter) CreatePublication(context.Context, string, []string) error {
	return errNoPublication()
}

// AlterSystem answers clients.ErrNotSupported before any I/O, whatever the name
// and value: this wave offers no SET PERSIST on MySQL.
func (w *SetupWriter) AlterSystem(_ context.Context, name, _ string) (bool, error) {
	return false, errNoAlterSystem(name)
}

// CreateCanaryTable creates the ladder's canary table ("db.table",
// CanaryTableSQL). It exists afterwards whether or not it did before.
func (w *SetupWriter) CreateCanaryTable(ctx context.Context, table string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	stmt, err := CanaryTableSQL(table)
	if err != nil {
		return fmt.Errorf("mysql setup writer: create canary table: %w", err)
	}
	if err := checkSetupScope(ctx, []string{table}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql setup writer: create canary table %s: %w", clipName(table), mapWriteErr(err, "CREATE on the database"))
	}
	return nil
}

// InsertCanary inserts one row with nonce into the canary table. The nonce is a
// ? parameter, never SQL text.
func (w *SetupWriter) InsertCanary(ctx context.Context, table, nonce string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	stmt, err := InsertCanarySQL(table)
	if err != nil {
		return fmt.Errorf("mysql setup writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	if err := checkSetupScope(ctx, []string{table}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	if _, err := w.db.ExecContext(ctx, stmt, nonce); err != nil {
		return fmt.Errorf("mysql setup writer: insert canary into %s: %w", clipName(table), mapWriteErr(err, "INSERT on the table"))
	}
	return nil
}

// CreateReadRole creates user (account 'user'@'%') with a generated password and
// the ReadUserGrants grants, stores the password in the secret store and returns
// only the store name (readPasswordSecretName). The password is in no return
// value, log or error. An existing user is errUserExists and nothing is changed:
// it is never re-passworded.
//
// The write user needs CREATE USER, and each privilege it grants with GRANT
// OPTION (SELECT on the tables, RELOAD, SHOW DATABASES, LOCK TABLES,
// REPLICATION SLAVE and REPLICATION CLIENT globally): an admin account, not the
// dev profile's piperoom_write user.
func (w *SetupWriter) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if w == nil || w.db == nil {
		return "", ErrNoWriter
	}
	if err := checkUser(role); err != nil {
		return "", fmt.Errorf("mysql setup writer: create read role: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("mysql setup writer: create read role %s: %w", clipName(role), err)
	}
	if w.secret == nil {
		return "", errors.New("mysql setup writer: CreateReadRole: no secret store to keep the generated password (pass WithSetupSecretStore)")
	}
	grants, err := ReadUserGrants(role, tables)
	if err != nil {
		return "", fmt.Errorf("mysql setup writer: create read role %s: %w", clipName(role), err)
	}
	if err := checkSetupScope(ctx, tables); err != nil {
		return "", err
	}
	password, err := randomPassword()
	if err != nil {
		return "", err
	}
	create, err := CreateUserSQL(role, password)
	if err != nil {
		return "", fmt.Errorf("mysql setup writer: create read role %s: %w", clipName(role), err)
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	// On failure the error is not wrapped: a driver or server message could echo
	// the statement and with it the password literal.
	if _, err := w.db.ExecContext(ctx, create); err != nil {
		switch errNumber(err) {
		case errCannotUser:
			return "", fmt.Errorf("mysql setup writer: create read role %s: %w", clipName(role), errUserExists)
		case errSpecificDenied, errDBAccessDenied:
			return "", fmt.Errorf("mysql setup writer: create read role %s: the write user needs the CREATE USER privilege (error %d)", clipName(role), errNumber(err))
		}
		return "", fmt.Errorf("mysql setup writer: create read role %s: %w (error %d)", clipName(role), errCreateUser, errNumber(err))
	}
	for _, stmt := range grants {
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			// The user exists now with a password nobody can read: drop it so
			// the retry is a clean create, and report both errors.
			dropErr := w.dropUserBestEffort(ctx, role)
			return "", errors.Join(fmt.Errorf("mysql setup writer: create read role %s: grant: %w", clipName(role), mapWriteErr(err, "the privilege WITH GRANT OPTION")), dropErr)
		}
	}
	if err := w.secret.Store(ctx, readPasswordSecretName, password); err != nil {
		// The password is lost, so the user must not stay behind: a retry would
		// hit errUserExists for an account whose password no one can supply.
		dropErr := w.dropUserBestEffort(ctx, role)
		return "", errors.Join(fmt.Errorf("mysql setup writer: create read role %s: store the password: %w", clipName(role), err), dropErr)
	}
	return readPasswordSecretName, nil
}

// dropUserBestEffort drops the user after a failed create, with its own
// deadline (the call's may be spent), and returns the drop error, if any.
func (w *SetupWriter) dropUserBestEffort(ctx context.Context, role string) error {
	stmt, err := DropUserSQL(role)
	if err != nil {
		return err
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := w.db.ExecContext(dctx, stmt); err != nil {
		return fmt.Errorf("mysql setup writer: drop read user %s after the failed create: %w", clipName(role), err)
	}
	return nil
}

// mapWriteErr turns the server errors a caller branches on into the contract's
// sentinels and names the missing privilege; anything else is returned as is.
func mapWriteErr(err error, need string) error {
	switch errNumber(err) {
	case errBadDB, errNoSuchTable:
		return fmt.Errorf("%w: %w", clients.ErrNotFound, err)
	case errDBAccessDenied, errTableDenied, errSpecificDenied, errNonexistingGrant:
		return fmt.Errorf("the write user needs %s: %w", need, err)
	case errNoMatchingUserRow:
		return fmt.Errorf("the account is gone: %w: %w", clients.ErrNotFound, err)
	}
	return wrapTimeout(err)
}

// errNumber is the MySQL error number of a server error, 0 for any other error.
func errNumber(err error) uint16 {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

// checkSetupScope is the contract's fail-closed scope gate: the room's whole
// MySQL pipeline in ctx (every table one of its source tables), the explicit
// tools.WithUnscoped opt-out, or tools.ErrNoScope / tools.ErrOutOfScope before
// any I/O. A context that names the pipeline only by id is ErrNoScope: this
// writer has no owners config to resolve it and must not guess.
func checkSetupScope(ctx context.Context, tables []string) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("mysql setup writer: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	if sc.Unscoped() {
		return nil
	}
	if sc.SourceKind() != owners.SourceKindMySQL {
		return fmt.Errorf("mysql setup writer: %w: pipeline %q is not a MySQL cdc pipeline (source_kind %q)", tools.ErrOutOfScope, sc.Pipeline().ID, sc.SourceKind())
	}
	p := sc.Pipeline()
	for _, t := range tables {
		if !slices.Contains(p.SourceTables, t) {
			return fmt.Errorf("mysql setup writer: %w: table %s is not a source table of pipeline %q", tools.ErrOutOfScope, t, p.ID)
		}
	}
	return nil
}

// randomPassword returns a fresh hex password (24 random bytes) for the read
// user. Lower-case hex cannot end the literal or the statement.
func randomPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mysql setup writer: generate the read-user password: %w", err)
	}
	return hex.EncodeToString(b), nil
}
