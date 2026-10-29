package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// SetupWriter is the executing Postgres clients.SetupSourceWriter: the
// runner's local implementation of the setup-only writes (U1-9). It opens its
// own pool on the source's write DSN (SOURCE_DB_WRITE_URL in production; the
// dev stack's source superuser in tests), never the read-only pool, and runs
// the SQL of this package's builders (CreatePublicationSQL, CanaryTableSQL,
// InsertCanarySQL, ReadRoleGrants) with a deadline and wrapped errors. It is
// what a setup room with its own database access uses; a room whose writes go
// through the paired runner's op protocol uses remoteclients.Source instead,
// and both satisfy the same contract.
//
// Every identifier is checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ and quoted with
// pgx.Identifier.Sanitize before it becomes SQL text; the canary nonce is a $1
// parameter. The generated read-role password is the one value interpolated as
// a quoted literal: CREATE ROLE's grammar has no parameter position for
// PASSWORD (see CreateReadRole). No method returns the password, logs it, or
// reads it back from the store.
//
// The fail-closed scope rule of the contract applies to every method: the
// room's whole pipeline in ctx (tools.WithPipeline) or the explicit
// tools.WithUnscoped opt-out, tools.ErrNoScope / tools.ErrOutOfScope before
// any I/O. Each call is bounded by the write timeout (10 s by default)
// whatever ctx says.
type SetupWriter struct {
	pool    *pgxpool.Pool // nil in tests
	db      queryExecer   // the pool; a fake in tests
	timeout time.Duration
	secret  SecretStore // nil = CreateReadRole refuses rather than drop a password
}

var _ clients.SetupSourceWriter = (*SetupWriter)(nil)

// SecretStore stores one generated credential under a name. The writer
// generates the read-role password itself and never reads it back; the
// implementation is the runner's secret store (docs/design/setup-agent.md
// §3.7), whose values leave the runner only after the person's terminal
// confirmation. internal/setup/runner.Store implements it.
type SecretStore interface {
	Store(ctx context.Context, name, value string) error
}

// readPasswordSecretName is the store name CreateReadRole keeps the generated
// password under and returns, so the connector config references it as
// ${secret:source_read_password}.
const readPasswordSecretName = "source_read_password"

// setupWriteTimeout bounds each call; a whole setup write is one or two short
// statements.
const setupWriteTimeout = 10 * time.Second

// queryExecer is the part of *pgxpool.Pool the setup writer uses; tests
// substitute a fake.
type queryExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// SetupWriterOption changes a NewSetupWriter default.
type SetupWriterOption func(*setupWriterOptions)

type setupWriterOptions struct {
	timeout time.Duration
	secret  SecretStore
}

// WithSetupTimeout sets the deadline of each call (connect included). d <= 0
// keeps 10 s.
func WithSetupTimeout(d time.Duration) SetupWriterOption {
	return func(o *setupWriterOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithSetupSecretStore injects the store that keeps the generated read-role
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

// NewSetupWriter opens the write pool on dsn and checks that its sessions are
// writable. An empty dsn returns ErrNoWriter and opens nothing. Errors never
// carry the DSN or its password.
func NewSetupWriter(ctx context.Context, dsn string, opts ...SetupWriterOption) (*SetupWriter, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, ErrNoWriter
	}
	o := newSetupWriterOptions(opts)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres setup writer: parse the write DSN: %w", pgconv.RedactDSN(err, dsn))
	}
	if cfg.MaxConns > 2 {
		cfg.MaxConns = 2
	}
	cfg.MinConns = 0
	if cfg.ConnConfig.ConnectTimeout <= 0 {
		cfg.ConnConfig.ConnectTimeout = o.timeout
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// Lets a DBA tell the setup writer's sessions apart in pg_stat_activity.
	if _, ok := cfg.ConnConfig.RuntimeParams["application_name"]; !ok {
		cfg.ConnConfig.RuntimeParams["application_name"] = "piperoom-setup-writer"
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres setup writer: connect: %w", pgconv.RedactDSN(err, dsn))
	}
	pctx, cancel := pgconv.WithDeadline(ctx, o.timeout)
	defer cancel()
	if err := checkSetupWritable(pctx, pool); err != nil {
		pool.Close()
		return nil, pgconv.RedactDSN(err, dsn)
	}
	return &SetupWriter{pool: pool, db: pool, timeout: o.timeout, secret: o.secret}, nil
}

// Close releases the write pool.
func (w *SetupWriter) Close() error {
	if w == nil || w.pool == nil {
		return nil
	}
	w.pool.Close()
	return nil
}

// setupWritableSQL reads what makes every write of the pool fail.
const setupWritableSQL = `SELECT current_setting('default_transaction_read_only'), pg_is_in_recovery()`

// checkSetupWritable connects (the first query opens the first connection) and
// refuses a role whose sessions start read-only (most likely the read-only
// role given by mistake) and a hot standby, where every write fails.
func checkSetupWritable(ctx context.Context, q queryExecer) error {
	var readOnly string
	var inRecovery bool
	if err := q.QueryRow(ctx, setupWritableSQL).Scan(&readOnly, &inRecovery); err != nil {
		return fmt.Errorf("postgres setup writer: connect: %w", err)
	}
	if inRecovery {
		return errors.New("postgres setup writer: the write DSN points at a hot standby (pg_is_in_recovery()); the setup needs the primary")
	}
	if readOnly == "on" {
		return errors.New("postgres setup writer: the write DSN role has default_transaction_read_only = on; the setup needs its own write role, never the read-only SOURCE_DB_URL role")
	}
	return nil
}

// Errors of the executing setup writer. Compare with errors.Is.
var (
	// errPublicationExists: CREATE PUBLICATION found a publication of that
	// name. The contract says an existing publication is an error, never
	// altered.
	errPublicationExists = errors.New("postgres setup writer: publication already exists (an existing publication is never altered; drop it in a maintenance window first)")
	// errRoleExists: CREATE ROLE found a role of that name. The contract says
	// an existing role is an error, never re-passworded.
	errRoleExists = errors.New("postgres setup writer: role already exists (an existing role is never re-passworded)")
	// errCreateRole is returned when CREATE ROLE fails for any other reason.
	// It is deliberately generic: the statement carries the generated
	// password literal, so the server/driver error text is not wrapped.
	errCreateRole = errors.New("postgres setup writer: CREATE ROLE failed")
)

// CreatePublication creates publication name FOR TABLE tables. Only the
// pipeline's tables are listed (never FOR ALL TABLES: that needs superuser and
// decodes every table in the database). An existing publication of that name is
// errPublicationExists; it is never altered.
func (w *SetupWriter) CreatePublication(ctx context.Context, name string, tables []string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	stmt, err := CreatePublicationSQL(name, tables)
	if err != nil {
		return fmt.Errorf("postgres setup writer: create publication: %w", err)
	}
	if err := checkSetupScope(ctx, tables); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()
	if _, err := w.db.Exec(ctx, stmt); err != nil {
		if sqlStateOf(err) == "42710" { // duplicate_object
			return fmt.Errorf("postgres setup writer: create publication %s: %w", clipName(name), errPublicationExists)
		}
		return fmt.Errorf("postgres setup writer: create publication %s: %w", clipName(name), err)
	}
	return nil
}

// CreateCanaryTable creates the ladder's canary table ("schema.table",
// CanaryTableSQL): the nonce as the primary key, an identity column and the
// insert time. It exists afterwards whether or not it did before.
func (w *SetupWriter) CreateCanaryTable(ctx context.Context, table string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	stmt, err := CanaryTableSQL(table)
	if err != nil {
		return fmt.Errorf("postgres setup writer: create canary table: %w", err)
	}
	if err := checkSetupScope(ctx, []string{table}); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()
	if _, err := w.db.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("postgres setup writer: create canary table %s: %w", clipName(table), err)
	}
	return nil
}

// InsertCanary inserts one row with nonce into the canary table. The nonce is
// a $1 parameter, never SQL text.
func (w *SetupWriter) InsertCanary(ctx context.Context, table, nonce string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	stmt, err := InsertCanarySQL(table)
	if err != nil {
		return fmt.Errorf("postgres setup writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	if err := checkSetupScope(ctx, []string{table}); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()
	if _, err := w.db.Exec(ctx, stmt, nonce); err != nil {
		return fmt.Errorf("postgres setup writer: insert canary into %s: %w", clipName(table), err)
	}
	return nil
}

// CreateReadRole creates role with LOGIN and REPLICATION, the ReadRoleGrants
// grants and a generated password, stores the password in the secret store and
// returns only the store name (readPasswordSecretName). The password is in no
// return value, log or error. An existing role is errRoleExists and nothing is
// changed: it is never re-passworded.
func (w *SetupWriter) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if w == nil || w.db == nil {
		return "", ErrNoWriter
	}
	qrole, err := quoteRole(role)
	if err != nil {
		return "", fmt.Errorf("postgres setup writer: create read role: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("postgres setup writer: create read role %s: %w", clipName(role), err)
	}
	if w.secret == nil {
		return "", errors.New("postgres setup writer: CreateReadRole: no secret store to keep the generated password (pass WithSetupSecretStore)")
	}
	grants, err := ReadRoleGrants(role, tables)
	if err != nil {
		return "", fmt.Errorf("postgres setup writer: create read role %s: %w", clipName(role), err)
	}
	if err := checkSetupScope(ctx, tables); err != nil {
		return "", err
	}
	password, err := randomPassword()
	if err != nil {
		return "", err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	// CREATE ROLE's grammar has no parameter position for PASSWORD, so the
	// generated password is a quoted literal. randomPassword is lower-case hex,
	// so it cannot end the literal or the statement; on failure the error is
	// not wrapped, because a driver or server message could echo the statement.
	create := "CREATE ROLE " + qrole + " LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '" + password + "'"
	if _, err := w.db.Exec(ctx, create); err != nil {
		if sqlStateOf(err) == "42710" { // duplicate_object
			return "", fmt.Errorf("postgres setup writer: create read role %s: %w", clipName(role), errRoleExists)
		}
		return "", fmt.Errorf("postgres setup writer: create read role %s: %w (SQLSTATE %s)", clipName(role), errCreateRole, sqlStateOf(err))
	}
	for _, stmt := range grants {
		if _, err := w.db.Exec(ctx, stmt); err != nil {
			// The role exists now with a password nobody can read: drop it so
			// the retry is a clean create, and report both errors.
			dropErr := w.dropRoleBestEffort(ctx, qrole, role)
			return "", errors.Join(fmt.Errorf("postgres setup writer: create read role %s: grant: %w", clipName(role), err), dropErr)
		}
	}
	if err := w.secret.Store(ctx, readPasswordSecretName, password); err != nil {
		// The password is lost, so the role must not stay behind: a retry would
		// hit errRoleExists on a role whose password no one can supply.
		dropErr := w.dropRoleBestEffort(ctx, qrole, role)
		return "", errors.Join(fmt.Errorf("postgres setup writer: create read role %s: store the password: %w", clipName(role), err), dropErr)
	}
	return readPasswordSecretName, nil
}

// dropRoleBestEffort drops qrole after a failed create, with its own deadline
// (the call's may be spent), and returns the drop error, if any.
func (w *SetupWriter) dropRoleBestEffort(ctx context.Context, qrole, role string) error {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := w.db.Exec(dctx, "DROP ROLE "+qrole); err != nil {
		return fmt.Errorf("postgres setup writer: drop read role %s after the failed create: %w", clipName(role), err)
	}
	return nil
}

// AlterSystem sets one allow-listed server setting (validateAlterSystem) and
// reports whether it needs a restart (pg_settings.pending_restart). The name
// comes from the allow-list and the value passed the allow-list's shape and
// meaning rules, so the single-quoted literal cannot be anything but the value
// the person approved.
func (w *SetupWriter) AlterSystem(ctx context.Context, name, value string) (bool, error) {
	if err := validateAlterSystem(name, value); err != nil {
		return false, err
	}
	if w == nil || w.db == nil {
		return false, ErrNoWriter
	}
	if err := checkSetupScope(ctx, nil); err != nil {
		return false, err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()
	if _, err := w.db.Exec(ctx, "ALTER SYSTEM SET "+name+" = '"+value+"'"); err != nil {
		return false, fmt.Errorf("postgres setup writer: alter system %s: %w", name, err)
	}
	var pending bool
	if err := w.db.QueryRow(ctx, `SELECT pending_restart FROM pg_settings WHERE name = $1`, name).Scan(&pending); err != nil {
		return false, fmt.Errorf("postgres setup writer: read pending_restart of %s: %w", name, err)
	}
	return pending, nil
}

// checkSetupScope is the contract's fail-closed scope gate: the room's whole
// pipeline in ctx (every table one of its source tables), the explicit
// tools.WithUnscoped opt-out, or tools.ErrNoScope / tools.ErrOutOfScope before
// any I/O. A context that names the pipeline only by id is ErrNoScope: this
// writer has no owners config to resolve it and must not guess.
func checkSetupScope(ctx context.Context, tables []string) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("postgres setup writer: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	if sc.Unscoped() {
		return nil
	}
	p := sc.Pipeline()
	for _, t := range tables {
		if !slices.Contains(p.SourceTables, t) {
			return fmt.Errorf("postgres setup writer: %w: table %s is not a source table of pipeline %q", tools.ErrOutOfScope, t, p.ID)
		}
	}
	return nil
}

// randomPassword returns a fresh hex password (24 random bytes) for the read
// role.
func randomPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("postgres setup writer: generate the read-role password: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// sqlStateOf is the SQLSTATE of a server error, "" for any other error.
func sqlStateOf(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}
