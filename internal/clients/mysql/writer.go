package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// The write side of the MySQL client (Q8f, docs/design/mysql-source.md
// §6.8): the item 2 SourceAdminWriter on the SOURCE_DB_WRITE_URL pool.
//
// Fail-closed, like the postgres Writer: every method needs the room's whole
// pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped
// opt-out, and refuses a table outside the pipeline, or a pipeline whose
// source_kind is not mysql, before any I/O. Identifiers pass the rule-6
// check and are backtick-quoted; an account host passes hostRe and is quoted
// as 'host'; a processlist id is an int64 formatted by strconv; every value
// a statement takes as a parameter is bound. MySQL does not take ? in GRANT
// or KILL, so those are the documented literal forms (design §6.8).

// writerProgram is the program_name connection attribute of the write pool;
// EndSession protects every session whose program_name starts "piperoom".
const writerProgram = "piperoom-writer"

// writeMaxOpen is the write pool size (design §6.1).
const writeMaxOpen = 2

// writeParams are the session variables of every write-pool connection: a
// GRANT or CREATE TABLE that waits for a metadata lock gives up after 10 s on
// the server too, not only at the client's deadline.
var writeParams = map[string]string{"lock_wait_timeout": "10"}

// maxUserBytes is MySQL's user-name limit (32 characters since 5.7.8).
const maxUserBytes = 32

// hostRe is the account-host rule of design §5.6. It admits the wildcard %
// and IPv6 colons but no quote, backslash or space, so 'host' is safe as a
// literal.
var hostRe = regexp.MustCompile(`^[A-Za-z0-9_.:%-]{1,255}$`)

// programPrefix marks Piperoom's own sessions (piperoom-reader,
// piperoom-writer). program_name is set by the client, so this only ever
// protects more sessions, never fewer.
const programPrefix = "piperoom"

// ErrNoWriter is returned by NewWriter when SOURCE_DB_WRITE_URL is empty, and
// by every method of a nil Writer.
var ErrNoWriter = errors.New("mysql: no write credentials (SOURCE_DB_WRITE_URL is not set)")

// ErrAmbiguousAccount is returned (wrapped) by GrantSelect when the user name
// has more than one account (user@host): a guess could widen the wrong one.
var ErrAmbiguousAccount = errors.New("mysql: the user has more than one account")

// More MySQL error numbers the writer maps (design §6.1).
const (
	errBadDB          = 1049 // ER_BAD_DB_ERROR
	errNoSuchThread   = 1094 // ER_NO_SUCH_THREAD
	errKillDenied     = 1095 // ER_KILL_DENIED_ERROR
	errNoMatchingUser = 1133 // ER_PASSWORD_NO_MATCH: no mysql.user row for the grantee
	errCantCreateUser = 1410 // ER_CANT_CREATE_USER_WITH_GRANT: 8.0 GRANT to a missing account
)

// Writer is the admin write pool of a MySQL CDC source.
type Writer struct {
	db     *sql.DB
	target string
	// protected are the user names whose sessions EndSession never ends:
	// the write URL's user and every WithReaderUser.
	protected []string
}

var _ SourceAdminWriter = (*Writer)(nil)

// WriterOption configures NewWriter.
type WriterOption func(*writerOptions)

type writerOptions struct {
	logger      *slog.Logger
	readerUsers []string
}

// WithWriterLogger sets the logger the driver's own error messages go to.
func WithWriterLogger(l *slog.Logger) WriterOption {
	return func(o *writerOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithReaderUser names the read pool's user (SOURCE_DB_URL), so EndSession
// protects its sessions too. Wiring passes it.
func WithReaderUser(user string) WriterOption {
	return func(o *writerOptions) {
		if user != "" {
			o.readerUsers = append(o.readerUsers, user)
		}
	}
}

// NewWriter opens the write pool for rawURL (a mysql:// URL, the same rules
// as New) and probes the server. An empty rawURL is ErrNoWriter. The session
// must be writable: a read-only one (a replica, a proxy forcing
// transaction_read_only) is refused at start. Errors never contain the URL.
func NewWriter(ctx context.Context, rawURL string, opts ...WriterOption) (*Writer, error) {
	if rawURL == "" {
		return nil, ErrNoWriter
	}
	o := writerOptions{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	u, err := parseURL(rawURL)
	if err != nil {
		return nil, err
	}
	cfg, err := u.config(writerProgram, writeParams, driverLogger{l: o.logger})
	if err != nil {
		return nil, err
	}
	conn, err := gomysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: write connector: %w", err)
	}
	sdb := sql.OpenDB(conn)
	sdb.SetMaxOpenConns(writeMaxOpen)
	sdb.SetMaxIdleConns(writeMaxOpen)
	sdb.SetConnMaxLifetime(connMaxLifetime)
	w, err := openWriter(ctx, sdb, u.target(), append([]string{u.user}, o.readerUsers...))
	if err != nil {
		_ = sdb.Close()
		return nil, err
	}
	return w, nil
}

// openWriter pings sdb, probes the version and checks the session can write.
// Tests call it with a fake driver.
func openWriter(ctx context.Context, sdb *sql.DB, target string, protected []string) (*Writer, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := sdb.PingContext(ctx); err != nil {
		return nil, writeFail("connect", err)
	}
	var (
		version, comment string
		readOnly         int64
	)
	if err := sdb.QueryRowContext(ctx, `SELECT VERSION(), @@version_comment, @@session.transaction_read_only`).Scan(&version, &comment, &readOnly); err != nil {
		return nil, writeFail("read server version", err)
	}
	if err := (&DB{}).setVersion(version, comment); err != nil {
		return nil, err
	}
	if readOnly != 0 {
		return nil, errors.New("mysql: the write session is read-only (transaction_read_only is on): refusing to start the writer")
	}
	return &Writer{db: sdb, target: target, protected: protected}, nil
}

// Close releases the pool.
func (w *Writer) Close() {
	if w != nil && w.db != nil {
		_ = w.db.Close()
	}
}

// SetReplicaIdentity is a Postgres concept: a MySQL binlog row image is set
// server-wide (binlog_row_image), not per table. clients.ErrNotSupported,
// before any I/O.
func (w *Writer) SetReplicaIdentity(context.Context, string, string, string) error {
	return fmt.Errorf("mysql: set replica identity: MySQL has no per-table replica identity (binlog_row_image is a server setting): %w", clients.ErrNotSupported)
}

// GrantSelect grants SELECT and nothing else (no GRANT OPTION) on tables
// ("db.table" each) to the one account of user role. The account is resolved
// in mysql.user with role bound: no account is clients.ErrNotFound, more than
// one is ErrAmbiguousAccount with the list, because granting to a guessed
// host could widen the wrong account. MySQL takes one table per GRANT, so a
// failure after the first leaves the earlier grants in place, and the error
// names them. The write role needs SELECT WITH GRANT OPTION on the tables
// (or their databases) and SELECT on mysql.user (User, Host). Repeated tables
// count once.
func (w *Writer) GrantSelect(ctx context.Context, role string, tables []string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	if err := checkGrantUser(role); err != nil {
		return err
	}
	if len(tables) == 0 {
		return errors.New("mysql: grant SELECT: no tables given")
	}
	dbs, names, err := splitTables(dedupe(tables))
	if err != nil {
		return fmt.Errorf("mysql: grant SELECT: %w", err)
	}
	if err := checkWriteScope(ctx, dbs, names); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return writeFail("grant SELECT: connect", err)
	}
	defer conn.Close()

	host, err := w.account(ctx, conn, role)
	if err != nil {
		return fmt.Errorf("mysql: grant SELECT to %q: %w", role, err)
	}
	var done []string
	for i := range dbs {
		t := dbs[i] + "." + names[i]
		if err := w.exec(ctx, conn, grantSelectSQL(dbs[i], names[i], role, host)); err != nil {
			switch errNumber(err) {
			case errNoSuchTable:
				err = fmt.Errorf("no such table: %w: %w", clients.ErrNotFound, err)
			case errBadDB:
				err = fmt.Errorf("no such database: %w: %w", clients.ErrNotFound, err)
			case errNoMatchingUser, errCantCreateUser:
				err = fmt.Errorf("the account is gone: %w: %w", clients.ErrNotFound, err)
			case errDBAccessDenied, errTableAccessDenied, errSpecificAccess:
				err = fmt.Errorf("the write role needs SELECT WITH GRANT OPTION on %s (or on database %s): %w", t, dbs[i], err)
			}
			if len(done) > 0 {
				return fmt.Errorf("mysql: grant SELECT to '%s'@'%s' on %s: %w (already granted on %s; a GRANT is not rolled back)", role, host, t, err, strings.Join(done, ", "))
			}
			return fmt.Errorf("mysql: grant SELECT to '%s'@'%s' on %s: %w", role, host, t, err)
		}
		done = append(done, t)
	}
	return nil
}

// accountSQL lists the hosts of a user name's accounts; the name is bound.
const accountSQL = `SELECT Host FROM mysql.user WHERE User = ? ORDER BY Host`

// maxAccounts bounds the account read: two are already ambiguous.
const maxAccounts = 20

// account resolves the one account of user and validates its host.
func (w *Writer) account(ctx context.Context, conn *sql.Conn, user string) (string, error) {
	rows, err := w.query(ctx, conn, accountSQL, user)
	if err != nil {
		return "", accountFail(err)
	}
	defer rows.Close()
	var hosts []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return "", fmt.Errorf("read accounts: %w", err)
		}
		if len(hosts) < maxAccounts {
			hosts = append(hosts, h)
		}
	}
	if err := rows.Err(); err != nil {
		return "", accountFail(err)
	}
	switch len(hosts) {
	case 0:
		return "", fmt.Errorf("user %q has no account: %w", user, clients.ErrNotFound)
	case 1:
	default:
		accts := make([]string, len(hosts))
		for i, h := range hosts {
			accts[i] = user + "@" + cutText(h, 64)
		}
		return "", fmt.Errorf("%w: user %s has accounts %s; grant by hand or remove one", ErrAmbiguousAccount, user, strings.Join(accts, " and "))
	}
	if !hostRe.MatchString(hosts[0]) {
		return "", fmt.Errorf("the account host of %s is not a plain host name (want %s); grant by hand", user, hostRe)
	}
	return hosts[0], nil
}

func accountFail(err error) error {
	switch errNumber(err) {
	case errTableAccessDenied, errColumnAccessDenied:
		return fmt.Errorf("read accounts: the write role needs SELECT on mysql.user (User, Host): %w", err)
	}
	return fmt.Errorf("read accounts: %w", err)
}

// grantSelectSQL is SELECT on one table to one account, no GRANT OPTION. Every
// part already passed its rule: db and table identRe, user identRe, host
// hostRe.
func grantSelectSQL(db, table, user, host string) string {
	return "GRANT SELECT ON " + quoteIdent(db) + "." + quoteIdent(table) + " TO '" + user + "'@'" + host + "'"
}

// checkGrantUser validates the grantee name before any I/O: the rule-6
// identifier check (no quote can appear) and MySQL's 32-character limit.
func checkGrantUser(user string) error {
	if user == "" {
		return errors.New("mysql: grant SELECT: no user given")
	}
	if len(user) > maxUserBytes {
		return fmt.Errorf("mysql: grant SELECT: user name is %d bytes, at most %d", len(user), maxUserBytes)
	}
	if !validIdent(user) {
		return fmt.Errorf("mysql: grant SELECT: invalid user name %q (want %s)", cutText(user, 64), identRe)
	}
	return nil
}

// heartbeatTableSQL is the table heartbeat.action.query inserts into: an id
// key and a timestamp. IF NOT EXISTS makes it idempotent; an existing
// table's shape is not checked (the heartbeat query is the check).
const heartbeatTableSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id INT NOT NULL PRIMARY KEY, ts TIMESTAMP(6) NOT NULL) ENGINE=InnoDB"

// CreateHeartbeatTable creates table ("db.table") as the Debezium heartbeat
// table (design §6.8, the Q2c naming rules): its name contains "heartbeat",
// and in a room it sits in a database that holds one of the pipeline's
// source tables and is not itself a source table (tools.ErrOutOfScope). A
// missing database is clients.ErrNotFound; an access error means the write
// role lacks CREATE on the database.
func (w *Writer) CreateHeartbeatTable(ctx context.Context, table string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	db, name, err := splitTable(table)
	if err != nil {
		return fmt.Errorf("mysql: create heartbeat table: %w", err)
	}
	if !strings.Contains(strings.ToLower(name), "heartbeat") {
		return fmt.Errorf("mysql: create heartbeat table: the name %q must contain \"heartbeat\", so it is never mistaken for a data table", name)
	}
	if err := checkHeartbeatScope(ctx, db, name); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return writeFail("create heartbeat table: connect", err)
	}
	defer conn.Close()
	if err := w.exec(ctx, conn, fmt.Sprintf(heartbeatTableSQL, quoteIdent(db), quoteIdent(name))); err != nil {
		switch errNumber(err) {
		case errBadDB:
			err = fmt.Errorf("no such database: %w: %w", clients.ErrNotFound, err)
		case errDBAccessDenied, errTableAccessDenied, errSpecificAccess:
			err = fmt.Errorf("the write role needs CREATE on database %s: %w", db, err)
		}
		return fmt.Errorf("mysql: create heartbeat table %s.%s: %w", db, name, err)
	}
	return nil
}

// sessionSQL re-reads the thread EndSession was asked to end: whether it is
// this connection, its user and host, command, thread type, program_name and
// the start of its open InnoDB transaction (Unix seconds, the reader's
// StartedAt precision; 0 without one), and the user name this connection
// runs as. No statement text is selected.
const sessionSQL = `
		SELECT t.PROCESSLIST_ID = CONNECTION_ID(),
		       COALESCE(t.PROCESSLIST_USER, ''),
		       COALESCE(t.PROCESSLIST_HOST, ''),
		       COALESCE(t.PROCESSLIST_COMMAND, ''),
		       COALESCE(t.TYPE, ''),
		       COALESCE(a.ATTR_VALUE, ''),
		       COALESCE(CAST(UNIX_TIMESTAMP(x.trx_started) AS SIGNED), 0),
		       SUBSTRING_INDEX(CURRENT_USER(), '@', 1)
		FROM performance_schema.threads t
		LEFT JOIN information_schema.innodb_trx x ON x.trx_mysql_thread_id = t.PROCESSLIST_ID
		LEFT JOIN performance_schema.session_connect_attrs a
		       ON a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = 'program_name'
		WHERE t.PROCESSLIST_ID = ?
		LIMIT 1`

// session is one re-read of a thread.
type session struct {
	self                     bool
	user, host, command, typ string
	program                  string
	trxStarted               int64
	currentUser              string
}

// identity is what must not change between the re-read and the kill.
func (s session) identity() string {
	return s.user + "@" + s.host + "#" + strconv.FormatInt(s.trxStarted, 10)
}

func (w *Writer) readSession(ctx context.Context, conn *sql.Conn, pid int64) (session, error) {
	var s session
	err := w.queryRow(ctx, conn, sessionSQL, pid).Scan(&s.self, &s.user, &s.host, &s.command, &s.typ, &s.program, &s.trxStarted, &s.currentUser)
	if errors.Is(err, sql.ErrNoRows) {
		return session{}, fmt.Errorf("no session of this processlist id: %w", ErrSessionChanged)
	}
	if err != nil {
		switch errNumber(err) {
		case errDBAccessDenied, errTableAccessDenied, errSpecificAccess:
			return session{}, fmt.Errorf("read session: the write role needs PROCESS and SELECT on performance_schema: %w", err)
		}
		return session{}, fmt.Errorf("read session: %w", err)
	}
	return s, nil
}

// protectedReason says why Piperoom never ends s, or "".
func (w *Writer) protectedReason(s session) string {
	switch {
	case s.self:
		return "it is this write connection"
	case !strings.EqualFold(s.typ, "FOREGROUND"):
		return "it is a server background thread"
	case s.command == "Binlog Dump" || s.command == "Binlog Dump GTID":
		return "it is a binlog dump thread (a CDC connector's or a replica's stream)"
	case s.command == "Daemon" || s.user == "system user" || s.user == "event_scheduler":
		return "it is a server thread (" + cutText(s.user, 32) + ")"
	case s.currentUser != "" && s.user == s.currentUser:
		return "it is another session of the write role " + cutText(s.user, 32)
	case slices.Contains(w.protected, s.user):
		return "it is a session of Piperoom's user " + cutText(s.user, 32)
	case strings.HasPrefix(strings.ToLower(s.program), programPrefix):
		return "it is Piperoom's own session (program_name " + cutText(s.program, 32) + ")"
	}
	return ""
}

// EndSession ends processlist id pid with KILL CONNECTION after re-reading
// it on the same pinned connection (design §6.8). The thread must exist,
// hold an open InnoDB transaction, and that transaction must have started at
// startedAt (to the second, as the reader reports it), or ErrSessionChanged;
// a zero startedAt is always refused, because a session without a
// transaction has no stable identity (D3). This connection, the server's
// background and event threads, binlog dump threads, sessions of the write
// role or of WithReaderUser's users, and program_name piperoom-* are
// ErrSessionProtected. KILL on a gone thread (1094) is ErrSessionChanged; a
// denied KILL (1095) on RDS/Aurora (basedir under /rdsdbbin/) re-reads the
// identity once more and calls mysql.rds_kill with the id bound (D7), and
// elsewhere names the missing CONNECTION_ADMIN.
func (w *Writer) EndSession(ctx context.Context, pid int64, startedAt time.Time) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	if pid <= 0 {
		return fmt.Errorf("mysql: end session: invalid processlist id %d", pid)
	}
	if startedAt.IsZero() {
		return errors.New("mysql: end session: no transaction start given (a session without an open transaction has no stable identity; end it by hand)")
	}
	if err := checkWriteScope(ctx, nil, nil); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return writeFail("end session: connect", err)
	}
	defer conn.Close()
	if err := w.endSession(ctx, conn, pid, startedAt); err != nil {
		return fmt.Errorf("mysql: end session %d: %w", pid, err)
	}
	return nil
}

func (w *Writer) endSession(ctx context.Context, conn *sql.Conn, pid int64, startedAt time.Time) error {
	s, err := w.readSession(ctx, conn, pid)
	if err != nil {
		return err
	}
	if s.trxStarted == 0 {
		return fmt.Errorf("the session has no open transaction now: %w", ErrSessionChanged)
	}
	if s.trxStarted != startedAt.Unix() {
		return fmt.Errorf("its transaction started %s, not %s as read: %w", time.Unix(s.trxStarted, 0).UTC().Format(time.RFC3339), startedAt.UTC().Format(time.RFC3339), ErrSessionChanged)
	}
	if why := w.protectedReason(s); why != "" {
		return fmt.Errorf("%w: %s", ErrSessionProtected, why)
	}
	err = w.exec(ctx, conn, "KILL CONNECTION "+strconv.FormatInt(pid, 10))
	switch errNumber(err) {
	case 0:
		if err != nil {
			return fmt.Errorf("kill: %w", err)
		}
		return nil
	case errNoSuchThread:
		return fmt.Errorf("the thread is gone: %w: %w", ErrSessionChanged, err)
	case errKillDenied:
		return w.rdsKill(ctx, conn, pid, s, err)
	}
	return fmt.Errorf("kill: %w", err)
}

// rdsBasedirSQL reads the server's basedir: RDS and Aurora run from
// /rdsdbbin/ (design D7).
const rdsBasedirSQL = `SELECT @@basedir`

// rdsKillSQL is RDS's procedure for ending another user's thread; it binds
// the id.
const rdsKillSQL = `CALL mysql.rds_kill(?)`

// rdsKill is the D7 fallback after KILL was denied: only on RDS/Aurora, and
// only after the identity read before the KILL is read again unchanged.
func (w *Writer) rdsKill(ctx context.Context, conn *sql.Conn, pid int64, before session, denied error) error {
	var basedir string
	if err := w.queryRow(ctx, conn, rdsBasedirSQL).Scan(&basedir); err != nil || !strings.HasPrefix(basedir, "/rdsdbbin/") {
		return fmt.Errorf("kill: the write role lacks CONNECTION_ADMIN (or SUPER): %w", denied)
	}
	now, err := w.readSession(ctx, conn, pid)
	if err != nil {
		return err
	}
	if now.identity() != before.identity() {
		return fmt.Errorf("the session changed between the reads: %w", ErrSessionChanged)
	}
	if err := w.exec(ctx, conn, rdsKillSQL, pid); err != nil {
		if errNumber(err) == errNoSuchThread {
			return fmt.Errorf("the thread is gone: %w: %w", ErrSessionChanged, err)
		}
		return fmt.Errorf("kill (RDS mysql.rds_kill): %w", err)
	}
	return nil
}

// ---- scope ----

// writeScope resolves the writer's fail-closed scope (the postgres rule): the
// room's whole pipeline or the explicit opt-out. A pipeline that is not a
// MySQL cdc pipeline is refused: this writer is its source only when
// source_kind is mysql.
func writeScope(ctx context.Context) (tools.Scope, error) {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return tools.Scope{}, fmt.Errorf("mysql: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	if !sc.Unscoped() && sc.SourceKind() != owners.SourceKindMySQL {
		return tools.Scope{}, fmt.Errorf("mysql: %w: pipeline %q is not a MySQL cdc pipeline (source_kind %q)", tools.ErrOutOfScope, sc.Pipeline().ID, sc.SourceKind())
	}
	return sc, nil
}

// checkWriteScope refuses a table that is not a source table of the room's
// pipeline. The comparison is exact, as the owners file spells the table.
func checkWriteScope(ctx context.Context, dbs, names []string) error {
	sc, err := writeScope(ctx)
	if err != nil || sc.Unscoped() {
		return err
	}
	p := sc.Pipeline()
	for i := range dbs {
		if t := dbs[i] + "." + names[i]; !slices.Contains(p.SourceTables, t) {
			return fmt.Errorf("mysql: %w: table %s is not a source table of pipeline %q", tools.ErrOutOfScope, t, p.ID)
		}
	}
	return nil
}

// checkHeartbeatScope: the new table must sit in a database that holds one of
// the pipeline's source tables, and must not be one of them.
func checkHeartbeatScope(ctx context.Context, db, name string) error {
	sc, err := writeScope(ctx)
	if err != nil || sc.Unscoped() {
		return err
	}
	p := sc.Pipeline()
	if slices.Contains(p.SourceTables, db+"."+name) {
		return fmt.Errorf("mysql: %w: %s.%s is a source table of pipeline %q, not a new heartbeat table", tools.ErrOutOfScope, db, name, p.ID)
	}
	for _, t := range p.SourceTables {
		if d, _, ok := strings.Cut(t, "."); ok && d == db {
			return nil
		}
	}
	return fmt.Errorf("mysql: %w: database %q holds none of pipeline %q's source tables", tools.ErrOutOfScope, db, p.ID)
}

// ---- statements: each one is recorded as a provenance call ----

func (w *Writer) exec(ctx context.Context, conn *sql.Conn, q string, args ...any) error {
	start := time.Now()
	_, err := conn.ExecContext(ctx, q, args...)
	w.record(ctx, start, q, args)
	return err
}

func (w *Writer) query(ctx context.Context, conn *sql.Conn, q string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := conn.QueryContext(ctx, q, args...)
	w.record(ctx, start, q, args)
	return rows, err
}

func (w *Writer) queryRow(ctx context.Context, conn *sql.Conn, q string, args ...any) *sql.Row {
	start := time.Now()
	row := conn.QueryRowContext(ctx, q, args...)
	w.record(ctx, start, q, args)
	return row
}

func (w *Writer) record(ctx context.Context, start time.Time, q string, args []any) {
	(&DB{target: w.target}).record(ctx, start, q, args)
}

// writeFail wraps a connection-level error; a deadline says "timed out".
func writeFail(what string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("mysql: %s: timed out after 10 s: %w", what, err)
	}
	return fmt.Errorf("mysql: %s: %w", what, err)
}

// dedupe drops repeated names, keeping the first.
func dedupe(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}
