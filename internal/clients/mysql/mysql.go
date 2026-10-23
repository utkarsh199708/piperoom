// Package mysql is the read-only client of a MySQL CDC source (Q8b,
// docs/design/mysql-source.md §6). It implements clients.SourceDB
// (LongTransactions and Row; ReplicationSlots and RecentDDL answer
// clients.ErrNotSupported, because MySQL has neither), the item 2 readers
// SourceSettingsReader and ScopedBlockerReader, and BinlogReader.
//
// Safety, by construction:
//   - The mysql:// URL is parsed here with a parameter allow-list (tls, tls_ca,
//     tls_server_name); the driver's own DSN parser never sees it, and every
//     driver switch (LOAD DATA LOCAL INFILE, multi statements, clear-text
//     passwords, interpolation) is set by the client, not the URL.
//   - Read pool sessions run with transaction_read_only = 1 and
//     max_execution_time = 10000; every call also has a 10 s context deadline.
//   - Values are always ? parameters on server-side prepared statements.
//     Identifiers pass ^[a-zA-Z_][a-zA-Z0-9_]*$ (at most 64 characters) and are
//     then backtick-quoted (design §5.6).
//   - The client never selects mysql.user.authentication_string, and never
//     runs SHOW GRANTS.
//   - Statement text of other sessions (PROCESSLIST_INFO) is untrusted: it is
//     redacted (N5) and cut to MaxSessionQueryBytes, or reduced to its first
//     keyword for LongTransactions.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// callTimeout bounds every call to the server (AGENTS.md §4 rule 4).
const callTimeout = 10 * time.Second

// readerProgram is the program_name connection attribute of the read pool.
const readerProgram = "piperoom-reader"

// Pool sizes and connection lifetime (design §6.1).
const (
	readMaxOpen     = 4
	connMaxLifetime = 5 * time.Minute
)

// DB is a read-only connection pool to a MySQL CDC source.
type DB struct {
	db     *sql.DB
	target string // receipt target: "mysql db@host:port", never credentials
	// version is the server's VERSION() as probed at start-up, e.g.
	// "8.4.3"; major and minor pick SHOW BINARY LOG STATUS or SHOW MASTER
	// STATUS.
	version      string
	major, minor int
}

// Option configures New.
type Option func(*options)

type options struct {
	logger *slog.Logger
}

// WithLogger sets the logger the driver's own error messages go to (default
// slog.Default()). Each message is redacted and cut first.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// New opens the read pool for rawURL (a mysql:// URL, design §6.1), pings it
// and probes the server version. MySQL 5.7 and MariaDB are refused with
// clients.ErrNotSupported. Errors never contain the URL or its password.
func New(ctx context.Context, rawURL string, opts ...Option) (*DB, error) {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	u, err := parseURL(rawURL)
	if err != nil {
		return nil, err
	}
	cfg, err := u.config(readerProgram, readParams, driverLogger{l: o.logger})
	if err != nil {
		return nil, err
	}
	conn, err := gomysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: connector: %w", err)
	}
	sdb := sql.OpenDB(conn)
	sdb.SetMaxOpenConns(readMaxOpen)
	sdb.SetMaxIdleConns(readMaxOpen)
	sdb.SetConnMaxLifetime(connMaxLifetime)
	d, err := open(ctx, sdb, u.target())
	if err != nil {
		_ = sdb.Close()
		return nil, err
	}
	return d, nil
}

// open pings sdb, probes the version and checks that the session is
// read-only. Tests call it with a fake driver.
func open(ctx context.Context, sdb *sql.DB, target string) (*DB, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := sdb.PingContext(ctx); err != nil {
		return nil, fail("connect", "", err)
	}
	d := &DB{db: sdb, target: target}
	var (
		version, comment string
		readOnly         int64
	)
	if err := d.queryRow(ctx, `SELECT VERSION(), @@version_comment, @@session.transaction_read_only`).Scan(&version, &comment, &readOnly); err != nil {
		return nil, fail("read server version", "", err)
	}
	if err := d.setVersion(version, comment); err != nil {
		return nil, err
	}
	// The session variable comes from the client's own SET; a proxy that
	// drops it would leave a writable session, so refuse to start.
	if readOnly != 1 {
		return nil, errors.New("mysql: the read session is not read-only (transaction_read_only is off): refusing to start")
	}
	return d, nil
}

var versionRe = regexp.MustCompile(`^(\d+)\.(\d+)`)

// setVersion keeps the server version and refuses the servers this wave does
// not support (design §12, D9).
func (d *DB) setVersion(version, comment string) error {
	if strings.Contains(strings.ToLower(version+" "+comment), "mariadb") {
		return fmt.Errorf("mysql: MariaDB is not a MySQL source; not supported in this wave: %w", clients.ErrNotSupported)
	}
	m := versionRe.FindStringSubmatch(version)
	if m == nil {
		return fmt.Errorf("mysql: server version %q is not MAJOR.MINOR...", cutText(version, 40))
	}
	d.major, _ = strconv.Atoi(m[1])
	d.minor, _ = strconv.Atoi(m[2])
	d.version = cutText(version, 64)
	if d.major < 8 {
		return fmt.Errorf("mysql: MySQL %d.%d is not supported (8.0 or later; 5.7 is end of life): %w", d.major, d.minor, clients.ErrNotSupported)
	}
	return nil
}

// Version returns the server version probed at start-up.
func (d *DB) Version() string { return d.version }

// Close releases the pool.
func (d *DB) Close() {
	if d != nil && d.db != nil {
		_ = d.db.Close()
	}
}

// ---- query helpers: every statement is recorded as a provenance call ----

func (d *DB) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.db.QueryContext(ctx, q, args...)
	d.record(ctx, start, q, args)
	return rows, err
}

// queryRow is query for a single row; database/sql reports its error at Scan.
func (d *DB) queryRow(ctx context.Context, q string, args ...any) *sql.Row {
	start := time.Now()
	row := d.db.QueryRowContext(ctx, q, args...)
	d.record(ctx, start, q, args)
	return row
}

func (d *DB) record(ctx context.Context, start time.Time, q string, args []any) {
	params := make([]string, 0, len(args))
	for i, a := range args {
		params = append(params, "?"+strconv.Itoa(i+1)+"="+renderArg(a))
	}
	tools.Record(ctx, tools.Call{
		Kind:           tools.CallSQL,
		Target:         d.target,
		Text:           strings.Join(strings.Fields(q), " "),
		ParamsRedacted: params,
		At:             start,
		TookMS:         time.Since(start).Milliseconds(),
	})
}

// renderArg shows a parameter in a receipt: a long string cut, never bytes.
func renderArg(a any) string {
	switch v := a.(type) {
	case nil:
		return "NULL"
	case string:
		return cutText(v, 64)
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(v))
	default:
		return cutText(fmt.Sprint(v), 64)
	}
}

// ---- identifiers (design §5.6) ----

// identRe is the AGENTS.md rule-6 identifier check; MySQL caps a name at 64
// characters. A name that passes it cannot hold a backtick.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

func validIdent(s string) bool { return identRe.MatchString(s) }

// quoteIdent backtick-quotes a name that already passed validIdent.
func quoteIdent(s string) string { return "`" + s + "`" }

// splitTable validates a "db.table" name and returns its parts.
func splitTable(table string) (db, name string, err error) {
	db, name, ok := strings.Cut(table, ".")
	if !ok || strings.Contains(name, ".") {
		return "", "", fmt.Errorf("mysql: table must be \"db.table\", got %q", cutText(table, 140))
	}
	if !validIdent(db) || !validIdent(name) {
		return "", "", fmt.Errorf("mysql: invalid identifier in %q (want %s)", cutText(table, 140), identRe)
	}
	return db, name, nil
}

// splitTables validates every name before any I/O.
func splitTables(tables []string) (dbs, names []string, err error) {
	if len(tables) > maxTables {
		return nil, nil, fmt.Errorf("mysql: %d tables, at most %d per call", len(tables), maxTables)
	}
	for _, t := range tables {
		db, n, err := splitTable(t)
		if err != nil {
			return nil, nil, err
		}
		dbs = append(dbs, db)
		names = append(names, n)
	}
	return dbs, names, nil
}

// maxTables bounds the tables of one call: the statement binds two
// parameters per table.
const maxTables = 1000

// pairsIn renders "(?, ?), (?, ?)" for n (schema, name) pairs; only the count
// shapes the text, never a name.
func pairsIn(n int) string {
	return strings.TrimSuffix(strings.Repeat("(?, ?), ", n), ", ")
}

// pairArgs interleaves dbs and names for pairsIn.
func pairArgs(dbs, names []string) []any {
	args := make([]any, 0, 2*len(dbs))
	for i := range dbs {
		args = append(args, dbs[i], names[i])
	}
	return args
}

// ---- untrusted text ----

// RedactSessionQuery masks secrets (N5) and cuts q to MaxSessionQueryBytes
// without splitting a UTF-8 sequence.
func RedactSessionQuery(q string) string {
	q, _ = redact.Text(strings.ToValidUTF8(q, "?"))
	if len(q) <= MaxSessionQueryBytes {
		return q
	}
	return cutText(q, MaxSessionQueryBytes-len("…")) + "…"
}

// cutText cuts s to at most n bytes on a rune boundary.
func cutText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// ---- errors ----

// ErrPrivilege is returned (wrapped) when the server refused a read for lack
// of a privilege; the message names the privilege the read needs.
var ErrPrivilege = errors.New("mysql: the read user lacks a privilege")

// MySQL error numbers the client maps (design §6.1).
const (
	errDBAccessDenied     = 1044 // ER_DBACCESS_DENIED_ERROR
	errTableAccessDenied  = 1142 // ER_TABLEACCESS_DENIED_ERROR
	errColumnAccessDenied = 1143 // ER_COLUMNACCESS_DENIED_ERROR
	errSpecificAccess     = 1227 // ER_SPECIFIC_ACCESS_DENIED_ERROR
	errNoSuchTable        = 1146 // ER_NO_SUCH_TABLE
	errParse              = 1064 // ER_PARSE_ERROR
	errNoBinaryLogging    = 1381 // ER_NO_BINARY_LOGGING
	errQueryTimeout       = 3024 // ER_QUERY_TIMEOUT (max_execution_time)
)

// fail wraps err for the read what. need names the privilege the read needs,
// for the access-denied numbers. A missing table is clients.ErrNotFound, a
// server or context timeout says "timed out after 10 s".
func fail(what, need string, err error) error {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case errDBAccessDenied, errTableAccessDenied, errColumnAccessDenied, errSpecificAccess:
			if need == "" {
				need = "the privilege the server names"
			}
			return fmt.Errorf("mysql: %s: needs %s: %w: %w", what, need, ErrPrivilege, err)
		case errNoSuchTable:
			return fmt.Errorf("mysql: %s: %w: %w", what, clients.ErrNotFound, err)
		case errQueryTimeout:
			return fmt.Errorf("mysql: %s: timed out after 10 s (max_execution_time): %w", what, err)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("mysql: %s: timed out after 10 s: %w", what, err)
	}
	return fmt.Errorf("mysql: %s: %w", what, err)
}

func errNumber(err error) uint16 {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

// driverLogger adapts the driver's Logger to slog. The driver logs packet and
// connection errors; each message is redacted and cut, so no packet bytes or
// statement text leave it whole.
type driverLogger struct{ l *slog.Logger }

func (d driverLogger) Print(v ...any) {
	msg, _ := redact.Text(strings.ToValidUTF8(fmt.Sprint(v...), "?"))
	d.l.Warn("mysql driver", "msg", cutText(msg, 200))
}

var (
	_ clients.SourceDB     = (*DB)(nil)
	_ SourceSettingsReader = (*DB)(nil)
	_ ScopedBlockerReader  = (*DB)(nil)
	_ BinlogReader         = (*DB)(nil)
)
