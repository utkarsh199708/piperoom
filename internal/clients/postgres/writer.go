package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// Writer is the source Postgres write client, clients.SourceWriter (P3b). It
// has a pool of its own on SOURCE_DB_WRITE_URL, a separate role, and never
// shares a connection with the read-only DB. Only package actions (through
// wiring, with write mode on) may hold one.
//
// Every method validates its identifiers against ^[a-zA-Z_][a-zA-Z0-9_]*$ and
// quotes them with pgx.Identifier.Sanitize, passes values as $n parameters,
// runs in one short transaction with its own lock and statement timeouts, and
// is bounded by the write timeout (10 s by default) whatever ctx says.
//
// The pipeline scope fails closed, like tools.ScopeFor: every method needs
// the room's whole pipeline in ctx (tools.WithPipeline), or the explicit
// tools.WithUnscoped opt-out of CLI and dev callers. Anything else, a context
// that names the pipeline only by id included, is tools.ErrNoScope before
// any I/O.
type Writer struct {
	pool        *pgxpool.Pool // nil in tests
	db          txBeginner    // the pool; a fake in tests
	target      string        // receipt target: database@host:port, never credentials
	timeout     time.Duration
	lockTimeout time.Duration
}

var _ clients.SourceWriter = (*Writer)(nil)

// ErrNoWriter is returned by NewWriter when SOURCE_DB_WRITE_URL is empty, and
// by every method of a nil or zero Writer. Callers read it as "no write
// credentials": wiring hands the actions no SourceWriter and the pg actions
// refuse at Validate. NewWriter returns this error rather than (nil, nil)
// because a nil *Writer stored in a clients.SourceWriter is a non-nil
// interface: a caller checking "w != nil" would believe it has a writer.
var ErrNoWriter = errors.New("postgres: no write credentials (SOURCE_DB_WRITE_URL is not set)")

// ErrAlreadyPublished is returned (wrapped) by AddPublicationTables when a
// table is already in the publication, or the publication is FOR ALL TABLES.
// Nothing is changed then, not even for the other tables of the call. It is
// the contract's sentinel, so callers holding a clients.SourceWriter match it
// with errors.Is(err, clients.ErrAlreadyPublished).
var ErrAlreadyPublished = clients.ErrAlreadyPublished

// errReadOnlyRole refuses a write URL whose sessions start read-only: that is
// the read-only role of docs/pilot/permissions.md, never the writer's.
var errReadOnlyRole = errors.New("the SOURCE_DB_WRITE_URL role has default_transaction_read_only = on; the writer needs its own write role, never the read-only SOURCE_DB_URL role")

// errStandby refuses a write URL that points at a hot standby, where every
// write fails: the writer needs the primary.
var errStandby = errors.New("the SOURCE_DB_WRITE_URL server is a hot standby (pg_is_in_recovery() is true); the writer needs the primary")

const (
	defaultWriteTimeout = 10 * time.Second
	defaultLockTimeout  = 5 * time.Second
	// writerMaxConns caps the pool: writes are rare and short, and every
	// connection is one more session on the production source.
	writerMaxConns = 2
	writerAppName  = "piperoom-writer"
	// maxWriteTables bounds one statement; a pipeline's source tables are
	// far fewer.
	maxWriteTables = 100
	// maxSignalDataBytes is the data column of the signal table as the
	// Debezium docs define it, varchar(2048). A longer value fails the
	// INSERT, so it is refused before the transaction.
	maxSignalDataBytes = 2048
)

// slotNameRe is what Postgres accepts as a replication slot name
// (ReplicationSlotValidateName): lower-case letters, digits and _, at most
// NAMEDATALEN-1 bytes.
var slotNameRe = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// WriterOption changes a NewWriter default.
type WriterOption func(*writerOptions)

type writerOptions struct {
	timeout     time.Duration
	lockTimeout time.Duration
}

// WithWriteTimeout sets the deadline of each method (connect included) and the
// server's statement_timeout inside its transaction. d <= 0 keeps 10 s.
func WithWriteTimeout(d time.Duration) WriterOption {
	return func(o *writerOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithLockTimeout sets the server's lock_timeout inside each write
// transaction, so ALTER PUBLICATION gives up rather than queue behind a long
// lock on the table. d <= 0 keeps 5 s.
func WithLockTimeout(d time.Duration) WriterOption {
	return func(o *writerOptions) {
		if d > 0 {
			o.lockTimeout = d
		}
	}
}

func newWriterOptions(opts []WriterOption) writerOptions {
	o := writerOptions{timeout: defaultWriteTimeout, lockTimeout: defaultLockTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// NewWriter opens the write pool on dsn (SOURCE_DB_WRITE_URL) and checks that
// its sessions are not read-only. An empty dsn returns ErrNoWriter and opens
// nothing. Errors never carry the DSN or its password.
func NewWriter(ctx context.Context, dsn string, opts ...WriterOption) (*Writer, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, ErrNoWriter
	}
	o := newWriterOptions(opts)
	cfg, err := writerPoolConfig(dsn, o)
	if err != nil {
		return nil, fmt.Errorf("postgres: writer: parse SOURCE_DB_WRITE_URL: %w", pgconv.RedactDSN(err, dsn))
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: writer: connect: %w", pgconv.RedactDSN(err, dsn))
	}
	pctx, cancel := pgconv.WithDeadline(ctx, o.timeout)
	defer cancel()
	if err := checkWritable(pctx, pool); err != nil {
		pool.Close()
		return nil, pgconv.RedactDSN(err, dsn)
	}
	return &Writer{pool: pool, db: poolBeginner{pool}, target: targetOf(pool), timeout: o.timeout, lockTimeout: o.lockTimeout}, nil
}

// writerPoolConfig parses dsn for the write pool. Unlike New there is no
// AfterConnect: the session must stay writable. The timeouts are set per
// transaction (begin), not as start-up parameters, which PgBouncer refuses.
func writerPoolConfig(dsn string, o writerOptions) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.MaxConns > writerMaxConns {
		cfg.MaxConns = writerMaxConns
	}
	cfg.MinConns = 0
	if cfg.ConnConfig.ConnectTimeout <= 0 {
		cfg.ConnConfig.ConnectTimeout = o.timeout
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// Lets a DBA tell the writer's sessions apart in pg_stat_activity.
	if _, ok := cfg.ConnConfig.RuntimeParams["application_name"]; !ok {
		cfg.ConnConfig.RuntimeParams["application_name"] = writerAppName
	}
	return cfg, nil
}

// rowQuerier is the part of *pgxpool.Pool checkWritable uses.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// checkWritableSQL reads what makes every write of the pool fail.
const checkWritableSQL = `SELECT current_setting('default_transaction_read_only'), pg_is_in_recovery()`

// checkWritable connects (the first query opens the first connection) and
// refuses a role whose sessions start read-only (most likely the read-only
// role given by mistake) and a hot standby: every write would fail.
func checkWritable(ctx context.Context, q rowQuerier) error {
	var readOnly string
	var inRecovery bool
	if err := q.QueryRow(ctx, checkWritableSQL).Scan(&readOnly, &inRecovery); err != nil {
		return fmt.Errorf("postgres: writer: ping: %w", err)
	}
	if inRecovery {
		return fmt.Errorf("postgres: writer: %w", errStandby)
	}
	if readOnly == "on" {
		return fmt.Errorf("postgres: writer: %w", errReadOnlyRole)
	}
	return nil
}

// Close releases the pool.
func (w *Writer) Close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

// writeTx is the part of pgx.Tx the writer uses; tests replace it.
type writeTx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// txBeginner opens a write transaction.
type txBeginner interface {
	begin(ctx context.Context) (writeTx, error)
}

type poolBeginner struct{ pool *pgxpool.Pool }

func (p poolBeginner) begin(ctx context.Context) (writeTx, error) { return p.pool.Begin(ctx) }

// setTimeoutsSQL scopes the timeouts to the transaction (is_local = true), so
// the pooled session goes back as it came.
const setTimeoutsSQL = `SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)`

// publicationCheckSQL reads publication $1: whether it is FOR ALL TABLES and
// which of the tables ($2 schemas, $3 names, in pairs) it already lists. No
// row: the publication does not exist.
const publicationCheckSQL = `
		SELECT p.puballtables,
		       ARRAY(SELECT pt.schemaname || '.' || pt.tablename
		             FROM pg_publication_tables pt
		             JOIN unnest($2::text[], $3::text[]) AS v(schemaname, tablename)
		               ON pt.schemaname = v.schemaname::name AND pt.tablename = v.tablename::name
		             WHERE pt.pubname = p.pubname
		             ORDER BY 1)
		FROM pg_publication p
		WHERE p.pubname = $1`

// slotCheckSQL re-reads the LOGICAL slot $1: whether it is active and whether
// it lives in this database (a logical slot can only be dropped from its
// own). A physical slot (a standby's) is no row: a CDC fix never drops one.
const slotCheckSQL = `
		SELECT active, COALESCE(database = current_database(), false)
		FROM pg_replication_slots
		WHERE slot_name = $1 AND slot_type = 'logical'`

// dropSlotSQL takes the slot name as a value, never as an identifier.
const dropSlotSQL = `SELECT pg_drop_replication_slot($1)`

// AddPublicationTables adds tables ("schema.table" each) to publication with
// one ALTER PUBLICATION ... ADD TABLE, in a transaction that first re-reads
// the publication: missing is clients.ErrNotFound; a table already in it (or
// a FOR ALL TABLES publication) is ErrAlreadyPublished and nothing changes. A
// table that does not exist is clients.ErrNotFound. Repeated tables count
// once. With the room's pipeline in ctx every table must be one of its source
// tables (tools.ErrOutOfScope); with no scope see Writer.
func (w *Writer) AddPublicationTables(ctx context.Context, publication string, tables []string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	pub, err := validateIdent(publication)
	if err != nil {
		return err
	}
	list, err := writeTables(tables)
	if err != nil {
		return err
	}
	if err := checkWriteScope(ctx, "", list); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	schemas, names := make([]string, len(list)), make([]string, len(list))
	quoted := make([]string, len(list))
	for i, t := range list {
		schemas[i], names[i] = t.schema, t.table
		quoted[i] = pgx.Identifier{t.schema, t.table}.Sanitize()
	}
	alter := "ALTER PUBLICATION " + pgx.Identifier{pub}.Sanitize() + " ADD TABLE " + strings.Join(quoted, ", ")

	err = w.inTx(ctx, func(tx writeTx) error {
		var allTables bool
		var already []string
		err := w.queryRow(ctx, tx, publicationCheckSQL, []any{pub, schemas, names}, &allTables, &already)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("publication does not exist: %w", clients.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("read publication: %w", err)
		}
		if allTables {
			return fmt.Errorf("publication is FOR ALL TABLES, so it has every table: %w", ErrAlreadyPublished)
		}
		if len(already) > 0 {
			return fmt.Errorf("%w: %s", ErrAlreadyPublished, strings.Join(already, ", "))
		}
		if err := w.exec(ctx, tx, alter); err != nil {
			switch pgCode(err) {
			case "42P01", "42704": // undefined_table, undefined_object
				return fmt.Errorf("alter publication: %w: %w", clients.ErrNotFound, err)
			case "42710": // duplicate_object: added since the check, or a partitioned root already listed
				return fmt.Errorf("alter publication: %w: %w", ErrAlreadyPublished, err)
			}
			return fmt.Errorf("alter publication: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: add %s to publication %q: %w", tableNames(list), pub, err)
	}
	return nil
}

// DropReplicationSlot drops the inactive logical replication slot named slot.
// In one transaction it re-reads pg_replication_slots: no such logical slot (a
// physical slot included: a standby's is never dropped) or one of another
// database is clients.ErrNotFound, and an active slot is
// clients.ErrSlotActive; only then it runs pg_drop_replication_slot($1). A
// slot that turns active in between is refused by Postgres, also as
// ErrSlotActive. The drop itself is not transactional: once it returned, the
// slot is gone even if the commit fails, and the error says so.
//
// A slot name cannot be mapped to a pipeline here (the connector's slot.name
// is in its config, which the action checks), but the call still needs a
// scope in ctx (see Writer).
func (w *Writer) DropReplicationSlot(ctx context.Context, slot string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	if !slotNameRe.MatchString(slot) {
		return fmt.Errorf("postgres: drop replication slot: invalid slot name %q (lower-case letters, digits and _, at most 63)", cutParam(slot))
	}
	if err := checkWriteScope(ctx, "", nil); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	dropped := false
	err := w.inTx(ctx, func(tx writeTx) error {
		var active, here bool
		err := w.queryRow(ctx, tx, slotCheckSQL, []any{slot}, &active, &here)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no logical slot of this name (physical slots are never dropped): %w", clients.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("read slot: %w", err)
		}
		if !here {
			return fmt.Errorf("the slot belongs to another database: %w", clients.ErrNotFound)
		}
		if active {
			return clients.ErrSlotActive
		}
		if err := w.exec(ctx, tx, dropSlotSQL, slot); err != nil {
			switch pgCode(err) {
			case "55006": // object_in_use: a consumer attached since the check
				return fmt.Errorf("drop: %w: %w", clients.ErrSlotActive, err)
			case "42704": // undefined_object: dropped by someone else since the check
				return fmt.Errorf("drop: %w: %w", clients.ErrNotFound, err)
			}
			return fmt.Errorf("drop: %w", err)
		}
		dropped = true
		return nil
	})
	if err != nil && dropped {
		return fmt.Errorf("postgres: drop replication slot %q: the slot is dropped (a drop does not roll back), but: %w", slot, err)
	}
	if err != nil {
		return fmt.Errorf("postgres: drop replication slot %q: %w", slot, err)
	}
	return nil
}

// snapshotSignal is the data column of a Debezium execute-snapshot signal.
type snapshotSignal struct {
	DataCollections []string `json:"data-collections"`
	Type            string   `json:"type"`
}

// InsertSnapshotSignal inserts one Debezium execute-snapshot row into
// signalTable ("schema.table", the connector's signal.data.collection) asking
// for an incremental snapshot of tables, and returns the row id (a new random
// UUID). A missing signal table is clients.ErrNotFound. The data value must
// fit the signal table's varchar(2048); more tables than fit are refused
// before the transaction. With the room's pipeline in ctx connectorID must be
// one of its connectors and every table one of its source tables
// (tools.ErrOutOfScope); with no scope see Writer.
//
// Debezium reads data-collections as regular expressions. The identifier
// check leaves "." as the only metacharacter, and a dot can only match the
// schema separator of a name that has the same length, so each entry
// matches its own table and no other.
func (w *Writer) InsertSnapshotSignal(ctx context.Context, signalTable, connectorID string, tables []string) (string, error) {
	if w == nil || w.db == nil {
		return "", ErrNoWriter
	}
	sigSchema, sigTable, err := splitTable(signalTable)
	if err != nil {
		return "", fmt.Errorf("postgres: snapshot signal table: %w", err)
	}
	if strings.TrimSpace(connectorID) == "" {
		return "", errors.New("postgres: snapshot signal: no connector given")
	}
	list, err := writeTables(tables)
	if err != nil {
		return "", err
	}
	if err := checkWriteScope(ctx, connectorID, list); err != nil {
		return "", err
	}
	collections := make([]string, len(list))
	for i, t := range list {
		collections[i] = t.String()
	}
	data, err := signalData(collections)
	if err != nil {
		return "", err
	}
	id, err := newUUID()
	if err != nil {
		return "", err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	insert := "INSERT INTO " + pgx.Identifier{sigSchema, sigTable}.Sanitize() + " (id, type, data) VALUES ($1, 'execute-snapshot', $2)"
	err = w.inTx(ctx, func(tx writeTx) error {
		if err := w.exec(ctx, tx, insert, id, string(data)); err != nil {
			if pgCode(err) == "42P01" { // undefined_table
				return fmt.Errorf("insert: signal table does not exist: %w: %w", clients.ErrNotFound, err)
			}
			return fmt.Errorf("insert: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("postgres: snapshot signal for connector %q into %s: %w", cutParam(connectorID), signalTable, err)
	}
	return id, nil
}

// inTx runs fn in one transaction with the writer's lock and statement
// timeouts, commits when fn succeeds and rolls back otherwise.
func (w *Writer) inTx(ctx context.Context, fn func(tx writeTx) error) error {
	tx, err := w.db.begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if _, err := tx.Exec(ctx, setTimeoutsSQL, millis(w.lockTimeout), millis(w.timeout)); err != nil {
		rollbackWrite(ctx, tx)
		return fmt.Errorf("set timeouts: %w", err)
	}
	if err := fn(tx); err != nil {
		rollbackWrite(ctx, tx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// exec runs one statement in tx and records it as a provenance call.
func (w *Writer) exec(ctx context.Context, tx writeTx, sql string, args ...any) error {
	start := time.Now()
	_, err := tx.Exec(ctx, sql, args...)
	recordSQL(ctx, w.target, start, sql, args)
	return err
}

// queryRow reads one row in tx into dest and records the statement.
func (w *Writer) queryRow(ctx context.Context, tx writeTx, sql string, args []any, dest ...any) error {
	start := time.Now()
	err := tx.QueryRow(ctx, sql, args...).Scan(dest...)
	recordSQL(ctx, w.target, start, sql, args)
	return err
}

// rollbackWrite ends tx even when ctx is already done, with its own short
// deadline. When it fails pgx closes the connection.
func rollbackWrite(ctx context.Context, tx writeTx) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(rctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.Warn("postgres: rollback of write transaction failed; connection discarded", "err", err)
	}
}

// writeTables validates tables ("schema.table" each) and drops repeats,
// keeping the first order.
func writeTables(tables []string) ([]sourceTable, error) {
	if len(tables) == 0 {
		return nil, errors.New("postgres: no tables given")
	}
	if len(tables) > maxWriteTables {
		return nil, fmt.Errorf("postgres: %d tables in one write, at most %d", len(tables), maxWriteTables)
	}
	out := make([]sourceTable, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return nil, err
		}
		st := sourceTable{schema: schema, table: tbl}
		if !slices.Contains(out, st) {
			out = append(out, st)
		}
	}
	return out, nil
}

// checkWriteScope is the writer's scope gate. It fails closed, in the order
// of tools.ScopeFor: with the room's whole pipeline in ctx a connector or
// table outside it is tools.ErrOutOfScope; the explicit tools.WithUnscoped
// opt-out passes; anything else is tools.ErrNoScope, a context that names the
// pipeline only by id (tools.WithPipelineID) included: the writer has no
// owners config to resolve it and must not guess. connector "" and no tables
// check only that a scope is there.
func checkWriteScope(ctx context.Context, connector string, tables []sourceTable) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("postgres: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	if sc.Unscoped() {
		return nil
	}
	p := sc.Pipeline()
	if connector != "" && !slices.Contains(p.Connectors, connector) {
		return fmt.Errorf("postgres: %w: connector %q is not one of pipeline %q's connectors", tools.ErrOutOfScope, cutParam(connector), p.ID)
	}
	for _, t := range tables {
		if !slices.Contains(p.SourceTables, t.String()) {
			return fmt.Errorf("postgres: %w: table %s is not a source table of pipeline %q", tools.ErrOutOfScope, t, p.ID)
		}
	}
	return nil
}

// signalData encodes the execute-snapshot data and refuses a value longer
// than the signal table's data column, naming how many of the tables fit.
func signalData(collections []string) ([]byte, error) {
	encode := func(c []string) ([]byte, error) {
		b, err := json.Marshal(snapshotSignal{DataCollections: c, Type: "incremental"})
		if err != nil {
			return nil, fmt.Errorf("postgres: snapshot signal: encode data: %w", err)
		}
		return b, nil
	}
	data, err := encode(collections)
	if err != nil || len(data) <= maxSignalDataBytes {
		return data, err
	}
	fit := 0
	for fit < len(collections) {
		b, err := encode(collections[:fit+1])
		if err != nil {
			return nil, err
		}
		if len(b) > maxSignalDataBytes {
			break
		}
		fit++
	}
	return nil, fmt.Errorf("postgres: snapshot signal: data for %d tables is %d bytes, over the %d of the signal table's data column: at most %d of these tables per snapshot signal; send the rest in another", len(collections), len(data), maxSignalDataBytes, fit)
}

// String is the "schema.table" form.
func (t sourceTable) String() string { return t.schema + "." + t.table }

func tableNames(list []sourceTable) string {
	s := make([]string, len(list))
	for i, t := range list {
		s[i] = t.String()
	}
	return strings.Join(s, ", ")
}

// pgCode is the SQLSTATE of a server error, "" for any other error.
func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// millis renders d for set_config in whole milliseconds, at least 1: "0"
// would turn the server-side timeout off.
func millis(d time.Duration) string { return strconv.FormatInt(max(d.Milliseconds(), 1), 10) }

// newUUID returns a random (version 4) UUID in its canonical form.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("postgres: snapshot signal id: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
