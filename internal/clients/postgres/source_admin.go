package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// SourceAdminWriter is the admin write contract of the Q board (row Q2b). It
// is not in internal/clients/clients.go yet (the Q0 gap): this local copy is
// the exact shape proposed there, and the PR asks for it in a
// "Contract change request". The writer's admin methods below implement it.
//
// It is built only with write mode on, on the SOURCE_DB_WRITE_URL pool, never
// on the read-only SourceDB pool. No SOURCE_DB_WRITE_URL = no writer
// (NewWriter answers ErrNoWriter and every method of a nil Writer refuses).
// The fail-closed scope rule of Writer applies to every method: the room's
// whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped
// opt-out, tools.ErrNoScope / tools.ErrOutOfScope before any I/O. Identifiers
// are checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ and quoted with
// pgx.Identifier.Sanitize; values are $n parameters; no statement is built
// from untrusted text.
type SourceAdminWriter interface {
	// SetReplicaIdentity sets ALTER TABLE ... REPLICA IDENTITY: mode full |
	// default | index, where index is the unique index's name and only mode
	// index takes one.
	SetReplicaIdentity(ctx context.Context, table, mode, index string) error
	// GrantSelect grants SELECT and nothing else on tables to role.
	GrantSelect(ctx context.Context, role string, tables []string) error
	// CreateHeartbeatTable creates the Debezium heartbeat table if it does
	// not exist.
	CreateHeartbeatTable(ctx context.Context, table string) error
	// EndSession ends the backend pid, refused as ErrSessionChanged when pid
	// no longer names the session that started at startedAt.
	EndSession(ctx context.Context, pid int64, startedAt time.Time) error
}

var _ SourceAdminWriter = (*Writer)(nil)

// ErrSessionChanged is returned (wrapped) by EndSession when pid no longer
// names the session that was read: the backend is gone, or the pid now names
// a session that started at another time. Nothing was terminated. It is the
// contract's sentinel (Q board item 2); the message keeps the contract's
// prefix until clients.ErrSessionChanged lands.
var ErrSessionChanged = errors.New("clients: database session changed since it was read")

// ErrSessionProtected is returned (wrapped) by EndSession for a session
// Piperoom must never end: the write pool's own backend, another session of
// the writer's role, a walsender (the pipeline's connector or a standby
// streaming from the source), or a session whose application_name marks it as
// Piperoom's (piperoom-reader, piperoom-writer). Nothing was terminated.
var ErrSessionProtected = errors.New("postgres: refusing to end a protected session")

// maxAdminRoleBytes is NAMEDATALEN - 1: longer role names cannot exist, so a
// longer one is refused before any I/O.
const maxAdminRoleBytes = 63

// reservedGrantRole reports whether name is one GRANT treats as the current
// session or as every role. PostgreSQL reserves these as role names, so a
// connector's database.user is never one; granting to them is refused rather
// than resolved to the caller or to everyone.
func reservedGrantRole(name string) bool {
	switch strings.ToLower(name) {
	case "public", "current_user", "current_role", "session_user":
		return true
	}
	return false
}

// appNamePrefix marks Piperoom's own sessions in pg_stat_activity: the read
// pool (readerAppName) and the write pool (writerAppName) both start with it.
const appNamePrefix = "piperoom"

// piperoomApp reports whether an application_name belongs to Piperoom.
// application_name is free text any session can set, so this only ever
// protects more sessions, never fewer (fail-closed).
func piperoomApp(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), appNamePrefix)
}

// replicaIdentitySQL builds the ALTER TABLE of SetReplicaIdentity; table,
// mode and index are already validated, and the identifiers are quoted again
// here.
func replicaIdentitySQL(table sourceTable, mode, index string) string {
	stmt := "ALTER TABLE " + pgx.Identifier{table.schema, table.table}.Sanitize() + " REPLICA IDENTITY "
	switch mode {
	case "full":
		return stmt + "FULL"
	case "default":
		return stmt + "DEFAULT"
	default: // "index", the only remaining validated mode
		return stmt + "USING INDEX " + pgx.Identifier{index}.Sanitize()
	}
}

// grantSelectSQL builds the one GRANT of GrantSelect: SELECT, on the named
// tables, to the one role; no other privilege and no GRANT OPTION.
func grantSelectSQL(role string, tables []sourceTable) string {
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = pgx.Identifier{t.schema, t.table}.Sanitize()
	}
	return "GRANT SELECT ON TABLE " + strings.Join(quoted, ", ") + " TO " + pgx.Identifier{role}.Sanitize()
}

// heartbeatTableSQL creates the table Debezium's heartbeat.action.query
// inserts into: an id and a timestamp, with the primary key the query's
// ON CONFLICT (id) needs. IF NOT EXISTS makes it idempotent; the shape of an
// existing table is not re-read (the heartbeat query is the check).
const heartbeatTableSQL = `CREATE TABLE IF NOT EXISTS %s (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())`

// adminSessionSQL re-reads the backend EndSession was asked to end: its start
// time (the identity), its type, application and login role, and whether it
// is this very backend or another session of the writer's role. No query text
// and no client address are selected.
const adminSessionSQL = `
		SELECT backend_start,
		       COALESCE(backend_type, ''),
		       COALESCE(application_name, ''),
		       COALESCE(usename, ''),
		       pid = pg_backend_pid(),
		       COALESCE(usename = current_user, false)
		FROM pg_stat_activity
		WHERE pid = $1`

// clearStatSnapshotSQL discards the transaction's cached pg_stat_activity
// rows. Within one transaction PostgreSQL serves the activity view from the
// snapshot taken by its first read, so without this the terminate statement's
// backend_start recheck would re-read the very rows the first check saw and
// could never detect a pid reused in between. Clearing the snapshot makes the
// recheck a genuinely fresh read.
const clearStatSnapshotSQL = `SELECT pg_stat_clear_snapshot()`

// terminateSessionSQL ends pid only if it still started at $2, so a pid
// reused between the re-read and the call is not killed. It runs after
// clearStatSnapshotSQL, so it sees fresh activity rows, not the transaction's
// first snapshot. A missing row means the session is gone or replaced
// (ErrSessionChanged).
const terminateSessionSQL = `
		SELECT pg_terminate_backend(a.pid)
		FROM pg_stat_activity a
		WHERE a.pid = $1 AND a.backend_start = $2`

// SetReplicaIdentity runs ALTER TABLE ... REPLICA IDENTITY for table
// ("schema.table"): FULL copies every column to WAL on each change, DEFAULT
// only the key, and index makes the named unique index's columns the key.
// mode is exactly "full", "default" or "index"; only "index" takes a
// non-empty index name, and the other modes refuse one. A missing table or
// index is clients.ErrNotFound. The table must be a source table of the
// room's pipeline (tools.ErrOutOfScope); with no scope see Writer.
func (w *Writer) SetReplicaIdentity(ctx context.Context, table, mode, index string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	st, err := adminTable(table)
	if err != nil {
		return fmt.Errorf("postgres: set replica identity: %w", err)
	}
	switch mode {
	case "full", "default":
		if index != "" {
			return fmt.Errorf("postgres: set replica identity of %s: mode %q takes no index", table, mode)
		}
	case "index":
		if _, err := validateIdent(index); err != nil {
			return fmt.Errorf("postgres: set replica identity of %s: mode index needs a valid index name: %w", table, err)
		}
	default:
		return fmt.Errorf("postgres: set replica identity of %s: unknown mode %q (full, default or index)", table, cutParam(mode))
	}
	if err := checkWriteScope(ctx, "", []sourceTable{st}); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	err = w.inTx(ctx, func(tx writeTx) error {
		if err := w.exec(ctx, tx, replicaIdentitySQL(st, mode, index)); err != nil {
			switch pgCode(err) {
			case "42P01": // undefined_table
				return fmt.Errorf("alter table: no such table: %w: %w", clients.ErrNotFound, err)
			case "42704": // undefined_object: no such index
				return fmt.Errorf("alter table: no such index %q: %w: %w", cutParam(index), clients.ErrNotFound, err)
			}
			return fmt.Errorf("alter table: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: set replica identity of %s: %w", st, err)
	}
	return nil
}

// GrantSelect grants SELECT and nothing else on tables ("schema.table" each)
// to role, in one GRANT statement: all tables or none. The action checks that
// role is the connector's database.user; here role must be a valid identifier
// (at most 63 bytes, not a reserved grantee such as PUBLIC, and not in the
// reserved pg_ namespace of the predefined roles) and every table
// must be a source table of the room's pipeline (tools.ErrOutOfScope). A
// missing table or role is clients.ErrNotFound. The write role must own the
// tables or hold GRANT OPTION on them; without that the server's 42501 is
// reported with that hint. Repeated tables count once. With no scope see
// Writer.
func (w *Writer) GrantSelect(ctx context.Context, role string, tables []string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	if err := checkGrantRole(role); err != nil {
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

	err = w.inTx(ctx, func(tx writeTx) error {
		if err := w.exec(ctx, tx, grantSelectSQL(role, list)); err != nil {
			switch pgCode(err) {
			case "42P01", "3F000": // undefined_table, invalid_schema_name
				return fmt.Errorf("grant: no such table: %w: %w", clients.ErrNotFound, err)
			case "42704": // undefined_object: no such role
				return fmt.Errorf("grant: no such role %q: %w: %w", cutParam(role), clients.ErrNotFound, err)
			case "42501": // insufficient_privilege
				return fmt.Errorf("grant: the write role must own the tables or hold GRANT OPTION on them: %w", err)
			}
			return fmt.Errorf("grant: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: grant SELECT to %q on %s: %w", cutParam(role), tableNames(list), err)
	}
	return nil
}

// checkGrantRole validates the grantee before any I/O. The name is quoted by
// Sanitize, so quoting alone is safe; the reserved names are refused because
// granting to PUBLIC would reach every role of the source.
func checkGrantRole(role string) error {
	if role == "" {
		return errors.New("postgres: grant SELECT: no role given")
	}
	if len(role) > maxAdminRoleBytes {
		return fmt.Errorf("postgres: grant SELECT: role name is %d bytes, at most %d", len(role), maxAdminRoleBytes)
	}
	if _, err := validateIdent(role); err != nil {
		return fmt.Errorf("postgres: grant SELECT: %w", err)
	}
	if reservedGrantRole(role) {
		return fmt.Errorf("postgres: grant SELECT: %q is a reserved grantee (it names the current session or every role)", role)
	}
	if strings.HasPrefix(strings.ToLower(role), "pg_") {
		return fmt.Errorf("postgres: grant SELECT: %q is in the reserved pg_ role namespace (a grant to a predefined role reaches every one of its members, and a connector's database.user is never one)", role)
	}
	return nil
}

// CreateHeartbeatTable creates table ("schema.table") as the Debezium
// heartbeat table: id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT
// now(), the shape heartbeat.action.query's INSERT ... ON CONFLICT (id) DO
// UPDATE needs. It exists afterwards whether or not it did before
// (CREATE TABLE IF NOT EXISTS); an existing table is not altered or checked.
//
// The table is new, so it cannot be one of the pipeline's source tables; the
// scope gate refuses a schema that holds none of them
// (tools.ErrOutOfScope), and an unscoped caller may use any schema. A missing
// schema is clients.ErrNotFound; 42501 means the write role lacks CREATE on
// it. Adding the table to the publication is the action's follow-up step,
// never this method's.
func (w *Writer) CreateHeartbeatTable(ctx context.Context, table string) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	st, err := adminTable(table)
	if err != nil {
		return fmt.Errorf("postgres: create heartbeat table: %w", err)
	}
	if err := checkHeartbeatScope(ctx, st); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	err = w.inTx(ctx, func(tx writeTx) error {
		if err := w.exec(ctx, tx, fmt.Sprintf(heartbeatTableSQL, pgx.Identifier{st.schema, st.table}.Sanitize())); err != nil {
			switch pgCode(err) {
			case "3F000": // invalid_schema_name
				return fmt.Errorf("create table: no such schema: %w: %w", clients.ErrNotFound, err)
			case "42501": // insufficient_privilege
				return fmt.Errorf("create table: the write role needs CREATE on schema %q: %w", st.schema, err)
			}
			return fmt.Errorf("create table: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: create heartbeat table %s: %w", st, err)
	}
	return nil
}

// EndSession ends the backend pid after re-reading it: the row of pid must
// still exist and its backend_start must still equal startedAt, or
// ErrSessionChanged. A walsender (any: the pipeline's connector or a standby
// streaming from the source), this write pool's own backend, another session
// of the writer's role and a session whose application_name starts with
// "piperoom" are ErrSessionProtected. Only then does it call
// pg_terminate_backend, in the same transaction, after discarding the
// transaction's pg_stat_activity snapshot (pg_stat_clear_snapshot) and again
// bound to pid and backend_start against the fresh rows; a session gone or
// replaced in between is ErrSessionChanged.
//
// The terminate is not transactional: once it returned true the session is
// gone even if the commit fails, and the error says so. It needs a superuser
// or pg_signal_backend. A pid cannot be mapped to a pipeline here (the
// blocking-session list is the action's check), but the call still needs the
// room's pipeline scope in ctx, like DropReplicationSlot.
func (w *Writer) EndSession(ctx context.Context, pid int64, startedAt time.Time) error {
	if w == nil || w.db == nil {
		return ErrNoWriter
	}
	if pid <= 0 {
		return fmt.Errorf("postgres: end session: invalid pid %d", pid)
	}
	if startedAt.IsZero() {
		return errors.New("postgres: end session: no backend_start given (it is the session's identity)")
	}
	if err := checkWriteScope(ctx, "", nil); err != nil {
		return err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	terminated := false
	err := w.inTx(ctx, func(tx writeTx) error {
		var start *time.Time
		var backendType, app, user string
		var self, sameRole bool
		err := w.queryRow(ctx, tx, adminSessionSQL, []any{pid}, &start, &backendType, &app, &user, &self, &sameRole)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no session of this pid: %w", ErrSessionChanged)
		}
		if err != nil {
			return fmt.Errorf("read session: %w", err)
		}
		if start == nil || !start.Equal(startedAt) {
			return fmt.Errorf("pid %d now names a session that started %s, not the one read: %w", pid, formatStart(start), ErrSessionChanged)
		}
		switch {
		case self:
			return fmt.Errorf("%w: pid %d is this write pool's own session", ErrSessionProtected, pid)
		case backendType == "walsender":
			return fmt.Errorf("%w: pid %d is a walsender (a replication session and the pipeline's connector, or a standby streaming from the source)", ErrSessionProtected, pid)
		case sameRole:
			return fmt.Errorf("%w: pid %d is another session of the writer's role %q", ErrSessionProtected, pid, cutParam(user))
		case piperoomApp(app):
			return fmt.Errorf("%w: pid %d is Piperoom's own session (application_name %q)", ErrSessionProtected, pid, cutParam(app))
		}
		// The checks above read the transaction's activity snapshot;
		// discard it so the terminate's backend_start recheck reads
		// fresh rows (a pid reused since the snapshot must not match).
		if err := w.exec(ctx, tx, clearStatSnapshotSQL); err != nil {
			return fmt.Errorf("refresh activity snapshot: %w", err)
		}
		var ok bool
		if err := w.queryRow(ctx, tx, terminateSessionSQL, []any{pid, startedAt}, &ok); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("the session is gone or was replaced since it was read: %w", ErrSessionChanged)
			}
			if pgCode(err) == "42501" {
				return fmt.Errorf("terminate: needs a superuser or pg_signal_backend: %w", err)
			}
			return fmt.Errorf("terminate: %w", err)
		}
		if !ok {
			return fmt.Errorf("postgres chose not to signal it (it may have exited): %w", ErrSessionChanged)
		}
		terminated = true
		return nil
	})
	if err != nil && terminated {
		return fmt.Errorf("postgres: end session %d: the session is terminated (a termination is not rolled back), but: %w", pid, err)
	}
	if err != nil {
		return fmt.Errorf("postgres: end session %d: %w", pid, err)
	}
	return nil
}

// formatStart renders a re-read backend_start for an error, or "an unknown
// time" when the server reported none.
func formatStart(t *time.Time) string {
	if t == nil {
		return "an unknown time"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// adminTable validates one "schema.table" identifier.
func adminTable(table string) (sourceTable, error) {
	schema, tbl, err := splitTable(table)
	if err != nil {
		return sourceTable{}, err
	}
	return sourceTable{schema: schema, table: tbl}, nil
}

// checkHeartbeatScope is the scope gate of CreateHeartbeatTable. The table is
// new, so it is not one of the pipeline's source tables; it must live in a
// schema that holds one of them. Unscoped callers (CLI, dev, tests) may use
// any schema. With no scope it refuses like checkWriteScope.
func checkHeartbeatScope(ctx context.Context, table sourceTable) error {
	sc, err := writeScope(ctx)
	if err != nil {
		return err
	}
	if sc.Unscoped() {
		return nil
	}
	p := sc.Pipeline()
	for _, t := range p.SourceTables {
		if schema, _, err := splitTable(t); err == nil && schema == table.schema {
			return nil
		}
	}
	return fmt.Errorf("postgres: %w: schema %q holds none of pipeline %q's source tables", tools.ErrOutOfScope, table.schema, p.ID)
}
