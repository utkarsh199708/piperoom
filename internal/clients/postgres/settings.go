package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/redact"
)

// The Q2a source-settings reads (Q board row Q2a). The interface and its DTOs
// are the local adapter for the Q board's clients.SourceSettingsReader: the
// contract file internal/clients/clients.go does not carry them yet, so they
// live here until Q0 adopts them (see the contract change request in the PR).
// The shapes and JSON tags are exactly the board's, so adopting them later is
// a move, not a rewrite.

// SourceSettingsReader is the read-only source-settings surface of a CDC
// source. Postgres implements it here; MySQL (Q8b) implements the same
// methods. Every method is read-only and bounded by a 10 s deadline. Table
// identifiers are validated (^[a-zA-Z_][a-zA-Z0-9_]*$) before any I/O.
type SourceSettingsReader interface {
	// Settings returns the fixed list of CDC-relevant settings that the
	// server reports, in the order of settingNames. A setting the server does
	// not have (max_slot_wal_keep_size before PG 13, rds.logical_replication
	// outside RDS/Aurora) is simply absent from the result.
	Settings(ctx context.Context) ([]DBSetting, error)
	// TableReplication returns one entry per table, in that order, with its
	// primary key presence and replica identity (default, full, index or
	// nothing). A table that does not exist is clients.ErrNotFound.
	TableReplication(ctx context.Context, tables []string) ([]TableReplication, error)
	// RoleInfo reports whether role exists, can log in and may replicate,
	// and its SELECT privilege on each table. A missing role is not an error:
	// Exists is false and every table is false in Selectable.
	RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error)
	// Blockers lists the sessions that can hold back logical replication:
	// an open transaction older than olderThan, or the session holding the
	// oldest xmin. Query is redacted (N5) and at most MaxSessionQueryBytes.
	Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error)
}

// DBSetting is one server setting with its changed-but-not-yet-applied flag.
type DBSetting struct {
	Name           string `json:"name"`
	Value          string `json:"value"`
	Unit           string `json:"unit,omitempty"`
	PendingRestart bool   `json:"pending_restart,omitempty"`
}

// TableReplication is a table's primary key presence and replica identity.
type TableReplication struct {
	Table           string `json:"table"`
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity,omitempty"` // default | full | index | nothing; "" when the source has none
}

// RoleInfo is a source role's login/replication rights and SELECT privilege
// per table.
type RoleInfo struct {
	Role        string          `json:"role"`
	Exists      bool            `json:"exists"`
	CanLogin    bool            `json:"can_login"`
	Replication bool            `json:"replication"` // Postgres REPLICATION; MySQL REPLICATION SLAVE and CLIENT
	Selectable  map[string]bool `json:"selectable"`  // table -> SELECT
}

// DBSession is one session that can hold back logical replication. Query is
// untrusted text: redacted and cut before it leaves the client. InScope is
// false for a session a scoped read anonymised (see BlockersOnTables): its
// User, App and Query are blank because they belong to another pipeline, user
// or database. (InScope is additive to the Q-board shape; see the PR's
// contract change request.)
type DBSession struct {
	PID        int64     `json:"pid"`
	StartedAt  time.Time `json:"started_at"` // backend start; with PID, the identity EndSession re-checks
	XactStart  time.Time `json:"xact_start"`
	User       string    `json:"user"`
	App        string    `json:"application"`
	State      string    `json:"state"`
	Query      string    `json:"query"` // untrusted, redacted, at most 200 bytes
	BlocksSlot bool      `json:"blocks_slot"`
	InScope    bool      `json:"in_scope"`
}

var _ SourceSettingsReader = (*DB)(nil)

// MaxSessionQueryBytes caps DBSession.Query. The query often holds literal row
// values, so only a short redacted fragment leaves the client.
const MaxSessionQueryBytes = 200

// settingNames is the fixed list Settings reads, in report order.
// rds.logical_replication exists only on RDS/Aurora; a server without it just
// returns the other five.
var settingNames = []string{
	"wal_level",
	"max_replication_slots",
	"max_wal_senders",
	"max_slot_wal_keep_size",
	"wal_sender_timeout",
	"rds.logical_replication",
}

// SettingNames returns a copy of the settings a Postgres source reports. The
// tools use it to say which of them the server does not have.
func SettingNames() []string { return append([]string(nil), settingNames...) }

// replicaIdentityNames maps pg_class.relreplident onto the board's words.
var replicaIdentityNames = map[string]string{"d": "default", "f": "full", "i": "index", "n": "nothing"}

// Settings reads the fixed CDC settings from pg_settings. pending_restart
// tells whether the running value differs from the configured one; it is the
// fact a wal_level change must be approved with.
func (d *DB) Settings(ctx context.Context) ([]DBSetting, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, `
		SELECT name, setting, COALESCE(unit, ''), pending_restart
		FROM pg_settings
		WHERE name = ANY($1::text[])
		ORDER BY array_position($1::text[], name)`, settingNames)
	if err != nil {
		return nil, fmt.Errorf("postgres: read settings: %w", err)
	}
	defer rows.Close()

	var out []DBSetting
	for rows.Next() {
		var s DBSetting
		if err := rows.Scan(&s.Name, &s.Value, &s.Unit, &s.PendingRestart); err != nil {
			return nil, fmt.Errorf("postgres: scan setting: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read settings: %w", err)
	}
	return out, nil
}

// tableReplicationSQL reads, for each requested table ($1 schemas, $2 names,
// matched pairwise, in order), whether the relation exists, its replica
// identity and whether it has a primary key. Only pg_catalog is read.
// relreplident stays 'i' after the REPLICA IDENTITY USING INDEX index is
// dropped or invalidated, while the table then behaves as NOTHING — the exact
// update/delete failure this read exists to show — so 'i' is reported as 'n'
// unless a valid identity index actually exists.
const tableReplicationSQL = `
		SELECT v.pos,
		       c.oid IS NOT NULL,
		       COALESCE(CASE WHEN c.relreplident = 'i' AND NOT EXISTS (
		           SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident AND i.indisvalid)
		           THEN 'n' ELSE c.relreplident::text END, ''),
		       COALESCE(EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary), false)
		FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS v(nsp, relname, pos)
		LEFT JOIN pg_namespace n ON n.nspname = v.nsp
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname AND c.relkind IN ('r', 'p')
		ORDER BY v.pos`

// TableReplication reads the replica identity and primary key of each table.
// A table that does not exist is clients.ErrNotFound, named by table only.
func (d *DB) TableReplication(ctx context.Context, tables []string) ([]TableReplication, error) {
	schemas, names, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, tableReplicationSQL, schemas, names)
	if err != nil {
		return nil, fmt.Errorf("postgres: read table replication: %w", err)
	}
	defer rows.Close()

	out := make([]TableReplication, len(tables))
	for rows.Next() {
		var (
			pos   int64
			found bool
			ident string
			hasPK bool
		)
		if err := rows.Scan(&pos, &found, &ident, &hasPK); err != nil {
			return nil, fmt.Errorf("postgres: scan table replication: %w", err)
		}
		if pos < 1 || pos > int64(len(tables)) {
			return nil, fmt.Errorf("postgres: table replication: row for table %d of %d", pos, len(tables))
		}
		if !found {
			return nil, fmt.Errorf("postgres: table replication of %s: %w", tables[pos-1], clients.ErrNotFound)
		}
		out[pos-1] = TableReplication{
			Table:           tables[pos-1],
			HasPrimaryKey:   hasPK,
			ReplicaIdentity: replicaIdentityNames[ident],
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read table replication: %w", err)
	}
	return out, nil
}

// roleGrantsSQL reports role $1's SELECT privilege on each table ($2 schemas,
// $3 names). A missing table reads as no privilege, and only real or
// partitioned tables count, like tableReplicationSQL (a stray index or type
// under the name would make has_table_privilege error out). Caveat:
// has_table_privilege counts privileges available via role membership even
// when the member role is NOINHERIT, so it can say yes where the connector's
// own queries would still be denied until SET ROLE.
const roleGrantsSQL = `
		SELECT v.pos, COALESCE(c.oid IS NOT NULL AND has_table_privilege($1::name, c.oid, 'SELECT'), false)
		FROM unnest($2::text[], $3::text[]) WITH ORDINALITY AS v(nsp, relname, pos)
		LEFT JOIN pg_namespace n ON n.nspname = v.nsp
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname AND c.relkind IN ('r', 'p')
		ORDER BY v.pos`

// RoleInfo reports whether role exists, can log in and may replicate, and its
// SELECT privilege on each table. The role name is a query parameter, never
// SQL text: a quoted role name is legal and needs no identifier rules.
func (d *DB) RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error) {
	if role == "" {
		return RoleInfo{}, errors.New("postgres: role info: empty role name")
	}
	schemas, names, err := splitTables(tables)
	if err != nil {
		return RoleInfo{}, err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	info := RoleInfo{Role: role, Selectable: make(map[string]bool, len(tables))}
	err = d.queryRow(ctx, `SELECT rolcanlogin, rolreplication FROM pg_roles WHERE rolname = $1`, role).
		Scan(&info.CanLogin, &info.Replication)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// A role that does not exist: not an error, and no table can be read.
		for _, t := range tables {
			info.Selectable[t] = false
		}
		return info, nil
	case err != nil:
		return RoleInfo{}, fmt.Errorf("postgres: read role %q: %w", role, err)
	}
	info.Exists = true
	if len(tables) == 0 {
		return info, nil
	}

	rows, err := d.query(ctx, roleGrantsSQL, role, schemas, names)
	if err != nil {
		return RoleInfo{}, fmt.Errorf("postgres: read role %q grants: %w", role, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			pos        int64
			selectable bool
		)
		if err := rows.Scan(&pos, &selectable); err != nil {
			return RoleInfo{}, fmt.Errorf("postgres: scan role grant: %w", err)
		}
		if pos < 1 || pos > int64(len(tables)) {
			return RoleInfo{}, fmt.Errorf("postgres: role grants: row for table %d of %d", pos, len(tables))
		}
		info.Selectable[tables[pos-1]] = selectable
	}
	if err := rows.Err(); err != nil {
		return RoleInfo{}, fmt.Errorf("postgres: read role %q grants: %w", role, err)
	}
	return info, nil
}

// blockersSQL lists the sessions that hold back a slot: an open transaction
// older than $1 seconds, or a session holding the server's oldest xid horizon
// (a logical slot cannot advance past it, and CREATE_REPLICATION_SLOT waits
// for it). The horizon is the larger of age(backend_xid) and
// age(backend_xmin): a READ COMMITTED writer that went idle in transaction
// releases its snapshot between statements, so backend_xmin is NULL while its
// assigned backend_xid still pins the horizon — exactly the classic blocker.
// xid has no min() aggregate and no ordering operators, so the oldest is the
// largest age(), which is also wraparound-safe; GREATEST ignores a NULL arm.
// Walsenders are left out (their own xmin is not a blocker) and so is this
// backend. Prepared transactions (pg_prepared_xacts) hold the horizon too but
// are not sessions and are not listed here.
//
// $4 scopes the read to a pipeline: when true, a session counts as in scope
// only if it holds or waits for a relation lock on one of the tables ($2
// schemas, $3 names, matched pairwise) in the current database. The identity
// of an out-of-scope session belongs to another pipeline, user or database,
// so its usename, application_name and query are blanked in SQL and never
// leave the server (the pg_long_transactions rule); pid, times, state and
// blocks_slot remain, which is what "a foreign session holds the horizon"
// needs. With $4 false every session is in scope and shown in full.
const blockersSQL = `
		WITH rel AS (
			SELECT c.oid
			FROM unnest($2::text[], $3::text[]) AS v(nsp, relname)
			JOIN pg_namespace n ON n.nspname = v.nsp
			JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname
		), activity AS (
			SELECT a.pid, a.backend_start, a.xact_start, a.usename, a.application_name, a.state, a.query,
			       GREATEST(age(a.backend_xid), age(a.backend_xmin)) AS xid_age,
			       (a.xact_start IS NOT NULL AND a.xact_start < now() - make_interval(secs => $1)) AS old,
			       (NOT $4::boolean OR COALESCE(a.datname = current_database() AND EXISTS (
			           SELECT 1 FROM pg_locks l
			           WHERE l.pid = a.pid
			             AND l.locktype = 'relation'
			             AND l.database = a.datid
			             AND l.relation IN (SELECT oid FROM rel)), false)) AS in_scope
			FROM pg_stat_activity a
			WHERE a.pid <> pg_backend_pid()
			  AND a.backend_type <> 'walsender'
		), horizon AS (
			SELECT max(xid_age) AS max_age FROM activity
		)
		SELECT a.pid,
		       a.backend_start,
		       COALESCE(a.xact_start, a.backend_start),
		       CASE WHEN a.in_scope THEN COALESCE(a.usename, '') ELSE '' END,
		       CASE WHEN a.in_scope THEN COALESCE(a.application_name, '') ELSE '' END,
		       COALESCE(a.state, ''),
		       CASE WHEN a.in_scope THEN COALESCE(a.query, '') ELSE '' END,
		       (a.xid_age IS NOT NULL AND a.xid_age >= h.max_age) AS blocks_slot,
		       a.in_scope
		FROM activity a, horizon h
		WHERE a.old OR (a.xid_age IS NOT NULL AND a.xid_age >= h.max_age)
		ORDER BY a.xact_start NULLS LAST, a.pid`

// Blockers lists the sessions that can hold back logical replication, with
// full identity on every row (InScope is always true). The query text of each
// is redacted (N5) and cut to MaxSessionQueryBytes; only the documented
// DBSession fields leave this method.
func (d *DB) Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error) {
	return d.blockers(ctx, olderThan, nil, nil, false)
}

// BlockersOnTables is Blockers limited to a pipeline's view: every blocker is
// still listed, but a session that does not hold or wait for a lock on one of
// tables (each "schema.table") in the current database has its user,
// application and query blanked — it belongs to another pipeline, user or
// database, and only its pid, times, state and blocks_slot are shown.
func (d *DB) BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]DBSession, error) {
	schemas, names, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	return d.blockers(ctx, olderThan, schemas, names, true)
}

func (d *DB) blockers(ctx context.Context, olderThan time.Duration, schemas, names []string, scoped bool) ([]DBSession, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("postgres: blockers: negative age %s", olderThan)
	}
	if schemas == nil {
		schemas, names = []string{}, []string{}
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, blockersSQL, olderThan.Seconds(), schemas, names, scoped)
	if err != nil {
		return nil, fmt.Errorf("postgres: list blocking sessions: %w", err)
	}
	defer rows.Close()

	var out []DBSession
	for rows.Next() {
		var s DBSession
		if err := rows.Scan(&s.PID, &s.StartedAt, &s.XactStart, &s.User, &s.App, &s.State, &s.Query, &s.BlocksSlot, &s.InScope); err != nil {
			return nil, fmt.Errorf("postgres: scan blocking session: %w", err)
		}
		s.Query = RedactSessionQuery(s.Query)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read blocking sessions: %w", err)
	}
	return out, nil
}

// RedactSessionQuery masks secrets (N5) and cuts q to MaxSessionQueryBytes
// without splitting a UTF-8 sequence. It is exported so the tools that show a
// session query apply the same rule to a client that does not.
func RedactSessionQuery(q string) string {
	q, _ = redact.Text(q)
	if len(q) <= MaxSessionQueryBytes {
		return q
	}
	cut := MaxSessionQueryBytes - len("…")
	for cut > 0 && !utf8.RuneStart(q[cut]) {
		cut--
	}
	return q[:cut] + "…"
}

// splitTables validates each "schema.table" and returns the parts for the
// pairwise unnest() reads. Nothing is read before every name passed.
func splitTables(tables []string) (schemas, names []string, err error) {
	schemas = make([]string, 0, len(tables))
	names = make([]string, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return nil, nil, err
		}
		schemas = append(schemas, schema)
		names = append(names, tbl)
	}
	return schemas, names, nil
}
