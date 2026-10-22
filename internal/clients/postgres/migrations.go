package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// Migration history (X4b, docs/api.md "External changes"). A schema migration
// run by Flyway or Liquibase leaves a row in its history table in the source
// database; what_changed shows those rows as migration changes. Only metadata
// columns are selected (version or changeset id, description, who and when,
// success): never the script name, its contents or its checksum, and never a
// row of any other table. These methods are not on clients.SourceDB; the
// what_changed tool finds them with a type assertion, like Freshness.

// Default history table names. A tool configured with another table name is
// not found (a known gap: owners.yaml has no setting for it yet).
const (
	flywayHistoryTable    = "flyway_schema_history"
	liquibaseHistoryTable = "databasechangelog"
)

// Caps of one RecentMigrations call.
const (
	// MaxMigrations is the most rows one call returns, over every table.
	MaxMigrations = 200
	// maxHistoryTables bounds the history tables read in one call: one per
	// tool in each schema searched, and few schemas per pipeline.
	maxHistoryTables = 8
	// maxMigrationSchemas bounds the schemas one call may search.
	maxMigrationSchemas = 64
	// The SQL left() cuts bound memory per row; ExternalChange.Validate then
	// drops a version over its cap rather than storing a cut one.
	maxMigrationVersionRunes = 129
	maxMigrationTextRunes    = 300
	maxMigrationActorRunes   = 128
)

// HistoryState is what RecentMigrations could do with one history table.
type HistoryState string

// History table states.
const (
	// HistoryRead: the table was read.
	HistoryRead HistoryState = "read"
	// HistoryUnreadable: the login lacks SELECT on the table or USAGE on its
	// schema. A fact for the room ("grant SELECT on ..."), not an error.
	HistoryUnreadable HistoryState = "unreadable"
	// HistoryUnknownShape: a table with the tool's name whose columns are not
	// the tool's (another product, or a very old version). Skipped.
	HistoryUnknownShape HistoryState = "unknown_shape"
)

// HistoryTable is one migration history table found by catalog lookup.
type HistoryTable struct {
	Source string       `json:"source"` // clients.ChangeSourceFlyway or clients.ChangeSourceLiquibase
	Schema string       `json:"schema"`
	Name   string       `json:"name"`
	State  HistoryState `json:"state"`
}

// Qualified returns "schema.name", the change's Object.
func (h HistoryTable) Qualified() string { return h.Schema + "." + h.Name }

// Migration is one applied migration: metadata only. Every string is
// untrusted text from the customer's database.
type Migration struct {
	Source string `json:"source"` // clients.ChangeSourceFlyway or clients.ChangeSourceLiquibase
	Table  string `json:"table"`  // the history table, "public.flyway_schema_history"
	// Version is the Flyway version ("" for a repeatable migration) or the
	// Liquibase changeset id.
	Version     string    `json:"version"`
	Description string    `json:"description"`
	InstalledBy string    `json:"installed_by"` // Flyway installed_by, Liquibase author
	InstalledOn time.Time `json:"installed_on"`
	// Success is Flyway's success column; for Liquibase, exectype is not
	// FAILED (Liquibase records few failures, so it is nearly always true).
	Success bool `json:"success"`
}

// MigrationHistory is the result of RecentMigrations.
type MigrationHistory struct {
	// Migrations are applied migrations at or after since, newest first, at
	// most the call's limit.
	Migrations []Migration `json:"migrations"`
	// Tables are the history tables found, read or not, by schema and name.
	// None found is not an error: the database simply has no history.
	Tables []HistoryTable `json:"tables"`
}

// historySQL selects the metadata columns of one tool's history table; table
// is the sanitized identifier. installed_on and dateexecuted are "timestamp
// without time zone" in both tools' default DDL: the cast reads them in the
// session's TimeZone.
func historySQL(source, table string) string {
	if source == clients.ChangeSourceLiquibase {
		return fmt.Sprintf(`
		SELECT COALESCE(left(id, %[1]d), ''), COALESCE(left(description, %[2]d), ''),
		       COALESCE(left(author, %[3]d), ''), dateexecuted::timestamptz, COALESCE(exectype, '') <> 'FAILED'
		FROM %[4]s
		WHERE dateexecuted::timestamptz >= $1 AND isfinite(dateexecuted::timestamptz)
		ORDER BY dateexecuted DESC, orderexecuted DESC
		LIMIT $2`, maxMigrationVersionRunes, maxMigrationTextRunes, maxMigrationActorRunes, table)
	}
	return fmt.Sprintf(`
		SELECT COALESCE(left(version, %[1]d), ''), COALESCE(left(description, %[2]d), ''),
		       COALESCE(left(installed_by, %[3]d), ''), installed_on::timestamptz, COALESCE(success, false)
		FROM %[4]s
		WHERE installed_on::timestamptz >= $1 AND isfinite(installed_on::timestamptz)
		ORDER BY installed_on DESC, installed_rank DESC
		LIMIT $2`, maxMigrationVersionRunes, maxMigrationTextRunes, maxMigrationActorRunes, table)
}

// RecentMigrations finds the Flyway and Liquibase history tables in the given
// schemas of this database (a catalog lookup, so a schema without one costs
// nothing) and returns the migrations applied at or after since, newest first,
// at most limit (1..MaxMigrations). schemas are the pipeline's schemas, so a
// history table of another schema, which may belong to another team, is never
// read. A table the login cannot read is HistoryUnreadable, not an error.
func (d *DB) RecentMigrations(ctx context.Context, schemas []string, since time.Time, limit int) (MigrationHistory, error) {
	if limit < 1 || limit > MaxMigrations {
		return MigrationHistory{}, fmt.Errorf("postgres: recent migrations: limit %d must be 1..%d: %w", limit, MaxMigrations, tools.ErrBadInput)
	}
	if since.IsZero() {
		return MigrationHistory{}, fmt.Errorf("postgres: recent migrations: since is required: %w", tools.ErrBadInput)
	}
	if len(schemas) > maxMigrationSchemas {
		return MigrationHistory{}, fmt.Errorf("postgres: recent migrations: %d schemas, at most %d: %w", len(schemas), maxMigrationSchemas, tools.ErrBadInput)
	}
	for _, s := range schemas {
		// Catalog names are at most 63 bytes; the schema is only a parameter
		// here, the identifier comes back from the catalog (checked below).
		if s == "" || len(s) > 63 || !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
			return MigrationHistory{}, fmt.Errorf("postgres: recent migrations: schema %q is not a schema name: %w", s, tools.ErrBadInput)
		}
	}
	out := MigrationHistory{Migrations: []Migration{}, Tables: []HistoryTable{}}
	if len(schemas) == 0 {
		return out, nil
	}

	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	tables, err := d.historyTables(ctx, schemas)
	if err != nil {
		return MigrationHistory{}, err
	}
	for _, h := range tables {
		if h.State == HistoryRead {
			ms, state, err := d.readHistory(ctx, h, since, limit)
			if err != nil {
				return MigrationHistory{}, err
			}
			h.State = state
			out.Migrations = append(out.Migrations, ms...)
		}
		out.Tables = append(out.Tables, h)
	}
	slices.SortStableFunc(out.Migrations, func(a, b Migration) int { return b.InstalledOn.Compare(a.InstalledOn) })
	if len(out.Migrations) > limit {
		out.Migrations = out.Migrations[:limit]
	}
	return out, nil
}

// historyTables looks the history tables up in pg_class (readable by every
// role) with the login's privileges, so an unreadable table is found without
// a failed SELECT. A name outside rule 6 is never quoted into SQL: such a
// table is reported HistoryUnknownShape.
func (d *DB) historyTables(ctx context.Context, schemas []string) ([]HistoryTable, error) {
	rows, err := d.query(ctx, `
		SELECT n.nspname, c.relname,
		       has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = ANY($1::text[]) AND n.nspname = ANY($2::text[]) AND c.relkind IN ('r', 'p')
		ORDER BY n.nspname, c.relname
		LIMIT $3`, []string{flywayHistoryTable, liquibaseHistoryTable}, schemas, maxHistoryTables)
	if err != nil {
		return nil, migrationErr("find history tables", err)
	}
	defer rows.Close()
	var out []HistoryTable
	for rows.Next() {
		var h HistoryTable
		var readable bool
		if err := rows.Scan(&h.Schema, &h.Name, &readable); err != nil {
			return nil, migrationErr("scan history table", err)
		}
		switch h.Name {
		case flywayHistoryTable:
			h.Source = clients.ChangeSourceFlyway
		case liquibaseHistoryTable:
			h.Source = clients.ChangeSourceLiquibase
		default:
			continue
		}
		switch {
		case !identRe.MatchString(h.Schema):
			h.State = HistoryUnknownShape
		case !readable:
			h.State = HistoryUnreadable
		default:
			h.State = HistoryRead
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, migrationErr("find history tables", err)
	}
	return out, nil
}

// readHistory reads one table. A privilege error (a grant revoked between the
// lookup and the read, or a column-level grant) is HistoryUnreadable; a
// missing column or a column of another type is HistoryUnknownShape.
func (d *DB) readHistory(ctx context.Context, h HistoryTable, since time.Time, limit int) ([]Migration, HistoryState, error) {
	q := historySQL(h.Source, pgx.Identifier{h.Schema, h.Name}.Sanitize())
	rows, err := d.query(ctx, q, since.UTC(), limit)
	if err != nil {
		return historyFailure(h, err)
	}
	defer rows.Close()
	var out []Migration
	for rows.Next() {
		m := Migration{Source: h.Source, Table: h.Qualified()}
		if err := rows.Scan(&m.Version, &m.Description, &m.InstalledBy, &m.InstalledOn, &m.Success); err != nil {
			return historyFailure(h, err)
		}
		m.InstalledOn = m.InstalledOn.UTC()
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return historyFailure(h, err)
	}
	return out, HistoryRead, nil
}

func historyFailure(h HistoryTable, err error) ([]Migration, HistoryState, error) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501": // insufficient_privilege
			return nil, HistoryUnreadable, nil
		case "42703", "42P01", "42804", "42846", "42883": // undefined column or table, type mismatch, cannot cast, no such function
			return nil, HistoryUnknownShape, nil
		}
	}
	return nil, "", migrationErr("read "+h.Qualified(), err)
}

// migrationErr wraps a failure without the server's message: a history
// table's text is the customer's, and the message of a data exception can
// quote it. The SQLSTATE says enough.
func migrationErr(doing string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("postgres: recent migrations: %s: server error SQLSTATE %s", doing, pgErr.Code)
	}
	return fmt.Errorf("postgres: recent migrations: %s: %w", doing, err)
}
