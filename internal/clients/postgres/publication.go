package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// PublicationTable is one relation's membership in a logical replication
// publication together with a role's SELECT privilege on that relation.
//
// Catalog facts used to build it (PostgreSQL 17 docs, verified on
// postgresql.org while writing this client):
//   - pg_publication (§51.40): pubname name, pubowner oid, ...
//   - pg_publication_tables (§52.17): pubname, schemaname, tablename; expands
//     FOR ALL TABLES / FOR TABLES IN SCHEMA publications into one row per table.
//   - pg_publication_rel (§52.42): prpubid (-> pg_publication.oid), prrelid
//     (-> pg_class.oid).
//   - has_table_privilege ([user name|oid], table text|oid, privilege text)
//     (§9.27): privilege types include SELECT.
type PublicationTable struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	InPublication bool   `json:"in_publication"`
	// Selectable is has_table_privilege(role, "schema"."table", 'SELECT').
	Selectable bool `json:"selectable"`
}

// PublicationTables lists the relations in the publication named publication
// and, for each table in tables (each "schema.table"), whether that table is a
// member of the publication and whether role has the SELECT privilege on it.
//
// The result holds one entry per table in tables (in that order) followed by
// the remaining publication members (sorted by schema, table); no entry is
// duplicated. A missing publication is reported as clients.ErrNotFound.
//
// This method is not yet on clients.SourceDB (see the contract change request
// in the PR); the pg_publication_tables tool calls it through a local
// structural interface until the contract lands.
func (d *DB) PublicationTables(ctx context.Context, publication, role string, tables []string) ([]PublicationTable, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()
	tx, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer d.end(ctx, tx)

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, publication).Scan(&exists); err != nil {
		return nil, fmt.Errorf("postgres: check publication %q: %w", publication, err)
	}
	if !exists {
		return nil, fmt.Errorf("postgres: publication %q: %w", publication, clients.ErrNotFound)
	}

	members, err := publicationMembers(ctx, tx, publication, role)
	if err != nil {
		return nil, err
	}
	sources, err := sourcePrivileges(ctx, tx, publication, role, tables)
	if err != nil {
		return nil, err
	}

	out := make([]PublicationTable, 0, len(sources)+len(members))
	seen := make(map[string]bool, len(sources)+len(members))
	for _, s := range sources {
		out = append(out, s)
		seen[s.Schema+"."+s.Table] = true
	}
	for _, m := range members {
		if seen[m.Schema+"."+m.Table] {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// publicationMembers lists the relations in publication with role's SELECT
// privilege on each. The relation oid is resolved through pg_class so the
// privilege check can use the oid form of has_table_privilege.
func publicationMembers(ctx context.Context, tx pgx.Tx, publication, role string) ([]PublicationTable, error) {
	rows, err := tx.Query(ctx, `
		SELECT pt.schemaname, pt.tablename,
		       has_table_privilege($2::name, c.oid, 'SELECT')
		FROM pg_publication_tables pt
		JOIN pg_namespace n ON n.nspname = pt.schemaname
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
		WHERE pt.pubname = $1
		ORDER BY pt.schemaname, pt.tablename`, publication, role)
	if err != nil {
		return nil, fmt.Errorf("postgres: list publication %q tables: %w", publication, err)
	}
	defer rows.Close()

	var out []PublicationTable
	for rows.Next() {
		var t PublicationTable
		if err := rows.Scan(&t.Schema, &t.Table, &t.Selectable); err != nil {
			return nil, fmt.Errorf("postgres: scan publication table: %w", err)
		}
		t.InPublication = true
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read publication %q tables: %w", publication, err)
	}
	return out, nil
}

// sourceTable is a validated "schema.table" pair.
type sourceTable struct {
	schema string
	table  string
}

// sourcePrivileges reports, for each table in tables, whether it is a member
// of publication and whether role has SELECT on it. Membership is checked
// against pg_publication_tables so FOR ALL TABLES / FOR TABLES IN SCHEMA
// publications are handled correctly.
func sourcePrivileges(ctx context.Context, tx pgx.Tx, publication, role string, tables []string) ([]PublicationTable, error) {
	sources := make([]sourceTable, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return nil, err
		}
		sources = append(sources, sourceTable{schema: schema, table: tbl})
	}
	if len(sources) == 0 {
		return nil, nil
	}

	var sb strings.Builder
	args := []any{publication, role}
	sb.WriteString(`
		SELECT v.schemaname, v.tablename,
		       (pt.pubname IS NOT NULL) AS in_publication,
		       (c.oid IS NOT NULL AND has_table_privilege($2::name, c.oid, 'SELECT')) AS selectable
		FROM (VALUES `)
	for i := range sources {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, $%d::name, $%d::name)", i+1, 2*i+3, 2*i+4)
		args = append(args, sources[i].schema, sources[i].table)
	}
	sb.WriteString(`) AS v(pos, schemaname, tablename)
		LEFT JOIN pg_publication_tables pt
		  ON pt.pubname = $1 AND pt.schemaname = v.schemaname AND pt.tablename = v.tablename
		LEFT JOIN pg_namespace n ON n.nspname = v.schemaname
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.tablename
		ORDER BY v.pos`)

	rows, err := tx.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: check publication %q source privileges: %w", publication, err)
	}
	defer rows.Close()

	var out []PublicationTable
	for rows.Next() {
		var t PublicationTable
		if err := rows.Scan(&t.Schema, &t.Table, &t.InPublication, &t.Selectable); err != nil {
			return nil, fmt.Errorf("postgres: scan source privilege: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read publication %q source privileges: %w", publication, err)
	}
	return out, nil
}
