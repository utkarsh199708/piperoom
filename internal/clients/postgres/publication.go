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
//   - pg_class.relowner names the table's owner role and
//     pg_get_userbyid(oid) turns it into a name; pg_has_role([user name|oid],
//     role name|oid, 'MEMBER') (§9.27) is true when the user is that role or
//     a member of it (and for a superuser).
type PublicationTable struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	InPublication bool   `json:"in_publication"`
	// Selectable is has_table_privilege(role, "schema"."table", 'SELECT').
	Selectable bool `json:"selectable"`
	// Owner is the table's owner role (pg_class.relowner); "" when the table
	// does not exist, or when the reader did not say (then the two flags below
	// mean nothing). (Q1, QA G3)
	Owner string `json:"owner,omitempty"`
	// RoleIsOwner is true when the connector role is Owner itself.
	RoleIsOwner bool `json:"role_is_owner,omitempty"`
	// RoleMemberOfOwner is pg_has_role(role, owner, 'MEMBER'): the role is the
	// owner, a member of it, or a superuser. Adding a table to a publication
	// needs ownership of the table, which membership gives. Omitted when
	// false, so check Owner too.
	RoleMemberOfOwner bool `json:"role_member_of_owner,omitempty"`
}

// PublicationInfo is what a connector role needs, beyond SELECT, to keep a
// publication working, read from the catalogs. (Q1, QA G3)
type PublicationInfo struct {
	Publication string `json:"publication"`
	// Owner is the publication's owner role (pg_publication.pubowner).
	Owner string `json:"owner"`
	// Role is the connector role the fields below describe.
	Role string `json:"role"`
	// RoleExists is false when pg_roles has no such role; the fields below are
	// then all false.
	RoleExists bool `json:"role_exists"`
	// RoleReplication is pg_roles.rolreplication: the REPLICATION attribute,
	// which a role needs to open a logical replication connection.
	RoleReplication bool `json:"role_replication"`
	// RoleSuperuser is pg_roles.rolsuper.
	RoleSuperuser bool `json:"role_superuser"`
	// RoleIsOwner and RoleMemberOfOwner are the same facts as on
	// PublicationTable, for the publication: ALTER PUBLICATION needs its owner.
	RoleIsOwner       bool `json:"role_is_owner"`
	RoleMemberOfOwner bool `json:"role_member_of_owner"`
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

	var exists bool
	if err := d.queryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, publication).Scan(&exists); err != nil {
		return nil, fmt.Errorf("postgres: check publication %q: %w", publication, err)
	}
	if !exists {
		return nil, fmt.Errorf("postgres: publication %q: %w", publication, clients.ErrNotFound)
	}

	members, err := d.publicationMembers(ctx, publication, role)
	if err != nil {
		return nil, err
	}
	sources, err := d.sourcePrivileges(ctx, publication, role, tables)
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
func (d *DB) publicationMembers(ctx context.Context, publication, role string) ([]PublicationTable, error) {
	rows, err := d.query(ctx, `
		SELECT pt.schemaname, pt.tablename,
		       has_table_privilege($2::name, c.oid, 'SELECT'),
		       pg_get_userbyid(c.relowner)::text,
		       pg_has_role($2::name, c.relowner, 'MEMBER')
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
		if err := rows.Scan(&t.Schema, &t.Table, &t.Selectable, &t.Owner, &t.RoleMemberOfOwner); err != nil {
			return nil, fmt.Errorf("postgres: scan publication table: %w", err)
		}
		t.InPublication = true
		t.RoleIsOwner = t.Owner != "" && t.Owner == role
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
func (d *DB) sourcePrivileges(ctx context.Context, publication, role string, tables []string) ([]PublicationTable, error) {
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
		       (c.oid IS NOT NULL AND has_table_privilege($2::name, c.oid, 'SELECT')) AS selectable,
		       COALESCE(pg_get_userbyid(c.relowner)::text, '') AS owner,
		       (c.oid IS NOT NULL AND pg_has_role($2::name, c.relowner, 'MEMBER')) AS role_member
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

	rows, err := d.query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: check publication %q source privileges: %w", publication, err)
	}
	defer rows.Close()

	var out []PublicationTable
	for rows.Next() {
		var t PublicationTable
		if err := rows.Scan(&t.Schema, &t.Table, &t.InPublication, &t.Selectable, &t.Owner, &t.RoleMemberOfOwner); err != nil {
			return nil, fmt.Errorf("postgres: scan source privilege: %w", err)
		}
		t.RoleIsOwner = t.Owner != "" && t.Owner == role
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read publication %q source privileges: %w", publication, err)
	}
	return out, nil
}

// PublicationInfo reads the publication's owner and role's attributes: the
// REPLICATION and SUPERUSER flags, and whether role owns the publication or is
// a member of its owner. It reads catalogs only, with parameters. A missing
// publication is clients.ErrNotFound; a missing role is not an error: it is
// reported as RoleExists=false, because a connector pointed at a role that was
// dropped is a finding, not a failure of this read.
//
// This method is not on clients.SourceDB; the pg_publication_tables tool finds
// it with a type assertion, like PublicationTables. (Q1, QA G3)
func (d *DB) PublicationInfo(ctx context.Context, publication, role string) (PublicationInfo, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	info := PublicationInfo{Publication: publication, Role: role}
	var roleName *string
	err := d.queryRow(ctx, `
		SELECT pg_get_userbyid(p.pubowner)::text,
		       r.rolname::text,
		       COALESCE(r.rolreplication, false),
		       COALESCE(r.rolsuper, false),
		       (r.oid IS NOT NULL AND pg_has_role(r.oid, p.pubowner, 'MEMBER'))
		FROM pg_publication p
		LEFT JOIN pg_roles r ON r.rolname = $2::name
		WHERE p.pubname = $1`, publication, role).
		Scan(&info.Owner, &roleName, &info.RoleReplication, &info.RoleSuperuser, &info.RoleMemberOfOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return PublicationInfo{}, fmt.Errorf("postgres: publication %q: %w", publication, clients.ErrNotFound)
	}
	if err != nil {
		return PublicationInfo{}, fmt.Errorf("postgres: read publication %q owner and role %q attributes: %w", publication, role, err)
	}
	info.RoleExists = roleName != nil
	info.RoleIsOwner = info.RoleExists && info.Owner == role
	return info, nil
}
