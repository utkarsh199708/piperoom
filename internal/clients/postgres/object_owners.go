package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/pgconv"
)

// ObjectOwners is who owns a pipeline's source tables and the publication its
// connector streams, set against the connector's role. Debezium needs to own
// the publication (or hold the owner's privileges) to ALTER it, and ALTER
// PUBLICATION ... ADD/SET TABLE also needs ownership of each table: a change
// of owner fails the connector with "must be owner of table ...".
type ObjectOwners struct {
	// Role is the connector role the read was asked about.
	Role string `json:"role"`
	// RoleExists is false when pg_roles has no such role; every role flag
	// below is then false.
	RoleExists  bool `json:"role_exists"`
	Superuser   bool `json:"superuser"`
	Replication bool `json:"replication"`
	// Tables has one entry per asked table, in the asked order.
	Tables      []TableOwner     `json:"tables"`
	Publication PublicationOwner `json:"publication"`
}

// TableOwner is one source table's owner (pg_class.relowner).
type TableOwner struct {
	Table  string `json:"table"`
	Exists bool   `json:"exists"`
	Owner  string `json:"owner,omitempty"`
	// RoleIsOwner is true when the connector role is the owner itself.
	RoleIsOwner bool `json:"role_is_owner"`
	// RoleHasOwnerPrivs is pg_has_role(role, owner, 'USAGE'): the role is
	// the owner or an inheriting member of it, which is what Postgres's own
	// ownership checks accept. A NOINHERIT member is not counted: its
	// statements fail until SET ROLE.
	RoleHasOwnerPrivs bool `json:"role_has_owner_privs"`
	// InPublication is the table's membership in the asked publication.
	InPublication bool `json:"in_publication"`
}

// PublicationOwner is the publication's owner (pg_publication.pubowner).
type PublicationOwner struct {
	Name              string `json:"name"`
	Exists            bool   `json:"exists"`
	Owner             string `json:"owner,omitempty"`
	AllTables         bool   `json:"all_tables"`
	RoleIsOwner       bool   `json:"role_is_owner"`
	RoleHasOwnerPrivs bool   `json:"role_has_owner_privs"`
}

// objectRoleSQL reads the connector role's attributes.
const objectRoleSQL = `SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname = $1`

// publicationOwnerSQL reads publication $1's owner set against role $2. The
// role is joined, not passed to pg_has_role by name: pg_has_role raises "role
// does not exist" for a missing name, while a NULL oid makes it NULL.
const publicationOwnerSQL = `
		SELECT p.puballtables, COALESCE(o.rolname, ''),
		       COALESCE(r.oid = p.pubowner, false),
		       COALESCE(pg_has_role(r.oid, p.pubowner, 'USAGE'), false)
		FROM pg_publication p
		LEFT JOIN pg_roles o ON o.oid = p.pubowner
		LEFT JOIN pg_roles r ON r.rolname = $2
		WHERE p.pubname = $1`

// tableOwnersSQL reads each table's owner ($3 schemas, $4 names), set against
// role $2, and its membership of publication $1 (the publishedCTEs rule that
// pg_publication_tables decides per leaf partition). Names are parameters,
// never SQL text. Only real or partitioned tables count, like roleGrantsSQL.
func tableOwnersSQL() string {
	return `
		WITH ` + publishedCTEs + `
		SELECT v.pos, c.oid IS NOT NULL, COALESCE(o.rolname, ''),
		       COALESCE(r.oid = c.relowner, false),
		       COALESCE(pg_has_role(r.oid, c.relowner, 'USAGE'), false),
		       (c.oid IS NOT NULL AND ` + publishedMember("c.oid") + `)
		FROM unnest($3::text[], $4::text[]) WITH ORDINALITY AS v(nsp, relname, pos)
		LEFT JOIN pg_namespace n ON n.nspname = v.nsp
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname AND c.relkind IN ('r', 'p')
		LEFT JOIN pg_roles o ON o.oid = c.relowner
		LEFT JOIN pg_roles r ON r.rolname = $2
		ORDER BY v.pos`
}

// ObjectOwners reports who owns each table in tables ("schema.table") and the
// publication, and whether role is or acts as that owner. A missing role,
// table or publication is reported in the result, not as an error. The role
// and publication names are query parameters: a quoted name is legal and
// needs no identifier rule; table names are validated before any I/O.
func (d *DB) ObjectOwners(ctx context.Context, role, publication string, tables []string) (ObjectOwners, error) {
	if role == "" {
		return ObjectOwners{}, errors.New("postgres: object owners: empty role name")
	}
	if publication == "" {
		return ObjectOwners{}, errors.New("postgres: object owners: empty publication name")
	}
	schemas, names, err := splitTables(tables)
	if err != nil {
		return ObjectOwners{}, err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	out := ObjectOwners{Role: role, Publication: PublicationOwner{Name: publication}}
	err = d.queryRow(ctx, objectRoleSQL, role).Scan(&out.Superuser, &out.Replication)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return ObjectOwners{}, fmt.Errorf("postgres: read role %q: %w", role, err)
	default:
		out.RoleExists = true
	}

	p := &out.Publication
	err = d.queryRow(ctx, publicationOwnerSQL, publication, role).Scan(&p.AllTables, &p.Owner, &p.RoleIsOwner, &p.RoleHasOwnerPrivs)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return ObjectOwners{}, fmt.Errorf("postgres: read publication %q owner: %w", publication, err)
	default:
		p.Exists = true
	}

	out.Tables = make([]TableOwner, len(tables))
	for i, t := range tables {
		out.Tables[i] = TableOwner{Table: t}
	}
	if len(tables) == 0 {
		return out, nil
	}
	rows, err := d.query(ctx, tableOwnersSQL(), publication, role, schemas, names)
	if err != nil {
		return ObjectOwners{}, fmt.Errorf("postgres: read table owners: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			pos int64
			t   TableOwner
		)
		if err := rows.Scan(&pos, &t.Exists, &t.Owner, &t.RoleIsOwner, &t.RoleHasOwnerPrivs, &t.InPublication); err != nil {
			return ObjectOwners{}, fmt.Errorf("postgres: scan table owner: %w", err)
		}
		if pos < 1 || pos > int64(len(tables)) {
			return ObjectOwners{}, fmt.Errorf("postgres: table owners: row for table %d of %d", pos, len(tables))
		}
		t.Table = tables[pos-1]
		out.Tables[pos-1] = t
	}
	if err := rows.Err(); err != nil {
		return ObjectOwners{}, fmt.Errorf("postgres: read table owners: %w", err)
	}
	return out, nil
}
