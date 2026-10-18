package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// PublicationAddState is what pg_add_publication_table (internal/actions/
// pgactions) reads before it proposes, previews or runs
// ALTER PUBLICATION ... ADD TABLE: the publication and the one table to add.
// It is read on the read-only pool, never the writer's.
type PublicationAddState struct {
	// AllTables is pg_publication.puballtables: the publication already has
	// every table.
	AllTables bool `json:"all_tables"`
	// Members are the publication's tables (pg_publication_tables,
	// "schema.table") that are among the visible tables of the call, sorted.
	Members []string `json:"members"`
	// OtherMembers counts the publication's other tables; their names are
	// never read.
	OtherMembers int `json:"other_members"`

	// TableExists is false when no relation of that schema and name exists;
	// the fields below are then zero.
	TableExists bool `json:"table_exists"`
	// RelKind is pg_class.relkind: "r" table, "p" partitioned table, "v"
	// view, and so on.
	RelKind string `json:"relkind"`
	// ReplicaIdentity is pg_class.relreplident: "d" default (the primary
	// key), "n" nothing, "f" full, "i" an index.
	ReplicaIdentity string `json:"replica_identity"`
	// HasPrimaryKey: the table has a primary key index.
	HasPrimaryKey bool `json:"has_primary_key"`
	// InPublication: the table's rows are already published (the publishedMember
	// rule of PublicationTables, partitions included).
	InPublication bool `json:"in_publication"`
	// EstimatedRows is pg_class.reltuples, -1 when never analyzed.
	EstimatedRows int64 `json:"estimated_rows"`
}

// publicationAddSQL reads publication $1 and table $2.$3 in one statement.
// $4/$5 are the visible tables (schemas, names, in pairs): only those member
// names are selected, the rest are counted. No row: no such publication.
var publicationAddSQL = `
		WITH ` + publishedCTEs + `
		SELECT p.puballtables,
		       ARRAY(SELECT pt.schemaname || '.' || pt.tablename
		             FROM pg_publication_tables pt
		             JOIN unnest($4::text[], $5::text[]) AS v(schemaname, tablename)
		               ON pt.schemaname = v.schemaname::name AND pt.tablename = v.tablename::name
		             WHERE pt.pubname = p.pubname
		             ORDER BY 1),
		       (SELECT count(*)::int
		        FROM pg_publication_tables pt
		        WHERE pt.pubname = p.pubname
		          AND NOT EXISTS (
		              SELECT 1 FROM unnest($4::text[], $5::text[]) AS v(schemaname, tablename)
		              WHERE pt.schemaname = v.schemaname::name AND pt.tablename = v.tablename::name)),
		       c.oid IS NOT NULL,
		       COALESCE(c.relkind::text, ''),
		       COALESCE(c.relreplident::text, ''),
		       COALESCE(EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary), false),
		       COALESCE(c.oid IS NOT NULL AND ` + publishedMember("c.oid") + `, false),
		       COALESCE(c.reltuples::bigint, -1)
		FROM pg_publication p
		LEFT JOIN pg_namespace n ON n.nspname = $2::name
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = $3::name
		WHERE p.pubname = $1`

// PublicationAddState reads publication and table ("schema.table") for
// pg_add_publication_table. visible ("schema.table" each, the room's source
// tables) are the member names it may return; the publication's other tables
// are only counted. A missing publication is clients.ErrNotFound; a missing
// table is TableExists false. Identifiers are checked before any I/O and
// travel as parameters. Not on clients.SourceDB: the action finds it with a
// type assertion, like PublicationTables.
func (d *DB) PublicationAddState(ctx context.Context, publication, table string, visible []string) (PublicationAddState, error) {
	pub, err := validateIdent(publication)
	if err != nil {
		return PublicationAddState{}, err
	}
	schema, name, err := splitTable(table)
	if err != nil {
		return PublicationAddState{}, err
	}
	vs, vn := []string{}, []string{} // never NULL arrays: unnest(NULL) would hide nothing but count wrongly
	for _, v := range visible {
		s, n, err := splitTable(v)
		if err != nil {
			return PublicationAddState{}, err
		}
		vs, vn = append(vs, s), append(vn, n)
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	var st PublicationAddState
	err = d.queryRow(ctx, publicationAddSQL, pub, schema, name, vs, vn).Scan(
		&st.AllTables, &st.Members, &st.OtherMembers,
		&st.TableExists, &st.RelKind, &st.ReplicaIdentity, &st.HasPrimaryKey, &st.InPublication, &st.EstimatedRows)
	if errors.Is(err, pgx.ErrNoRows) {
		return PublicationAddState{}, fmt.Errorf("postgres: publication %q: %w", pub, clients.ErrNotFound)
	}
	if err != nil {
		return PublicationAddState{}, fmt.Errorf("postgres: read publication %q and table %s: %w", pub, table, err)
	}
	if !st.TableExists {
		st = PublicationAddState{AllTables: st.AllTables, Members: st.Members, OtherMembers: st.OtherMembers, EstimatedRows: -1}
	}
	return st, nil
}
