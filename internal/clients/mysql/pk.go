package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"piperoom/internal/clients"
)

// primaryKeySQL lists a table's PRIMARY KEY columns in key order, the table
// row first so that one statement tells a missing table (no row) from a
// table without a primary key (one row with a NULL column). Both names are
// bound; LIMIT 2 is enough to see a composite key.
const primaryKeySQL = `
		SELECT k.COLUMN_NAME
		FROM information_schema.TABLES t
		LEFT JOIN information_schema.KEY_COLUMN_USAGE k
		       ON k.TABLE_SCHEMA = t.TABLE_SCHEMA
		      AND k.TABLE_NAME = t.TABLE_NAME
		      AND k.CONSTRAINT_NAME = 'PRIMARY'
		WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
		ORDER BY k.ORDINAL_POSITION
		LIMIT 2`

// PrimaryKeyColumn returns the single primary-key column of table
// ("db.table", validated like Row), as the server spells it. A missing (or
// invisible) table, or a table without a primary key, is clients.ErrNotFound;
// a composite key wraps clients.ErrNotSupported, because Row reads by one
// column. 10 s deadline.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	db, name, err := splitTable(table)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	rows, err := d.query(ctx, primaryKeySQL, db, name)
	if err != nil {
		return "", fail("read primary key of "+table, "SELECT on information_schema for "+table, err)
	}
	defer rows.Close()
	var cols []sql.NullString
	for rows.Next() {
		var c sql.NullString
		if err := rows.Scan(&c); err != nil {
			return "", fmt.Errorf("mysql: read primary key of %s: %w", table, err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", fail("read primary key of "+table, "SELECT on information_schema for "+table, err)
	}
	switch {
	case len(cols) == 0:
		return "", fmt.Errorf("mysql: table %s is missing, or not visible to the read user: %w", table, clients.ErrNotFound)
	case len(cols) > 1:
		return "", fmt.Errorf("mysql: table %s has a composite primary key; a row is read by one column: %w", table, clients.ErrNotSupported)
	case !cols[0].Valid || cols[0].String == "":
		return "", fmt.Errorf("mysql: table %s has no primary key: %w", table, clients.ErrNotFound)
	}
	col := cols[0].String
	if !validIdent(col) {
		return "", fmt.Errorf("mysql: primary key column of %s is not a plain identifier (want %s)", table, identRe)
	}
	return col, nil
}
