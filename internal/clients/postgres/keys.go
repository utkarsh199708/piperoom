package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/pgconv"
)

// maxWindowKeys bounds the keys one WindowKeys call may return. It is the last
// safety bound behind the tools' own, smaller cap.
const maxWindowKeys = 5000

// WindowKeys returns the distinct values of keyColumn among the rows of table
// ("schema.table") whose tsColumn is in [since, until), as text and ordered as
// text, at most limit of them, plus the total number of distinct keys in the
// window (which can exceed limit). It selects keys only, never another column
// value. A reconciliation counts rows this way: one key is one row, however
// often that row was updated, so it compares like with like against the sink;
// the topic's event count is not a row count.
func (d *DB) WindowKeys(ctx context.Context, table, keyColumn, tsColumn string, since, until time.Time, limit int) ([]string, int64, error) {
	// The context deadline is the statement timeout: pgx cancels the query on
	// the server when it expires.
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	if limit <= 0 || limit > maxWindowKeys {
		return nil, 0, fmt.Errorf("postgres: window keys: limit %d outside 1..%d", limit, maxWindowKeys)
	}
	if !until.After(since) {
		return nil, 0, fmt.Errorf("postgres: window keys: bad window %s..%s", since.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	schema, tbl, err := splitTable(table)
	if err != nil {
		return nil, 0, err
	}
	key, err := validateIdent(keyColumn)
	if err != nil {
		return nil, 0, err
	}
	ts, err := validateIdent(tsColumn)
	if err != nil {
		return nil, 0, err
	}
	// $1 and $2 are untyped on purpose, as in CountByBucket: they take the
	// type of the timestamp column.
	rows, err := d.query(ctx, windowKeysQuery(schema, tbl, key, ts), since.UTC(), until.UTC(), limit)
	if err != nil {
		return nil, 0, fmt.Errorf("postgres: window keys of %s: %w", table, err)
	}
	defer rows.Close()

	var keys []string
	var total int64
	for rows.Next() {
		var k string
		if err := rows.Scan(&k, &total); err != nil {
			return nil, 0, fmt.Errorf("postgres: window keys of %s: scan: %w", table, err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("postgres: window keys of %s: %w", table, err)
	}
	return keys, total, nil
}

// windowKeysQuery builds the distinct-key read. schema, table and columns are
// already validated and are quoted again here. count(*) OVER () is evaluated
// before LIMIT, so one query returns the bounded key list and the exact total
// from one snapshot.
func windowKeysQuery(schema, tbl, key, ts string) string {
	return fmt.Sprintf(`SELECT k, count(*) OVER () AS total
		FROM (SELECT DISTINCT %[1]s::text AS k FROM %[2]s WHERE %[3]s >= $1 AND %[3]s < $2) AS window_keys
		ORDER BY k LIMIT $3`,
		pgx.Identifier{key}.Sanitize(), pgx.Identifier{schema, tbl}.Sanitize(), pgx.Identifier{ts}.Sanitize())
}
