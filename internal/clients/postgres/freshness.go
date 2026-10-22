package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// Freshness returns max(tsColumn) of table ("schema.table"). A zero time means
// the table has no rows (or only '-infinity'); 'infinity' returns the year-9999
// stand-in. It is the source-side read warehouse_freshness compares the sink
// against (KAN-403): source activity tells an idle pipeline from a broken sink.
//
// tsColumn must be a timestamp or timestamptz column. Its type is checked from
// the result's row description before the value is decoded, because a failed
// decode (pgx's scan error) quotes the value: max() of an email or a card
// number column would reach the agent and the room log (#564). No error from
// this method carries a value of the column.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, err := splitTable(table)
	if err != nil {
		return time.Time{}, err
	}
	col, err := validateIdent(tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	q := fmt.Sprintf("SELECT max(%s) FROM %s", pgx.Identifier{col}.Sanitize(), pgx.Identifier{schema, tbl}.Sanitize())
	rows, err := d.query(ctx, q)
	if err != nil {
		return time.Time{}, sourceFreshnessErr(table, tsColumn, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, sourceFreshnessErr(table, tsColumn, err)
		}
		return time.Time{}, nil // max() always returns a row; none is no data
	}
	if fds := rows.FieldDescriptions(); len(fds) != 1 || !isTimestampOID(fds[0].DataTypeOID) {
		return time.Time{}, fmt.Errorf("postgres: freshness of %s: column %q is not a timestamp or timestamptz column: %w", table, tsColumn, errNotTimestamp)
	}
	var ts pgtype.Timestamptz
	if err := rows.Scan(&ts); err != nil {
		return time.Time{}, sourceFreshnessErr(table, tsColumn, err)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, sourceFreshnessErr(table, tsColumn, err)
	}
	if !ts.Valid {
		return time.Time{}, nil
	}
	switch ts.InfinityModifier {
	case pgtype.Infinity:
		// time.Time cannot hold it. The year-9999 stand-in takes the tool's
		// "sentinel or bad value" path instead of a failed scan.
		return infinityTime, nil
	case pgtype.NegativeInfinity:
		return time.Time{}, nil
	}
	return ts.Time, nil
}

// errNotTimestamp marks a column Freshness cannot read. It is unexported: the
// capability is local to this client until clients.SourceReader gains
// Freshness (contract change request in the KAN-403 PR).
var errNotTimestamp = errors.New("not a timestamp column")

func isTimestampOID(oid uint32) bool {
	return oid == pgtype.TimestamptzOID || oid == pgtype.TimestampOID
}

// infinityTime stands in for a Postgres 'infinity' timestamp, as in the
// warehouse client.
var infinityTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// sourceFreshnessErr turns a Freshness failure into an error that never
// carries a value of the column. A missing table or column keeps its
// clients.ErrNotFound marker; a data exception (class 22, whose message can
// quote the value) and a decode error become fixed phrases; every other
// server error is reported by SQLSTATE only, never by its message.
func sourceFreshnessErr(table, col string, err error) error {
	var pgErr *pgconn.PgError
	var scanErr pgx.ScanArgError
	switch {
	case errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22"):
		return fmt.Errorf("postgres: freshness of %s: max(%s) could not be read as a timestamp (SQLSTATE %s)", table, col, pgErr.Code)
	case errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "42703"):
		return fmt.Errorf("postgres: freshness of %s: %w", table, clients.ErrNotFound)
	case errors.As(err, &pgErr):
		return fmt.Errorf("postgres: freshness of %s: server error SQLSTATE %s", table, pgErr.Code)
	case errors.As(err, &scanErr):
		return fmt.Errorf("postgres: freshness of %s: max(%s) could not be read as a timestamp", table, col)
	}
	return fmt.Errorf("postgres: freshness of %s: %w", table, err)
}
