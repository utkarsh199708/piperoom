package warehouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// The sink side of the TOAST truncation check (X1d, pg_toast_risk): how many
// sink rows hold Debezium's unavailable-value placeholder instead of the
// column's value. Not on clients.Warehouse (contract change request in the
// X1d report); the tool finds it with a type assertion.

const (
	// MaxPlaceholderBytes caps the placeholder string: Debezium's default,
	// __debezium_unavailable_value, is 28 bytes.
	MaxPlaceholderBytes = 256
	// MaxPlaceholderWindow caps until - since: the count scans the window.
	MaxPlaceholderWindow = 7 * 24 * time.Hour
)

// CountPlaceholder counts the rows of table ("schema.table", one of the
// sink_tables of the pipeline in ctx) whose column, read as text, equals
// placeholder and whose tsColumn is in [since, until). It returns the count
// only: no value of any column is selected, and no error quotes one.
// Identifiers follow ^[a-zA-Z_][a-zA-Z0-9_]*$ and a bad one, an empty or
// over-long placeholder or a bad window is tools.ErrBadInput before any I/O;
// so is a table outside the pipeline's sink_tables (tools.ErrOutOfScope), or
// a ctx with no pipeline and no tools.WithUnscoped opt-out
// (tools.ErrNoScope). A missing table or column wraps clients.ErrNotFound.
// Deadline 10 s.
func (d *DB) CountPlaceholder(ctx context.Context, table, column, placeholder, tsColumn string, since, until time.Time) (int64, error) {
	schema, tbl, col, err := validateTableColumn(table, column)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", tools.ErrBadInput, err)
	}
	if !identRe.MatchString(tsColumn) {
		return 0, fmt.Errorf("warehouse: invalid column identifier %q: %w", tsColumn, tools.ErrBadInput)
	}
	if placeholder == "" || len(placeholder) > MaxPlaceholderBytes {
		return 0, fmt.Errorf("warehouse: placeholder must be 1 to %d bytes: %w", MaxPlaceholderBytes, tools.ErrBadInput)
	}
	if !until.After(since) || until.Sub(since) > MaxPlaceholderWindow {
		return 0, fmt.Errorf("warehouse: placeholder count window %s..%s: want until after since, at most %s: %w",
			since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339), MaxPlaceholderWindow, tools.ErrBadInput)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return 0, fmt.Errorf("warehouse: placeholder count refused: it needs the pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	if err := sc.CheckSinkTable(table); err != nil {
		return 0, fmt.Errorf("warehouse: placeholder count: %w", err)
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	var n int64
	// The bounds are untyped: they take the timestamp column's type.
	if err := d.queryRow(ctx, placeholderQuery(schema, tbl, col, tsColumn), placeholder, since.UTC(), until.UTC()).Scan(&n); err != nil {
		return 0, placeholderErr(table, err)
	}
	return n, nil
}

// placeholderQuery builds the count; every identifier is already validated
// and is quoted again here. column::text lets a json or varchar column be
// compared with the placeholder too.
func placeholderQuery(schema, tbl, col, ts string) string {
	t := pgx.Identifier{ts}.Sanitize()
	return fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s::text = $1 AND %s >= $2 AND %s < $3`,
		pgx.Identifier{schema, tbl}.Sanitize(), pgx.Identifier{col}.Sanitize(), t, t)
}

// placeholderErr keeps values out of the error: a data exception (class 22,
// whose message can quote a value) becomes a fixed phrase.
func placeholderErr(table string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
		return fmt.Errorf("warehouse: placeholder count of %s: a bound is not a value of the timestamp column's type (SQLSTATE %s): %w", table, pgErr.Code, tools.ErrBadInput)
	}
	return fmt.Errorf("warehouse: placeholder count of %s: %w", table, notFound(err))
}
