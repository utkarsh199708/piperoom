package warehouse

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// The sink side of the TOAST truncation check (X1d, pg_toast_risk): how many
// sink rows hold Debezium's unavailable-value placeholder instead of the
// column's value. DB implements the optional clients.PlaceholderCounter; the
// tool finds it with a type assertion.

// Caps of CountPlaceholder (clients.MaxPlaceholderBytes,
// clients.MaxPlaceholderWindow).
const (
	MaxPlaceholderBytes  = clients.MaxPlaceholderBytes
	MaxPlaceholderWindow = clients.MaxPlaceholderWindow
)

var _ clients.PlaceholderCounter = (*DB)(nil)

// CountPlaceholder counts the rows of table ("schema.table", one of the
// sink_tables of the pipeline in ctx) whose column, read as text, is one of
// the placeholder's forms (placeholderForms) and whose tsColumn is in
// [since, until). It returns the count only: no value of any column is
// selected, and no error quotes one.
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
	forms, err := placeholderForms(placeholder)
	if err != nil {
		return 0, err
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
	if err := d.queryRow(ctx, placeholderQuery(schema, tbl, col, tsColumn), forms, since.UTC(), until.UTC()).Scan(&n); err != nil {
		return 0, placeholderErr(table, err)
	}
	return n, nil
}

// placeholderQuery builds the count; every identifier is already validated
// and is quoted again here. column::text lets a json, varchar or bytea column
// be compared with the placeholder's forms too.
func placeholderQuery(schema, tbl, col, ts string) string {
	t := pgx.Identifier{ts}.Sanitize()
	return fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s::text = ANY($1::text[]) AND %s >= $2 AND %s < $3`,
		pgx.Identifier{schema, tbl}.Sanitize(), pgx.Identifier{col}.Sanitize(), t, t)
}

// placeholderForms is what a sink column holding the placeholder reads as
// text. Debezium sends the placeholder's bytes for a binary column (and
// decodes a "hex:" placeholder to bytes first), so besides the text itself a
// bytea column reads as its hex text (bytea_output hex, Postgres's default)
// and a JSON sink that kept Debezium's encoding holds base64. Without these a
// bytea column would always count 0. The text form is left out when the
// bytes are not valid UTF-8 or hold a NUL, which a text value cannot.
func placeholderForms(placeholder string) ([]string, error) {
	b := []byte(placeholder)
	if h, ok := strings.CutPrefix(placeholder, "hex:"); ok {
		d, err := hex.DecodeString(h)
		if err != nil || len(d) == 0 {
			return nil, fmt.Errorf("warehouse: placeholder has Debezium's hex: prefix but no hex octets after it: %w", tools.ErrBadInput)
		}
		b = d
	}
	forms := make([]string, 0, 3)
	if utf8.Valid(b) && !bytes.ContainsRune(b, 0) {
		forms = append(forms, string(b))
	}
	return append(forms, `\x`+hex.EncodeToString(b), base64.StdEncoding.EncodeToString(b)), nil
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
