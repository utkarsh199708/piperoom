package databricks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
)

// ErrPermissionDenied is wrapped when the credential lacks a privilege
// (SQLSTATE 42501, INSUFFICIENT_PERMISSIONS) or the workspace refuses it
// (HTTP 401/403: bad or expired token, no CAN USE on the SQL warehouse).
// See README.md for the grants.
var ErrPermissionDenied = errors.New("databricks: permission denied")

// ErrEndpointNotFound is wrapped on HTTP 404: the DSN's http path names no
// SQL warehouse. It is not clients.ErrNotFound, which means a missing table.
var ErrEndpointNotFound = errors.New("databricks: http path not found (no such SQL warehouse)")

// ErrNotTimestamp is wrapped by Freshness when the column is not a
// TIMESTAMP or TIMESTAMP_NTZ column. The error never carries a value.
var ErrNotTimestamp = errors.New("not a timestamp column")

// maxErrRunes caps the server text kept in an error message.
const maxErrRunes = 300

// SQLSTATEs Databricks returns (Spark error classes).
const (
	stateTableNotFound   = "42P01" // TABLE_OR_VIEW_NOT_FOUND
	stateSchemaNotFound  = "42704" // SCHEMA_NOT_FOUND
	stateCatalogNotFound = "42P08" // CATALOG_NOT_FOUND
	stateColumnNotFound  = "42703" // UNRESOLVED_COLUMN
	stateInsufficient    = "42501" // INSUFFICIENT_PERMISSIONS
)

// sqlStater is the part of dbsqlerr.DBExecutionError this client reads.
// Declared locally so a fake error in tests needs no driver internals.
type sqlStater interface {
	SqlState() string
}

// queryError carries a cleaned message and unwraps to both a sentinel and
// the original driver error, so errors.Is and errors.As keep working.
type queryError struct {
	msg  string
	errs []error
}

func (e *queryError) Error() string   { return e.msg }
func (e *queryError) Unwrap() []error { return e.errs }

// wrapQueryErr maps a failed call to the client's error vocabulary. Server
// text is untrusted: it can quote a table, a column or a fragment of a bad
// value, so it is redacted (secret patterns and the DSN's own credentials),
// folded onto one line and cut before it becomes part of the message.
func (d *DB) wrapQueryErr(ctx context.Context, st *callStatus, op, table string, err error) error {
	prefix := fmt.Sprintf("databricks: %s %s", op, table)
	clean := d.cleanText(err.Error())
	// A data exception (SQLSTATE class 22, e.g. CAST_INVALID_INPUT) quotes
	// the offending value: keep only the SQLSTATE, as the Postgres
	// warehouse client does.
	var s sqlStater
	if errors.As(err, &s) && strings.HasPrefix(s.SqlState(), "22") {
		clean = "data exception (SQLSTATE " + d.cleanText(s.SqlState()) + ")"
	}
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return &queryError{msg: prefix + ": " + ctxErr.Error() + ": " + clean, errs: []error{ctxErr, err}}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &queryError{msg: prefix + ": " + clean, errs: []error{err}}
	}
	sentinel := classify(st, err)
	if sentinel == nil {
		return &queryError{msg: prefix + ": " + clean, errs: []error{err}}
	}
	return &queryError{msg: prefix + ": " + sentinel.Error() + ": " + clean, errs: []error{sentinel, err}}
}

// classify picks a sentinel from what the transport saw, then from the
// statement's SQLSTATE. Message text is never matched.
func classify(st *callStatus, err error) error {
	if errors.Is(err, ErrResponseTooLarge) {
		return ErrResponseTooLarge
	}
	if st != nil {
		if st.oversize.Load() {
			return ErrResponseTooLarge
		}
		switch st.httpCode.Load() {
		case http.StatusUnauthorized, http.StatusForbidden:
			return ErrPermissionDenied
		case http.StatusNotFound:
			return ErrEndpointNotFound
		}
	}
	var s sqlStater
	if errors.As(err, &s) {
		switch s.SqlState() {
		case stateTableNotFound, stateSchemaNotFound, stateCatalogNotFound, stateColumnNotFound:
			return clients.ErrNotFound
		case stateInsufficient:
			return ErrPermissionDenied
		}
	}
	return nil
}

// cleanText masks secrets, folds control characters and whitespace, and
// cuts to maxErrRunes. Used for every driver- or server-written string.
func (d *DB) cleanText(s string) string {
	for _, sec := range d.secrets {
		s = strings.ReplaceAll(s, sec, redacted)
	}
	s, _ = redact.Text(s)
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > maxErrRunes {
		s = string([]rune(s)[:maxErrRunes-1]) + "…"
	}
	return s
}
