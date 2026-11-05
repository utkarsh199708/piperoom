package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
)

// ErrPermissionDenied is wrapped when the credential lacks a permission
// (HTTP 403 / reason accessDenied). See README.md for the roles.
var ErrPermissionDenied = errors.New("bigquery: permission denied")

// ErrNotTimestamp is wrapped by Freshness when the column is not a TIMESTAMP
// or DATETIME column. The error never carries the column's value.
var ErrNotTimestamp = errors.New("not a timestamp column")

// maxErrRunes caps the server text kept in an error message.
const maxErrRunes = 300

// queryError carries a cleaned message and unwraps to both a sentinel
// (clients.ErrNotFound, ErrPermissionDenied, a context error) and the
// original library error, so errors.Is and errors.As keep working.
type queryError struct {
	msg  string
	errs []error
}

func (e *queryError) Error() string   { return e.msg }
func (e *queryError) Unwrap() []error { return e.errs }

// wrapQueryErr maps a failed query to the client's error vocabulary. Server
// text (googleapi and job errors) is untrusted: it can quote a table, a
// column or a fragment of the bad value, so it is redacted, folded onto one
// line and cut before it becomes part of the message.
func wrapQueryErr(ctx context.Context, op, table string, err error) error {
	prefix := fmt.Sprintf("bigquery: %s %s", op, table)
	// A cancelled or expired ctx is reported as such, whatever the library
	// wrapped around it.
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return &queryError{msg: prefix + ": " + ctxErr.Error(), errs: []error{ctxErr, err}}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &queryError{msg: prefix + ": " + cleanText(err.Error()), errs: []error{err}}
	}
	sentinel := classify(err)
	if sentinel == nil {
		return &queryError{msg: prefix + ": " + cleanText(err.Error()), errs: []error{err}}
	}
	return &queryError{msg: prefix + ": " + sentinel.Error() + ": " + cleanText(err.Error()), errs: []error{sentinel, err}}
}

// classify maps an error to clients.ErrNotFound or ErrPermissionDenied, or
// nil. The BigQuery reason wins over the HTTP code where there is one: a 403
// with reason quotaExceeded or rateLimitExceeded is not access denied.
func classify(err error) error {
	rs := reasons(err)
	if len(rs) == 0 {
		var ge *googleapi.Error
		if errors.As(err, &ge) {
			switch ge.Code {
			case http.StatusNotFound:
				return clients.ErrNotFound
			case http.StatusForbidden:
				return ErrPermissionDenied
			}
		}
		return nil
	}
	for _, r := range rs {
		switch r {
		case "notFound":
			return clients.ErrNotFound
		case "accessDenied":
			return ErrPermissionDenied
		}
	}
	return nil
}

// reasons lists the reasons of a googleapi error or a BigQuery job error.
func reasons(err error) []string {
	var out []string
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		for _, it := range ge.Errors {
			if it.Reason != "" {
				out = append(out, it.Reason)
			}
		}
	}
	var be *bq.Error
	if errors.As(err, &be) && be.Reason != "" {
		out = append(out, be.Reason)
	}
	var bv bq.Error
	if errors.As(err, &bv) && bv.Reason != "" {
		out = append(out, bv.Reason)
	}
	return out
}

// cleanText masks secrets, folds control characters and whitespace, and
// cuts to maxErrRunes. Used for every server-written string.
func cleanText(s string) string {
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
