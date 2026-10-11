package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"

	"piperoom/internal/clients"
)

// fakeRows is a canned sqlRows for tests.
type fakeRows struct {
	cols    []string
	data    [][]any
	pos     int
	err     error
	closed  bool
	scanErr error
}

func (r *fakeRows) Next() bool {
	if r.pos >= len(r.data) {
		return false
	}
	r.pos++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.data[r.pos-1]
	if len(dest) != len(row) {
		return errors.New("fakeRows: column count mismatch")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *any:
			*p = row[i]
		case **time.Time:
			*p, _ = row[i].(*time.Time)
		default:
			return errors.New("fakeRows: unsupported dest type")
		}
	}
	return nil
}

func (r *fakeRows) Columns() ([]string, error) { return r.cols, nil }
func (r *fakeRows) Err() error                 { return r.err }
func (r *fakeRows) Close() error               { r.closed = true; return nil }

// fakeQuerier returns a canned response (or error) to every query, and
// records the last query and args it was called with.
type fakeQuerier struct {
	rows      *fakeRows
	err       error
	lastQuery string
	lastArgs  []any
}

func (q *fakeQuerier) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	q.lastQuery = query
	q.lastArgs = args
	if q.err != nil {
		return nil, q.err
	}
	return q.rows, nil
}

func TestFreshnessHappyPath(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"max"}, data: [][]any{{&ts}}}}
	db := &DB{q: fq}

	got, err := db.Freshness(context.Background(), "public.orders", "updated_at")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if !got.Equal(ts) {
		t.Fatalf("Freshness = %v, want %v", got, ts)
	}
	if !strings.Contains(fq.lastQuery, "PUBLIC.ORDERS") || !strings.Contains(fq.lastQuery, "UPDATED_AT") {
		t.Fatalf("query %q did not reference the upper-cased table/column", fq.lastQuery)
	}
	if strings.Contains(fq.lastQuery, `"`) {
		t.Fatalf("query %q quotes the identifier, which would make it case-sensitive", fq.lastQuery)
	}
}

func TestFreshnessEmptyTable(t *testing.T) {
	var nilTS *time.Time
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"max"}, data: [][]any{{nilTS}}}}
	db := &DB{q: fq}

	got, err := db.Freshness(context.Background(), "public.orders", "updated_at")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("Freshness = %v, want zero time", got)
	}
}

func TestFreshnessInvalidIdentifier(t *testing.T) {
	db := &DB{q: &fakeQuerier{}}
	if _, err := db.Freshness(context.Background(), "orders", "updated_at"); err == nil {
		t.Fatal("expected error for unqualified table name")
	}
	if _, err := db.Freshness(context.Background(), "public.orders", "ts; DROP TABLE x"); err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestFreshnessObjectNotFound(t *testing.T) {
	fq := &fakeQuerier{err: &gosnowflake.SnowflakeError{Number: errObjectDoesNotExist, SQLState: "42S02", Message: "Object does not exist"}}
	db := &DB{q: fq}

	_, err := db.Freshness(context.Background(), "public.missing", "updated_at")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Freshness err = %v, want wrapping clients.ErrNotFound", err)
	}
}

func TestRowFound(t *testing.T) {
	fq := &fakeQuerier{rows: &fakeRows{
		cols: []string{"id", "amount", "updated_at"},
		data: [][]any{{"42", "19.99", "2026-01-02T03:04:05Z"}},
	}}
	db := &DB{q: fq}

	row, found, err := db.Row(context.Background(), "public.orders", "id", "42")
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if !found {
		t.Fatal("Row: expected found")
	}
	if row["id"] != "42" || row["amount"] != "19.99" {
		t.Fatalf("Row = %v", row)
	}
	if len(fq.lastArgs) != 1 || fq.lastArgs[0] != "42" {
		t.Fatalf("expected pk bound as a parameter, got args=%v query=%q", fq.lastArgs, fq.lastQuery)
	}
	if strings.Contains(fq.lastQuery, "42") {
		t.Fatalf("pk value leaked into query text: %q", fq.lastQuery)
	}
}

func TestRowNotFound(t *testing.T) {
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"id"}, data: nil}}
	db := &DB{q: fq}

	_, found, err := db.Row(context.Background(), "public.orders", "id", "999")
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if found {
		t.Fatal("Row: expected not found")
	}
}

func TestRowTableNotFound(t *testing.T) {
	fq := &fakeQuerier{err: &gosnowflake.SnowflakeError{Number: errObjectDoesNotExist, SQLState: "42S02"}}
	db := &DB{q: fq}

	_, _, err := db.Row(context.Background(), "public.missing", "id", "1")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Row err = %v, want wrapping clients.ErrNotFound", err)
	}
}

func TestRowOtherError(t *testing.T) {
	fq := &fakeQuerier{err: errors.New("connection reset")}
	db := &DB{q: fq}

	_, _, err := db.Row(context.Background(), "public.orders", "id", "1")
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, clients.ErrNotFound) {
		t.Fatal("a generic error must not be mapped to clients.ErrNotFound")
	}
}

func TestNewInvalidDSN(t *testing.T) {
	// ParseDSN rejects a DSN with no account/host information before any
	// network call is made, so this is safe to run without a Snowflake
	// account.
	_, err := New(context.Background(), "not a valid dsn")
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
	if !strings.Contains(err.Error(), "snowflake") {
		t.Fatalf("error not wrapped: %v", err)
	}
}

func TestWrapQueryErrGeneric(t *testing.T) {
	err := wrapQueryErr("query row from", "public.orders", errors.New("boom"))
	if errors.Is(err, clients.ErrNotFound) {
		t.Fatal("generic error must not be mapped to ErrNotFound")
	}
	if !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), "public.orders") {
		t.Fatalf("error lost context: %v", err)
	}
}
