package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// freshRows is one row whose single column has oid and holds ts.
type freshRows struct {
	pgx.Rows // only Next/Close/Err/FieldDescriptions/Scan are used
	oid      uint32
	ts       pgtype.Timestamptz
	done     bool
}

func (r *freshRows) Next() bool { r.done = !r.done; return r.done }
func (r *freshRows) Close()     {}
func (r *freshRows) Err() error { return nil }
func (r *freshRows) FieldDescriptions() []pgconn.FieldDescription {
	return []pgconn.FieldDescription{{DataTypeOID: r.oid}}
}
func (r *freshRows) Scan(dest ...any) error {
	p, ok := dest[0].(*pgtype.Timestamptz)
	if !ok {
		return errors.New("unexpected destination")
	}
	*p = r.ts
	return nil
}

// rowsQuerier answers Query with one prepared row set.
type rowsQuerier struct {
	fakeQuerier
	rows pgx.Rows
}

func (q *rowsQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	return q.rows, nil
}

func TestFreshnessReadsMaxTimestamp(t *testing.T) {
	want := time.Date(2026, 10, 22, 9, 30, 0, 0, time.UTC)
	q := &rowsQuerier{rows: &freshRows{oid: pgtype.TimestamptzOID, ts: pgtype.Timestamptz{Time: want, Valid: true}}}
	got, err := testDB(q).Freshness(context.Background(), "public.orders", "updated_at")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("Freshness = %v, want %v", got, want)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("%d statements, want 1", len(q.sqls))
	}
	for _, part := range []string{`max("updated_at")`, `FROM "public"."orders"`} {
		if !strings.Contains(q.sqls[0], part) {
			t.Errorf("sql %q lacks %q", q.sqls[0], part)
		}
	}
	if strings.Contains(q.sqls[0], "SELECT *") {
		t.Errorf("sql selects whole rows: %s", q.sqls[0])
	}
}

func TestFreshnessRejectsBadIdentifiers(t *testing.T) {
	q := &fakeQuerier{}
	d := testDB(q)
	for _, c := range []struct{ table, col string }{
		{"public.orders; drop", "updated_at"},
		{"public.orders", "updated_at; drop"},
		{"orders", "updated_at"},
		{"a.b.c", "updated_at"},
		{"public.orders", "1=1"},
	} {
		if _, err := d.Freshness(context.Background(), c.table, c.col); err == nil {
			t.Errorf("table %q col %q: want an error", c.table, c.col)
		}
	}
	if len(q.sqls) != 0 {
		t.Fatalf("ran %d statements for bad input", len(q.sqls))
	}
}

func TestFreshnessEmptyTable(t *testing.T) {
	got, err := testDB(&fakeQuerier{}).Freshness(context.Background(), "public.orders", "updated_at")
	if err != nil || !got.IsZero() {
		t.Fatalf("Freshness = %v, %v; want zero time and no error", got, err)
	}
}

func TestFreshnessRejectsNonTimestampColumn(t *testing.T) {
	q := &rowsQuerier{rows: &freshRows{oid: pgtype.TextOID, ts: pgtype.Timestamptz{Valid: false}}}
	_, err := testDB(q).Freshness(context.Background(), "public.orders", "email")
	if err == nil || !strings.Contains(err.Error(), "not a timestamp") {
		t.Fatalf("err = %v, want the not-a-timestamp error", err)
	}
}

// A data exception (SQLSTATE class 22) can quote the column value; the error
// must keep the SQLSTATE and drop the message.
func TestFreshnessErrorNeverQuotesValue(t *testing.T) {
	const secret = "zoe.secret@example.com"
	q := &fakeQuerier{err: &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type timestamp: "` + secret + `"`}}
	_, err := testDB(q).Freshness(context.Background(), "public.orders", "updated_at")
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error quotes the value: %v", err)
	}
	if !strings.Contains(err.Error(), "22P02") {
		t.Fatalf("error should keep the SQLSTATE: %v", err)
	}
}
