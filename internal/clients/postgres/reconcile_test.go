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

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// seqRows is one canned result: column OIDs and rows; Scan fills *string,
// *int64 and *int64-typed catalog fields only, enough for these tests.
type seqRows struct {
	pgx.Rows
	oids []uint32
	rows [][]any
	i    int
}

func (r *seqRows) Next() bool { r.i++; return r.i <= len(r.rows) }
func (r *seqRows) Close()     {}
func (r *seqRows) Err() error { return nil }
func (r *seqRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(r.oids))
	for i, o := range r.oids {
		fds[i].DataTypeOID = o
	}
	return fds
}

func (r *seqRows) Scan(dest ...any) error {
	for i, v := range r.rows[r.i-1] {
		switch d := dest[i].(type) {
		case *string:
			*d = v.(string)
		case *int64:
			*d = v.(int64)
		case *pgtype.Timestamptz:
			*d = pgtype.Timestamptz{Time: v.(time.Time), Valid: true}
		}
	}
	return nil
}

// seqQuerier answers statements in order and records them.
type seqQuerier struct {
	fakeQuerier
	results []*seqRows
}

func (q *seqQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if _, err := q.fakeQuerier.Query(ctx, sql, args...); err != nil {
		return nil, err
	}
	return q.results[len(q.sqls)-1], nil
}

var reconcilePipeline = owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders"}, SinkTables: []string{"analytics.orders"}}

func TestDBReconcilerSourceSide(t *testing.T) {
	var _ clients.Reconciler = (*DB)(nil)
	ctx, rec := recordCtx()
	ctx = tools.WithPipeline(ctx, reconcilePipeline)
	q := &seqQuerier{results: []*seqRows{{oids: []uint32{pgtype.Int8OID}, rows: [][]any{{int64(42)}}}}}
	from := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	n, err := testDB(q).CountBetween(ctx, "public.orders", "updated_at", from, from.Add(time.Hour))
	if err != nil || n != 42 {
		t.Fatalf("CountBetween = %d, %v", n, err)
	}
	if q.sqls[0] != `SELECT count(*) FROM "public"."orders" WHERE "updated_at" >= $1 AND "updated_at" < $2` {
		t.Errorf("sql = %s", q.sqls[0])
	}
	if calls := rec.Calls(); len(calls) != 1 || calls[0].Target != "postgres orders@db.internal:5432" {
		t.Errorf("receipts = %+v", calls)
	}
	// The source client checks source_tables: the sink table is refused
	// before any statement.
	if _, err := testDB(q).CountBetween(ctx, "analytics.orders", "updated_at", from, from.Add(time.Hour)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("sink table on the source: err = %v", err)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("ran %d statements", len(q.sqls))
	}
}

// The RowTimes receipt names how many keys it read, never which.
func TestDBRowTimesReceiptHidesKeys(t *testing.T) {
	ctx, rec := recordCtx()
	ctx = tools.WithPipeline(ctx, reconcilePipeline)
	at := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	q := &seqQuerier{results: []*seqRows{
		{oids: []uint32{pgtype.NameOID, pgtype.Int8OID, pgtype.NameOID, pgtype.NameOID}, rows: [][]any{
			{"id", int64(pgtype.TextOID), "pg_catalog", "text"},
			{"updated_at", int64(pgtype.TimestamptzOID), "pg_catalog", "timestamptz"},
		}},
		{oids: []uint32{pgtype.TextOID, pgtype.TimestamptzOID}, rows: [][]any{{"cust-7781", at}}},
	}}
	got, err := testDB(q).RowTimes(ctx, "public.orders", "id", []string{"cust-7781", "cust-9001"}, []string{"updated_at"})
	if err != nil || !got["cust-7781"]["updated_at"].Equal(at) || len(got) != 1 {
		t.Fatalf("RowTimes = %v, %v", got, err)
	}
	calls := rec.Calls()
	if len(calls) != 2 {
		t.Fatalf("%d receipts, want 2", len(calls))
	}
	params := strings.Join(calls[1].ParamsRedacted, " ")
	if strings.Contains(params, "cust-") || !strings.Contains(params, "<2 keys>") {
		t.Errorf("receipt params = %q, want the key count only", params)
	}
	if strings.Contains(calls[1].Text, "cust-") {
		t.Errorf("receipt text holds a key: %s", calls[1].Text)
	}
}
