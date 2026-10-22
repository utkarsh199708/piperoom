package warehouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// sampleRows answers SampleKeys: one text key per row.
type sampleRows struct {
	pgx.Rows
	keys []string
	i    int
}

func (r *sampleRows) Next() bool { r.i++; return r.i <= len(r.keys) }
func (r *sampleRows) Close()     {}
func (r *sampleRows) Err() error { return nil }
func (r *sampleRows) FieldDescriptions() []pgconn.FieldDescription {
	return []pgconn.FieldDescription{{DataTypeOID: pgtype.TextOID}}
}
func (r *sampleRows) Scan(dest ...any) error { *dest[0].(*string) = r.keys[r.i-1]; return nil }

type sampleQuerier struct {
	fakeQuerier
	rows *sampleRows
}

func (q *sampleQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if _, err := q.fakeQuerier.Query(ctx, sql, args...); err != nil {
		return nil, err
	}
	return q.rows, nil
}

// The warehouse is the sink side: it checks sink_tables, not source_tables.
func TestDBReconcilerSinkSide(t *testing.T) {
	var _ clients.Reconciler = (*DB)(nil)
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{
		ID: "orders", SourceTables: []string{"public.orders"}, SinkTables: []string{"analytics.orders"},
	})
	q := &sampleQuerier{rows: &sampleRows{keys: []string{"9", "8"}}}
	keys, err := testDB(q).SampleKeys(ctx, "analytics.orders", "id", "updated_at", time.Now().Add(-time.Hour), 2)
	if err != nil || len(keys) != 2 || keys[0] != "9" {
		t.Fatalf("SampleKeys = %v, %v", keys, err)
	}
	if want := `SELECT "id"::text FROM "analytics"."orders" WHERE "updated_at" >= $1 AND "id" IS NOT NULL ORDER BY "updated_at" DESC, "id" DESC LIMIT $2`; q.sqls[0] != want {
		t.Errorf("sql = %s", q.sqls[0])
	}
	if _, err := testDB(q).SampleKeys(ctx, "public.orders", "id", "updated_at", time.Now(), 2); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("source table on the sink: err = %v", err)
	}
	if _, err := testDB(q).CountBetween(context.Background(), "analytics.orders", "updated_at", time.Now(), time.Now()); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: err = %v", err)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("ran %d statements, want 1", len(q.sqls))
	}
}
