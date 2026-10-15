package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
)

// information_schema hides constraints from a SELECT-only role, so pg_row
// failed on every table for the documented read-only role (#385). The key
// comes from pg_catalog, and a table without one is clients.ErrNotFound.
func TestPrimaryKeyColumnReadsCatalog(t *testing.T) {
	q := &fakeQuerier{err: pgx.ErrNoRows}
	_, err := testDB(q).PrimaryKeyColumn(context.Background(), "public.orders")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("no key: err %v, want clients.ErrNotFound", err)
	}
	if len(q.sqls) != 1 || strings.Contains(q.sqls[0], "information_schema") || !strings.Contains(q.sqls[0], "indisprimary") {
		t.Fatalf("query must read pg_index, not information_schema: %q", q.sqls)
	}

	q = &fakeQuerier{err: errors.New("connection refused")}
	if _, err := testDB(q).PrimaryKeyColumn(context.Background(), "public.orders"); err == nil || errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("a failed query is not a missing key: %v", err)
	}
	if _, err := testDB(&fakeQuerier{}).PrimaryKeyColumn(context.Background(), "orders; drop"); err == nil {
		t.Fatal("bad table name accepted")
	}
}

// pkRow answers the primary key query with a first column and the number of
// key columns.
type pkRow struct {
	col  string
	cols int
}

func (r pkRow) Scan(dest ...any) error {
	*dest[0].(*string) = r.col
	*dest[1].(*int) = r.cols
	return nil
}

type pkQuerier struct {
	fakeQuerier
	row pkRow
}

func (q *pkQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return q.row }

// With PRIMARY KEY (tenant_id, id), "tenant_id" alone is not one row: the key
// is reported as composite, not as its first column (#483).
func TestPrimaryKeyColumnComposite(t *testing.T) {
	col, err := testDB(&pkQuerier{row: pkRow{col: "tenant_id", cols: 2}}).PrimaryKeyColumn(context.Background(), "public.orders")
	if !errors.Is(err, clients.ErrNotSupported) || col != "" {
		t.Fatalf("composite key: %q, %v; want clients.ErrNotSupported", col, err)
	}
	col, err = testDB(&pkQuerier{row: pkRow{col: "id", cols: 1}}).PrimaryKeyColumn(context.Background(), "public.orders")
	if err != nil || col != "id" {
		t.Fatalf("single key: %q, %v; want id", col, err)
	}
}
