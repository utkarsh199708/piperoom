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
