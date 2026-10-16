package warehouse

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
)

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

// With PRIMARY KEY (tenant_id, order_id), "tenant_id" alone is not one row:
// the key is reported as composite, not as its first column (#483).
func TestPrimaryKeyColumnComposite(t *testing.T) {
	col, err := testDB(&pkQuerier{row: pkRow{col: "tenant_id", cols: 2}}).PrimaryKeyColumn(context.Background(), "analytics.orders")
	if !errors.Is(err, clients.ErrNotSupported) || col != "" {
		t.Fatalf("composite key: %q, %v; want clients.ErrNotSupported", col, err)
	}
	col, err = testDB(&pkQuerier{row: pkRow{col: "order_id", cols: 1}}).PrimaryKeyColumn(context.Background(), "analytics.orders")
	if err != nil || col != "order_id" {
		t.Fatalf("single key: %q, %v; want order_id", col, err)
	}
}
