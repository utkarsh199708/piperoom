package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// With PRIMARY KEY (TENANT_ID, ID), TENANT_ID alone is not one row: the key is
// reported as composite, not as its first column (#531, as #526 for Postgres).
func TestPrimaryKeyColumnComposite(t *testing.T) {
	d := &DB{q: &fakeQuerier{rows: pkShow("TENANT_ID", "ID")}}
	col, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders")
	if !errors.Is(err, clients.ErrNotSupported) || col != "" {
		t.Fatalf("composite key: %q, %v; want clients.ErrNotSupported", col, err)
	}
	if !strings.Contains(err.Error(), "composite (2 columns)") || !strings.Contains(err.Error(), `"TENANT_ID"`) {
		t.Errorf("error %q does not say the key is composite and name its first column", err)
	}
	d = &DB{q: &fakeQuerier{rows: pkShow("ID")}}
	col, err = d.PrimaryKeyColumn(context.Background(), "analytics.orders")
	if err != nil || col != "ID" {
		t.Fatalf("single key: %q, %v; want ID", col, err)
	}
}
