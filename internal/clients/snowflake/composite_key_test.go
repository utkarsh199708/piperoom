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

// Row must not read by one column of a composite key either: the topology
// trace passes the source key ("id") for the sink table, which would match ID
// of PRIMARY KEY (TENANT_ID, ID) and read another tenant's row (#531). It is
// refused before any SELECT runs, for every requested column.
func TestRowRefusesCompositeKey(t *testing.T) {
	for _, pk := range []string{"id", "ID", "TENANT_ID", "other"} {
		fq := &fakeQuerier{rows: &fakeRows{cols: []string{"ID"}, data: [][]any{{"1"}}}, showRows: pkShow("TENANT_ID", "ID")}
		d := &DB{q: fq}
		row, found, err := d.Row(context.Background(), "analytics.orders", pk, "1")
		if !errors.Is(err, clients.ErrNotSupported) || found || row != nil {
			t.Errorf("Row(pk=%q) = %v, %v, %v; want clients.ErrNotSupported", pk, row, found, err)
		}
		if strings.HasPrefix(fq.lastQuery, "SELECT") {
			t.Errorf("Row(pk=%q) ran %q; want no SELECT", pk, fq.lastQuery)
		}
	}
}
