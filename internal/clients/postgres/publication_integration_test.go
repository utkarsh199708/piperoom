//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/testenv"
)

// TestPublicationTablesIntegration verifies the dev stack's publication
// (dbz_publication, created by the pg-orders-connector with
// publication.autocreate.mode=filtered) contains public.orders and that the
// connector role (piperoom, dev/connectors/pg-orders-connector.json
// database.user) has SELECT on it.
func TestPublicationTablesIntegration(t *testing.T) {
	db := sourceDB(t)
	tables, err := db.PublicationTables(context.Background(), "dbz_publication", "piperoom", []string{"public.orders"})
	if err != nil {
		t.Fatalf("PublicationTables: %v", err)
	}
	var orders *PublicationTable
	for i := range tables {
		if tables[i].Schema == "public" && tables[i].Table == "orders" {
			orders = &tables[i]
		}
	}
	if orders == nil {
		t.Fatalf("public.orders not found in result: %+v", tables)
	}
	if !orders.InPublication {
		t.Fatalf("public.orders should be in the publication: %+v", *orders)
	}
	if !orders.Selectable {
		t.Fatalf("piperoom should have SELECT on public.orders: %+v", *orders)
	}
}

// TestPublicationTablesMissingIntegration reports clients.ErrNotFound for an
// unknown publication (the connector never created it).
func TestPublicationTablesMissingIntegration(t *testing.T) {
	db := sourceDB(t)
	_, err := db.PublicationTables(context.Background(), "it_no_such_publication", "piperoom", []string{"public.orders"})
	if err == nil {
		t.Fatal("expected ErrNotFound for a missing publication")
	}
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("error %v does not wrap clients.ErrNotFound", err)
	}
}

// TestPublicationTablesSourceNotInPublicationIntegration creates a table that
// is not in the publication and verifies the method reports InPublication=false
// while Selectable reflects the role's real privilege.
func TestPublicationTablesSourceNotInPublicationIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table := testenv.Name("pub")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id int primary key)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tables, err := db.PublicationTables(context.Background(), "dbz_publication", "piperoom", []string{"public." + table})
	if err != nil {
		t.Fatalf("PublicationTables: %v", err)
	}
	var got *PublicationTable
	for i := range tables {
		if tables[i].Schema == "public" && tables[i].Table == table {
			got = &tables[i]
		}
	}
	if got == nil {
		t.Fatalf("public.%s not found in result: %+v", table, tables)
	}
	if got.InPublication {
		t.Fatalf("public.%s should not be in the publication: %+v", table, *got)
	}
	if !got.Selectable {
		t.Fatalf("piperoom (owner) should have SELECT on public.%s: %+v", table, *got)
	}
}
