//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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

// TestPublicationOwnersIntegration rebuilds the connector-auth break in miniature
// (QA G3): a table owned by one role, a publication owned by the connector
// role, and a connector role that is neither the table's owner nor a member of
// it. It then makes the role a member and checks the answer changes. Needs a
// superuser SOURCE_DB_URL (the dev stack's piperoom), like the other write tests.
func TestPublicationOwnersIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table, pub := testenv.Name("ownt"), testenv.Name("ownp")
	ownerRole, connRole := testenv.Name("owno"), testenv.Name("ownc")
	id := func(n string) string { return pgx.Identifier{n}.Sanitize() }
	exec := func(sql string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, sql := range []string{
			"DROP PUBLICATION IF EXISTS " + id(pub),
			"DROP TABLE IF EXISTS public." + id(table),
			"DROP ROLE IF EXISTS " + id(connRole),
			"DROP ROLE IF EXISTS " + id(ownerRole),
		} {
			_, _ = conn.Exec(ctx, sql)
		}
	})
	exec("CREATE ROLE " + id(ownerRole))
	exec("CREATE ROLE " + id(connRole) + " REPLICATION")
	exec("CREATE TABLE public." + id(table) + " (id int primary key)")
	exec("ALTER TABLE public." + id(table) + " OWNER TO " + id(ownerRole))
	exec("GRANT SELECT ON public." + id(table) + " TO " + id(connRole))
	exec("CREATE PUBLICATION " + id(pub) + " FOR TABLE public." + id(table))
	exec("ALTER PUBLICATION " + id(pub) + " OWNER TO " + id(connRole))

	ctx := context.Background()
	tables, other, err := db.PublicationSourceTables(ctx, pub, connRole, []string{"public." + table})
	if err != nil {
		t.Fatalf("PublicationSourceTables: %v", err)
	}
	if len(tables) != 1 || other != 0 {
		t.Fatalf("tables = %+v, other = %d", tables, other)
	}
	got := tables[0]
	if !got.InPublication || !got.Selectable || got.Owner != ownerRole || got.RoleIsOwner || got.RoleMemberOfOwner {
		t.Fatalf("connector role is neither owner nor member: %+v", got)
	}

	info, err := db.PublicationInfo(ctx, pub, connRole)
	if err != nil {
		t.Fatalf("PublicationInfo: %v", err)
	}
	want := PublicationInfo{Publication: pub, Owner: connRole, Role: connRole, RoleExists: true, RoleReplication: true, RoleIsOwner: true, RoleMemberOfOwner: true}
	if info != want {
		t.Fatalf("PublicationInfo = %+v, want %+v", info, want)
	}

	// Membership of the table owner is what DBAs grant to fix it.
	exec("GRANT " + id(ownerRole) + " TO " + id(connRole))
	tables, _, err = db.PublicationSourceTables(ctx, pub, connRole, []string{"public." + table})
	if err != nil {
		t.Fatal(err)
	}
	if !tables[0].RoleMemberOfOwner || tables[0].RoleIsOwner {
		t.Fatalf("after GRANT: %+v", tables[0])
	}

	// A role that does not exist, and a publication that does not.
	info, err = db.PublicationInfo(ctx, pub, testenv.Name("norole"))
	if err != nil || info.RoleExists || info.RoleReplication || info.RoleIsOwner {
		t.Fatalf("missing role: %+v, %v", info, err)
	}
	if _, err := db.PublicationInfo(ctx, testenv.Name("nopub"), connRole); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing publication: err = %v, want ErrNotFound", err)
	}
}
