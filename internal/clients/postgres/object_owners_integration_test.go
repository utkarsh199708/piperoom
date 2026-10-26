//go:build integration

package postgres

import (
	"context"
	"testing"
)

// TestObjectOwnersIntegration reads the dev stack's owners of public.orders
// and dbz_publication against the connector role piperoom. Read-only: it
// asserts what any state of the stack (healthy, or after devctl break
// connector-auth) must satisfy, not who the owner is.
func TestObjectOwnersIntegration(t *testing.T) {
	db := sourceDB(t)
	got, err := db.ObjectOwners(context.Background(), "piperoom", "dbz_publication", []string{"public.orders", "public.it_no_such_table"})
	if err != nil {
		t.Fatalf("ObjectOwners: %v", err)
	}
	if !got.RoleExists {
		t.Fatalf("role piperoom should exist: %+v", got)
	}
	if len(got.Tables) != 2 {
		t.Fatalf("tables = %+v", got.Tables)
	}
	o := got.Tables[0]
	if !o.Exists || o.Owner == "" || o.Table != "public.orders" {
		t.Fatalf("public.orders = %+v", o)
	}
	if o.RoleIsOwner != (o.Owner == "piperoom") {
		t.Fatalf("role_is_owner %v disagrees with owner %q", o.RoleIsOwner, o.Owner)
	}
	if o.RoleIsOwner && !o.RoleHasOwnerPrivs {
		t.Fatalf("the owner lacks its own privileges: %+v", o)
	}
	if g := got.Tables[1]; g.Exists || g.Owner != "" {
		t.Fatalf("missing table = %+v", g)
	}
	if p := got.Publication; p.Exists && p.Owner == "" {
		t.Fatalf("publication without an owner: %+v", p)
	}
}

func TestObjectOwnersMissingIntegration(t *testing.T) {
	db := sourceDB(t)
	got, err := db.ObjectOwners(context.Background(), "it_no_such_role", "it_no_such_publication", []string{"public.orders"})
	if err != nil {
		t.Fatalf("a missing role and publication are a verdict, not an error: %v", err)
	}
	if got.RoleExists || got.Publication.Exists || got.Tables[0].RoleIsOwner || got.Tables[0].RoleHasOwnerPrivs || got.Tables[0].InPublication {
		t.Fatalf("got %+v", got)
	}
}
