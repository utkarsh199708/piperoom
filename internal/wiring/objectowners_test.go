package wiring

import (
	"slices"
	"testing"
)

// pg_object_owners is registered for a Postgres source, after dbtools'
// fixed list, and not for a MySQL source (it has no publication).
func TestObjectOwnersBySourceKind(t *testing.T) {
	pg, _ := registered(t, &Wired{Source: binlogSource{}, Owners: ownersOf(cdcPipe("p"))})
	names := regNames(pg)
	i := slices.Index(names, "pg_object_owners")
	if i < 0 {
		t.Fatalf("Postgres set lacks pg_object_owners: %v", names)
	}
	if j := slices.Index(names, "source_blocking_sessions"); j < 0 || j > i {
		t.Fatalf("pg_object_owners at %d is not after dbtools' fixed list (source_blocking_sessions at %d): %v", i, j, names)
	}
	my, _ := registered(t, &Wired{Source: binlogSource{}, Owners: ownersOf(mysqlPipe("m"))})
	if slices.Contains(regNames(my), "pg_object_owners") {
		t.Fatalf("MySQL set registers pg_object_owners: %v", regNames(my))
	}
}
