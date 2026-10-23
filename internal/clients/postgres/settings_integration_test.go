//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// TestSettingsIntegration reads the dev stack's settings: the compose file
// runs Postgres with wal_level=logical, and a plain Postgres has no
// rds.logical_replication.
func TestSettingsIntegration(t *testing.T) {
	db := sourceDB(t)
	settings, err := db.Settings(context.Background())
	if err != nil {
		t.Fatalf("Settings: %v", err)
	}
	byName := map[string]DBSetting{}
	for _, s := range settings {
		byName[s.Name] = s
		if s.Name == "rds.logical_replication" {
			t.Fatalf("a plain Postgres reported the RDS setting: %+v", s)
		}
	}
	for _, name := range []string{"wal_level", "max_replication_slots", "max_wal_senders", "wal_sender_timeout"} {
		if _, ok := byName[name]; !ok {
			t.Fatalf("%s missing from the settings: %+v", name, settings)
		}
	}
	if got := byName["wal_level"].Value; got != "logical" {
		t.Fatalf("wal_level = %q, want logical (dev/docker-compose.yml sets it)", got)
	}
}

// TestTableReplicationAndRoleIntegration creates it_ tables with and without
// a primary key and a REPLICA IDENTITY FULL table, then reads them and the
// dev user's grants.
func TestTableReplicationAndRoleIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	withPK := testenv.Name("repl_pk")
	noPK := testenv.Name("repl_nopk")
	full := testenv.Name("repl_full")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, stmt := range []string{
		"CREATE TABLE public." + withPK + " (id int primary key)",
		"CREATE TABLE public." + noPK + " (id int)",
		"CREATE TABLE public." + full + " (id int primary key)",
		"ALTER TABLE public." + full + " REPLICA IDENTITY FULL",
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		for _, tbl := range []string{withPK, noPK, full} {
			_, _ = conn.Exec(cctx, "DROP TABLE IF EXISTS public."+tbl)
		}
	})

	tables := []string{"public." + withPK, "public." + noPK, "public." + full}
	repl, err := db.TableReplication(context.Background(), tables)
	if err != nil {
		t.Fatalf("TableReplication: %v", err)
	}
	if len(repl) != 3 {
		t.Fatalf("got %d rows, want 3", len(repl))
	}
	if !repl[0].HasPrimaryKey || repl[0].ReplicaIdentity != "default" {
		t.Fatalf("pk table = %+v", repl[0])
	}
	if repl[1].HasPrimaryKey || repl[1].ReplicaIdentity != "default" {
		t.Fatalf("no-pk table = %+v", repl[1])
	}
	if !repl[2].HasPrimaryKey || repl[2].ReplicaIdentity != "full" {
		t.Fatalf("full table = %+v", repl[2])
	}

	info, err := db.RoleInfo(context.Background(), "piperoom", tables)
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !info.Exists || !info.CanLogin || !info.Replication {
		t.Fatalf("dev role = %+v, want an existing logging-in replication role", info)
	}
	for _, tbl := range tables {
		if !info.Selectable[tbl] {
			t.Fatalf("dev role cannot SELECT its own table %s: %+v", tbl, info.Selectable)
		}
	}
	missing, err := db.RoleInfo(context.Background(), testenv.Name("missingrole"), tables)
	if err != nil {
		t.Fatalf("RoleInfo of a missing role: %v", err)
	}
	if missing.Exists || missing.Selectable[tables[0]] {
		t.Fatalf("missing role = %+v", missing)
	}
}

// TestBlockersIntegration holds an open writing transaction and expects the
// session in Blockers, with a query no longer than the cap.
func TestBlockersIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table := testenv.Name("blockers")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id int primary key)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = conn.Exec(cctx, "ROLLBACK")
		_, _ = conn.Exec(cctx, "DROP TABLE IF EXISTS public."+table)
	})

	var pid int64
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("backend pid: %v", err)
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO public."+table+" (id) VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// olderThan 0 lists every open transaction. Ours is idle in transaction
	// with an assigned xid (backend_xmin may already be NULL between
	// statements), so it must count as a slot blocker.
	sessions, err := db.Blockers(context.Background(), 0)
	if err != nil {
		t.Fatalf("Blockers: %v", err)
	}
	found := false
	for _, s := range sessions {
		if s.PID != pid {
			continue
		}
		if !s.BlocksSlot || s.StartedAt.IsZero() || s.XactStart.IsZero() || !s.InScope {
			t.Fatalf("session = %+v", s)
		}
		if len(s.Query) > MaxSessionQueryBytes {
			t.Fatalf("query is %d bytes, cap %d", len(s.Query), MaxSessionQueryBytes)
		}
		found = true
	}
	if !found {
		t.Fatalf("session %d not in %+v", pid, sessions)
	}

	// Scoped to our table, the session keeps its identity; scoped to another
	// table, the same session is anonymised but still listed as a blocker.
	inScope, err := db.BlockersOnTables(context.Background(), 0, []string{"public." + table})
	if err != nil {
		t.Fatalf("BlockersOnTables(own table): %v", err)
	}
	foreign, err := db.BlockersOnTables(context.Background(), 0, []string{"public.it_blockers_no_such_table"})
	if err != nil {
		t.Fatalf("BlockersOnTables(other table): %v", err)
	}
	check := func(sessions []DBSession, wantInScope bool, label string) {
		t.Helper()
		for _, s := range sessions {
			if s.PID != pid {
				continue
			}
			if s.InScope != wantInScope || !s.BlocksSlot {
				t.Fatalf("%s: session = %+v", label, s)
			}
			if !wantInScope && (s.User != "" || s.App != "" || s.Query != "") {
				t.Fatalf("%s: identity not anonymised: %+v", label, s)
			}
			if wantInScope && s.Query == "" {
				t.Fatalf("%s: in-scope session lost its query: %+v", label, s)
			}
			return
		}
		t.Fatalf("%s: session %d not in %+v", label, pid, sessions)
	}
	check(inScope, true, "own table")
	check(foreign, false, "other table")
}
