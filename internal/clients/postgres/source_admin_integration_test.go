//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestReaderApplicationNameIntegration: the read pool names its sessions
// piperoom-reader, which is how the admin writer refuses to end them; a DSN
// that sets application_name keeps its own.
func TestReaderApplicationNameIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	named, err := url.Parse(cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("bad SOURCE_DB_URL: %v", err)
	}
	q := named.Query()
	q.Set("application_name", "mine")
	named.RawQuery = q.Encode()
	cases := []struct{ name, dsn, want string }{
		{"default", cfg.SourceDBURL, readerAppName},
		{"DSN wins", named.String(), "mine"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := New(ctx, tc.dsn)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer db.Close()
			var app string
			if err := db.queryRow(ctx, "SELECT current_setting('application_name')").Scan(&app); err != nil {
				t.Fatal(err)
			}
			if app != tc.want {
				t.Fatalf("application_name = %q, want %q", app, tc.want)
			}
		})
	}
}

// TestAdminWriterIntegration runs the admin writer against the dev stack. It
// connects as the dev SOURCE_DB_URL role, which may create roles and tables
// there (production gives the writer its own role). Every object is named
// it_<random> and dropped at the end.
func TestAdminWriterIntegration(t *testing.T) {
	f := newWriterFixture(t)
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 30*time.Second)
	defer cancel()

	role := testenv.Name("grantee")
	const pw = "it-grantee-pw" // a throwaway role on the local dev stack
	quotedRole := pgx.Identifier{role}.Sanitize()
	if _, err := f.admin.Exec(ctx, "CREATE ROLE "+quotedRole+" LOGIN PASSWORD '"+pw+"'"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = f.admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1", role)
		_, _ = f.admin.Exec(ctx, "DROP OWNED BY "+quotedRole)
		_, _ = f.admin.Exec(ctx, "DROP ROLE IF EXISTS "+quotedRole)
	})

	t.Run("grant select is SELECT only", func(t *testing.T) {
		if err := f.w.GrantSelect(ctx, role, []string{f.tableIn}); err != nil {
			t.Fatalf("GrantSelect: %v", err)
		}
		if err := f.w.GrantSelect(ctx, role, []string{f.tableIn}); err != nil {
			t.Fatalf("GrantSelect again (idempotent): %v", err)
		}
		for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"} {
			var ok bool
			if err := f.admin.QueryRow(ctx, "SELECT has_table_privilege($1, $2, $3)", role, f.tableIn, priv).Scan(&ok); err != nil {
				t.Fatal(err)
			}
			if want := priv == "SELECT"; ok != want {
				t.Fatalf("%s on %s = %v, want %v", priv, f.tableIn, ok, want)
			}
		}
		rows, err := f.admin.Query(ctx, `SELECT privilege_type FROM information_schema.role_table_grants
			WHERE grantee = $1 AND table_schema = 'public' AND table_name = $2`, role, strings.TrimPrefix(f.tableIn, "public."))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var granted []string
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				t.Fatal(err)
			}
			granted = append(granted, p)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(granted) != 1 || granted[0] != "SELECT" {
			t.Fatalf("granted = %v, want only SELECT", granted)
		}

		if err := f.w.GrantSelect(ctx, role+"_missing", []string{f.tableIn}); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("missing role: %v, want ErrNotFound", err)
		}
		if err := f.w.GrantSelect(ctx, role, []string{f.tableIn + "_missing"}); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("missing table: %v, want ErrNotFound", err)
		}

		// A table outside the room's pipeline is refused before any I/O.
		scoped := tools.WithPipeline(ctx, owners.Pipeline{
			ID:           "orders",
			SourceTables: []string{f.tableIn},
			Connectors:   []string{"pg-orders-connector"},
		})
		if err := f.w.GrantSelect(scoped, role, []string{f.tableOut}); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("out-of-scope table: %v, want ErrOutOfScope", err)
		}
		var ok bool
		if err := f.admin.QueryRow(ctx, "SELECT has_table_privilege($1, $2, 'SELECT')", role, f.tableOut).Scan(&ok); err != nil || ok {
			t.Fatalf("the out-of-scope grant reached the table: %v, %v", ok, err)
		}
	})

	t.Run("replica identity", func(t *testing.T) {
		ident := func() string {
			var c byte
			if err := f.admin.QueryRow(ctx, "SELECT relreplident FROM pg_class WHERE oid = $1::regclass", f.tableIn).Scan(&c); err != nil {
				t.Fatal(err)
			}
			return string(c)
		}
		if err := f.w.SetReplicaIdentity(ctx, f.tableIn, "full", ""); err != nil {
			t.Fatalf("full: %v", err)
		}
		if got := ident(); got != "f" {
			t.Fatalf("relreplident = %q, want f", got)
		}
		index := strings.TrimPrefix(f.tableIn, "public.") + "_pkey"
		if err := f.w.SetReplicaIdentity(ctx, f.tableIn, "index", index); err != nil {
			t.Fatalf("index: %v", err)
		}
		if got := ident(); got != "i" {
			t.Fatalf("relreplident = %q, want i", got)
		}
		if err := f.w.SetReplicaIdentity(ctx, f.tableIn, "default", ""); err != nil {
			t.Fatalf("default: %v", err)
		}
		if got := ident(); got != "d" {
			t.Fatalf("relreplident = %q, want d", got)
		}
		if err := f.w.SetReplicaIdentity(ctx, f.tableIn, "index", index+"_missing"); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("missing index: %v, want ErrNotFound", err)
		}
	})

	t.Run("heartbeat table", func(t *testing.T) {
		name := f.tableIn + "_hb"
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			schema, table, _ := strings.Cut(name, ".")
			_, _ = f.admin.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{schema, table}.Sanitize())
		})
		if err := f.w.CreateHeartbeatTable(ctx, name); err != nil {
			t.Fatalf("CreateHeartbeatTable: %v", err)
		}
		if err := f.w.CreateHeartbeatTable(ctx, name); err != nil {
			t.Fatalf("CreateHeartbeatTable again (idempotent): %v", err)
		}
		// The shape Debezium's heartbeat query needs: ON CONFLICT (id) works.
		schema, table, _ := strings.Cut(name, ".")
		quoted := pgx.Identifier{schema, table}.Sanitize()
		if _, err := f.admin.Exec(ctx, "INSERT INTO "+quoted+" (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = now()"); err != nil {
			t.Fatalf("heartbeat insert: %v", err)
		}
	})

	t.Run("end session", func(t *testing.T) {
		// A session of another role: the writer may end it.
		u, err := url.Parse(cfg.SourceDBURL)
		if err != nil {
			t.Fatalf("bad SOURCE_DB_URL: %v", err)
		}
		u.User = url.UserPassword(role, pw)
		hc, err := pgx.ParseConfig(u.String())
		if err != nil {
			t.Fatal(err)
		}
		hc.RuntimeParams["application_name"] = testenv.Name("holder")
		holder, err := pgx.ConnectConfig(ctx, hc)
		if err != nil {
			t.Fatalf("connect as %s: %v", role, err)
		}
		var pid int64
		if err := holder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			holder.Close(context.Background())
			t.Fatal(err)
		}
		var start time.Time
		if err := f.admin.QueryRow(ctx, "SELECT backend_start FROM pg_stat_activity WHERE pid = $1", pid).Scan(&start); err != nil {
			holder.Close(context.Background())
			t.Fatal(err)
		}

		// A different backend_start is a changed session; the holder lives.
		if err := f.w.EndSession(ctx, pid, start.Add(time.Second)); !errors.Is(err, ErrSessionChanged) {
			t.Fatalf("mismatched backend_start: %v, want ErrSessionChanged", err)
		}
		var one int
		if err := holder.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("the holder was ended by a refused call: %v", err)
		}

		if err := f.w.EndSession(ctx, pid, start); err != nil {
			t.Fatalf("EndSession: %v", err)
		}
		hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer hcancel()
		if err := holder.QueryRow(hctx, "SELECT 1").Scan(&one); err == nil {
			t.Fatal("the holder still answers after EndSession")
		}
		holder.Close(context.Background())

		// The dev role's own session (the admin connection) is protected.
		var selfPID int64
		var selfStart time.Time
		if err := f.admin.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&selfPID); err != nil {
			t.Fatal(err)
		}
		if err := f.admin.QueryRow(ctx, "SELECT backend_start FROM pg_stat_activity WHERE pid = $1", selfPID).Scan(&selfStart); err != nil {
			t.Fatal(err)
		}
		if err := f.w.EndSession(ctx, selfPID, selfStart); !errors.Is(err, ErrSessionProtected) {
			t.Fatalf("own session: %v, want ErrSessionProtected", err)
		}
		if err := f.admin.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("the admin session was ended: %v", err)
		}

		if err := f.w.EndSession(context.Background(), selfPID, selfStart); !errors.Is(err, tools.ErrNoScope) {
			t.Fatalf("no scope: %v, want ErrNoScope", err)
		}
	})
}
