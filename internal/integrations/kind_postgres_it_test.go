//go:build integration

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/integrations/spec"
)

// itAdminURL is a superuser URL of a scratch Postgres: PIPEROOM_IT_PG_ADMIN_URL,
// or the dev stack's SOURCE_DB_URL (its user owns the cluster).
func itAdminURL(t *testing.T) *url.URL {
	t.Helper()
	raw := os.Getenv("PIPEROOM_IT_PG_ADMIN_URL")
	if raw == "" {
		raw = config.Load().SourceDBURL
		if !strings.Contains(raw, "localhost") {
			t.Skip("no scratch Postgres: set PIPEROOM_IT_PG_ADMIN_URL")
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func itExec(t *testing.T, ctx context.Context, u *url.URL, sqls ...string) {
	t.Helper()
	c, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for _, q := range sqls {
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// TestIntegrationPostgresSetupScriptAndColumnGrants runs the generated setup
// script unedited under psql -1 on a fresh database, then checks that the
// role has no known password and that column and TRUNCATE grants fail the
// read-only checks.
func TestIntegrationPostgresSetupScriptAndColumnGrants(t *testing.T) {
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql is not installed")
	}
	admin := itAdminURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	name := "it_" + hex.EncodeToString(b)
	role := name + "_ro"
	itExec(t, ctx, admin, "CREATE DATABASE "+name)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		itExec(t, cctx, admin, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)", "DROP ROLE IF EXISTS "+role)
	})
	db := *admin
	db.Path = "/" + name
	itExec(t, ctx, &db,
		"CREATE TABLE public.orders (id int PRIMARY KEY, status text)",
		"CREATE TABLE public.order_items (id int PRIMARY KEY, order_id int)")

	script := pgSetupScript(true)(vals(map[string]string{"user": role, "database": name, "tables": "public.orders, public.order_items"}, nil))
	f := filepath.Join(t.TempDir(), "setup.sql")
	if err := os.WriteFile(f, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.CommandContext(ctx, psql, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-1", "-f", f, db.String()).CombinedOutput()
	if err != nil {
		t.Fatalf("setup script failed: %v\n%s\n%s", err, out, script)
	}

	login := db
	login.User = url.UserPassword(role, "<generated, from your secret store>")
	if c, err := pgx.Connect(ctx, login.String()); err == nil {
		c.Close(ctx)
		t.Fatal("the unedited script left a role that logs in with the placeholder password")
	}

	itExec(t, ctx, &db,
		"ALTER ROLE "+role+" PASSWORD 'it-pw'",
		"GRANT UPDATE (status) ON public.orders TO "+role,
		"GRANT TRUNCATE ON public.order_items TO "+role)
	port := db.Port()
	if port == "" {
		port = "5432"
	}
	v := vals(map[string]string{"host": db.Hostname(), "port": port, "database": name, "user": role,
		"sslmode": "disable", "tables": "public.orders, public.order_items"}, map[string]string{"password": "it-pw"})
	checks := devDeps(t).postgresKinds()[0].Test(ctx, v)
	wantStatus(t, checks, "login", spec.CheckPass)
	wantStatus(t, checks, "select:public.orders", spec.CheckPass)
	if c := wantStatus(t, checks, "read_only:public.orders", spec.CheckFail); !strings.Contains(c.Fix, "REVOKE UPDATE (status) ON public.orders FROM "+role) {
		t.Fatalf("orders fix = %q", c.Fix)
	}
	if c := wantStatus(t, checks, "read_only:public.order_items", spec.CheckFail); !strings.Contains(c.Fix, "REVOKE TRUNCATE ON public.order_items FROM "+role) {
		t.Fatalf("order_items fix = %q", c.Fix)
	}

	// The scratch server has ssl off: verify-full must say so, not "check the password".
	v.Config["sslmode"] = "verify-full"
	checks = devDeps(t).postgresKinds()[0].Test(ctx, v)
	if c := wantStatus(t, checks, "tls", spec.CheckFail); !strings.Contains(c.Detail, "did not use TLS") {
		t.Fatalf("tls = %+v", c)
	}
}
