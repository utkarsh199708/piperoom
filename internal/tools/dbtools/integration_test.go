//go:build integration

package dbtools

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// repoRoot walks up from the package dir until it finds go.mod, so the
// relative dev/owners.yaml default resolves when go test runs from the package.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from cwd")
		}
		dir = parent
	}
}

// TestPgPublicationTablesIntegration runs the pg_publication_tables tool end
// to end against the dev stack: it resolves the orders pipeline from
// dev/owners.yaml, reads the connector user and publication name from the
// dev stack's Connect (the tool has no default role) and reads the real
// dbz_publication from the source Postgres.
func TestPgPublicationTablesIntegration(t *testing.T) {
	cfg := testenv.Require(t)

	db, err := postgres.New(context.Background(), cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres: %v", err)
	}
	t.Cleanup(db.Close)

	o, err := owners.Load(filepath.Join(repoRoot(t), "dev", "owners.yaml"))
	if err != nil {
		t.Fatalf("owners: %v", err)
	}

	reg := tools.NewRegistry()
	cc := connect.New(cfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})
	if err := Register(reg, Deps{Src: db, Owners: o, Connect: cc}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tt, ok := reg.Get("pg_publication_tables")
	if !ok {
		t.Fatal("pg_publication_tables not registered")
	}

	res, err := tt.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatalf("pg_publication_tables: %v", err)
	}
	if !strings.Contains(res.Summary, "publication dbz_publication") {
		t.Fatalf("summary missing publication name: %q", res.Summary)
	}
	if !strings.Contains(res.Summary, "public.orders: in_publication=yes, selectable=yes") {
		t.Fatalf("summary missing expected orders line: %q", res.Summary)
	}
}
