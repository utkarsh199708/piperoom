//go:build integration

package dbtools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestPgToastRiskIntegration (X1d TC5): an it_ table with REPLICA IDENTITY
// DEFAULT and 5 KB values stored out of line is reported; after REPLICA
// IDENTITY FULL it is not. No Connect or warehouse: the result says the
// placeholder could not be counted.
func TestPgToastRiskIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The tool's client is read-only; the fixture needs a writer.
	conn, err := pgx.Connect(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	table := testenv.Name("toast")
	qualified := "public." + table
	ident := pgx.Identifier{"public", table}.Sanitize()
	for _, stmt := range []string{
		"CREATE TABLE " + ident + " (id bigint PRIMARY KEY, status text, notes text)",
		// EXTERNAL: no compression, so a 5 KB value is moved out of line.
		"ALTER TABLE " + ident + " ALTER COLUMN notes SET STORAGE EXTERNAL",
		"ALTER TABLE " + ident + " REPLICA IDENTITY DEFAULT",
		"INSERT INTO " + ident + " SELECT g, 'new', repeat('x', 5000) FROM generate_series(1, 20) g",
		"ANALYZE " + ident,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP TABLE IF EXISTS "+ident)
	})

	db, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres: %v", err)
	}
	t.Cleanup(db.Close)
	p := owners.Pipeline{ID: "it", SourceTables: []string{qualified}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Src: db, Owners: &owners.Owners{Pipelines: []owners.Pipeline{p}}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tt, _ := reg.Get("pg_toast_risk")
	run := func() ToastRiskData {
		t.Helper()
		res, err := tt.Run(tools.WithPipeline(ctx, p), json.RawMessage(`{"pipeline_id":"it"}`))
		if err != nil {
			t.Fatalf("pg_toast_risk: %v", err)
		}
		t.Logf("summary:\n%s", res.Summary)
		return res.Data.(ToastRiskData)
	}

	data := run()
	if len(data.Risks) != 1 || data.Risks[0].Column != "notes" || data.Risks[0].ReplicaIdentity != "DEFAULT" || data.Risks[0].ToastBytes == 0 {
		t.Fatalf("risks = %+v, want notes on DEFAULT with out-of-line values", data.Risks)
	}
	if !strings.Contains(strings.Join(data.Notes, "\n"), noPlaceholderCount) {
		t.Errorf("notes = %v", data.Notes)
	}

	if _, err := conn.Exec(ctx, "ALTER TABLE "+ident+" REPLICA IDENTITY FULL"); err != nil {
		t.Fatalf("replica identity full: %v", err)
	}
	if data := run(); len(data.Risks) != 0 || data.Tables[0].ReplicaIdentity != "FULL" {
		t.Fatalf("after FULL: risks = %+v tables = %+v", data.Risks, data.Tables)
	}
}
