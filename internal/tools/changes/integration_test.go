//go:build integration

package changes

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TC3: an it_ schema with a Flyway-shaped table and three rows in the window
// (and one older) gives three migration changes, newest first.
func TestWhatChangedFlywayIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("write connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	schema := testenv.Name("flyway")
	ident := pgx.Identifier{schema}.Sanitize()
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP SCHEMA IF EXISTS "+ident+" CASCADE")
	})
	// Flyway 10's DDL for its history table.
	for _, stmt := range []string{
		"CREATE SCHEMA " + ident,
		"CREATE TABLE " + ident + ".orders (id bigint primary key)",
		`CREATE TABLE ` + ident + `.flyway_schema_history (
			installed_rank integer PRIMARY KEY, version varchar(50), description varchar(200) NOT NULL,
			type varchar(20) NOT NULL, script varchar(1000) NOT NULL, checksum integer,
			installed_by varchar(100) NOT NULL, installed_on timestamp NOT NULL DEFAULT now(),
			execution_time integer NOT NULL, success boolean NOT NULL)`,
		`INSERT INTO ` + ident + `.flyway_schema_history VALUES
			(1, '1', 'init', 'SQL', 'V1__init.sql', 1, 'deployer', now() - interval '3 days', 10, true),
			(2, '2', 'add amount', 'SQL', 'V2__add_amount.sql', 2, 'deployer', now() - interval '3 hours', 10, true),
			(3, '3', 'widen amount', 'SQL', 'V3__widen.sql', 3, 'deployer', now() - interval '2 hours', 10, true),
			(4, '4', 'add index', 'SQL', 'V4__index.sql', 4, 'ci', now() - interval '1 hour', 10, false)`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt[:min(len(stmt), 40)], err)
		}
	}

	db, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres: %v", err)
	}
	t.Cleanup(db.Close)
	tl := New(Deps{Src: db})
	sctx := tools.WithPipeline(ctx, owners.Pipeline{ID: "it", TopicPrefix: "it", SourceTables: []string{schema + ".orders"}})
	res, err := tl.Run(sctx, json.RawMessage(`{"window":"6h","kinds":["migration"]}`))
	if err != nil {
		t.Fatalf("what_changed: %v", err)
	}
	d := res.Data.(Data)
	if len(d.Changes) != 3 {
		t.Fatalf("changes = %d, want 3: %s", len(d.Changes), res.Summary)
	}
	for i, wantVersion := range []string{"4", "3", "2"} {
		c := d.Changes[i]
		if c.Kind != clients.ChangeMigration || c.Source != clients.ChangeSourceFlyway || c.Object != schema+".flyway_schema_history" || migrationVersion(c) != wantVersion {
			t.Errorf("change %d = %+v", i, c)
		}
	}
	if d.Changes[0].Summary != "FAILED add index" || d.Changes[0].Actor != "ci" {
		t.Errorf("newest = %+v", d.Changes[0])
	}
}
