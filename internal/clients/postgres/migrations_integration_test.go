//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/testenv"
)

// A Liquibase-shaped table is read; the same table seen by a role without
// SELECT on it is HistoryUnreadable, not an error (TC2).
func TestRecentMigrationsLiquibaseAndUnreadableIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	conn := writeConn(t, cfg.SourceDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema := testenv.Name("liquibase")
	ident := pgx.Identifier{schema}.Sanitize()
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP SCHEMA IF EXISTS "+ident+" CASCADE")
	})
	for _, stmt := range []string{
		"CREATE SCHEMA " + ident,
		"CREATE TABLE " + ident + ".orders (id bigint primary key)",
		// Liquibase 4's DDL for its changelog table.
		`CREATE TABLE ` + ident + `.databasechangelog (
			id varchar(255) NOT NULL, author varchar(255) NOT NULL, filename varchar(255) NOT NULL,
			dateexecuted timestamp NOT NULL, orderexecuted integer NOT NULL, exectype varchar(10) NOT NULL,
			md5sum varchar(35), description varchar(255), comments varchar(255), tag varchar(255),
			liquibase varchar(20), contexts varchar(255), labels varchar(255), deployment_id varchar(10))`,
		`INSERT INTO ` + ident + `.databasechangelog (id, author, filename, dateexecuted, orderexecuted, exectype, md5sum, description)
			VALUES ('orders-1', 'dana', 'db/changelog.xml', now() - interval '2 hours', 1, 'EXECUTED', '9:abc', 'createTable tableName=orders'),
			       ('orders-2', 'dana', 'db/changelog.xml', now() - interval '1 hour', 2, 'EXECUTED', '9:def', 'addColumn tableName=orders')`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt[:min(len(stmt), 40)], err)
		}
	}

	got, err := sourceDB(t).RecentMigrations(ctx, []string{schema}, time.Now().Add(-6*time.Hour), 200)
	if err != nil {
		t.Fatalf("RecentMigrations: %v", err)
	}
	if len(got.Migrations) != 2 || got.Migrations[0].Version != "orders-2" || got.Migrations[0].InstalledBy != "dana" || !got.Migrations[0].Success {
		t.Fatalf("migrations = %+v", got.Migrations)
	}

	// selectOnlyDB grants SELECT on the orders table only (and no USAGE on
	// the schema): the changelog is found in the catalog but not readable.
	ro := selectOnlyDB(t, ident+".orders")
	got, err = ro.RecentMigrations(ctx, []string{schema}, time.Now().Add(-6*time.Hour), 200)
	if err != nil {
		t.Fatalf("RecentMigrations as a role without SELECT: %v", err)
	}
	if len(got.Tables) != 1 || got.Tables[0].State != HistoryUnreadable || len(got.Migrations) != 0 {
		t.Fatalf("tables = %+v, migrations = %+v", got.Tables, got.Migrations)
	}
}
