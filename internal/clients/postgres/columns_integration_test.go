//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestColumnsIntegration (X7a E7): the catalog read lists a table's columns in
// ordinal order with their type names, skips a dropped column, and refuses a
// table outside the pipeline; an unknown table is ErrNotFound.
func TestColumnsIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table := testenv.Name("columns")
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id bigint primary key, gone int, amount numeric(10,2), note varchar(40), updated_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP TABLE IF EXISTS public."+table)
	})
	if _, err := conn.Exec(ctx, "ALTER TABLE public."+table+" DROP COLUMN gone"); err != nil {
		t.Fatalf("drop column: %v", err)
	}

	sctx := tools.WithPipeline(ctx, owners.Pipeline{ID: "it", SourceTables: []string{"public." + table, "public." + table + "_missing"}})
	got, err := db.Columns(sctx, "public."+table)
	if err != nil {
		t.Fatal(err)
	}
	want := []clients.ColumnInfo{
		{Name: "id", Type: "bigint", Position: 1},
		{Name: "amount", Type: "numeric", Position: 3, Nullable: true},
		{Name: "note", Type: "character varying", Position: 4, Nullable: true},
		{Name: "updated_at", Type: "timestamp with time zone", Position: 5},
	}
	if len(got) != len(want) {
		t.Fatalf("Columns = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("column %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if _, err := db.Columns(sctx, "public."+table+"_missing"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: %v", err)
	}
	if _, err := db.Columns(sctx, "public.orders"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope: %v", err)
	}
}
