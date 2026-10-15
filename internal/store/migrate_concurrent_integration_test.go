//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/config"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// Replicas that start together on a fresh schema all migrate under the
// migrations lock (cmd/server: store.WithMigrationLock around migrations.Up):
// every one succeeds and the client_id index is valid. A migration that waits
// for older snapshots (CREATE INDEX CONCURRENTLY) would wait for the replicas
// blocked in pg_advisory_lock and never finish.
func TestMigrateReplicasTogether(t *testing.T) {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("store: room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("schema")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("store: cannot create schema: %v", err)
	}
	t.Cleanup(func() { dropSchema(cfg.RoomDBURL, schema) })
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema

	const replicas = 3
	errs := make([]error, replicas)
	var wg sync.WaitGroup
	for i := range replicas {
		db := stdlib.OpenDB(*cc)
		t.Cleanup(func() { _ = db.Close() })
		wg.Add(1)
		go func() {
			defer wg.Done()
			mctx, mcancel := context.WithTimeout(ctx, 30*time.Second)
			defer mcancel()
			errs[i] = store.WithMigrationLock(mctx, db, func() error {
				_, err := migrations.Up(mctx, db)
				return err
			})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d: migrate: %v", i, err)
		}
	}
	var valid bool
	err = conn.QueryRow(ctx, `SELECT i.indisvalid FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, schema, store.ClientIDIndex).Scan(&valid)
	if err != nil {
		t.Fatalf("look up %s: %v", store.ClientIDIndex, err)
	}
	if !valid {
		t.Fatalf("%s is INVALID", store.ClientIDIndex)
	}
}
