//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/integrations/spec"
)

// TestIntegrationsSuitePostgres runs the shared spec.Store suite against
// Postgres. requireStore migrates a fresh schema that is dropped on cleanup,
// and every subtest uses its own it_<n> workspace and ids.
func TestIntegrationsSuitePostgres(t *testing.T) {
	s, _ := requireStore(t)
	ints := s.Integrations()
	runIntegrationsSuite(t, func(t *testing.T) spec.Store { return ints })
}

// The partial indexes in the migration are what enforce UniqueKey on
// Postgres; this checks a raw insert that skips the store is refused too.
func TestIntegrationsUniqueIndexesOnPostgres(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ints := s.Integrations()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connCfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	connCfg.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	ids := newSuiteIDs()
	mustCreate(t, ints, baseRec(ids.ws("a"), ids.id("k1"), spec.CategoryKafka, "kafka-generic"))
	_, err = conn.Exec(ctx,
		`INSERT INTO integrations (workspace_id, id, category, kind, updated_by) VALUES ($1, $2, 'kafka', 'x', 'u')`,
		ids.ws("a"), ids.id("raw"))
	if err == nil {
		t.Fatal("raw second kafka row accepted; want unique violation")
	}
	if _, err := conn.Exec(ctx,
		`INSERT INTO integration_audit (workspace_id, integration, action, actor) VALUES ($1, $2, 'exploded', 'u')`,
		ids.ws("a"), ids.id("k1")); err == nil {
		t.Fatal("unknown audit action accepted by the CHECK constraint")
	}
}

// A cancelled context surfaces as an error, not as ErrNotFound.
func TestIntegrationsCancelledContextOnPostgres(t *testing.T) {
	s, _ := requireStore(t)
	ints := s.Integrations()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ids := newSuiteIDs()
	if _, err := ints.Get(ctx, ids.ws("a"), ids.id("x")); err == nil || errors.Is(err, spec.ErrNotFound) {
		t.Fatalf("get with cancelled ctx: %v, want a context error", err)
	}
	if _, err := ints.List(ctx, ids.ws("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("list with cancelled ctx: %v, want context.Canceled", err)
	}
}
