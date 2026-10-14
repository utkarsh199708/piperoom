//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

func TestQueryTextIntegration(t *testing.T) {
	cfg := testenv.RequirePostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := queryText(ctx, cfg.SourceDBURL, "SELECT $1::text", "it_devctl")
	if err != nil || got != "it_devctl" {
		t.Fatalf("queryText = %q, %v", got, err)
	}
	got, err = queryText(ctx, cfg.SourceDBURL, "SELECT 'x' WHERE false")
	if err != nil || got != "" {
		t.Fatalf("no row = %q, %v; want empty", got, err)
	}
	got, err = queryText(ctx, cfg.SourceDBURL, "SELECT NULL::text")
	if err != nil || got != "" {
		t.Fatalf("NULL = %q, %v; want empty", got, err)
	}
	// The real health query against the configured source.
	s := &stack{cfg: cfg}
	if _, err := s.slot(ctx); err != nil {
		t.Fatalf("slot: %v", err)
	}
	if _, err := s.unsynced(ctx, time.Time{}); err != nil {
		t.Fatalf("unsynced: %v", err)
	}
}
