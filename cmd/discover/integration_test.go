//go:build integration

package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/testenv"
)

// TestDiscoverDevStack runs discovery against the dev stack
// (go run ./cmd/devctl up) and checks the orders pipeline it seeds.
func TestDiscoverDevStack(t *testing.T) {
	testenv.RequireConnect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	deps, closeDeps, err := buildDeps(ctx, config.Load(), false)
	if err != nil {
		t.Fatalf("build clients: %v", err)
	}
	defer closeDeps()

	res, err := discover.Discover(ctx, deps, discover.Options{})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	var p *discover.Pipeline
	for i := range res.Pipelines {
		if res.Pipelines[i].Connector == "pg-orders-connector" {
			p = &res.Pipelines[i]
			break
		}
	}
	if p == nil {
		t.Fatalf("no pipeline for pg-orders-connector; got %+v (skipped %+v)", res.Pipelines, res.Skipped)
	}
	if p.TopicPrefix != "pg" {
		t.Errorf("TopicPrefix = %q, want pg", p.TopicPrefix)
	}
	if !slices.Contains(p.SourceTables, "public.orders") {
		t.Errorf("SourceTables = %v, want public.orders", p.SourceTables)
	}
	if !slices.Contains(p.ConsumerGroups, "order-sync") {
		t.Errorf("ConsumerGroups = %v, want order-sync", p.ConsumerGroups)
	}
}
