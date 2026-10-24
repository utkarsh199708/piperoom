//go:build integration

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// TestDiscoverDevStack runs discovery against the dev stack
// (go run ./cmd/devctl up) and checks the orders pipeline it seeds.
func TestDiscoverDevStack(t *testing.T) {
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

// TestWriteOwnersDevStack runs the whole command (-write-owners, -users,
// -owner) against the dev stack and loads both files with the loaders the
// server uses. The dev stack has no sink connector (cmd/devconsumer fills the
// warehouse), so the pipeline's sink table must be reported as missing, not
// guessed.
func TestWriteOwnersDevStack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dir := t.TempDir()
	ownersPath, usersPath := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
	var stdout, stderr bytes.Buffer
	args := []string{"-write-owners", ownersPath, "-users", usersPath, "-owner", "it_discover"}
	if code := run(ctx, args, &stdout, &stderr); code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr.String())
	}

	own, err := owners.Load(ownersPath)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	p, ok := own.Pipeline("pg-orders")
	if !ok {
		t.Fatalf("no pg-orders pipeline in %+v", own.Pipelines)
	}
	if !slices.Contains(p.SourceTables, "public.orders") || !slices.Contains(p.Connectors, "pg-orders-connector") {
		t.Errorf("pg-orders = %+v", p)
	}
	if len(p.SinkTables) != 0 {
		t.Errorf("sink_tables = %v, want empty: the column is not discoverable", p.SinkTables)
	}
	want := owners.Principal{Kind: owners.KindUser, Name: "it_discover"}
	for _, layer := range cdcLayers() {
		if !own.Layers[layer].Contains(want) {
			t.Errorf("layer %s = %v, want user:it_discover", layer, own.Layers[layer])
		}
	}

	users, err := room.LoadUsers(usersPath)
	if err != nil {
		t.Fatalf("room.LoadUsers: %v", err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		t.Fatalf("room.ValidateUsers: %v", err)
	}

	table := stdout.String()
	if !strings.Contains(table, "pg-orders") || !strings.Contains(table, "MISSING") {
		t.Errorf("summary table lacks pg-orders / MISSING:\n%s", table)
	}
}

// TestDiscoverDevStackMySQL runs discovery against the dev stack with the
// mysql profile (dev/scenarios.md: docker compose -f dev/docker-compose.yml
// --profile mysql up -d --wait mysql; any MySQL `devctl break` registers
// orders-mysql) and checks that the drafted pipeline loads with owners.Load
// as source_kind: mysql, its mysql layer owned (Q8h).
func TestDiscoverDevStackMySQL(t *testing.T) {
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
		if res.Pipelines[i].Connector == "orders-mysql" {
			p = &res.Pipelines[i]
		}
	}
	if p == nil {
		t.Skipf("no orders-mysql connector: start the mysql profile (dev/scenarios.md); got %+v", res.Pipelines)
	}
	if p.SourceKind != "mysql" || p.TopicPrefix != "shop" {
		t.Errorf("orders-mysql = %+v, want source_kind mysql, topic_prefix shop", p)
	}
	for _, table := range []string{"shop.orders", "shop.order_items"} {
		if !slices.Contains(p.SourceTables, table) {
			t.Errorf("SourceTables = %v, want %s", p.SourceTables, table)
		}
	}

	var buf bytes.Buffer
	if err := discover.RenderOwned(&buf, res, "it_discover"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v\n%s", err, buf.String())
	}
	op, ok := own.Pipeline(p.ID)
	if !ok || !op.IsMySQL() {
		t.Fatalf("pipeline %s = %+v (found %v), want source_kind mysql", p.ID, op, ok)
	}
	if _, ok := own.CanApprove(p.ID, owners.LayerMySQL, "it_discover", nil); !ok {
		t.Errorf("it_discover cannot approve the mysql layer of %s", p.ID)
	}
}
