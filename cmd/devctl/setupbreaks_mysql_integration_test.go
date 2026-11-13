//go:build integration

package main

// The MySQL setup breaks on the real mysql profile (U2b): each break makes the
// U2a driver's own named check fail — read through the MySQL client the setup
// run uses, as the profile's read user — and its fix makes it pass again.
// Start the profile first:
//
//	docker compose -f dev/docker-compose.yml --profile mysql up -d --wait mysql
//
// The test skips when the profile is not reachable, like the other integration
// tests skip without the dev stack, and it leaves the profile as the fixes
// leave it: the command-line baseline.

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	mysqlclient "piperoom/internal/clients/mysql"
	mysqldriver "piperoom/internal/setup/sources/mysql"
)

// mysqlDevReadURL is the profile's read user (dev-only credentials, AGENTS.md
// §3; dev/mysql/init/01-shop.sql). It is fixed: the breaks run mysql inside
// the profile's container (mysqlContainer), so the client must read that same
// server.
const mysqlDevReadURL = "mysql://piperoom:piperoom-dev@localhost:13306/shop"

func TestIntegrationMySQLSetupBreaks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := mysqlclient.New(ctx, mysqlDevReadURL)
	if err != nil {
		t.Skipf("mysql profile not reachable (%s): %v", mysqlUpHint, err)
	}
	t.Cleanup(db.Close)
	s := &stack{log: slog.New(slog.NewTextHandler(io.Discard, nil)), poll: 200 * time.Millisecond}

	// The setup run's driver for the dev connector user and tables.
	d := mysqldriver.Driver{Role: "debezium", Tables: []string{"shop.orders", "shop.order_items"}}
	checkOf := func(id string) bool {
		t.Helper()
		results, err := d.Run(ctx, db)
		if err != nil {
			t.Fatalf("driver Run: %v", err)
		}
		for _, r := range results {
			if r.Check.ID == id {
				if r.Err != nil {
					t.Fatalf("%s: read error: %v", id, r.Err)
				}
				return r.OK
			}
		}
		t.Fatalf("the driver has no check %s", id)
		return false
	}

	for _, name := range []string{"binlog-statement", "no-replication-grant", "server-id-zero"} {
		sc := setupScenarios[name]
		t.Run(name, func(t *testing.T) {
			if !checkOf(sc.check) {
				t.Fatalf("%s fails before the break; run devctl setup-fix %s or restart the profile", sc.check, name)
			}
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break: %v", err)
			}
			t.Cleanup(func() {
				if err := setupFixCmd(context.Background(), s, name); err != nil {
					t.Errorf("cleanup setup-fix: %v", err)
				}
			})
			if checkOf(sc.check) {
				t.Fatalf("the driver's %s check passes after the break", sc.check)
			}
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix: %v", err)
			}
			if !checkOf(sc.check) {
				t.Fatalf("the driver's %s check still fails after the fix", sc.check)
			}
		})
	}
}
