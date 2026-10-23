//go:build integration

package main

// The real run of the MongoDB setup breaks (U7c): on the `mongo` compose
// profile, each break makes its named check of internal/clients/mongodb fail,
// and its fix makes it pass again. It skips when the profile is not up, so the
// integration suite stays green without it.

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestIntegrationMongoSetupBreaks(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	s := &stack{root: root, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Both checks pass on the profile as it starts; anything else is a
	// profile that is down or someone else's break.
	for _, sc := range mongoSetupScenarios {
		ok, detail, err := sc.verify(ctx, s)
		if err != nil {
			t.Skipf("the mongo profile is not reachable (docker compose -f dev/docker-compose.yml --profile mongo up -d --wait mongo): %v", err)
		}
		if !ok {
			t.Skipf("the mongo profile is not in its baseline state (%s: %s); run `devctl setup-fix` first", sc.check, detail)
		}
	}
	acquireStackLock(t)

	for _, name := range []string{"no-oplog-role", "standalone-mongod"} {
		t.Run(name, func(t *testing.T) {
			sc := mongoSetupScenarios[name]
			t.Cleanup(func() {
				cctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
				defer stop()
				if err := setupFixCmd(cctx, s, name); err != nil {
					t.Errorf("cleanup fix %s: %v", name, err)
				}
			})
			start := time.Now()
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break %s: %v", name, err)
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil || ok {
				t.Fatalf("check %s after the break: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			t.Logf("break %s: %s, check %s fails: %s", name, time.Since(start).Round(time.Millisecond), sc.check, detail)

			start = time.Now()
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix %s: %v", name, err)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check %s after the fix: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			t.Logf("fix %s: %s", name, time.Since(start).Round(time.Millisecond))
		})
	}
}
