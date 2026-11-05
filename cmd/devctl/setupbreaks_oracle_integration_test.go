//go:build integration

package main

// The integration test of U9c: on the `oracle` compose profile, each Oracle
// setup break makes the U9a client (internal/clients/oracle, over go-ora)
// report the failed prerequisite, and its fix makes the client report it
// healthy again. devctl's own checks (sqlplus in the container) are asserted
// alongside, so the two readings cannot drift.
//
//	docker compose -f dev/docker-compose.yml --profile oracle up -d --wait oracle
//	go test -tags integration ./cmd/devctl -run TestIntegrationOracleSetupBreaks

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/oracle"
	"piperoom/internal/testenv"
)

// u9aCheck reads the prerequisites through the U9a client and evaluates the
// scenario's named check on what it returns. A new connection per read: the
// archive log break restarts the instance under any open session.
func u9aCheck(ctx context.Context, t *testing.T, dsn, check string) (bool, string) {
	t.Helper()
	var c *oracle.Client
	var err error
	// The listener may still be registering the PDB right after a restart.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		c, err = oracle.New(ctx, dsn)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		t.Fatalf("U9a client: %v", err)
	}
	defer c.Close()
	tables := []string{oracleCapturedTable}
	switch check {
	case checkOracleArchiveLog:
		info, err := c.Database(ctx)
		if err != nil {
			t.Fatalf("U9a Database: %v", err)
		}
		return oracleArchiveLogCheck(info)
	case checkOracleSupplementalLogging:
		info, err := c.Database(ctx)
		if err != nil {
			t.Fatalf("U9a Database: %v", err)
		}
		tl, err := c.TableSupplementalLogging(ctx, tables)
		if err != nil {
			t.Fatalf("U9a TableSupplementalLogging: %v", err)
		}
		return oracleSupplementalCheck(info, tl)
	default:
		g, err := c.LogMinerGrants(ctx, oracleLogMinerUser, tables)
		if err != nil {
			t.Fatalf("U9a LogMinerGrants: %v", err)
		}
		// LogMinerGrants does not report existence; a user with no
		// privilege and no role reads as missing.
		return oracleGrantsCheck(len(g.SysPrivs)+len(g.Roles) > 0, g, tables)
	}
}

func TestIntegrationOracleSetupBreaks(t *testing.T) {
	dsn := testenv.RequireOracle(t)
	acquireStackLock(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	s := &stack{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	want := map[string]string{
		"no-archivelog":           "LOG_MODE=NOARCHIVELOG",
		"no-supplemental-logging": "ALL COLUMNS supplemental log group on PIPEROOM.ORDERS",
		"missing-logminer-grant":  "missing LOGMINING",
	}
	for _, name := range setupScenarioNamesFor(oracleProfile) {
		t.Run(name, func(t *testing.T) {
			sc := setupScenarios[name]
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				// The profile is up (RequireOracle reached it) but not healthy:
				// a real failure, not a missing profile.
				t.Fatalf("the oracle profile is not in its baseline state for %s (%s, %v); run `devctl setup-fix %s`", sc.check, detail, err, name)
			}
			if ok, detail := u9aCheck(ctx, t, dsn, sc.check); !ok {
				t.Fatalf("U9a reads %s as failing on the healthy profile: %s", sc.check, detail)
			}

			t.Cleanup(func() {
				cctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
				defer stop()
				if err := setupFixCmd(cctx, s, name); err != nil {
					t.Errorf("cleanup: setup-fix %s: %v", name, err)
				}
			})
			start := time.Now()
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break %s: %v", name, err)
			}
			t.Logf("break %s: %s", name, time.Since(start).Round(time.Second))
			ok, detail := u9aCheck(ctx, t, dsn, sc.check)
			if ok {
				t.Fatalf("U9a still reads %s as passing after the break", sc.check)
			}
			if !strings.Contains(detail, want[name]) {
				t.Errorf("U9a detail %q does not contain %q", detail, want[name])
			}
			if _, devctlDetail, err := sc.verify(ctx, s); err != nil || devctlDetail != detail {
				t.Errorf("devctl reads %q (err %v), U9a reads %q", devctlDetail, err, detail)
			}

			start = time.Now()
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix %s: %v", name, err)
			}
			t.Logf("fix %s: %s", name, time.Since(start).Round(time.Second))
			if ok, detail := u9aCheck(ctx, t, dsn, sc.check); !ok {
				t.Fatalf("U9a reads %s as failing after the fix: %s", sc.check, detail)
			}
		})
	}
}
