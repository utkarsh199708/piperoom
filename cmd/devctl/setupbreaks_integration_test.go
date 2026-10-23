//go:build integration

package main

// The Linux integration test of U1-15: on the dev stack, each setup break
// makes its named check fail, and the recorded fix makes it pass again.
//
// The checks and the fixes are the ones setupbreaks.go defines; this test
// proves them against a real Connect worker, a real Postgres and real Docker.
// The "recorded agent" part is replayed as the scenario's fix op sequence —
// the model recording and its replay are U1-19/U1-20.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// setupBreakStack is the dev stack with the repo root, so the fixes can read
// dev/connectors. It skips like devStack when the stack is unreachable.
func setupBreakStack(t *testing.T) *stack {
	t.Helper()
	cfg := testenv.Require(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("%v", err)
	}
	return &stack{
		root: root,
		cfg:  cfg,
		http: &http.Client{Timeout: 10 * time.Second},
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestIntegrationSetupBreaks applies every setup break on the dev stack in
// turn: the named check passes, the break makes it fail, the fix makes it
// pass again. Any break still applied when the test ends is fixed in cleanup.
func TestIntegrationSetupBreaks(t *testing.T) {
	s := setupBreakStack(t)
	acquireStackLock(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	// The breaks are defined on the baseline stack (the connector on its dev
	// config). Another scenario's break in the way is someone else's run, not
	// a failure of this test.
	if st, err := s.connectorStatus(ctx); err != nil || st.Connector.State != "RUNNING" || len(st.Tasks) == 0 || st.Tasks[0].State != "RUNNING" {
		t.Skipf("the dev stack is not in its baseline state (connector %v, err %v); run `devctl fix <scenario>` first", st.Connector.State, err)
	}

	applied := map[string]bool{}
	t.Cleanup(func() {
		for name := range applied {
			cctx, stop := context.WithTimeout(context.Background(), 15*time.Minute) // a fix may wait out two Connect plugin scans
			if err := setupFixCmd(cctx, s, name); err != nil {
				t.Logf("cleanup: setup-fix %s: %v", name, err)
			}
			stop()
		}
	})

	for _, name := range integrationSetupBreaks() {
		name := name
		t.Run(name, func(t *testing.T) {
			sc := setupScenarios[name]
			if ok, detail, err := sc.verify(ctx, s); err != nil {
				t.Fatalf("check %s: %v", sc.check, err)
			} else if !ok {
				t.Fatalf("check %s fails on the baseline stack: %s", sc.check, detail)
			}

			applied[name] = true
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break %s: %v", name, err)
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil {
				t.Fatalf("check %s after the break: %v", sc.check, err)
			}
			if ok {
				t.Fatalf("check %s still passes after the break", sc.check)
			}
			t.Logf("broken: check %s fails: %s", sc.check, detail)

			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix %s: %v", name, err)
			}
			delete(applied, name)
			ok, detail, err = sc.verify(ctx, s)
			if err != nil {
				t.Fatalf("check %s after the fix: %v", sc.check, err)
			}
			if !ok {
				t.Fatalf("check %s still fails after the fix: %s", sc.check, detail)
			}
		})
	}
}

// sharedStackDisruptive are the breaks that restart a shared server or take
// the shared connector down; other integration packages run against the same
// Postgres, Connect worker and pg-orders-connector in parallel.
var sharedStackDisruptive = map[string]bool{
	"wal-level-replica":           true, // restarts Postgres
	"plugin-after-scan":           true, // restarts Connect twice, removes the plugin between
	"db-unreachable-from-connect": true, // restarts Connect
	"missing-grant":               true, // moves pg-orders-connector to a role whose task fails
}

// integrationSetupBreaks is the breaks this test may apply to the shared dev
// stack while the integration job runs packages in parallel. The others keep
// their break/fix pair covered against the fake stack (TestSetupBreakFixPair);
// their real-stack run belongs to the dedicated setup matrix job (U1-16),
// where the test owns the stack.
func integrationSetupBreaks() []string {
	out := make([]string, 0, len(setupScenarios))
	for _, name := range setupScenarioNamesFor("") {
		if sharedStackDisruptive[name] {
			continue
		}
		out = append(out, name)
	}
	return out
}
