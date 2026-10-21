//go:build integration

package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// TestBatchScenariosAgainstProfile brings up the batch profile through testenv
// and runs each batch break and its fix against it. Each scenario runs break
// then fix, so a later scenario never has to succeed against the previous one's
// broken state (the DAG paused, a broken model still deployed). The room-opening
// side (devctl alert <scenario>) is covered by the unit tests; this checks the
// break/fix mechanics.
func TestBatchScenariosAgainstProfile(t *testing.T) {
	cfg := testenv.Require(t)
	if cfg.AirflowURL == "" {
		cfg.AirflowURL = "http://127.0.0.1:8080"
	}
	cleanup := testenv.StartBatchProfile(t, cfg)
	defer cleanup()

	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	s := &stack{
		root: root,
		cfg:  cfg,
		log:  slog.New(slog.NewTextHandler(os.Stderr, nil)),
		http: &http.Client{Timeout: 10 * time.Second},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Whatever a subtest did, leave the batch pipeline healthy for the devctl
	// health step that follows this package: the source-table variable back to
	// the real table, the broken model removed, and the DAG unpaused.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), time.Minute)
		defer ccancel()
		if _, err := s.airflowCLI(cctx, "variables", "set", batchSourceVar, batchSourceDefault); err != nil {
			t.Errorf("cleanup: restore %s: %v", batchSourceVar, err)
		}
		if err := os.Remove(filepath.Join(root, batchModelsRel, batchBrokenModel)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("cleanup: remove broken model: %v", err)
		}
		if _, err := s.airflowCLI(cctx, "dags", "unpause", batchDAG); err != nil {
			t.Errorf("cleanup: unpause %s: %v", batchDAG, err)
		}
	})

	for _, sc := range []struct {
		name string
		brk  func(context.Context, *stack) error
		fix  func(context.Context, *stack) error
	}{
		{"dag-failed", breakDagFailed, fixDagFailed},
		{"dbt-model-broken", breakDbtModelBroken, fixDbtModelBroken},
		{"late-batch", breakLateBatch, fixLateBatch},
	} {
		t.Run(sc.name, func(t *testing.T) {
			if err := sc.brk(ctx, s); err != nil {
				t.Fatalf("break: %v", err)
			}
			if err := sc.fix(ctx, s); err != nil {
				t.Fatalf("fix: %v", err)
			}
		})
	}
}
