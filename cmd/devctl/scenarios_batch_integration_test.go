//go:build integration

package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// TestBatchScenariosAgainstProfile brings up the batch profile through testenv
// and runs each batch break and its fix against it. Each break mutates the
// batch pipeline (dag-failed renames the dbt source, dbt-model-broken deploys a
// broken model, late-batch pauses the DAG) and each fix restores it, so the
// three can run in one session. The room-opening side (devctl alert <scenario>)
// is covered by the unit tests; this checks the break/fix mechanics.
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

	for _, sc := range []struct {
		name string
		run  func(context.Context, *stack) error
	}{
		{"dag-failed", breakDagFailed},
		{"dbt-model-broken", breakDbtModelBroken},
		{"late-batch", breakLateBatch},
	} {
		t.Run(sc.name, func(t *testing.T) {
			if err := sc.run(ctx, s); err != nil {
				t.Fatalf("break: %v", err)
			}
		})
	}
	for _, sc := range []struct {
		name string
		run  func(context.Context, *stack) error
	}{
		{"dag-failed", fixDagFailed},
		{"dbt-model-broken", fixDbtModelBroken},
		{"late-batch", fixLateBatch},
	} {
		t.Run(sc.name, func(t *testing.T) {
			if err := sc.run(ctx, s); err != nil {
				t.Fatalf("fix: %v", err)
			}
		})
	}
}
