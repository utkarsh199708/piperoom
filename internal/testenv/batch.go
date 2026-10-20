//go:build integration

package testenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/airflow"
	"piperoom/internal/clients/dbt"
	"piperoom/internal/config"
)

const (
	batchAirflowURL = "http://127.0.0.1:8080" // dev/docker-compose.yml, service airflow
	batchDAG        = "revenue_daily"
	// Whole test: a cold build installs dbt with pip before Airflow starts.
	batchTimeout = 8 * time.Minute
)

// StartBatchProfile starts the `batch` compose profile's airflow service next
// to the running dev stack, waits for it to become healthy and for one
// successful revenue_daily run, and returns a cleanup that stops and removes
// the service and deletes the dbt artifacts and models it created (the base
// stack stays up). It is the testenv hook the batch scenarios' integration
// tests use to bring the profile up, the counterpart of TestBatchProfile's
// step-by-step assertions.
func StartBatchProfile(t *testing.T, cfg config.Config) func() {
	t.Helper()
	root := repoRoot(t)
	artifacts := filepath.Join(root, "dev", "batch", "artifacts")
	compose := batchCompose(root)

	ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)

	// Rows in the source table first: fct_revenue groups analytics.orders, so
	// an empty source would build an empty (but successful) table.
	waitSourceRows(ctx, t, cfg.WarehouseDBURL)
	// Old artifacts would pass the file checks without this run writing them.
	for _, f := range []string{dbt.RunResultsFile, dbt.ManifestFile} {
		if err := os.Remove(filepath.Join(artifacts, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("remove old %s: %v", f, err)
		}
	}

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if err := dropBatchModels(cctx, cfg.WarehouseDBURL); err != nil {
			t.Errorf("cleanup: %v", err)
		}
		for _, f := range []string{dbt.RunResultsFile, dbt.ManifestFile} {
			if err := os.Remove(filepath.Join(artifacts, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("cleanup: remove %s: %v", f, err)
			}
		}
	})
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer ccancel()
		if out, err := compose(cctx, "rm", "--stop", "--force", "airflow"); err != nil {
			t.Errorf("remove airflow service: %v\n%s", err, out)
		}
	})

	// --force-recreate: a fresh container has a fresh SQLite metadata DB, so the
	// run checked below is this test's run, not one left by an earlier test.
	if out, err := compose(ctx, "up", "-d", "--build", "--force-recreate", "airflow"); err != nil {
		t.Fatalf("compose up airflow: %v\n%s", err, out)
	}
	waitHealthy(ctx, t, compose)

	client, err := airflow.New(batchAirflowURL) // no login option: none is needed
	if err != nil {
		t.Fatalf("airflow.New: %v", err)
	}
	run := waitRunSuccess(ctx, t, client)
	t.Logf("batch profile: first successful %s run %s (%s)", batchDAG, run.ID, run.RunType)
	return cancel
}

// batchCompose returns a compose runner for the batch profile.
func batchCompose(root string) func(context.Context, ...string) (string, error) {
	return func(ctx context.Context, args ...string) (string, error) {
		full := append([]string{"compose", "-f", filepath.Join(root, "dev", "docker-compose.yml"), "--profile", "batch"}, args...)
		cmd := exec.CommandContext(ctx, "docker", full...)
		cmd.Env = os.Environ()
		if runtime.GOOS == "linux" {
			// The image's airflow user (50000) cannot write a bind mount owned by the runner.
			cmd.Env = append(cmd.Env, "AIRFLOW_UID="+strconv.Itoa(os.Getuid()))
		}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
}

// repoRoot walks up from the package directory to the directory holding go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// poll calls cond every interval until it reports done, fails, or ctx ends.
func poll(ctx context.Context, t *testing.T, what string, interval time.Duration, cond func() (bool, error)) {
	t.Helper()
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: gave up: %v", what, ctx.Err())
		case <-time.After(interval):
		}
	}
}

func waitSourceRows(ctx context.Context, t *testing.T, dsn string) {
	t.Helper()
	wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	poll(wctx, t, "rows in analytics.orders (the dbt source; loaded by devconsumer)", 2*time.Second, func() (bool, error) {
		n, err := countRows(wctx, dsn, "SELECT count(*) FROM analytics.orders")
		return n > 0, err
	})
}

func countRows(ctx context.Context, dsn, query string) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(qctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("connect warehouse: %w", err)
	}
	defer conn.Close(context.Background())
	var n int64
	if err := conn.QueryRow(qctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("%s: %w", query, err)
	}
	return n, nil
}

// waitHealthy waits for the compose healthcheck (the API answers for the DAG
// without login), as `docker inspect` reports it.
func waitHealthy(ctx context.Context, t *testing.T, compose func(context.Context, ...string) (string, error)) {
	t.Helper()
	out, err := compose(ctx, "ps", "-q", "airflow")
	id := strings.TrimSpace(out)
	if err != nil || id == "" {
		t.Fatalf("compose ps airflow: %v\n%s", err, out)
	}
	poll(ctx, t, "airflow healthcheck", 2*time.Second, func() (bool, error) {
		ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		b, err := exec.CommandContext(ictx, "docker", "inspect", "--format", "{{.State.Status}} {{.State.Health.Status}}", id).CombinedOutput()
		if err != nil {
			return false, fmt.Errorf("docker inspect: %w: %s", err, b)
		}
		state := strings.TrimSpace(string(b))
		switch {
		case state == "running healthy":
			return true, nil
		case strings.HasPrefix(state, "exited"), strings.HasPrefix(state, "dead"), strings.HasSuffix(state, "unhealthy"):
			// The healthcheck's own output says why (status code, connection refused).
			hl, _ := exec.CommandContext(ictx, "docker", "inspect", "--format",
				"{{range .State.Health.Log}}exit {{.ExitCode}}: {{.Output}}\n{{end}}", id).CombinedOutput()
			return false, fmt.Errorf("container %s; healthcheck log:\n%s", state, hl)
		}
		return false, nil
	})
}

// dropBatchModels removes the two relations dbt built in the shared warehouse.
// fct_revenue is a table, not a view over stg_orders, so the order is free.
func dropBatchModels(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect warehouse: %w", err)
	}
	defer conn.Close(context.Background())
	for _, q := range []string{
		"DROP TABLE IF EXISTS analytics.fct_revenue",
		"DROP VIEW IF EXISTS analytics.stg_orders",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

// waitRunSuccess waits for a revenue_daily run to succeed: the scheduler makes
// one at start (catchup=False). The API is read-only for anonymous callers
// (Viewer), so the test cannot trigger one and does not need to. A failed run
// fails the test with each task's state and the failed task's log tail.
func waitRunSuccess(ctx context.Context, t *testing.T, c *airflow.Client) airflow.DAGRun {
	t.Helper()
	var ok airflow.DAGRun
	poll(ctx, t, "a successful "+batchDAG+" run", 3*time.Second, func() (bool, error) {
		runs, err := c.DAGRuns(ctx, batchDAG, 5)
		if err != nil {
			return false, fmt.Errorf("DAGRuns: %w", err)
		}
		for _, r := range runs {
			switch r.State {
			case "success":
				ok = r
				return true, nil
			case "failed":
				return false, fmt.Errorf("run %s failed:\n%s", r.ID, describeRun(ctx, c, r.ID))
			}
		}
		return false, nil
	})
	return ok
}

func describeRun(ctx context.Context, c *airflow.Client, runID string) string {
	tis, err := c.TaskInstances(ctx, batchDAG, runID)
	if err != nil {
		return "task instances: " + err.Error()
	}
	var b strings.Builder
	for _, ti := range tis {
		fmt.Fprintf(&b, "  %s: %s (try %d)\n", ti.TaskID, ti.State, ti.Try)
		if ti.State == "failed" && ti.Try > 0 {
			tail, err := c.TaskLogTail(ctx, batchDAG, runID, ti.TaskID, ti.Try, 8<<10)
			if err != nil {
				tail = "log: " + err.Error()
			}
			fmt.Fprintf(&b, "  --- %s log tail ---\n%s\n", ti.TaskID, tail)
		}
	}
	return b.String()
}
