//go:build integration

package testenv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/airflow"
	"piperoom/internal/clients/dbt"
)

// TestBatchProfile starts the `batch` profile's airflow service next to the
// running dev stack and checks the P4g-1 Done-when items at runtime: the
// service turns healthy, its REST API v1 answers without login, one
// revenue_daily run succeeds, analytics.fct_revenue has rows, and dbt left
// run_results.json and manifest.json in dev/batch/artifacts. Only the airflow
// service is stopped and removed afterwards; the base stack stays up.
func TestBatchProfile(t *testing.T) {
	cfg := Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)
	defer cancel()

	root := repoRoot(t)
	artifacts := filepath.Join(root, "dev", "batch", "artifacts")
	compose := batchCompose(root)

	// Rows in the source table first: fct_revenue groups analytics.orders, so
	// an empty source would build an empty (but successful) table.
	waitSourceRows(ctx, t, cfg.WarehouseDBURL)

	// Old artifacts would pass the file checks without this run writing them.
	for _, f := range []string{dbt.RunResultsFile, dbt.ManifestFile} {
		if err := os.Remove(filepath.Join(artifacts, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("remove old %s: %v", f, err)
		}
	}

	// Cleanups run last-registered first: logs (on failure), then the airflow
	// service, then what the run left in the shared warehouse and artifacts dir
	// (after the service is gone, so the scheduler cannot rebuild them).
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
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		out, _ := compose(cctx, "logs", "--no-color", "--tail", "300", "airflow")
		t.Logf("airflow logs (tail):\n%s", out)
	})

	start := time.Now()
	// --force-recreate: a fresh container has a fresh SQLite metadata DB, so the
	// run checked below is this test's run, not one left by an earlier test.
	if out, err := compose(ctx, "up", "-d", "--build", "--force-recreate", "airflow"); err != nil {
		t.Fatalf("compose up airflow: %v\n%s", err, out)
	}
	upDone := time.Now()
	// Logged before the health wait so CI shows the build cost even on a failure.
	t.Logf("batch profile: compose up --build airflow took %s (image build included)", upDone.Sub(start).Round(time.Second))

	waitHealthy(ctx, t, compose)
	healthy := time.Now()
	t.Logf("batch profile: healthy %s after up returned, %s after start",
		healthy.Sub(upDone).Round(time.Second), healthy.Sub(start).Round(time.Second))

	assertNoLogin(ctx, t)

	client, err := airflow.New(batchAirflowURL) // no login option: none is needed
	if err != nil {
		t.Fatalf("airflow.New: %v", err)
	}
	dag, err := client.DAG(ctx, batchDAG)
	if err != nil {
		t.Fatalf("DAG %s: %v", batchDAG, err)
	}
	if dag.Paused {
		t.Fatalf("DAG %s is paused: AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION should be false", batchDAG)
	}

	run := waitRunSuccess(ctx, t, client)
	done := time.Now()
	t.Logf("batch profile: first successful %s run %s (%s) %s after healthy, %s after start",
		batchDAG, run.ID, run.RunType, done.Sub(healthy).Round(time.Second), done.Sub(start).Round(time.Second))

	assertFctRevenueRows(ctx, t, cfg.WarehouseDBURL)
	assertArtifacts(ctx, t, artifacts)
}

// assertNoLogin calls the API with no Authorization header: 200 means the
// no-login backend is on; 401/403 means the client could never read it over http.
func assertNoLogin(ctx context.Context, t *testing.T) {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, batchAirflowURL+"/api/v1/dags/"+batchDAG, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/dags/%s: %v", batchDAG, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/dags/%s without login: status %d, want 200 (AIRFLOW__API__AUTH_BACKENDS and AUTH_ROLE_PUBLIC in webserver_config.py)", batchDAG, resp.StatusCode)
	}
}

func assertFctRevenueRows(ctx context.Context, t *testing.T, dsn string) {
	t.Helper()
	n, err := countRows(ctx, dsn, "SELECT count(*) FROM analytics.fct_revenue")
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("analytics.fct_revenue has no rows after a successful run")
	}
	t.Logf("analytics.fct_revenue: %d rows", n)
}

// assertArtifacts checks both files parse as JSON and that the server's own
// reader (DBT_ARTIFACTS_DIR) accepts them: schema versions and the two models.
func assertArtifacts(ctx context.Context, t *testing.T, dir string) {
	t.Helper()
	for _, f := range []string{dbt.RunResultsFile, dbt.ManifestFile} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatalf("artifact %s: %v", f, err)
		}
		var v map[string]any
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatalf("artifact %s does not parse: %v", f, err)
		}
	}
	r, err := dbt.NewDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	rr, err := r.RunResults(ctx)
	if err != nil {
		t.Fatalf("dbt reader RunResults: %v", err)
	}
	status := map[string]string{}
	for _, m := range rr.Results {
		status[m.Model] = m.Status
	}
	for _, m := range []string{"model.analytics.stg_orders", "model.analytics.fct_revenue"} {
		if status[m] != "success" {
			t.Errorf("run_results.json: %s status %q, want success (all: %v)", m, status[m], status)
		}
	}
	lin, err := r.ModelLineage(ctx, "fct_revenue")
	if err != nil {
		t.Fatalf("dbt reader ModelLineage(fct_revenue): %v", err)
	}
	if !strings.Contains(strings.Join(lin.Parents, " "), "model.analytics.stg_orders") {
		t.Errorf("fct_revenue parents = %v, want model.analytics.stg_orders", lin.Parents)
	}
}
