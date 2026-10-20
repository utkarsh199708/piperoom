package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// batchAirflow plays the Airflow REST API v1 for the batch scenarios: one DAG
// run whose state the test sets, and the DAG's paused flag.
type batchAirflow struct {
	mu     sync.Mutex
	state  string // the state every dagRuns reply reports
	paused bool
}

func (a *batchAirflow) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case r.URL.Path == "/api/v1/dags/revenue_daily":
		writeJSON(w, http.StatusOK, fmt.Sprintf(
			`{"dag_id":"revenue_daily","is_paused":%t,"schedule_interval":{"__type":"CronExpression","value":"@daily"},"next_dagrun_create_after":%q}`,
			a.paused, time.Now().UTC().Format(time.RFC3339)))
	case r.URL.Path == "/api/v1/dags/revenue_daily/dagRuns":
		now := time.Now().UTC().Format(time.RFC3339)
		writeJSON(w, http.StatusOK, fmt.Sprintf(
			`{"dag_runs":[{"dag_run_id":"manual__test","state":%q,"start_date":%q,"end_date":%q,"logical_date":%q,"execution_date":%q,"run_type":"manual"}],"total_entries":1}`,
			a.state, now, now, now, now))
	default:
		writeJSON(w, http.StatusNotFound, `{}`)
	}
}

// batchWorld wires a stack to the fake Airflow, a fake Postgres and a fake
// airflow CLI; the dbt models dir is a temp dir.
type batchWorld struct {
	t     *testing.T
	af    *batchAirflow
	sql   []string
	cli   []string
	root  string
	logs  *bytes.Buffer
	stack *stack
}

func newBatchWorld(t *testing.T, state string) *batchWorld {
	t.Helper()
	af := &batchAirflow{state: state}
	srv := httptest.NewServer(af)
	t.Cleanup(srv.Close)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, batchModelsRel), 0o755); err != nil {
		t.Fatal(err)
	}
	w := &batchWorld{t: t, af: af, root: root}
	w.stack = &stack{
		root: root,
		cfg:  config.Config{AirflowURL: srv.URL},
		log:  slog.New(slog.NewTextHandler(w.logsBuf(), nil)),
		sqlFn: func(_ context.Context, db string, stmts ...string) (string, error) {
			if db != "warehouse" {
				return "", fmt.Errorf("fake postgres: unexpected db %q", db)
			}
			w.sql = append(w.sql, stmts...)
			return "", nil
		},
		airflowCLIFn: func(_ context.Context, args ...string) (string, error) {
			joined := strings.Join(args, " ")
			w.cli = append(w.cli, joined)
			switch joined {
			case "dags pause revenue_daily":
				w.af.mu.Lock()
				w.af.paused = true
				w.af.mu.Unlock()
			case "dags unpause revenue_daily":
				w.af.mu.Lock()
				w.af.paused = false
				w.af.mu.Unlock()
			}
			return "Created <DagRun revenue_daily manual__test>", nil
		},
		poll: 10 * time.Millisecond,
	}
	return w
}

func (w *batchWorld) logsBuf() *bytes.Buffer {
	if w.logs == nil {
		w.logs = &bytes.Buffer{}
	}
	return w.logs
}

func (w *batchWorld) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	w.t.Cleanup(cancel)
	return ctx
}

func (w *batchWorld) modelPath() string {
	return filepath.Join(w.root, batchModelsRel, batchBrokenModel)
}

func TestDagFailedBreakAndFix(t *testing.T) {
	w := newBatchWorld(t, "failed")
	ctx := w.ctx()
	if err := breakDagFailed(ctx, w.stack); err != nil {
		t.Fatalf("break: %v\n%s", err, w.logsBuf())
	}
	if !contains(w.sql, sqlRenameOrdersAway) {
		t.Errorf("break did not rename analytics.orders away (%v)", w.sql)
	}
	if !contains(w.cli, "dags trigger revenue_daily") {
		t.Errorf("break did not trigger a run (%v)", w.cli)
	}

	w.af.mu.Lock()
	w.af.state = "success"
	w.af.mu.Unlock()
	if err := fixDagFailed(ctx, w.stack); err != nil {
		t.Fatalf("fix: %v\n%s", err, w.logsBuf())
	}
	if !contains(w.sql, sqlRenameOrdersBack) {
		t.Errorf("fix did not rename analytics.orders back (%v)", w.sql)
	}
	// Idempotent: running the fix again succeeds.
	if err := fixDagFailed(ctx, w.stack); err != nil {
		t.Fatalf("second fix: %v\n%s", err, w.logsBuf())
	}
}

func TestDbtModelBrokenBreakAndFix(t *testing.T) {
	w := newBatchWorld(t, "failed")
	ctx := w.ctx()
	if err := breakDbtModelBroken(ctx, w.stack); err != nil {
		t.Fatalf("break: %v\n%s", err, w.logsBuf())
	}
	if _, err := os.Stat(w.modelPath()); err != nil {
		t.Fatalf("break did not write the broken model: %v", err)
	}
	if !contains(w.cli, "dags trigger revenue_daily") {
		t.Errorf("break did not trigger a run (%v)", w.cli)
	}

	w.af.mu.Lock()
	w.af.state = "success"
	w.af.mu.Unlock()
	if err := fixDbtModelBroken(ctx, w.stack); err != nil {
		t.Fatalf("fix: %v\n%s", err, w.logsBuf())
	}
	if _, err := os.Stat(w.modelPath()); !os.IsNotExist(err) {
		t.Fatalf("fix left the broken model: %v", err)
	}
	// Idempotent: removing a missing file is fine.
	if err := fixDbtModelBroken(ctx, w.stack); err != nil {
		t.Fatalf("second fix: %v\n%s", err, w.logsBuf())
	}
}

func TestLateBatchBreakAndFix(t *testing.T) {
	w := newBatchWorld(t, "success")
	ctx := w.ctx()
	if err := breakLateBatch(ctx, w.stack); err != nil {
		t.Fatalf("break: %v\n%s", err, w.logsBuf())
	}
	if !contains(w.cli, "dags pause revenue_daily") {
		t.Errorf("break did not pause the DAG (%v)", w.cli)
	}

	if err := fixLateBatch(ctx, w.stack); err != nil {
		t.Fatalf("fix: %v\n%s", err, w.logsBuf())
	}
	if !contains(w.cli, "dags unpause revenue_daily") {
		t.Errorf("fix did not unpause the DAG (%v)", w.cli)
	}
	if !contains(w.cli, "dags trigger revenue_daily") {
		t.Errorf("fix did not trigger a run (%v)", w.cli)
	}
}

// The break fails with a clear error when AIRFLOW_URL is not set (the batch
// profile is not started), instead of a confusing connection error.
func TestBatchBreakNeedsAirflowURL(t *testing.T) {
	w := newBatchWorld(t, "failed")
	w.stack.cfg.AirflowURL = ""
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := breakDagFailed(ctx, w.stack); err == nil || !strings.Contains(err.Error(), "AIRFLOW_URL is not set") {
		t.Fatalf("break = %v, want AIRFLOW_URL error", err)
	}
}

// Each batch scenario raises an alert for the daily-revenue pipeline in
// dev/owners.yaml (a batch pipeline has no watcher signals).
func TestBatchScenariosTripAlert(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(filepath.Join(root, "dev", "owners.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := own.Pipeline(batchPipelineID)
	if !ok {
		t.Fatalf("dev/owners.yaml has no pipeline %q", batchPipelineID)
	}
	for _, name := range []string{"dag-failed", "dbt-model-broken", "late-batch"} {
		t.Run(name, func(t *testing.T) {
			sc, ok := scenarios[name]
			if !ok {
				t.Fatalf("scenario %q not registered", name)
			}
			if sc.breakIt == nil || sc.fix == nil {
				t.Fatalf("scenario %q has no break or fix", name)
			}
			if len(sc.signals) != 0 || sc.alert == "" {
				t.Fatalf("scenario %q = signals %v alert %q: a batch pipeline has no watcher, so it needs an alert", name, sc.signals, sc.alert)
			}
			var a struct {
				Title      string `json:"title"`
				Severity   string `json:"severity"`
				PipelineID string `json:"pipeline_id"`
				DedupKey   string `json:"dedup_key"`
			}
			if err := json.Unmarshal([]byte(sc.alert), &a); err != nil {
				t.Fatalf("alert of %q is not JSON: %v", name, err)
			}
			if a.Title == "" || a.Severity == "" || a.PipelineID != p.ID || !strings.HasPrefix(a.DedupKey, "devctl-") {
				t.Errorf("alert of %q = %+v: want a title, a severity, pipeline %q and a devctl- dedup key", name, a, p.ID)
			}
		})
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
