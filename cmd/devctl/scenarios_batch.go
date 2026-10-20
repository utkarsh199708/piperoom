package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"piperoom/internal/clients/airflow"
)

// The batch scenarios (P4g-2): one break/fix pair per way the daily-revenue
// batch pipeline fails. The pipeline is kind batch, so the watcher watches
// nothing (owners.batchWatch): each break raises the alert through
// `devctl alert <scenario>` instead. They need the batch compose profile up
// and AIRFLOW_URL set (dev/batch.md).

const (
	batchDAG         = "revenue_daily"
	batchPipelineID  = "daily-revenue"
	airflowContainer = "piperoom-dev-airflow-1"

	// batchBrokenModel is the model the dbt-model-broken break deploys; dbt
	// builds every .sql file under models/, so it fails the next run.
	batchBrokenModel = "fct_revenue_v2.sql"

	// batchRunTimeout is how long a break/fix waits for the triggered run to
	// reach the wanted state. Airflow starts the run at once (no retries).
	batchRunTimeout = 3 * time.Minute
)

// batchModelsRel is the dbt models directory under the repo root, bind mounted
// into the airflow container (dev/docker-compose.yml).
var batchModelsRel = filepath.Join("dev", "batch", "dbt", "analytics", "models")

// SQL the batch scenarios run against the warehouse; the unit tests' fake
// Postgres answers exactly these statements.
const (
	sqlRenameOrdersAway = `DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'analytics' AND tablename = 'orders') THEN
    ALTER TABLE analytics.orders RENAME TO analytics.orders_broken;
  END IF;
END $$`
	sqlRenameOrdersBack = `DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'analytics' AND tablename = 'orders_broken') THEN
    ALTER TABLE analytics.orders_broken RENAME TO analytics.orders;
  END IF;
END $$`
)

// brokenModelSQL is what the dbt-model-broken break writes into the models
// directory: it references a column that does not exist in stg_orders, so the
// next dbt run fails to build it.
const brokenModelSQL = `-- devctl break dbt-model-broken: a deploy that references a column that no
-- longer exists upstream, so the next revenue_daily run fails in dbt_run.
select
    order_id,
    revenue_amt
from {{ ref('stg_orders') }}
`

// The alerts each batch break raises (no watcher signal exists for a batch
// pipeline, so the room opens through `devctl alert <scenario>`). The bodies
// say what a team's own check would observe, never the cause.
const (
	dagFailedAlert = `{
  "title": "daily-revenue: the latest revenue_daily run failed",
  "severity": "sev2",
  "pipeline_id": "daily-revenue",
  "dedup_key": "devctl-dag-failed",
  "source": "reconciliation",
  "description": "The latest revenue_daily DAG run failed at check_source: the dbt source table analytics.orders is missing."
}`
	dbtModelBrokenAlert = `{
  "title": "daily-revenue: a dbt model failed to build",
  "severity": "sev2",
  "pipeline_id": "daily-revenue",
  "dedup_key": "devctl-dbt-model-broken",
  "source": "reconciliation",
  "description": "The latest revenue_daily run failed in dbt_run: model fct_revenue_v2 references a column that does not exist."
}`
	lateBatchAlert = `{
  "title": "daily-revenue: analytics.fct_revenue is stale",
  "severity": "sev2",
  "pipeline_id": "daily-revenue",
  "dedup_key": "devctl-late-batch",
  "source": "freshness",
  "description": "analytics.fct_revenue has not been refreshed: the revenue_daily DAG is paused and no new run is scheduled."
}`
)

// ------------------------------------------------------------- dag-failed

// dag-failed: a bad migration renamed the dbt source table, so the DAG's first
// task (check_source, which counts analytics.orders) fails and the run is
// failed before dbt ever starts. Layer: orchestrator.
func breakDagFailed(ctx context.Context, s *stack) error {
	if _, err := s.psql(ctx, "warehouse", sqlRenameOrdersAway); err != nil {
		return err
	}
	after, err := s.triggerBatchRun(ctx)
	if err != nil {
		return err
	}
	if _, err := s.waitBatchRun(ctx, "failed", after, batchRunTimeout); err != nil {
		return err
	}
	s.log.Info("broken: analytics.orders renamed so check_source fails — the latest " + batchDAG + " run is failed")
	return nil
}

// The fix: rename the table back, then prove the pipeline runs again.
func fixDagFailed(ctx context.Context, s *stack) error {
	if _, err := s.psql(ctx, "warehouse", sqlRenameOrdersBack); err != nil {
		return err
	}
	after, err := s.triggerBatchRun(ctx)
	if err != nil {
		return err
	}
	if _, err := s.waitBatchRun(ctx, "success", after, batchRunTimeout); err != nil {
		return err
	}
	s.log.Info("fixed: analytics.orders restored, " + batchDAG + " run succeeded")
	return nil
}

// --------------------------------------------------------- dbt-model-broken

// dbt-model-broken: a deploy dropped a broken model into the dbt project, so
// check_source passes but dbt_run fails to build the model. Layer: transform,
// the broken model.
func breakDbtModelBroken(ctx context.Context, s *stack) error {
	path := filepath.Join(s.root, batchModelsRel, batchBrokenModel)
	if err := os.WriteFile(path, []byte(brokenModelSQL), 0o644); err != nil {
		return fmt.Errorf("devctl: write broken model: %w", err)
	}
	after, err := s.triggerBatchRun(ctx)
	if err != nil {
		return err
	}
	if _, err := s.waitBatchRun(ctx, "failed", after, batchRunTimeout); err != nil {
		return err
	}
	s.log.Info("broken: a broken dbt model was deployed — the latest "+batchDAG+" run is failed", "model", batchBrokenModel)
	return nil
}

// The fix: remove the broken model (the cause), then prove the pipeline runs.
func fixDbtModelBroken(ctx context.Context, s *stack) error {
	path := filepath.Join(s.root, batchModelsRel, batchBrokenModel)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("devctl: remove broken model: %w", err)
	}
	after, err := s.triggerBatchRun(ctx)
	if err != nil {
		return err
	}
	if _, err := s.waitBatchRun(ctx, "success", after, batchRunTimeout); err != nil {
		return err
	}
	s.log.Info("fixed: broken dbt model removed, " + batchDAG + " run succeeded")
	return nil
}

// -------------------------------------------------------------- late-batch

// late-batch: the DAG is paused, so no new run lands and the sink
// analytics.fct_revenue goes stale. dag_schedule_gap reports it paused (layer
// orchestrator); a real freshness check raises the alert.
func breakLateBatch(ctx context.Context, s *stack) error {
	if _, err := s.airflowCLI(ctx, "dags", "pause", batchDAG); err != nil {
		return err
	}
	cl, err := s.batchAirflowClient()
	if err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		dag, err := cl.DAG(ctx, batchDAG)
		if err != nil {
			return false, nil
		}
		return dag.Paused, nil
	}); err != nil {
		return fmt.Errorf("devctl: %s did not report paused: %w", batchDAG, err)
	}
	s.log.Info("broken: " + batchDAG + " paused — no new runs, analytics.fct_revenue goes stale")
	return nil
}

// The fix: unpause and prove a run succeeds (the sink refreshes).
func fixLateBatch(ctx context.Context, s *stack) error {
	if _, err := s.airflowCLI(ctx, "dags", "unpause", batchDAG); err != nil {
		return err
	}
	after, err := s.triggerBatchRun(ctx)
	if err != nil {
		return err
	}
	if _, err := s.waitBatchRun(ctx, "success", after, batchRunTimeout); err != nil {
		return err
	}
	s.log.Info("fixed: " + batchDAG + " unpaused and a run succeeded")
	return nil
}

// -------------------------------------------------------------------- helpers

// airflowCLI runs an `airflow` command inside the airflow container.
func (s *stack) airflowCLI(ctx context.Context, args ...string) (string, error) {
	if s.airflowCLIFn != nil {
		return s.airflowCLIFn(ctx, args...)
	}
	full := append([]string{"exec", airflowContainer, "airflow"}, args...)
	out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("devctl: docker exec airflow %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// batchAirflowClient builds a client for the dev Airflow REST API. It refuses
// to run without AIRFLOW_URL, with a pointer to the batch profile docs.
func (s *stack) batchAirflowClient() (*airflow.Client, error) {
	if strings.TrimSpace(s.cfg.AirflowURL) == "" {
		return nil, errors.New("devctl: AIRFLOW_URL is not set: start the batch profile (dev/batch.md) and export AIRFLOW_URL=http://127.0.0.1:8080")
	}
	return airflow.New(s.cfg.AirflowURL)
}

// triggerBatchRun triggers a run and returns the time just before, used to tell
// this run apart from earlier ones when polling.
func (s *stack) triggerBatchRun(ctx context.Context) (time.Time, error) {
	after := time.Now()
	out, err := s.airflowCLI(ctx, "dags", "trigger", batchDAG)
	if err != nil {
		return after, err
	}
	s.log.Info("triggered "+batchDAG, "output", strings.TrimSpace(out))
	return after, nil
}

// waitBatchRun waits until a run newer than after reaches state want.
func (s *stack) waitBatchRun(ctx context.Context, want string, after time.Time, timeout time.Duration) (airflow.DAGRun, error) {
	cl, err := s.batchAirflowClient()
	if err != nil {
		return airflow.DAGRun{}, err
	}
	var got airflow.DAGRun
	if err := s.waitFor(ctx, timeout, func() (bool, error) {
		runs, err := cl.DAGRuns(ctx, batchDAG, 3)
		if err != nil {
			return false, nil
		}
		for _, r := range runs {
			if r.State == want && !r.LogicalDate.Before(after) {
				got = r
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return airflow.DAGRun{}, fmt.Errorf("devctl: %s did not reach state %s: %w", batchDAG, want, err)
	}
	return got, nil
}
