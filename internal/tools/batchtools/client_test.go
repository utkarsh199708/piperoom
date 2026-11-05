package batchtools

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/airflow"
	"piperoom/internal/tools"
)

// airflowFake serves the airflow client's own fixtures, plus a hostile log.
func airflowFake(t *testing.T, h http.HandlerFunc) *airflow.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := airflow.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func clientFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "clients", "airflow", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Against the real client: the tools work end to end, every request leaves a
// receipt, and no request is made for a refused DAG.
func TestWithAirflowClient(t *testing.T) {
	var requests int
	c := airflowFake(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		p := r.URL.EscapedPath()
		switch {
		case strings.Contains(p, "/logs/"):
			b, err := os.ReadFile(filepath.Join("testdata", "task_log_hostile.txt"))
			if err != nil {
				t.Error(err)
			}
			w.Write(b)
		case strings.HasSuffix(p, "/taskInstances"):
			w.Write(clientFixture(t, "task_instances.json"))
		case strings.HasSuffix(p, "/dagRuns"):
			w.Write(clientFixture(t, "dag_runs.json"))
		case strings.HasSuffix(p, "/dags/revenue_daily"):
			w.Write(clientFixture(t, "dag.json"))
		default:
			http.NotFound(w, r)
		}
	})
	reg := newRegistry(t, c)
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(batchCtx(), rec)

	for _, tc := range []struct{ name, in string }{
		{NameDAGRuns, `{"dag_id":"revenue_daily"}`},
		{NameTaskInstances, `{"dag_id":"revenue_daily","run_id":"scheduled__2026-10-17T00:00:00+00:00"}`},
		{NameTaskLogTail, `{"dag_id":"revenue_daily","run_id":"scheduled__2026-10-17T00:00:00+00:00","task_id":"dbt_run_fct_revenue","try":2}`},
		{NameScheduleGap, `{"dag_id":"revenue_daily"}`},
	} {
		res, err := run(t, reg, ctx, tc.name, tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if res.Summary == "" || len(res.Summary) > tools.MaxSummaryBytes {
			t.Fatalf("%s: summary of %d bytes", tc.name, len(res.Summary))
		}
		if strings.Contains(res.Summary, "Tr0ub4dor-fixture-pw") {
			t.Fatalf("%s: secret in summary", tc.name)
		}
	}
	calls := rec.Calls()
	if len(calls) != requests || len(calls) != 5 {
		t.Fatalf("%d receipts for %d requests, want 5", len(calls), requests)
	}
	for _, call := range calls {
		if call.Kind != tools.CallHTTP || !strings.HasPrefix(call.Text, "GET /api/v1/dags/revenue_daily") {
			t.Errorf("receipt %+v", call)
		}
	}

	before := requests
	if _, err := run(t, reg, ctx, NameDAGRuns, `{"dag_id":"kpi_weekly"}`); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope: %v", err)
	}
	if requests != before || len(rec.Calls()) != len(calls) {
		t.Fatal("a refused DAG reached the server")
	}
}

func TestClientErrorPaths(t *testing.T) {
	t.Run("404", func(t *testing.T) {
		c := airflowFake(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"title":"DAG not found","status":404}`)
		})
		_, err := run(t, newRegistry(t, c), batchCtx(), NameDAGRuns, `{"dag_id":"revenue_daily"}`)
		if !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), "DAG not found") {
			t.Fatalf("err = %v, want ErrNotFound with the title", err)
		}
	})
	t.Run("500", func(t *testing.T) {
		c := airflowFake(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "scheduler exploded", http.StatusInternalServerError)
		})
		_, err := run(t, newRegistry(t, c), batchCtx(), NameTaskInstances, `{"dag_id":"revenue_daily","run_id":"r1"}`)
		var se *airflow.StatusError
		if !errors.As(err, &se) || se.Code != 500 {
			t.Fatalf("err = %v, want a 500 StatusError", err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		release := make(chan struct{})
		c := airflowFake(t, func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		defer close(release)
		ctx, cancel := context.WithTimeout(batchCtx(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := run(t, newRegistry(t, c), ctx, NameTaskLogTail, `{"dag_id":"revenue_daily","run_id":"r1","task_id":"t1","try":1}`)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("the call did not stop at the deadline")
		}
	})
	t.Run("bad json", func(t *testing.T) {
		c := airflowFake(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"dag_runs": [`)
		})
		if _, err := run(t, newRegistry(t, c), batchCtx(), NameScheduleGap, `{"dag_id":"revenue_daily"}`); err == nil {
			t.Fatal("bad JSON: want an error")
		}
	})
}
