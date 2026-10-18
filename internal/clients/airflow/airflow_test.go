package airflow

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

const (
	testPassword = "airflow-test-password"
	testToken    = "airflow-test-token"
	failedRun    = "scheduled__2026-10-17T00:00:00+00:00"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// plainClient is a client without credentials against a plain-http fake.
func plainClient(t *testing.T, h http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv
}

// tlsFake starts an https fake and returns the TLS settings that trust it.
// Every httptest TLS server presents the same certificate.
func tlsFake(t *testing.T, h http.HandlerFunc) (*httptest.Server, netsec.TLS) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return srv, netsec.TLS{CAFile: ca}
}

func TestDAGRuns(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/dags/revenue_daily/dagRuns" ||
			r.URL.RawQuery != "limit=5&order_by=-execution_date" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("request %s %s Accept=%q", r.Method, r.RequestURI, r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture(t, "dag_runs.json"))
	})
	runs, err := c.DAGRuns(context.Background(), "revenue_daily", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %+v", runs)
	}
	running, failed := runs[0], runs[1]
	if running.ID != "manual__2026-10-18T09:12:44.102938+00:00" || running.State != "running" ||
		running.StartedAt.IsZero() || !running.EndedAt.IsZero() || running.Note != "" {
		t.Errorf("running run = %+v", running)
	}
	wantStart := time.Date(2026, 10, 18, 0, 0, 2, 511873000, time.UTC)
	wantEnd := time.Date(2026, 10, 18, 0, 7, 41, 204512000, time.UTC)
	if failed.ID != failedRun || failed.State != "failed" || !failed.StartedAt.Equal(wantStart) ||
		!failed.EndedAt.Equal(wantEnd) || failed.Note != "paged on-call: fct_revenue broke" {
		t.Errorf("failed run = %+v", failed)
	}
}

func TestDAGRunsLimit(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want string
	}{
		{0, "25"}, {-3, "25"}, {7, "7"}, {100, "100"}, {5000, "100"},
	} {
		var got string
		c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
			got = r.URL.Query().Get("limit")
			w.Write([]byte(`{"dag_runs":[],"total_entries":0}`))
		})
		runs, err := c.DAGRuns(context.Background(), "d", tc.in)
		if err != nil || len(runs) != 0 {
			t.Fatalf("limit %d: runs %v, err %v", tc.in, runs, err)
		}
		if got != tc.want {
			t.Errorf("limit %d: sent limit=%s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestTaskInstances(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/dags/revenue_daily/dagRuns/"+failedRun+"/taskInstances" || r.URL.RawQuery != "limit=100" {
			t.Errorf("request %s", r.RequestURI)
		}
		w.Write(fixture(t, "task_instances.json"))
	})
	tis, err := c.TaskInstances(context.Background(), "revenue_daily", failedRun)
	if err != nil {
		t.Fatal(err)
	}
	want := []TaskInstance{
		{TaskID: "extract_orders", Try: 1, State: "success", Operator: "PythonOperator",
			StartedAt: time.Date(2026, 10, 18, 0, 0, 3, 701288000, time.UTC), EndedAt: time.Date(2026, 10, 18, 0, 0, 7, 912004000, time.UTC)},
		{TaskID: "dbt_run_fct_revenue", Try: 2, State: "failed", Operator: "BashOperator",
			StartedAt: time.Date(2026, 10, 18, 0, 5, 12, 98127000, time.UTC), EndedAt: time.Date(2026, 10, 18, 0, 5, 21, 118540000, time.UTC)},
		{TaskID: "publish_dashboard", Try: 0, State: "upstream_failed", Operator: "EmptyOperator"},
	}
	if len(tis) != len(want) {
		t.Fatalf("got %+v", tis)
	}
	for i, w := range want {
		g := tis[i]
		if g.TaskID != w.TaskID || g.Try != w.Try || g.State != w.State || g.Operator != w.Operator ||
			!g.StartedAt.Equal(w.StartedAt) || !g.EndedAt.Equal(w.EndedAt) {
			t.Errorf("[%d] = %+v, want %+v", i, g, w)
		}
	}
}

func TestTaskLogTail(t *testing.T) {
	log := fixture(t, "task_log.txt")
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/dags/revenue_daily/dagRuns/"+failedRun+"/taskInstances/dbt_run_fct_revenue/logs/2" ||
			r.URL.RawQuery != "" || r.Header.Get("Accept") != "text/plain" {
			t.Errorf("request %s Accept=%q", r.RequestURI, r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(log)
	})
	ctx := context.Background()
	for _, max := range []int{0, -1, len(log), 1 << 20} {
		got, err := c.TaskLogTail(ctx, "revenue_daily", failedRun, "dbt_run_fct_revenue", 2, max)
		if err != nil || got != string(log) {
			t.Fatalf("maxBytes %d: err %v, got %d bytes, want the whole %d-byte log", max, err, len(got), len(log))
		}
	}
	got, err := c.TaskLogTail(ctx, "revenue_daily", failedRun, "dbt_run_fct_revenue", 2, 150)
	if err != nil {
		t.Fatal(err)
	}
	if got != "[2026-10-18, 00:05:21 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 1\n" {
		t.Errorf("tail of 150 = %q", got)
	}
	if _, err := c.TaskLogTail(ctx, "revenue_daily", failedRun, "dbt_run_fct_revenue", 0, 0); err == nil {
		t.Error("try 0: want an error")
	}
}

func TestTailFrom(t *testing.T) {
	for _, tc := range []struct {
		name, in string
		n        int
		want     string
	}{
		{"fits", "a\nb\n", 10, "a\nb\n"},
		{"mid-line cut starts at next line", "line one\nline two\nline three\n", 15, "line three\n"},
		{"window starts at a line", "line one\nline two\nline three\n", 20, "line two\nline three\n"},
		{"late break keeps the long line's end", "x\n" + strings.Repeat("a", 30) + "\nend\n", 20, strings.Repeat("a", 15) + "\nend\n"},
		{"only break is the last byte", "abcdefghij\n", 5, "ghij\n"},
		{"no break: rune boundary", "ééééé", 5, "éé"},
	} {
		if got := tailFrom([]byte(tc.in), tc.n); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestOversizedBodies(t *testing.T) {
	line := "0123456789abcde\n" // 16 bytes; MaxResponseBytes is a multiple
	big := strings.Repeat(line, MaxResponseBytes/len(line))
	var body string
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	})
	ctx := context.Background()

	body = big + "THE REAL END\n"
	tail, cut, err := c.TaskLogTailCut(ctx, "d", "r", "t", 1, 64)
	if err != nil || !cut || tail != strings.Repeat(line, 4) {
		t.Fatalf("over the cap: tail %q, cut %v, err %v", tail, cut, err)
	}
	body = big
	if tail, cut, err = c.TaskLogTailCut(ctx, "d", "r", "t", 1, 64); err != nil || cut || tail != strings.Repeat(line, 4) {
		t.Fatalf("at the cap: tail %q, cut %v, err %v", tail, cut, err)
	}

	elem := `{"dag_run_id":"r","state":"success"},`
	body = `{"dag_runs":[` + strings.Repeat(elem, MaxResponseBytes/len(elem)+1) + `{}],"total_entries":1}`
	if _, err := c.DAGRuns(ctx, "d", 1); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("JSON over the cap: err %v, want a decode error", err)
	}
}

func TestErrors(t *testing.T) {
	calls := map[string]func(*Client) error{
		"DAGRuns": func(c *Client) error { _, err := c.DAGRuns(context.Background(), "d", 1); return err },
		"TaskInstances": func(c *Client) error {
			_, err := c.TaskInstances(context.Background(), "d", "r")
			return err
		},
		"TaskLogTail": func(c *Client) error {
			_, err := c.TaskLogTail(context.Background(), "d", "r", "t", 1, 0)
			return err
		},
	}
	long := strings.Repeat("x", 10<<10)
	for _, tc := range []struct {
		name     string
		code     int
		body     string
		notFound bool
		auth     bool
		jsonOnly bool
		contains string
	}{
		{name: "404", code: 404, body: `{"detail":"DAG with dag_id: 'd' not found","status":404,"title":"DAG not found","type":"x"}`, notFound: true, contains: "(DAG not found)"},
		{name: "404 no body", code: 404, notFound: true},
		{name: "401", code: 401, body: `{"detail":null,"status":401,"title":"Unauthorized","type":"x"}`, auth: true, contains: "status 401"},
		{name: "403", code: 403, body: `{"detail":null,"status":403,"title":"Forbidden","type":"x"}`, auth: true, contains: "status 403"},
		{name: "500 long body", code: 500, body: long, contains: "status 500"},
		{name: "bad JSON", code: 200, body: `{"dag_runs": [{"dag_run_id": `, jsonOnly: true, contains: "decode"},
	} {
		for name, call := range calls {
			if tc.jsonOnly && name == "TaskLogTail" {
				continue
			}
			c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				w.Write([]byte(tc.body))
			})
			err := call(c)
			if err == nil {
				t.Errorf("%s %s: no error", tc.name, name)
				continue
			}
			if errors.Is(err, clients.ErrNotFound) != tc.notFound || errors.Is(err, ErrAuth) != tc.auth {
				t.Errorf("%s %s: err %v: notFound %v auth %v", tc.name, name, err, errors.Is(err, clients.ErrNotFound), errors.Is(err, ErrAuth))
			}
			if !strings.HasPrefix(err.Error(), "airflow: ") || !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("%s %s: err %q, want it to contain %q", tc.name, name, err, tc.contains)
			}
			var se *StatusError
			if tc.code >= 400 && tc.code != 404 && (!errors.As(err, &se) || se.Code != tc.code || len(se.Body) > maxErrorBody) {
				t.Errorf("%s %s: err %v: want a *StatusError with a capped body", tc.name, name, err)
			}
		}
	}
}

func TestTimeout(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.TaskLogTail(ctx, "d", "r", "t", 1, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
}

func TestAuthHeaders(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want string
	}{
		{"none", nil, ""},
		{"basic", []Option{WithBasicAuth("viewer", testPassword)}, "Basic "},
		{"token", []Option{WithToken(testToken)}, "Bearer " + testToken},
	} {
		var got string
		var user, pass string
		srv, tlsCfg := tlsFake(t, func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("Authorization")
			user, pass, _ = r.BasicAuth()
			w.Write([]byte(`{"dag_runs":[]}`))
		})
		c, err := New(srv.URL, append(tc.opts, WithTLS(tlsCfg))...)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if _, err := c.DAGRuns(context.Background(), "d", 1); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !strings.HasPrefix(got, tc.want) || (tc.want == "") != (got == "") {
			t.Errorf("%s: Authorization %q, want prefix %q", tc.name, got, tc.want)
		}
		if tc.name == "basic" && (user != "viewer" || pass != testPassword) {
			t.Errorf("basic auth = %q/%q", user, pass)
		}
	}
}

func TestTokenNotSentToOtherHost(t *testing.T) {
	var other string
	otherSrv, _ := tlsFake(t, func(w http.ResponseWriter, r *http.Request) {
		other = "seen:" + r.Header.Get("Authorization")
		w.Write([]byte(`{"dag_runs":[]}`))
	})
	srv, tlsCfg := tlsFake(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, otherSrv.URL+r.URL.RequestURI(), http.StatusFound)
	})
	c, err := New(srv.URL, WithToken(testToken), WithTLS(tlsCfg))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DAGRuns(context.Background(), "d", 1); err != nil {
		t.Fatal(err)
	}
	if other != "seen:" {
		t.Fatalf("redirect target got %q, want no Authorization", other)
	}
}

func TestNewRefuses(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		opts      []Option
	}{
		{"basic over http", "http://airflow:8080", []Option{WithBasicAuth("viewer", testPassword)}},
		{"token over http", "http://airflow:8080", []Option{WithToken(testToken)}},
		{"basic and token", "https://airflow", []Option{WithBasicAuth("viewer", testPassword), WithToken(testToken)}},
		{"user without password", "https://airflow", []Option{WithBasicAuth("viewer", "")}},
		{"credentials in URL", "https://viewer:" + testPassword + "@airflow", nil},
		{"not http", "ftp://airflow", nil},
		{"no host", "https://", nil},
	} {
		_, err := New(tc.url, tc.opts...)
		if !errors.Is(err, netsec.ErrInvalid) {
			t.Errorf("%s: err %v, want netsec.ErrInvalid", tc.name, err)
			continue
		}
		if !strings.HasPrefix(err.Error(), "airflow: ") || strings.Contains(err.Error(), testPassword) || strings.Contains(err.Error(), testToken) {
			t.Errorf("%s: err %q: want the package prefix and no secret", tc.name, err)
		}
	}
}

func TestPathEscaping(t *testing.T) {
	var got []string
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.RequestURI)
		w.Write([]byte(`{}`))
	})
	ctx := context.Background()
	c.DAGRuns(ctx, "team/a b", 1)
	c.TaskInstances(ctx, "team/a b", "manual__2026-10-18T09:12:44+00:00")
	c.TaskLogTail(ctx, "team/a b", "manual__2026-10-18T09:12:44+00:00", "load?x=1#y", 3, 0)
	want := []string{
		"/api/v1/dags/team%2Fa%20b/dagRuns?limit=1&order_by=-execution_date",
		"/api/v1/dags/team%2Fa%20b/dagRuns/manual__2026-10-18T09:12:44+00:00/taskInstances?limit=100",
		"/api/v1/dags/team%2Fa%20b/dagRuns/manual__2026-10-18T09:12:44+00:00/taskInstances/load%3Fx=1%23y/logs/3",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sent\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// url.PathEscape leaves "." and ".." alone; such ids must never be sent.
	got = nil
	for _, id := range []string{"", ".", ".."} {
		errs := []error{}
		_, err := c.DAGRuns(ctx, id, 1)
		errs = append(errs, err)
		_, err = c.TaskInstances(ctx, "d", id)
		errs = append(errs, err)
		_, err = c.TaskLogTail(ctx, "d", "r", id, 1, 0)
		errs = append(errs, err)
		for i, err := range errs {
			if !errors.Is(err, netsec.ErrBadPath) {
				t.Errorf("id %q call %d: err %v, want netsec.ErrBadPath", id, i, err)
			}
		}
	}
	if len(got) != 0 {
		t.Fatalf("refused ids sent requests: %v", got)
	}
}

func TestProvenance(t *testing.T) {
	c, srv := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/logs/") {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"dag_runs":[]}`))
	})
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	c.DAGRuns(ctx, "d", 0)
	c.TaskLogTail(ctx, "d", "r", "t", 1, 0)
	c.TaskLogTail(ctx, "d", "..", "t", 1, 0) // refused: no receipt
	calls := rec.Calls()
	want := []string{
		"GET /api/v1/dags/d/dagRuns?limit=25&order_by=-execution_date",
		"GET /api/v1/dags/d/dagRuns/r/taskInstances/t/logs/1",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %+v", calls)
	}
	for i, call := range calls {
		if call.Kind != tools.CallHTTP || call.Target != srv.URL || call.Text != want[i] || call.At.IsZero() {
			t.Errorf("call %d = %+v, want text %q", i, call, want[i])
		}
	}
}
