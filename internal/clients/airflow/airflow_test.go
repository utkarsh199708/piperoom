package airflow

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

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
	if !running.LogicalDate.Equal(time.Date(2026, 10, 18, 9, 12, 44, 102938000, time.UTC)) || running.RunType != "manual" ||
		!failed.LogicalDate.Equal(time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)) || failed.RunType != "scheduled" {
		t.Errorf("logical date / run type: running %+v, failed %+v", running, failed)
	}
}

func TestDAGRunLogicalDate(t *testing.T) {
	day := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, run string
		want      time.Time
		runType   string
	}{
		{"both dates", `{"logical_date":"2026-10-17T00:00:00+00:00","execution_date":"2026-10-16T00:00:00+00:00","run_type":"backfill"}`, day, "backfill"},
		{"execution_date only (Airflow 2.1)", `{"execution_date":"2026-10-17T00:00:00+00:00","run_type":"scheduled"}`, day, "scheduled"},
		{"null logical_date", `{"logical_date":null,"execution_date":"2026-10-17T00:00:00+00:00"}`, day, ""},
		{"neither", `{"dag_run_id":"r"}`, time.Time{}, ""},
	} {
		c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"dag_runs":[` + tc.run + `],"total_entries":1}`))
		})
		runs, err := c.DAGRuns(context.Background(), "d", 1)
		if err != nil || len(runs) != 1 {
			t.Fatalf("%s: runs %v, err %v", tc.name, runs, err)
		}
		if !runs[0].LogicalDate.Equal(tc.want) || runs[0].RunType != tc.runType {
			t.Errorf("%s: got %+v, want logical %v run type %q", tc.name, runs[0], tc.want, tc.runType)
		}
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
		if r.URL.Path != "/api/v1/dags/revenue_daily/dagRuns/"+failedRun+"/taskInstances" || r.URL.RawQuery != "limit=100&offset=0" {
			t.Errorf("request %s", r.RequestURI)
		}
		w.Write(fixture(t, "task_instances.json"))
	})
	tis, err := c.TaskInstances(context.Background(), "revenue_daily", failedRun)
	if err != nil {
		t.Fatal(err)
	}
	want := []TaskInstance{
		{TaskID: "extract_orders", Try: 1, State: "success", Operator: "PythonOperator", MapIndex: -1,
			StartedAt: time.Date(2026, 10, 18, 0, 0, 3, 701288000, time.UTC), EndedAt: time.Date(2026, 10, 18, 0, 0, 7, 912004000, time.UTC)},
		{TaskID: "dbt_run_fct_revenue", Try: 2, State: "failed", Operator: "BashOperator", MapIndex: -1,
			StartedAt: time.Date(2026, 10, 18, 0, 5, 12, 98127000, time.UTC), EndedAt: time.Date(2026, 10, 18, 0, 5, 21, 118540000, time.UTC)},
		{TaskID: "publish_dashboard", Try: 0, State: "upstream_failed", Operator: "EmptyOperator", MapIndex: -1},
		{TaskID: "load_partition", Try: 1, State: "success", Operator: "_PythonDecoratedOperator", MapIndex: 0,
			StartedAt: time.Date(2026, 10, 18, 0, 1, 2, 350144000, time.UTC), EndedAt: time.Date(2026, 10, 18, 0, 1, 4, 220917000, time.UTC)},
		{TaskID: "load_partition", Try: 1, State: "failed", Operator: "_PythonDecoratedOperator", MapIndex: 1,
			StartedAt: time.Date(2026, 10, 18, 0, 1, 2, 581190000, time.UTC), EndedAt: time.Date(2026, 10, 18, 0, 1, 5, 12733000, time.UTC)},
	}
	if len(tis) != len(want) {
		t.Fatalf("got %+v", tis)
	}
	for i, w := range want {
		g := tis[i]
		if g.TaskID != w.TaskID || g.Try != w.Try || g.State != w.State || g.Operator != w.Operator || g.MapIndex != w.MapIndex ||
			!g.StartedAt.Equal(w.StartedAt) || !g.EndedAt.Equal(w.EndedAt) {
			t.Errorf("[%d] = %+v, want %+v", i, g, w)
		}
	}
}

func TestTaskInstanceMapIndex(t *testing.T) {
	for _, tc := range []struct {
		name, ti string
		want     int
	}{
		{"unmapped", `{"task_id":"t","map_index":-1}`, -1},
		{"first mapped", `{"task_id":"t","map_index":0}`, 0},
		{"mapped", `{"task_id":"t","map_index":7}`, 7},
		{"absent (Airflow < 2.3)", `{"task_id":"t"}`, -1},
		{"null", `{"task_id":"t","map_index":null}`, -1},
	} {
		c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"task_instances":[` + tc.ti + `],"total_entries":1}`))
		})
		tis, err := c.TaskInstances(context.Background(), "d", "r")
		if err != nil || len(tis) != 1 || tis[0].MapIndex != tc.want {
			t.Errorf("%s: tis %+v, err %v, want map index %d", tc.name, tis, err, tc.want)
		}
	}
}

func TestTaskLogTailMapped(t *testing.T) {
	var got []string
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.RequestURI)
		w.Write([]byte("log line\n"))
	})
	ctx := context.Background()
	for _, idx := range []int{2, 0, -1} {
		tail, cut, err := c.TaskLogTailMapped(ctx, "d", "r", "load_partition", idx, 1, 0)
		if err != nil || cut || tail != "log line\n" {
			t.Fatalf("map index %d: tail %q, cut %v, err %v", idx, tail, cut, err)
		}
	}
	base := "/api/v1/dags/d/dagRuns/r/taskInstances/load_partition/logs/1"
	want := []string{base + "?map_index=2", base + "?map_index=0", base}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sent\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if _, _, err := c.TaskLogTailMapped(ctx, "d", "r", "t", 0, 0, 0); err == nil {
		t.Error("try 0: want an error")
	}
}

func TestDAG(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/dags/revenue_daily" || r.URL.RawQuery != "" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("request %s Accept=%q", r.RequestURI, r.Header.Get("Accept"))
		}
		w.Write(fixture(t, "dag.json"))
	})
	d, err := c.DAG(context.Background(), "revenue_daily")
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != "revenue_daily" || d.Schedule != "0 0 * * * (At 00:00)" || d.Paused ||
		!d.NextRunAt.Equal(time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("DAG = %+v", d)
	}
}

func TestDAGFields(t *testing.T) {
	next := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, body string
		want       DAG
	}{
		{"cron, no description (Airflow < 2.3)",
			`{"dag_id":"d","schedule_interval":{"__type":"CronExpression","value":"@daily"}}`,
			DAG{ID: "d", Schedule: "@daily"}},
		{"description only",
			`{"dag_id":"d","schedule_interval":null,"timetable_description":"Triggered by datasets"}`,
			DAG{ID: "d", Schedule: "Triggered by datasets"}},
		{"same text once",
			`{"dag_id":"d","schedule_interval":{"__type":"CronExpression","value":"Dataset"},"timetable_description":"Dataset"}`,
			DAG{ID: "d", Schedule: "Dataset"}},
		{"timedelta",
			`{"dag_id":"d","schedule_interval":{"__type":"TimeDelta","days":0,"seconds":21600,"microseconds":0}}`,
			DAG{ID: "d", Schedule: "every 6h0m0s"}},
		{"relativedelta",
			`{"dag_id":"d","schedule_interval":{"__type":"RelativeDelta","years":0,"months":1,"days":0,"leapdays":0,"hours":0,"minutes":0,"seconds":0,"microseconds":0,"year":null,"month":null,"day":null,"weekday":null,"hour":null,"minute":null,"second":null,"microsecond":null}}`,
			DAG{ID: "d", Schedule: "every 1mo"}},
		{"unknown schedule shape keeps the read",
			`{"dag_id":"d","schedule_interval":"0 0 * * *","timetable_description":"At 00:00"}`,
			DAG{ID: "d", Schedule: "At 00:00"}},
		{"paused, next_dagrun only",
			`{"dag_id":"d","is_paused":true,"next_dagrun":"2026-10-18T00:00:00+00:00","next_dagrun_create_after":null}`,
			DAG{ID: "d", Paused: true, NextRunAt: next}},
		{"no optional field", `{"dag_id":"d"}`, DAG{ID: "d"}},
	} {
		c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(tc.body))
		})
		d, err := c.DAG(context.Background(), "d")
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if d.ID != tc.want.ID || d.Schedule != tc.want.Schedule || d.Paused != tc.want.Paused || !d.NextRunAt.Equal(tc.want.NextRunAt) {
			t.Errorf("%s: got %+v, want %+v", tc.name, d, tc.want)
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

// memTransport answers every request in-process with a 200 whose body is
// body(), and counts the bytes the client pulls from it. TestOversizedBodies
// uses it instead of httptest: over a loopback socket each MiB is many
// writer-to-reader handoffs, each one a scheduler wake-up, so on a loaded CI
// runner one 5 MiB body ran into the client's 10 s timeout (#675) though it
// takes milliseconds idle. In memory the cost is copying only, and the byte
// count lets the test assert where the client stops reading, not just that
// it returned.
type memTransport struct {
	body func() io.Reader
	read int64 // bytes the client read from the last body
}

func (m *memTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.read = 0
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{},
		Body:          &countingBody{r: m.body(), n: &m.read},
		ContentLength: -1, // unknown length, as Airflow streams a log
		Request:       req,
	}, nil
}

type countingBody struct {
	r io.Reader
	n *int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	*b.n += int64(n)
	return n, err
}

func (b *countingBody) Close() error { return nil }

func TestOversizedBodies(t *testing.T) {
	c, err := New("http://airflow.test")
	if err != nil {
		t.Fatal(err)
	}
	mt := &memTransport{}
	c.hc.Transport = mt
	// Each request gets its own deadline so a slow one cannot eat the next
	// one's budget; the client's own 10 s timeout still applies inside it.
	call := func(maxBytes int) (string, bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return c.TaskLogTailCut(ctx, "d", "r", "t", 1, maxBytes)
	}

	// 5 MiB, more than MaxResponseBytes: the tail is the real end of the
	// log, so the client streamed all of it through the bounded tail buffer.
	row := "[2026-10-18, 00:05:59 UTC] {subprocess.py:93} INFO - row batch loaded\n"
	last := "[2026-10-18, 00:06:00 UTC] {taskinstance.py:2698} ERROR - unique last line 7f3a\n"
	log := strings.Repeat(row, (5<<20)/len(row)+1) + last
	mt.body = func() io.Reader { return strings.NewReader(log) }
	tail, cut, err := call(200)
	if err != nil || cut || len(tail) > 200 || !strings.HasSuffix(tail, last) || !strings.HasPrefix(tail, "[2026-10-18, ") {
		t.Fatalf("5 MiB log: tail %q, cut %v, err %v", tail, cut, err)
	}
	if mt.read != int64(len(log)) {
		t.Fatalf("5 MiB log: read %d bytes, want all %d", mt.read, len(log))
	}

	// Past the stream ceiling reading stops: cut, the tail ends at the
	// ceiling, and the client reads one byte past it and no more. The real
	// 64 MiB ceiling is lowered to 5 MiB (still above MaxResponseBytes, and
	// a whole number of lines) to keep the test fast.
	if c.logCeiling != MaxLogStreamBytes {
		t.Fatalf("New set logCeiling %d, want MaxLogStreamBytes", c.logCeiling)
	}
	c.logCeiling = 5 << 20
	line := "0123456789abcde\n"
	chunk := strings.Repeat(line, (1<<20)/len(line)) // 1 MiB of whole lines
	stream := func(end string) func() io.Reader {
		return func() io.Reader {
			parts := make([]io.Reader, 0, c.logCeiling>>20+1)
			for range c.logCeiling >> 20 {
				parts = append(parts, strings.NewReader(chunk))
			}
			return io.MultiReader(append(parts, strings.NewReader(end))...)
		}
	}
	mt.body = stream("past the ceiling\n")
	if tail, cut, err = call(64); err != nil || !cut || tail != strings.Repeat(line, 4) {
		t.Fatalf("over the ceiling: tail %q, cut %v, err %v", tail, cut, err)
	}
	if mt.read != c.logCeiling+1 {
		t.Fatalf("over the ceiling: read %d bytes, want ceiling+1 = %d", mt.read, c.logCeiling+1)
	}
	mt.body = stream("")
	if tail, cut, err = call(64); err != nil || cut || tail != strings.Repeat(line, 4) {
		t.Fatalf("at the ceiling: tail %q, cut %v, err %v", tail, cut, err)
	}
	if mt.read != c.logCeiling {
		t.Fatalf("at the ceiling: read %d bytes, want %d", mt.read, c.logCeiling)
	}

	// JSON past MaxResponseBytes: the decoder sees at most the cap (rule 7),
	// so the cut body fails to decode and the error says so.
	elem := `{"dag_run_id":"r","state":"success"},`
	body := `{"dag_runs":[` + strings.Repeat(elem, MaxResponseBytes/len(elem)+1) + `{}],"total_entries":1}`
	mt.body = func() io.Reader { return strings.NewReader(body) }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.DAGRuns(ctx, "d", 1); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("JSON over the cap: err %v, want a decode error", err)
	}
	if mt.read > MaxResponseBytes {
		t.Fatalf("JSON over the cap: read %d bytes, want at most MaxResponseBytes (%d)", mt.read, MaxResponseBytes)
	}
}

func TestTailBuffer(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog\n")
	for _, size := range []int{1, 7, len(data), 100} {
		for _, chunk := range []int{1, 3, 7, 8, 50} {
			tb := &tailBuffer{buf: make([]byte, size)}
			for i := 0; i < len(data); i += chunk {
				tb.Write(data[i:min(i+chunk, len(data))])
			}
			if got, want := string(tb.Bytes()), string(data[max(0, len(data)-size):]); got != want {
				t.Errorf("size %d chunk %d: got %q, want %q", size, chunk, got, want)
			}
		}
	}
}

func TestTaskInstancesPaging(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		total, pageSize, overlap int
		want, requests           int
		truncated                bool
	}{
		{name: "three pages", total: 250, pageSize: 100, want: 250, requests: 3},
		{name: "overlapping pages kept once", total: 250, pageSize: 100, overlap: 10, want: 250, requests: 3},
		{name: "exactly the ceiling", total: 1000, pageSize: 100, want: 1000, requests: 10},
		{name: "over the ceiling", total: 2000, pageSize: 100, want: 1000, requests: 10, truncated: true},
		{name: "small server pages hit the request cap", total: 2000, pageSize: 10, want: 200, requests: 20, truncated: true},
	} {
		var offsets []string
		c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			offsets = append(offsets, q.Get("offset"))
			from, _ := strconv.Atoi(q.Get("offset"))
			from = max(0, from-tc.overlap)
			var items []string
			for i := from; i < min(from+tc.pageSize, tc.total); i++ {
				items = append(items, fmt.Sprintf(`{"task_id":"load_partition","map_index":%d,"try_number":1,"state":"success"}`, i))
			}
			fmt.Fprintf(w, `{"task_instances":[%s],"total_entries":%d}`, strings.Join(items, ","), tc.total)
		})
		tis, err := c.TaskInstances(context.Background(), "d", "r")
		if errors.Is(err, ErrTruncated) != tc.truncated || (!tc.truncated && err != nil) {
			t.Errorf("%s: err %v, want truncated %v", tc.name, err, tc.truncated)
		}
		if len(tis) != tc.want || len(offsets) != tc.requests {
			t.Errorf("%s: %d instances in %d requests, want %d in %d", tc.name, len(tis), len(offsets), tc.want, tc.requests)
			continue
		}
		for i, ti := range tis {
			if ti.MapIndex != i {
				t.Errorf("%s: [%d] map index %d", tc.name, i, ti.MapIndex)
				break
			}
		}
		if offsets[0] != "0" || offsets[1] != strconv.Itoa(tc.pageSize) {
			t.Errorf("%s: offsets %v", tc.name, offsets)
		}
	}
}

func TestStatusErrorText(t *testing.T) {
	body := "line one\r\n\x1b[31mred\x1b[0m\ttab " + strings.Repeat("é", 300)
	e := &StatusError{Path: "/p", Code: 502, Body: body}
	shown, ok := strings.CutPrefix(e.Error(), "airflow: GET /p: status 502: ")
	if !ok || len(shown) > 256 || !utf8.ValidString(shown) || strings.ContainsFunc(shown, unicode.IsControl) ||
		!strings.HasPrefix(shown, "line one [31mred [0m tab éé") || !strings.HasSuffix(shown, "…") {
		t.Errorf("Error() = %q", e.Error())
	}
	if e.Body != body {
		t.Error("Body must keep the full text")
	}
}

func TestErrors(t *testing.T) {
	calls := map[string]func(*Client) error{
		"DAG":     func(c *Client) error { _, err := c.DAG(context.Background(), "d"); return err },
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
		"/api/v1/dags/team%2Fa%20b/dagRuns/manual__2026-10-18T09:12:44+00:00/taskInstances?limit=100&offset=0",
		"/api/v1/dags/team%2Fa%20b/dagRuns/manual__2026-10-18T09:12:44+00:00/taskInstances/load%3Fx=1%23y/logs/3",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sent\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// url.PathEscape leaves "." and ".." alone; such ids must never be sent.
	got = nil
	for _, id := range []string{"", ".", ".."} {
		errs := []error{}
		_, err := c.DAG(ctx, id)
		errs = append(errs, err)
		_, err = c.DAGRuns(ctx, id, 1)
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
