package debeziumtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/prom"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

type fakeRows struct {
	est   map[string]int64
	err   error
	calls []string
}

func (f *fakeRows) EstimatedRows(_ context.Context, table string) (int64, error) {
	f.calls = append(f.calls, table)
	if f.err != nil {
		return 0, f.err
	}
	n, ok := f.est[table]
	if !ok {
		return 0, clients.ErrNotFound
	}
	return n, nil
}

func snapTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	d.Owners = testOwners()
	if d.Connect == nil {
		d.Connect = connects()
	}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("debezium_snapshot_progress")
	if !ok {
		t.Fatal("debezium_snapshot_progress not registered")
	}
	return tl
}

func snap(name string, v float64, extra ...string) prom.Sample {
	l := map[string]string{"context": "snapshot", "name": "pg", "plugin": "postgres", "task": "0"}
	for i := 0; i+1 < len(extra); i += 2 {
		l[extra[i]] = extra[i+1]
	}
	return prom.Sample{Name: "debezium_metrics_" + name, Labels: l, Value: v}
}

// running is TC3's fixture: 2.1 M rows scanned in 420 s (5 k/s).
func running() []prom.Sample {
	return []prom.Sample{
		snap("SnapshotRunning", 1), snap("SnapshotCompleted", 0), snap("SnapshotAborted", 0), snap("SnapshotPaused", 0),
		snap("SnapshotDurationInSeconds", 420), snap("SnapshotPausedDurationInSeconds", 0),
		snap("TotalTableCount", 1), snap("RemainingTableCount", 1),
		snap("RowsScanned", 2_100_000, "table", "public.orders"),
		// Another pipeline's table on the same connector label set: counted
		// in the rate, never named.
		snap("RowsScanned", 0, "table", "secret.payroll"),
		// Another connector on the same worker.
		{Name: "debezium_metrics_SnapshotRunning", Labels: map[string]string{"context": "snapshot", "name": "pay"}, Value: 0},
	}
}

func runSnap(t *testing.T, tl tools.Tool) (tools.Result, SnapshotProgress) {
	t.Helper()
	r, err := tl.Run(roomCtx(), json.RawMessage(in))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := r.Data.(SnapshotProgress)
	if !ok {
		t.Fatalf("Data = %T", r.Data)
	}
	return r, p
}

// TC3: 2.1 M of 10 M rows at 5 k/s is an ETA of about 26 min.
func TestSnapshotProgressETA(t *testing.T) {
	rows := &fakeRows{est: map[string]int64{"public.orders": 10_000_000}}
	r, p := runSnap(t, snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{samples: running()}, Rows: rows}))
	if !p.Running || p.RowsScanned != 2_100_000 || p.RowsPerSecond != 5000 || p.ETASeconds == nil || *p.ETASeconds != 1580 {
		t.Fatalf("progress = %+v", p)
	}
	for _, want := range []string{
		"pg-orders-connector: snapshot running for 7 min",
		"2100000 rows scanned at 5000 rows/s",
		"ETA ~26 min for ~7900000 rows left of ~10000000",
		"1 of 1 tables left",
		"public.orders 2100000 of ~10000000 (21%)",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "payroll") {
		t.Fatalf("another table's label reached the result: %s", b)
	}
	if len(rows.calls) != 1 || rows.calls[0] != "public.orders" {
		t.Fatalf("estimates read for %v", rows.calls)
	}
}

// TC3: no snapshot running says so.
func TestSnapshotProgressNotRunning(t *testing.T) {
	done := []prom.Sample{
		snap("SnapshotRunning", 0), snap("SnapshotCompleted", 1), snap("SnapshotDurationInSeconds", 11520),
		snap("RowsScanned", 48_203_110, "table", "public.orders"),
	}
	rows := &fakeRows{}
	r, p := runSnap(t, snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{samples: done}, Rows: rows}))
	if p.Running || !strings.Contains(r.Summary, "no snapshot in progress") || !strings.Contains(r.Summary, "completed: 48203110 rows in 3.2 h") {
		t.Fatalf("%+v\n%s", p, r.Summary)
	}
	if len(rows.calls) != 0 || p.ETASeconds != nil {
		t.Fatal("an ETA was computed for a finished snapshot")
	}
	never := []prom.Sample{snap("SnapshotRunning", 0), snap("SnapshotCompleted", 0)}
	r, _ = runSnap(t, snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{samples: never}}))
	if !strings.Contains(r.Summary, "no snapshot in progress; none has completed") {
		t.Fatalf("summary: %s", r.Summary)
	}
	// The repo's exporter fixture (running, no duration metric) parses too.
	r, p = runSnap(t, snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{samples: fixture(t)}}))
	if !p.Running || p.RowsScanned != 48_203_110 || !strings.Contains(r.Summary, "ETA unknown") {
		t.Fatalf("%+v\n%s", p, r.Summary)
	}
	// Prefix mismatch: no metrics found, a clear line.
	r, _ = runSnap(t, snapTool(t, Deps{Metrics: config.DebeziumMetrics{URL: enabled.URL, Prefix: "dbz_"}, Scraper: &fakeScraper{samples: running()}}))
	if !strings.Contains(r.Summary, `no snapshot metrics found under prefix "dbz_"`) {
		t.Fatalf("summary: %s", r.Summary)
	}
}

// TC4: an endpoint that does not answer is a clear fact, not a tool error,
// and the URL's path and query never show.
func TestSnapshotProgressEndpointDown(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL + "/metrics?token=hunter2"
	dead.Close()
	tl := snapTool(t, Deps{Metrics: config.DebeziumMetrics{URL: url}})
	r, err := tl.Run(roomCtx(), json.RawMessage(in))
	if err != nil {
		t.Fatalf("err = %v, want a result", err)
	}
	p := r.Data.(SnapshotProgress)
	if p.MetricsAvailable || p.Reason == "" || !strings.Contains(r.Summary, "snapshot progress unknown") || !strings.Contains(r.Summary, "will not help") {
		t.Fatalf("%+v\n%s", p, r.Summary)
	}
	if strings.Contains(r.Summary, "hunter2") {
		t.Fatalf("URL query leaked: %s", r.Summary)
	}
	for _, e := range []error{errors.New("prom: scrape connect:9404: status 503"), prom.ErrTooLarge} {
		r, err = snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{err: e}}).Run(roomCtx(), json.RawMessage(in))
		if err != nil || !strings.Contains(r.Summary, "snapshot progress unknown") {
			t.Fatalf("%v: %v %q", e, err, r.Summary)
		}
	}
}

// E5: why there is no ETA is said; an estimator error is not swallowed.
func TestSnapshotProgressNoETA(t *testing.T) {
	cases := []struct {
		name string
		rows RowEstimator
		smp  []prom.Sample
		want string
	}{
		{"no source", nil, running(), "the source database is not connected"},
		{"never analysed", &fakeRows{est: map[string]int64{"public.orders": -1}}, running(), "never analysed"},
		{"missing table", &fakeRows{est: map[string]int64{}}, running(), "not found in the source database"},
		{"estimator down", &fakeRows{err: errors.New("postgres: row estimate of public.orders: timeout")}, running(), "timeout"},
		{"nothing scanned", &fakeRows{est: map[string]int64{"public.orders": 10}}, []prom.Sample{snap("SnapshotRunning", 1), snap("SnapshotDurationInSeconds", 3)}, "no rows scanned"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, p := runSnap(t, snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{samples: c.smp}, Rows: c.rows}))
			if p.ETASeconds != nil || !strings.Contains(r.Summary, "ETA unknown") || !strings.Contains(r.Summary, c.want) {
				t.Fatalf("%+v\n%s", p, r.Summary)
			}
		})
	}
	// Scanned past a stale estimate: ETA 0 with a note.
	r, p := runSnap(t, snapTool(t, Deps{Metrics: enabled, Scraper: &fakeScraper{samples: running()}, Rows: &fakeRows{est: map[string]int64{"public.orders": 2_000_000}}}))
	if p.ETASeconds == nil || *p.ETASeconds != 0 || !strings.Contains(r.Summary, "lag recent inserts") {
		t.Fatalf("%+v\n%s", p, r.Summary)
	}
}

// E2, E3: input and scope are checked before any I/O; another pipeline's
// connector is refused; without a URL the tool says "not configured".
func TestSnapshotProgressScope(t *testing.T) {
	sc := &fakeScraper{samples: running()}
	rows := &fakeRows{est: map[string]int64{"public.orders": 1}}
	tl := snapTool(t, Deps{Metrics: enabled, Scraper: sc, Rows: rows, Connect: fakeConnect{err: errors.New("must not be called")}})
	for _, c := range []struct {
		ctx context.Context
		in  string
	}{
		{roomCtx(), `{"connector":"pay-connector"}`},
		{context.Background(), in},
		{tools.WithPipelineID(context.Background(), "nope"), in},
		{roomCtx(), `{"connector":""}`},
		{roomCtx(), `{`},
	} {
		if _, err := tl.Run(c.ctx, json.RawMessage(c.in)); err == nil {
			t.Errorf("%s: want error", c.in)
		}
	}
	if sc.calls != 0 || len(rows.calls) != 0 {
		t.Fatalf("I/O before the checks: %d scrapes, %v estimates", sc.calls, rows.calls)
	}
	// A connector whose topic.prefix is another pipeline's is refused before the scrape.
	tl = snapTool(t, Deps{Metrics: enabled, Scraper: sc, Connect: fakeConnect{cfg: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pay"}}}})
	if _, err := tl.Run(roomCtx(), json.RawMessage(in)); !errors.Is(err, tools.ErrBadInput) || sc.calls != 0 {
		t.Fatalf("err = %v, scrapes %d", err, sc.calls)
	}
	r, err := snapTool(t, Deps{Scraper: sc}).Run(roomCtx(), json.RawMessage(in))
	if err != nil || r.Summary != "not configured: set DEBEZIUM_METRICS_URL" || sc.calls != 0 {
		t.Fatalf("unconfigured: %q %v", r.Summary, err)
	}
}

// E1 over HTTP with the real scraper; E4: a summary with many tables stays
// within the cap.
func TestSnapshotProgressHTTPAndCaps(t *testing.T) {
	var body strings.Builder
	body.WriteString("# TYPE debezium_metrics_SnapshotRunning gauge\n")
	for _, s := range running() {
		fmt.Fprintf(&body, "%s{context=%q,name=%q", s.Name, s.Labels["context"], s.Labels["name"])
		if tbl, ok := s.Labels["table"]; ok {
			fmt.Fprintf(&body, ",table=%q", tbl)
		}
		fmt.Fprintf(&body, "} %g\n", s.Value)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body.String())) }))
	defer srv.Close()
	rows := &fakeRows{est: map[string]int64{"public.orders": 10_000_000}}
	r, p := runSnap(t, snapTool(t, Deps{Metrics: config.DebeziumMetrics{URL: srv.URL + "/metrics"}, Rows: rows}))
	if p.ETASeconds == nil || *p.ETASeconds != 1580 || len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("%+v\n%s", p, r.Summary)
	}

	// Many tables: the estimates stop at the cap, the summary is truncated.
	var tables []string
	for i := 0; i < maxEstimateTables+1; i++ {
		tables = append(tables, fmt.Sprintf("public.t%02d", i))
	}
	p2 := SnapshotProgress{Connector: "c", Running: true, Found: 1, RowsPerSecond: 1}
	for _, tbl := range tables {
		p2.Tables = append(p2.Tables, TableProgress{Table: tbl, EstimatedRows: -1})
	}
	(&snapshotTool{d: Deps{Rows: rows}}).eta(context.Background(), &p2)
	if !strings.Contains(p2.ETANote, "more than 50") {
		t.Fatalf("note = %q", p2.ETANote)
	}
	if len(rows.calls) != 1 {
		t.Fatalf("estimates read past the cap: %d", len(rows.calls))
	}
	if s := renderSnapshotProgress(p2, "x"); !strings.Contains(s, "more in Data") {
		t.Fatalf("summary: %s", s)
	}
}
