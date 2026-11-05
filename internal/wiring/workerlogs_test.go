package wiring

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
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

	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/workerlogs"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/logtools"
)

const (
	wlWorkerSel   = `{service="connect"}`
	wlConsumerSel = `{service="devconsumer"}`
	wlToken       = "tok-s3cret-do-not-show"
)

// fakeLoki is an httptest Loki that answers every query_range with one ERROR
// line and records what each request carried.
type fakeLoki struct {
	mu       sync.Mutex
	queries  []string
	auths    []string
	orgIDs   []string
	basicOK  []bool
	user     string
	password string
}

func (f *fakeLoki) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.queries = append(f.queries, r.URL.Query().Get("query"))
		f.auths = append(f.auths, r.Header.Get("Authorization"))
		f.orgIDs = append(f.orgIDs, r.Header.Get("X-Scope-OrgID"))
		u, p, ok := r.BasicAuth()
		f.basicOK = append(f.basicOK, ok && u == f.user && p == f.password)
		f.mu.Unlock()
		ts := time.Now().Add(-time.Minute).UnixNano()
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"level":"ERROR","container":"w-1"},"values":[["%d","ERROR orders-cdc-0 fakeloki-line-42"]]}]}}`, ts)
	})
}

func wlOwners() *owners.Owners {
	p := cdcPipe("orders")
	p.Connectors = []string{"orders-cdc"}
	p.ConsumerGroups = []string{"order-sync"}
	p.ConsumerLogs = &owners.ConsumerLogsSpec{Selector: wlConsumerSel}
	return ownersOf(p)
}

func wlConfig(url string) config.Config {
	c := devA2Config()
	c.WorkerLogsLokiURL = url
	c.WorkerLogsSelector = wlWorkerSel
	c.WorkerLogsToken = wlToken
	return c
}

// captureLogs sends slog's default logger to a buffer for the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func runLogTool(t *testing.T, reg *tools.Registry, name, in string, o *owners.Owners) tools.Result {
	t.Helper()
	tl, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s is not registered", name)
	}
	res, err := tl.Run(tools.WithPipeline(context.Background(), o.Pipelines[0]), json.RawMessage(in))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

func openWL(t *testing.T, cfg config.Config, op clientOpeners) *Wired {
	t.Helper()
	w, err := openClientsWith(context.Background(), cfg, nil, wlOwners(), false, op)
	if err != nil {
		t.Fatalf("openClientsWith: %v", err)
	}
	t.Cleanup(w.Close)
	return w
}

// TestOpenWiresWorkerLogs (H2): with WORKER_LOGS_* set, the set Open builds
// reads that Loki in both log tools: connect_worker_logs over
// WORKER_LOGS_SELECTOR, consumer_logs over the pipeline's own selector, both
// with the bearer token and the tenant header.
func TestOpenWiresWorkerLogs(t *testing.T) {
	logs := captureLogs(t)
	f := &fakeLoki{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	cfg := wlConfig(srv.URL)
	cfg.WorkerLogsOrgID = "tenant-1"

	w := openWL(t, cfg, (&openRecorder{}).openers(t))
	if _, ok := w.WorkerLogs.(*workerlogs.Client); !ok {
		t.Fatalf("WorkerLogs = %T, want *workerlogs.Client", w.WorkerLogs)
	}
	reg := registerQ3(t, w)
	o := wlOwners()

	wres := runLogTool(t, reg, logtools.Name, `{"connector":"orders-cdc"}`, o)
	cres := runLogTool(t, reg, logtools.ConsumerName, `{}`, o)
	for name, res := range map[string]tools.Result{"connect_worker_logs": wres, "consumer_logs": cres} {
		if !strings.Contains(res.Summary, "fakeloki-line-42") {
			t.Errorf("%s summary = %q, want the fake Loki's line", name, res.Summary)
		}
	}
	if len(f.queries) != 2 {
		t.Fatalf("Loki saw %d queries, want 2: %q", len(f.queries), f.queries)
	}
	if !strings.HasPrefix(f.queries[0], wlWorkerSel) || strings.Contains(f.queries[0], "devconsumer") {
		t.Errorf("worker query = %q, want it on %s", f.queries[0], wlWorkerSel)
	}
	if !strings.HasPrefix(f.queries[1], wlConsumerSel) || strings.Contains(f.queries[1], `service="connect"`) {
		t.Errorf("consumer query = %q, want it on %s", f.queries[1], wlConsumerSel)
	}
	for i := range f.queries {
		if f.auths[i] != "Bearer "+wlToken || f.orgIDs[i] != "tenant-1" {
			t.Errorf("request %d: Authorization %q, X-Scope-OrgID %q", i, f.auths[i], f.orgIDs[i])
		}
	}
	if strings.Contains(logs.String(), wlToken) {
		t.Errorf("token in log output: %s", logs.String())
	}
}

// TestOpenWiresWorkerLogsBasicAuth: WORKER_LOGS_USER sends basic auth with
// the token as the password (https only, so the fake Loki is TLS and the test
// adds its CA through the opener seam).
func TestOpenWiresWorkerLogsBasicAuth(t *testing.T) {
	f := &fakeLoki{user: "loki-user", password: wlToken}
	srv := httptest.NewTLSServer(f.handler())
	defer srv.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := wlConfig(srv.URL)
	cfg.WorkerLogsUser = "loki-user"
	cfg.WorkerLogsOrgID = "tenant-2"
	op := (&openRecorder{}).openers(t)
	op.workerLogs = func(c config.Config) (*workerlogs.Client, error) {
		return openWorkerLogs(c, workerlogs.WithTLS(netsec.TLS{CAFile: ca}))
	}
	w := openWL(t, cfg, op)
	reg := registerQ3(t, w)
	o := wlOwners()
	runLogTool(t, reg, logtools.Name, `{"connector":"orders-cdc"}`, o)
	runLogTool(t, reg, logtools.ConsumerName, `{}`, o)
	if len(f.queries) != 2 {
		t.Fatalf("Loki saw %d queries, want 2", len(f.queries))
	}
	for i := range f.queries {
		if !f.basicOK[i] || f.orgIDs[i] != "tenant-2" {
			t.Errorf("request %d: basic auth ok %v, X-Scope-OrgID %q", i, f.basicOK[i], f.orgIDs[i])
		}
	}
}

// TestOpenWorkerLogsUnset: without WORKER_LOGS_LOKI_URL the reader stays a
// plain nil interface and both tools answer "not configured".
func TestOpenWorkerLogsUnset(t *testing.T) {
	w := openWL(t, devA2Config(), (&openRecorder{}).openers(t))
	if w.WorkerLogs != nil {
		t.Fatalf("WorkerLogs = %#v, want nil", w.WorkerLogs)
	}
	reg := registerQ3(t, w)
	o := wlOwners()
	if res := runLogTool(t, reg, logtools.Name, `{"connector":"orders-cdc"}`, o); !strings.Contains(res.Summary, "not configured") {
		t.Errorf("connect_worker_logs = %q", res.Summary)
	}
	if res := runLogTool(t, reg, logtools.ConsumerName, `{}`, o); !strings.Contains(res.Summary, "not configured") {
		t.Errorf("consumer_logs = %q", res.Summary)
	}
}

// TestOpenWorkerLogsInvalid: a bad WORKER_LOGS_* value fails Open with an
// error naming the variable and never the token, before any client is used.
func TestOpenWorkerLogsInvalid(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{"bad url", func(c *config.Config) { c.WorkerLogsLokiURL = "loki:3100" }, "WORKER_LOGS_LOKI_URL"},
		{"user info in url", func(c *config.Config) { c.WorkerLogsLokiURL = "http://u:p@localhost:3100" }, "WORKER_LOGS_LOKI_URL"},
		{"no selector", func(c *config.Config) { c.WorkerLogsSelector = "" }, "WORKER_LOGS_SELECTOR"},
		{"pipeline in selector", func(c *config.Config) { c.WorkerLogsSelector = `{service="connect"} |= "x"` }, "WORKER_LOGS_SELECTOR"},
		{"user without token", func(c *config.Config) { c.WorkerLogsUser = "u"; c.WorkerLogsToken = "" }, "WORKER_LOGS_USER"},
		{"settings without url", func(c *config.Config) { c.WorkerLogsLokiURL = "" }, "WORKER_LOGS_LOKI_URL"},
		{"basic auth over http", func(c *config.Config) { c.WorkerLogsUser = "u" }, "WORKER_LOGS_*"},
		{"token over remote http", func(c *config.Config) { c.WorkerLogsLokiURL = "http://loki.example.com:3100" }, "WORKER_LOGS_*"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureLogs(t)
			cfg := wlConfig("http://localhost:3100")
			tt.edit(&cfg)
			rec := &openRecorder{}
			w, err := openClientsWith(context.Background(), cfg, nil, wlOwners(), false, rec.openers(t))
			if err == nil {
				w.Close()
				t.Fatal("openClientsWith succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name %s", err, tt.want)
			}
			if strings.Contains(err.Error(), wlToken) || strings.Contains(logs.String(), wlToken) {
				t.Errorf("token leaked: error %q, logs %q", err, logs.String())
			}
			if len(rec.calls) != 0 {
				t.Errorf("clients opened before the config error: %v", rec.calls)
			}
		})
	}
}
