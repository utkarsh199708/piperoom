package workerlogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		selector string
		token    string
		want     Config
		wantErr  string
	}{
		{name: "off", want: Config{Selector: DefaultSelector}},
		{name: "dev defaults", url: "http://localhost:3100", want: Config{URL: "http://localhost:3100", Selector: DefaultSelector}},
		{name: "selector kept", url: "http://localhost:3100", selector: `{container="connect"}`, want: Config{URL: "http://localhost:3100", Selector: `{container="connect"}`}},
		{name: "token with https", url: "https://logs.example.net", selector: `{service="connect"}`, token: "s3cret", want: Config{URL: "https://logs.example.net", Selector: `{service="connect"}`, Token: "s3cret"}},
		{name: "token with http loopback", url: "http://127.0.0.1:3100", selector: `{service="connect"}`, token: "s3cret", want: Config{URL: "http://127.0.0.1:3100", Selector: `{service="connect"}`, Token: "s3cret"}},
		{name: "token without url", token: "s3cret", wantErr: "WORKER_LOGS_LOKI_URL is empty"},
		{name: "token over http off loopback", url: "http://loki.example.net:3100", selector: `{service="connect"}`, token: "s3cret", wantErr: "only over https"},
		{name: "user info", url: "http://user:pw@localhost:3100", selector: `{service="connect"}`, wantErr: "user info"},
		{name: "not a url", url: "localhost:3100", selector: `{service="connect"}`, wantErr: "http(s)"},
		{name: "empty selector braces", url: "http://localhost:3100", selector: `{}`, wantErr: "at least one matcher"},
		{name: "pipeline stage", url: "http://localhost:3100", selector: `{service="connect"} |~ ".+"`, wantErr: "stream selector"},
		{name: "nested braces", url: "http://localhost:3100", selector: `{service="connect",a={b}}`, wantErr: "nested braces"},
		{name: "unterminated value", url: "http://localhost:3100", selector: `{service="connect}`, wantErr: "unterminated"},
		{name: "no braces", url: "http://localhost:3100", selector: `service="connect"`, wantErr: "starting with {"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConfig(tt.url, tt.selector, tt.token)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseConfig err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ParseConfig = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("WORKER_LOGS_LOKI_URL", "http://localhost:3100")
	t.Setenv("WORKER_LOGS_SELECTOR", `{container="connect"}`)
	t.Setenv("WORKER_LOGS_TOKEN", "")
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.URL != "http://localhost:3100" || c.Selector != `{container="connect"}` {
		t.Fatalf("ConfigFromEnv = %+v", c)
	}
}

func TestSelectorIsFixedAndFieldsAreFilters(t *testing.T) {
	c := &Client{selector: `{service="connect", container="connect"}`}
	tests := []struct {
		name string
		q    LogQuery
		want string
	}{
		{
			name: "warn any task",
			q:    LogQuery{Connector: "orders-connector", Task: -1, MinLevel: "WARN"},
			want: `{service="connect", container="connect"} |~ "(?i)\\b(?:ERROR|FATAL|WARN)\\b" |= "orders-connector"`,
		},
		{
			name: "info task 2",
			q:    LogQuery{Connector: "orders-connector", Task: 2, MinLevel: "INFO"},
			want: `{service="connect", container="connect"} |~ "(?i)\\b(?:ERROR|FATAL|INFO|WARN)\\b" |= "orders-connector" |~ "task-2\\b"`,
		},
		{
			name: "trace has no level filter",
			q:    LogQuery{Connector: "c", Task: -1, MinLevel: "TRACE"},
			want: `{service="connect", container="connect"} |= "c"`,
		},
		{
			name: "error",
			q:    LogQuery{Connector: "c", Task: -1, MinLevel: "error"},
			want: `{service="connect", container="connect"} |~ "(?i)\\b(?:ERROR|FATAL)\\b" |= "c"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.logql(tt.q)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("logql =\n%s\nwant\n%s", got, tt.want)
			}
		})
	}
}

func TestLogQLRefusesModelTextInTheGrammar(t *testing.T) {
	c := &Client{selector: `{service="connect"}`}
	for _, q := range []LogQuery{
		{Connector: `x"} |~ ".*`, Task: -1},
		{Connector: "x y", Task: -1},
		{Connector: "-x", Task: -1},
		{Connector: "x", Task: 256},
		{Connector: "x", Task: -2},
		{Connector: "x", Task: -1, MinLevel: "TRACE|ERROR"},
		{Connector: "", Task: -1},
	} {
		if got, err := c.logql(q); err == nil {
			t.Errorf("logql(%+v) = %q, want an error", q, got)
		}
	}
}

// answer builds a query_range streams answer with the given streams.
func answer(streams ...string) string {
	return `{"status":"success","data":{"resultType":"streams","result":[` + strings.Join(streams, ",") + `]}}`
}

// stream builds one stream; values are "ns,line" pairs.
func stream(labels string, values ...string) string {
	vs := make([]string, 0, len(values))
	for _, v := range values {
		ts, line, _ := strings.Cut(v, ",")
		vs = append(vs, fmt.Sprintf(`["%s",%q]`, ts, line))
	}
	return `{"stream":` + labels + `,"values":[` + strings.Join(vs, ",") + `]}`
}

func newTestClient(t *testing.T, srv *httptest.Server, hc *http.Client) *Client {
	t.Helper()
	c, err := New(Config{URL: srv.URL, Selector: `{service="connect"}`}, hc)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestWorkerLogsReadsNewestOldestFirst(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/loki/api/v1/query_range" {
			t.Errorf("%s %s, want GET /loki/api/v1/query_range", r.Method, r.URL.Path)
		}
		gotQuery = r.URL.Query()
		// Newest first, as Loki answers a backward query.
		fmt.Fprint(w, answer(
			stream(`{"level":"ERROR","service":"connect"}`, "3000,2026-10-23 10:00:03,000 ERROR [Worker-1] io.debezium.connector.postgresql.PostgresStreamingChangeEventSource boom",
				"1000,2026-10-23 10:00:01,000 ERROR [Worker-1] io.debezium.connector.postgresql.PostgresStreamingChangeEventSource first"),
			stream(`{"level":"INFO","service":"connect"}`, "2000,2026-10-23 10:00:02,000 INFO [Worker-1] org.apache.kafka.connect.runtime.WorkerSourceTask middle"),
		))
	}))
	defer srv.Close()
	c := newTestClient(t, srv, nil)
	c.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	lines, err := c.WorkerLogs(context.Background(), LogQuery{Connector: "orders-connector", Task: -1, MinLevel: "INFO"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 3 {
		t.Fatalf("%d lines, want 3", len(lines))
	}
	for i, want := range []string{"first", "middle", "boom"} {
		if !strings.HasSuffix(lines[i].Text, want) {
			t.Errorf("line %d = %q, want it to end %q (oldest first)", i, lines[i].Text, want)
		}
		if !lines[i].At.Equal(time.Unix(0, int64(1000*(i+1)))) {
			t.Errorf("line %d at %s", i, lines[i].At)
		}
	}
	if lines[0].Level != "ERROR" || lines[0].Worker != "Worker-1" ||
		lines[0].Logger != "io.debezium.connector.postgresql.PostgresStreamingChangeEventSource" {
		t.Errorf("line 0 = %+v", lines[0])
	}
	if got := gotQuery.Get("direction"); got != "backward" {
		t.Errorf("direction = %q", got)
	}
	if got := gotQuery.Get("limit"); got != "200" {
		t.Errorf("limit = %q", got)
	}
	if !strings.Contains(gotQuery.Get("query"), `{service="connect"}`) || !strings.Contains(gotQuery.Get("query"), `|= "orders-connector"`) {
		t.Errorf("query = %q", gotQuery.Get("query"))
	}
	start, err1 := strconv.ParseInt(gotQuery.Get("start"), 10, 64)
	end, err2 := strconv.ParseInt(gotQuery.Get("end"), 10, 64)
	if err1 != nil || err2 != nil || start >= end {
		t.Errorf("start %v end %v (errs %v %v)", start, end, err1, err2)
	}
}

func TestWorkerLogsKeepsNewestAndDropsLowerLevels(t *testing.T) {
	var values []string
	for i := 1; i <= 250; i++ {
		values = append(values, fmt.Sprintf("%d,2026-10-23 10:00:00,000 WARN worker line %d", i*1000, i))
	}
	// Two INFO lines newer than every WARN: the text filter let them through
	// because "info" appears in the message, and finish must drop them.
	values = append(values,
		"251000,2026-10-23 10:00:00,000 INFO counts info 1",
		"252000,2026-10-23 10:00:00,000 INFO counts info 2",
	)
	// Loki answers newest first.
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, answer(stream(`{"service":"connect"}`, values...)))
	}))
	defer srv.Close()
	c := newTestClient(t, srv, nil)
	c.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	lines, err := c.WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1, MinLevel: "WARN"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != MaxLimit {
		t.Fatalf("%d lines, want %d", len(lines), MaxLimit)
	}
	if !strings.HasSuffix(lines[len(lines)-1].Text, "line 250") {
		t.Errorf("newest line = %q, want line 250", lines[len(lines)-1].Text)
	}
	for _, l := range lines {
		if l.Level != "WARN" {
			t.Fatalf("an INFO line survived the level filter: %+v", l)
		}
	}
}

func TestWorkerLogsStatuses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   error
		inErr  string
	}{
		{name: "401", status: http.StatusUnauthorized, body: "no token", want: ErrUnauthorized},
		{name: "403", status: http.StatusForbidden, want: ErrUnauthorized},
		{name: "404", status: http.StatusNotFound, body: "404 page not found", want: ErrNotFound, inErr: "WORKER_LOGS_LOKI_URL"},
		{name: "500", status: http.StatusInternalServerError, body: "boom", inErr: "500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				fmt.Fprint(w, tt.body)
			}))
			defer srv.Close()
			_, err := newTestClient(t, srv, nil).WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
			if err == nil {
				t.Fatal("want an error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if tt.inErr != "" && !strings.Contains(err.Error(), tt.inErr) {
				t.Fatalf("err = %v, want it to name %q", err, tt.inErr)
			}
		})
	}
}

func TestWorkerLogsContextDeadline(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := newTestClient(t, srv, nil).WorkerLogs(ctx, LogQuery{Connector: "c", Task: -1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestWorkerLogsClientTimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)
	_, err := newTestClient(t, srv, &http.Client{Timeout: 30 * time.Millisecond}).WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
	if err == nil {
		t.Fatal("want a timeout error")
	}
}

func TestWorkerLogsOversized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.CopyN(w, strings.NewReader(strings.Repeat("a", MaxResponseBytes+2)), MaxResponseBytes+2)
	}))
	defer srv.Close()
	_, err := newTestClient(t, srv, nil).WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

func TestWorkerLogsBadJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "truncated", body: `{"status":"success","data":{"resultType":"streams","result":[`},
		{name: "html page", body: `<html><body>proxy error</body></html>`},
		{name: "empty", body: ``},
		{name: "loki error status", body: `{"status":"error","error":"parse error at line 1"}`},
		{name: "metric answer", body: `{"status":"success","data":{"resultType":"matrix","result":[]}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, tt.body)
			}))
			defer srv.Close()
			_, err := newTestClient(t, srv, nil).WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
			if !errors.Is(err, ErrBadResponse) {
				t.Fatalf("err = %v, want ErrBadResponse", err)
			}
		})
	}
}

func TestWorkerLogsBearerToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer s3cret" {
			t.Errorf("Authorization = %q, want the bearer token", got)
		}
		fmt.Fprint(w, answer())
	}))
	defer srv.Close()
	c, err := New(Config{URL: srv.URL, Selector: `{service="connect"}`, Token: "s3cret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerLogsCutsLongLineAndSkipsJunk(t *testing.T) {
	long := "2026-10-23 10:00:00,000 ERROR " + strings.Repeat("x", MaxLineBytes+100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, answer(stream(`{"service":"connect"}`,
			"1000,2026-10-23 10:00:00,000 ERROR "+long,
			"2000,",
			"3000,2026-10-23 10:00:00,000 WARN fine",
		)))
	}))
	defer srv.Close()
	lines, err := newTestClient(t, srv, nil).WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("%d lines, want 2 (the empty one is skipped)", len(lines))
	}
	if !strings.HasSuffix(lines[0].Text, "…[line cut]") || len(lines[0].Text) > MaxLineBytes+20 {
		t.Fatalf("long line not cut: %d bytes", len(lines[0].Text))
	}
}

func TestNewRequiresURL(t *testing.T) {
	if _, err := New(Config{}, nil); err == nil {
		t.Fatal("want an error without a URL")
	}
}

func TestEndpointURL(t *testing.T) {
	tests := map[string]string{
		"http://localhost:3100":          "http://localhost:3100/loki/api/v1/query_range",
		"http://localhost:3100/":         "http://localhost:3100/loki/api/v1/query_range",
		"https://proxy.example.net/loki": "https://proxy.example.net/loki/api/v1/query_range",
	}
	for in, want := range tests {
		if got := endpointURL(in); got != want {
			t.Errorf("endpointURL(%q) = %q, want %q", in, got, want)
		}
	}
}
