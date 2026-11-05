package workerlogs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"text/scanner"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

const sel = `{service="connect"}`

var t0 = time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

func newTest(t *testing.T, h http.HandlerFunc, opts ...Option) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, sel, "", opts...)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return t0 }
	return c
}

func ns(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func TestCheckSelector(t *testing.T) {
	good := []string{
		`{service="connect"}`,
		` { service = "connect" , container=~"conn.*", level!="DEBUG", x!~` + "`a|b`" + ` } `,
		`{a="}|= \"x\""}`,
	}
	for _, s := range good {
		if err := CheckSelector(s); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
	bad := []string{
		``, `service="connect"`, `{}`, `{service="connect"} |= "x"`, `{service="connect"} | json`,
		`sum(rate({a="b"}[1m]))`, `{service=connect}`, `{service='c'}`, `{9a="b"}`, `{a=="b"}`,
		`{a="b" b="c"}`, `{a="b"`, `{a="unterminated}`, `{a="b"}}`, `{a="b"} or vector(1)`,
		"{" + strings.Repeat(`a="b",`, 200) + `a="b"}`,
	}
	for _, s := range bad {
		if err := CheckSelector(s); err == nil {
			t.Errorf("%q: accepted", s)
		}
	}
}

func TestNewChecks(t *testing.T) {
	cases := []struct {
		name, url, sel string
		tok            netsec.Secret
		opts           []Option
		ok             bool
	}{
		{"plain", "http://localhost:3100", sel, "", nil, true},
		{"token loopback http", "http://127.0.0.1:3100", sel, "t", nil, true},
		{"token https", "https://loki.example.com/gw", sel, "t", nil, true},
		{"token remote http", "http://loki.example.com", sel, "t", nil, false},
		{"basic over http", "http://localhost:3100", sel, "t", []Option{WithBasicAuthUser("u")}, false},
		{"basic https", "https://logs.grafana.net", sel, "t", []Option{WithBasicAuthUser("u")}, true},
		{"user no token", "https://logs.grafana.net", sel, "", []Option{WithBasicAuthUser("u")}, false},
		{"userinfo", "https://u:p@loki.example.com", sel, "", nil, false},
		{"query in url", "https://loki.example.com/?x=1", sel, "", nil, false},
		{"bad scheme", "ftp://loki", sel, "", nil, false},
		{"bad selector", "http://localhost:3100", `{a="b"} |= "x"`, "", nil, false},
		{"bad org", "http://localhost:3100", sel, "", []Option{WithOrgID("a\nb")}, false},
	}
	for _, tc := range cases {
		_, err := New(tc.url, tc.sel, tc.tok, tc.opts...)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v", tc.name, err)
		}
		if err != nil && strings.Contains(err.Error(), "p@") {
			t.Errorf("%s: error quotes userinfo: %v", tc.name, err)
		}
	}
}

// tokens splits a LogQL query the way Loki's lexer does (text/scanner with
// Go strings) and returns the tokens after the selector's closing brace.
func tailTokens(t *testing.T, q string) []string {
	t.Helper()
	i := strings.Index(q, "} ")
	if !strings.HasPrefix(q, sel) || i != len(sel)-1 {
		t.Fatalf("query does not start with the selector: %s", q)
	}
	var s scanner.Scanner
	s.Init(strings.NewReader(q[len(sel):]))
	s.Mode = scanner.ScanStrings | scanner.ScanRawStrings | scanner.ScanIdents | scanner.ScanInts
	s.Error = func(*scanner.Scanner, string) { t.Fatalf("scanner error in %s", q) }
	var out []string
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		out = append(out, s.TokenText())
	}
	return out
}

func TestBuildQueryHostile(t *testing.T) {
	hostile := []string{
		`orders"} |= "" or {job=~".+`,
		`x" | json | line_format "{{.password}}`,
		"back`tick\\\\",
		`a|task-0]`,
		`.*(?:)` + "‮",
		`"`,
		`\`,
	}
	for _, name := range hostile {
		for _, task := range []int{-1, 3} {
			q := buildQuery(sel, LogQuery{Connector: name, Task: task, MinLevel: "WARN"})
			toks := tailTokens(t, q)
			// Exactly two filters: | op "string", | ~ "string".
			if len(toks) != 6 || toks[0] != "|" || toks[3] != "|" || toks[4] != "~" {
				t.Fatalf("%q task %d: tokens %q", name, task, toks)
			}
			lit, err := strconv.Unquote(toks[2])
			if err != nil {
				t.Fatalf("%q: literal does not unquote: %v", name, err)
			}
			if task < 0 {
				if toks[1] != "=" || lit != name {
					t.Errorf("%q: filter = %s %q", name, toks[1], lit)
				}
			} else if toks[1] != "~" || !strings.HasPrefix(lit, regexpQuoteMeta(name)) {
				t.Errorf("%q: regex filter = %q", name, lit)
			}
		}
	}
	if q := buildQuery(sel, LogQuery{Connector: "c", Task: -1, MinLevel: "INFO"}); q != sel+` |= "c"` {
		t.Errorf("INFO adds a level filter: %s", q)
	}
}

func regexpQuoteMeta(s string) string {
	// Mirror of regexp.QuoteMeta, kept here so the test does not merely
	// repeat the implementation's call.
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(`\.+*?()|[]{}^$`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func TestWorkerLogsQueryAndParse(t *testing.T) {
	var got http.Header
	var params map[string]string
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/loki/api/v1/query_range" {
			http.NotFound(w, r)
			return
		}
		got = r.Header.Clone()
		params = map[string]string{}
		for k := range r.URL.Query() {
			params[k] = r.URL.Query().Get(k)
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"streams","result":[
		 {"stream":{"service":"connect","container":"piperoom-dev-connect-1"},"values":[
		   ["%s","2026-10-23 09:59:00,000 ERROR  Postgres|dbserver1|streaming  orders-cdc-0 boom   [io.debezium.pipeline.ErrorHandler]\n\tat x.y(Z.java:1)"],
		   ["%s","[2026-10-23 09:58:00,000] INFO [orders-cdc|task-0] WARN mentioned (org.apache.kafka.connect.runtime.WorkerTask:212)"]
		 ]},
		 {"stream":{"service":"connect","level":"warning","pod":"connect-0"},"values":[
		   ["%s","orders-cdc-0 something", {"trace_id":"x"}]
		 ]}
		],"stats":{}}}`, ns(t0.Add(-time.Minute)), ns(t0.Add(-2*time.Minute)), ns(t0.Add(-30*time.Second)))
	}, WithOrgID("tenant-a"))
	rec := tools.NewRecorder()
	lines, err := c.WorkerLogs(tools.WithRecorder(context.Background(), rec), LogQuery{Connector: "orders-cdc", Task: -1})
	if err != nil {
		t.Fatal(err)
	}
	if params["direction"] != "backward" || params["limit"] != "200" || params["query"] != sel+` |= "orders-cdc" |~ "\\b(WARN|WARNING|ERROR|SEVERE|FATAL)\\b"` {
		t.Errorf("params = %v", params)
	}
	if params["start"] != ns(t0.Add(-time.Hour)) || params["end"] != ns(t0) {
		t.Errorf("window = %s..%s", params["start"], params["end"])
	}
	if got.Get("X-Scope-OrgID") != "tenant-a" || got.Get("Authorization") != "" {
		t.Errorf("headers = %v", got)
	}
	// The INFO line is dropped (its WARN is in the message); the rest is oldest first.
	if len(lines) != 2 {
		t.Fatalf("lines = %+v", lines)
	}
	if lines[0].Level != "ERROR" || lines[0].Logger != "io.debezium.pipeline.ErrorHandler" || lines[0].Worker != "piperoom-dev-connect-1" || !lines[0].At.Equal(t0.Add(-time.Minute)) {
		t.Errorf("line 0 = %+v", lines[0])
	}
	if lines[1].Level != "WARN" || lines[1].Worker != "connect-0" || lines[1].Text != "orders-cdc-0 something" {
		t.Errorf("line 1 = %+v", lines[1])
	}
	calls := rec.Calls()
	if len(calls) != 1 || calls[0].Text != "GET /loki/api/v1/query_range" || !strings.HasPrefix(calls[0].ParamsRedacted[0], "query={service=") {
		t.Errorf("provenance = %+v", calls)
	}
}

func TestWorkerLogsNewestKept(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		var vals []string
		for i := 0; i < 5; i++ { // Loki ignoring the limit; newest first as backward returns
			vals = append(vals, fmt.Sprintf(`["%s","ERROR c line %d"]`, ns(t0.Add(-time.Duration(i)*time.Second)), i))
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[%s]}]}}`, strings.Join(vals, ","))
	})
	lines, err := c.WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: 2, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 3 || lines[0].Text != "ERROR c line 2" || lines[2].Text != "ERROR c line 0" {
		t.Fatalf("lines = %+v", lines)
	}
}

func TestWorkerLogsBearer(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	}))
	defer srv.Close()
	c, err := New(srv.URL+"/gateway/", sel, "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	lines, err := c.WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
	if err != nil || len(lines) != 0 || auth != "Bearer s3cret" {
		t.Fatalf("lines %v err %v auth %q", lines, err, auth)
	}
}

func TestWorkerLogsErrors(t *testing.T) {
	big := strings.Repeat("x", MaxBodyBytes)
	cases := []struct {
		name string
		h    http.HandlerFunc
		want func(error) bool
	}{
		{"401", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "no org id\x1b[31m", http.StatusUnauthorized)
		}, func(err error) bool { return errors.Is(err, ErrAuth) && !strings.Contains(err.Error(), "\x1b") }},
		{"404", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }, func(err error) bool {
			var se *StatusError
			return errors.As(err, &se) && se.Code == 404 && !errors.Is(err, ErrAuth)
		}},
		{"400 parse error", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "parse error at line 1", http.StatusBadRequest)
		}, func(err error) bool { return strings.Contains(err.Error(), "status 400: parse error") }},
		{"oversized", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","`+big+`"]]}]}}`)
		}, func(err error) bool { return errors.Is(err, ErrTooLarge) }},
		{"bad json", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `<html>gateway</html>`) },
			func(err error) bool { return strings.Contains(err.Error(), "not query_range JSON") }},
		{"matrix", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		}, func(err error) bool { return strings.Contains(err.Error(), "want streams") }},
		{"status error", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"status":"error","data":{}}`)
		}, func(err error) bool { return strings.Contains(err.Error(), `status "error"`) }},
		{"short pair", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1"]]}]}}`)
		}, func(err error) bool { return strings.Contains(err.Error(), "1 elements") }},
		{"bad ts", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["soon","x"]]}]}}`)
		}, func(err error) bool { return strings.Contains(err.Error(), "not nanoseconds") }},
		{"numeric line", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1",5]]}]}}`)
		}, func(err error) bool { return strings.Contains(err.Error(), "line is not a string") }},
		{"redirect elsewhere", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://example.invalid/x", http.StatusFound)
		}, func(err error) bool { return strings.Contains(err.Error(), "another host refused") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTest(t, tc.h)
			_, err := c.WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
			if err == nil || !tc.want(err) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestWorkerLogsTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	c, err := New(srv.URL, sel, "", WithTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.WorkerLogs(context.Background(), LogQuery{Connector: "c", Task: -1})
	if err == nil || strings.Contains(err.Error(), "query=") {
		t.Fatalf("err = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c2, _ := New(srv.URL, sel, "")
	if _, err := c2.WorkerLogs(ctx, LogQuery{Connector: "c", Task: -1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx deadline: err = %v", err)
	}
}

func TestWorkerLogsBadQuery(t *testing.T) {
	called := false
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) { called = true })
	for _, q := range []LogQuery{
		{Connector: "", Task: -1},
		{Connector: strings.Repeat("a", 250), Task: -1},
		{Connector: "a\nb", Task: -1},
		{Connector: "c", Task: -2},
		{Connector: "c", Task: MaxTask + 1},
		{Connector: "c", Task: -1, MinLevel: "LOUD"},
		{Connector: "c", Task: -1, Since: t0.Add(time.Minute)},
		{Connector: "c", Task: -1, Since: t0.Add(-8 * 24 * time.Hour)},
		{Connector: "c", Task: -1, Limit: 201},
		{Connector: "c", Task: -1, Limit: -1},
	} {
		if _, err := c.WorkerLogs(context.Background(), q); !errors.Is(err, ErrBadQuery) {
			t.Errorf("%+v: err = %v", q, err)
		}
	}
	if called {
		t.Error("a bad query reached Loki")
	}
}

func TestLevelAndLogger(t *testing.T) {
	cases := []struct{ line, level, logger string }{
		{"2026-10-23 10:00:00,123 WARN  x [io.debezium.X]", "WARN", "io.debezium.X"},
		{"[2026-10-23 10:00:00,123] ERROR msg (org.apache.kafka.connect.runtime.WorkerTask:212)\n\tat a.b(C.java:1)", "ERROR", "org.apache.kafka.connect.runtime.WorkerTask"},
		{"no level here", "", ""},
		{strings.Repeat("p", 70) + " ERROR late", "", ""},
		{"SEVERE x", "ERROR", ""},
	}
	for _, tc := range cases {
		if l := lineLevel(tc.line); l != tc.level {
			t.Errorf("%q: level %q", tc.line, l)
		}
		if l := loggerOf(tc.line); l != tc.logger {
			t.Errorf("%q: logger %q", tc.line, l)
		}
	}
}
