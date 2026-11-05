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
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/workerlogs/logql"
	"piperoom/internal/tools"
)

// TestCheckSelectorDelegates: WORKER_LOGS_SELECTOR and a pipeline's
// consumer_logs.selector (internal/owners) follow one rule, logql's.
func TestCheckSelectorDelegates(t *testing.T) {
	for _, s := range []string{sel, `{a="b"} |= "x"`, `{}`, `{a='b'}`, ` {a=~"x.*"} `, `{a="b"} | json`} {
		if (CheckSelector(s) == nil) != (logql.CheckSelector(s) == nil) {
			t.Errorf("%q: workerlogs and logql disagree", s)
		}
	}
}

const consumerSel = `{service="devconsumer"}`

// TestBuildStreamQueryHostile: a model's contains text stays one string
// literal of one |= filter, whatever it holds, so it can never add a stage
// or widen the selector.
func TestBuildStreamQueryHostile(t *testing.T) {
	for _, contains := range []string{
		`"} |= "x`, `x" | json | line_format "{{.password}}`, `orders"} or {job=~".+`, "back`tick\\\\", `"`, `\`, "order 1648",
	} {
		q := buildStreamQuery(sel, StreamQuery{Contains: contains, MinLevel: "WARN"})
		toks := tailTokens(t, q)
		if len(toks) != 6 || toks[0] != "|" || toks[1] != "=" || toks[3] != "|" || toks[4] != "~" {
			t.Fatalf("%q: tokens %q", contains, toks)
		}
		lit, err := strconv.Unquote(toks[2])
		if err != nil || lit != contains {
			t.Errorf("%q: literal %q (%v)", contains, lit, err)
		}
	}
	if q := buildStreamQuery(sel, StreamQuery{MinLevel: "INFO"}); q != sel {
		t.Errorf("no contains, INFO: %s", q)
	}
	if q := buildStreamQuery(sel, StreamQuery{MinLevel: "ERROR"}); q != sel+` |~ "\\b(ERROR|SEVERE|FATAL)\\b"` {
		t.Errorf("ERROR only: %s", q)
	}
}

func TestWithSelectorSharesLokiAndAuth(t *testing.T) {
	var auth, org, query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, org, query = r.Header.Get("Authorization"), r.Header.Get("X-Scope-OrgID"), r.URL.Query().Get("query")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"streams","result":[
		 {"stream":{"service":"devconsumer","container":"piperoom-dev-devconsumer-1","level":"ERROR"},"values":[
		   ["%s","time=2026-10-23T09:59:00.000Z level=ERROR msg=\"devconsumer stopped\" err=\"devconsumer: cannot process order 1648 at pg.public.orders/2@799 (poison pill)\""]
		 ]},
		 {"stream":{"service":"devconsumer","container":"piperoom-dev-devconsumer-1","level":"INFO"},"values":[
		   ["%s","time=2026-10-23T09:58:59.000Z level=INFO msg=\"devconsumer started\""]
		 ]}]}}`, ns(t0.Add(-time.Minute)), ns(t0.Add(-61*time.Second)))
	}))
	defer srv.Close()
	base, err := New(srv.URL, sel, "s3cret", WithOrgID("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	base.now = func() time.Time { return t0 }
	c, err := base.WithSelector(" " + consumerSel + " ")
	if err != nil {
		t.Fatal(err)
	}
	if c.Selector() != consumerSel || base.Selector() != sel {
		t.Fatalf("selectors: copy %q, base %q", c.Selector(), base.Selector())
	}
	rec := tools.NewRecorder()
	lines, err := c.StreamLogs(tools.WithRecorder(context.Background(), rec), StreamQuery{MinLevel: "INFO", Contains: "order"})
	if err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer s3cret" || org != "tenant-a" || query != consumerSel+` |= "order"` {
		t.Errorf("auth %q org %q query %q", auth, org, query)
	}
	if len(lines) != 2 || lines[0].Level != "INFO" || lines[1].Level != "ERROR" || lines[1].Worker != "piperoom-dev-devconsumer-1" {
		t.Fatalf("lines = %+v", lines)
	}
	if calls := rec.Calls(); len(calls) != 1 || calls[0].ParamsRedacted[0] != "query="+consumerSel+` |= "order"` {
		t.Errorf("provenance = %+v", calls)
	}
	// The default level is WARN, as for WorkerLogs: the INFO line is dropped.
	lines, err = c.StreamLogs(context.Background(), StreamQuery{})
	if err != nil || len(lines) != 1 || lines[0].Level != "ERROR" {
		t.Fatalf("default level: %+v %v", lines, err)
	}

	for _, bad := range []string{``, `{service="devconsumer"} |= "x"`, `{service=devconsumer}`} {
		if _, err := base.WithSelector(bad); !errors.Is(err, netsec.ErrInvalid) {
			t.Errorf("WithSelector(%q): err = %v", bad, err)
		}
	}
}

func TestStreamLogsBadQuery(t *testing.T) {
	called := false
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) { called = true })
	for _, q := range []StreamQuery{
		{Contains: strings.Repeat("a", MaxContainsBytes+1)},
		{Contains: "a\nb"},
		{MinLevel: "LOUD"},
		{Since: t0.Add(time.Minute)},
		{Since: t0.Add(-8 * 24 * time.Hour)},
		{Limit: MaxLines + 1},
		{Limit: -1},
	} {
		if _, err := c.StreamLogs(context.Background(), q); !errors.Is(err, ErrBadQuery) {
			t.Errorf("%+v: err = %v", q, err)
		}
	}
	if called {
		t.Error("a bad query reached Loki")
	}
}

func TestStreamLogsErrors(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusUnauthorized) })
	if _, err := c.StreamLogs(context.Background(), StreamQuery{}); !errors.Is(err, ErrAuth) {
		t.Errorf("401: %v", err)
	}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	slow, err := New(srv.URL, consumerSel, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := slow.StreamLogs(ctx, StreamQuery{}); !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "query=") {
		t.Fatalf("timeout: err = %v", err)
	}
}
