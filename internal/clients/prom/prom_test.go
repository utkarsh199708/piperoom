package prom

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

func TestPromParseEscapes(t *testing.T) {
	in := "# HELP x help\n# TYPE x gauge\n\n" +
		`a_b:c{l1="plain",l2="q\"uote",l3="back\\slash",l4="new\nline",l5="}, {",} 1.5e3 1700000000` + "\n" +
		`bare 3` + "\n" +
		`inf_m +Inf` + "\n" +
		`nan_m NaN` + "\n" +
		`empty{} 7` + "\n" +
		`spaced{ a = "b" , c="d" } 2` + "\n"
	got, err := Parse(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("got %d samples: %+v", len(got), got)
	}
	a := got[0]
	want := map[string]string{"l1": "plain", "l2": `q"uote`, "l3": `back\slash`, "l4": "new\nline", "l5": "}, {"}
	if a.Name != "a_b:c" || a.Value != 1500 || len(a.Labels) != 5 {
		t.Fatalf("a = %+v", a)
	}
	for k, v := range want {
		if a.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, a.Labels[k], v)
		}
	}
	if got[1].Name != "bare" || got[1].Labels != nil || got[1].Value != 3 {
		t.Errorf("bare = %+v", got[1])
	}
	if got[5].Labels["a"] != "b" || got[5].Labels["c"] != "d" {
		t.Errorf("spaced = %+v", got[5])
	}
}

func TestPromParseMalformed(t *testing.T) {
	for _, in := range []string{
		"<html><body>nope</body></html>",
		`m{a="x" 1`,
		`m{a="unterminated} 1`,
		`m{a=x} 1`,
		`m{="x"} 1`,
		`m abc`,
		`m 1 2 3`,
		`m 1 notatime`,
		`{a="b"} 1`,
		`m`,
	} {
		if _, err := Parse(strings.NewReader(in)); err == nil || !strings.Contains(err.Error(), "line 1") {
			t.Errorf("%q: err = %v", in, err)
		}
	}
}

func TestPromParseFixture(t *testing.T) {
	f, err := os.Open("testdata/debezium.prom")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := Parse(f)
	if err != nil || len(got) != 18 {
		t.Fatalf("%d samples, err %v", len(got), err)
	}
}

func TestPromScrapeCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 5<<20/20; i++ { // 5 MB of valid lines
			_, _ = w.Write([]byte("metric_one{a=\"b\"} 1\n"))
		}
	}))
	defer srv.Close()
	_, err := NewClient().Scrape(context.Background(), srv.URL+"/metrics", 0)
	if !errors.Is(err, ErrTooLarge) || !strings.Contains(err.Error(), "4 MB") {
		t.Fatalf("err = %v, want ErrTooLarge naming 4 MB", err)
	}
	// Just under the cap works.
	small := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("m 1\n")) }))
	defer small.Close()
	got, err := NewClient().Scrape(context.Background(), small.URL, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestPromScrapeErrors(t *testing.T) {
	c := NewClient()
	notFound := httptest.NewServer(http.NotFoundHandler())
	defer notFound.Close()
	if _, err := c.Scrape(context.Background(), notFound.URL, 0); err == nil || !strings.Contains(err.Error(), "status 404") {
		t.Errorf("404: %v", err)
	}
	for _, u := range []string{"ftp://x/metrics", "not a url", "http://"} {
		if _, err := c.Scrape(context.Background(), u, 0); err == nil {
			t.Errorf("%q: want error", u)
		}
	}
	// A refused connection does not echo the URL (it may carry a token).
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL + "/metrics?token=hunter2"
	dead.Close()
	if _, err := c.Scrape(context.Background(), url, 0); err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("down: %v", err)
	}
	// A redirect to another host is refused.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("m 1\n")) }))
	defer other.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, http.StatusFound) }))
	defer redir.Close()
	if _, err := c.Scrape(context.Background(), redir.URL, 0); err == nil || !strings.Contains(err.Error(), "another host") {
		t.Errorf("redirect: %v", err)
	}
	// Cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Scrape(ctx, other.URL, 0); err == nil {
		t.Error("cancelled ctx: want error")
	}
}

func TestPromScrapeRecordsHostOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("m 1\n")) }))
	defer srv.Close()
	rec := tools.NewRecorder()
	if _, err := NewClient().Scrape(tools.WithRecorder(context.Background(), rec), srv.URL+"/metrics?token=hunter2", 0); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 1 || calls[0].Kind != tools.CallHTTP || calls[0].Text != "GET /metrics" || strings.Contains(calls[0].Target+calls[0].Text, "hunter2") {
		t.Errorf("calls = %+v", calls)
	}
}
