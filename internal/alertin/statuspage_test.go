package alertin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/connprobe"
	"piperoom/internal/store/memstore"
)

var feedNow = time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)

// feedServer serves whatever body holds; status 0 = 200.
type feedServer struct {
	*httptest.Server
	mu     sync.Mutex
	body   []byte
	status int
}

func newFeedServer(t *testing.T, body []byte) *feedServer {
	t.Helper()
	f := &feedServer{body: body}
	f.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.status != 0 {
			w.WriteHeader(f.status)
			return
		}
		_, _ = w.Write(f.body)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *feedServer) set(body []byte, status int) {
	f.mu.Lock()
	f.body, f.status = body, status
	f.mu.Unlock()
}

type pollEnv struct {
	p     *StatusPoller
	store *memstore.Store
	logs  *bytes.Buffer
	now   time.Time
}

func newPollEnv(t *testing.T, st *memstore.Store, client *http.Client, feeds ...VendorFeed) *pollEnv {
	t.Helper()
	e := &pollEnv{store: st, logs: &bytes.Buffer{}, now: feedNow}
	p, err := NewStatusPoller(StatusPollerConfig{
		Feeds: feeds, Writer: st, Client: client,
		Log: slog.New(slog.NewTextHandler(e.logs, nil)),
		Now: func() time.Time { return e.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	e.p = p
	return e
}

func (e *pollEnv) rows(t *testing.T) []clients.PipelineChange {
	t.Helper()
	got, err := e.store.ListChanges(context.Background(), "orders", time.Time{}, e.now.Add(time.Hour), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func confluentFeed(url string) VendorFeed {
	return VendorFeed{PipelineID: "orders", URL: url, Component: "confluent-cloud"}
}

// TC4: one active incident in the fixture is one vendor_incident row; the
// resolved one, the maintenance and the one older than 7 days are not rows.
func TestStatusPollerRecordsActiveIncident(t *testing.T) {
	srv := newFeedServer(t, fixture(t, "statuspage.rss"))
	e := newPollEnv(t, memstore.New(), srv.Client(), confluentFeed(srv.URL+"/history.rss"))
	e.p.PollOnce(context.Background())
	rows := e.rows(t)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want 1", rows)
	}
	c := rows[0]
	if c.Kind != clients.ChangeVendorIncident || c.Source != clients.ChangeSourceStatuspage || c.Object != "confluent-cloud" ||
		c.Summary != "Elevated produce latency in AWS us-east-1" || c.Actor != "" ||
		!c.At.Equal(time.Date(2026, 10, 20, 13, 58, 0, 0, time.UTC)) {
		t.Fatalf("row = %+v", c)
	}
	var ext clients.ExternalChange
	if err := json.Unmarshal(c.After, &ext); err != nil {
		t.Fatal(err)
	}
	if ext != (clients.ExternalChange{URL: "https://status.confluent.cloud/incidents/abc123", Service: "confluent-cloud", Status: clients.VendorMonitoring}) {
		t.Fatalf("after = %+v", ext)
	}
	if strings.Contains(e.logs.String(), "Elevated") {
		t.Fatalf("feed text was logged: %s", e.logs)
	}
}

// TC4, E5: the same incident twice is one row, also across a restart (the
// store's dedup); a new status of it is a new row, its resolution is not.
func TestStatusPollerDeduplicates(t *testing.T) {
	srv := newFeedServer(t, fixture(t, "statuspage.rss"))
	st := memstore.New()
	e := newPollEnv(t, st, srv.Client(), confluentFeed(srv.URL))
	e.p.PollOnce(context.Background())
	e.p.PollOnce(context.Background())
	restarted := newPollEnv(t, st, srv.Client(), confluentFeed(srv.URL))
	restarted.p.PollOnce(context.Background())
	if n := len(e.rows(t)); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	if strings.Count(e.logs.String()+restarted.logs.String(), "stored=true") != 1 {
		t.Fatalf("logs:\n%s\n%s", e.logs, restarted.logs)
	}

	rss := string(fixture(t, "statuspage.rss"))
	srv.set([]byte(strings.Replace(rss, "&lt;strong&gt;Monitoring", "&lt;strong&gt;Identified", 1)), 0)
	e.p.PollOnce(context.Background())
	srv.set([]byte(strings.Replace(rss, "&lt;strong&gt;Monitoring", "&lt;strong&gt;Resolved", 1)), 0)
	e.p.PollOnce(context.Background())
	if n := len(e.rows(t)); n != 2 {
		t.Fatalf("rows = %d, want 2 (monitoring, identified)", n)
	}
}

// TC4, E6: an unreachable feed (down, 5xx, too large, not a feed) writes
// nothing and logs one warning an hour, without the feed's URL.
func TestStatusPollerUnreachableFeed(t *testing.T) {
	down := httptest.NewTLSServer(http.NotFoundHandler())
	client := down.Client()
	down.Close()
	e := newPollEnv(t, memstore.New(), client, confluentFeed(down.URL+"/history.rss?token=feed-token-123"))
	for _, after := range []time.Duration{0, 10 * time.Minute, 59 * time.Minute, 61 * time.Minute} {
		e.now = feedNow.Add(after)
		e.p.PollOnce(context.Background())
	}
	if n := len(e.rows(t)); n != 0 {
		t.Fatalf("rows = %d", n)
	}
	logs := e.logs.String()
	if got := strings.Count(logs, "vendor status feed unreachable"); got != 2 {
		t.Fatalf("warnings = %d, want 2 (one per hour):\n%s", got, logs)
	}
	if strings.Contains(logs, "feed-token-123") || strings.Contains(logs, "history.rss") {
		t.Fatalf("the feed URL was logged: %s", logs)
	}

	srv := newFeedServer(t, nil)
	for name, set := range map[string]func(){
		"500":        func() { srv.set(nil, http.StatusInternalServerError) },
		"not a feed": func() { srv.set([]byte("<html><body>maintenance"), 0) },
		"too large": func() {
			srv.set([]byte("<rss><channel>"+strings.Repeat(" ", MaxStatusFeedBytes)+"</channel></rss>"), 0)
		},
	} {
		t.Run(name, func(t *testing.T) {
			set()
			e := newPollEnv(t, memstore.New(), srv.Client(), confluentFeed(srv.URL))
			e.p.PollOnce(context.Background())
			if n := len(e.rows(t)); n != 0 || !strings.Contains(e.logs.String(), "unreachable") {
				t.Fatalf("rows = %d, logs = %s", n, e.logs)
			}
		})
	}
}

// Atom feeds (statuspage.io history.atom) work the same way.
func TestStatusPollerAtom(t *testing.T) {
	srv := newFeedServer(t, fixture(t, "statuspage.atom"))
	e := newPollEnv(t, memstore.New(), srv.Client(), VendorFeed{PipelineID: "orders", URL: srv.URL, Component: "aiven-kafka"})
	e.p.PollOnce(context.Background())
	rows := e.rows(t)
	if len(rows) != 1 || rows[0].Object != "aiven-kafka" || !rows[0].At.Equal(time.Date(2026, 10, 20, 13, 40, 0, 0, time.UTC)) {
		t.Fatalf("rows = %+v", rows)
	}
	var ext clients.ExternalChange
	if err := json.Unmarshal(rows[0].After, &ext); err != nil || ext.Status != clients.VendorIdentified || ext.URL != "https://status.aiven.io/incidents/777" {
		t.Fatalf("after = %s (%v)", rows[0].After, err)
	}
}

// TC3: a hostile feed title (markup, 10 KB, control characters) is capped
// and stored as text; a link the contract refuses drops the row.
func TestStatusPollerHostileItem(t *testing.T) {
	title := "&lt;script&gt;x&lt;/script&gt;\n\t" + strings.Repeat("é", 10<<10)
	item := func(id, link string) string {
		return "<item><title>" + title + "</title><description>&lt;strong&gt;Investigating&lt;/strong&gt;</description>" +
			"<pubDate>Tue, 20 Oct 2026 13:00:00 +0000</pubDate><link>" + link + "</link><guid>" + id + "</guid></item>"
	}
	feed := "<rss><channel>" + item("a", "https://status.example.com/a") + item("b", "javascript:alert(1)") + "</channel></rss>"
	srv := newFeedServer(t, []byte(feed))
	e := newPollEnv(t, memstore.New(), srv.Client(), confluentFeed(srv.URL))
	e.p.PollOnce(context.Background())
	rows := e.rows(t)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1 (the javascript: link is dropped)", len(rows))
	}
	s := rows[0].Summary
	if utf8.RuneCountInString(s) != MaxChangeTitleRunes || !strings.HasPrefix(s, "<script>x</script>  é") ||
		strings.ContainsFunc(s, unicode.IsControl) {
		t.Fatalf("summary = %q", s)
	}
}

func TestNewStatusPollerRefusesBadFeeds(t *testing.T) {
	st := memstore.New()
	for name, f := range map[string]VendorFeed{
		"http":            {PipelineID: "orders", URL: "http://status.confluent.cloud/history.rss"},
		"user info":       {PipelineID: "orders", URL: "https://u:p@status.confluent.cloud/history.rss"},
		"relative":        {PipelineID: "orders", URL: "/history.rss"},
		"bad pipeline":    {PipelineID: "../x", URL: "https://status.confluent.cloud/history.rss"},
		"component ctrl":  {PipelineID: "orders", URL: "https://status.confluent.cloud/", Component: "a\nb"},
		"component >128B": {PipelineID: "orders", URL: "https://status.confluent.cloud/", Component: strings.Repeat("c", 129)},
	} {
		if _, err := NewStatusPoller(StatusPollerConfig{Feeds: []VendorFeed{f}, Writer: st}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := NewStatusPoller(StatusPollerConfig{}); err == nil {
		t.Error("no writer: want an error")
	}
}

func TestVendorComponent(t *testing.T) {
	for host, want := range map[string]string{
		"status.confluent.cloud":   "confluent-cloud",
		"STATUS.Confluent.Cloud":   "confluent-cloud",
		"status.aiven.io":          "aiven-kafka",
		"health.aws.amazon.com":    "msk",
		"status.example.com":       "status.example.com",
		"evilconfluent.cloud.test": "evilconfluent.cloud.test",
	} {
		if got := vendorComponent(host); got != want {
			t.Errorf("%s: got %q, want %q", host, got, want)
		}
	}
}

func TestStatusFeedRedirects(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://status.example.com/", nil)
	if httpsRedirectsOnly(req, nil) == nil {
		t.Error("a redirect to http must be refused")
	}
	req = httptest.NewRequest(http.MethodGet, "https://status.example.com/", nil)
	if httpsRedirectsOnly(req, make([]*http.Request, 5)) == nil {
		t.Error("a sixth redirect must be refused")
	}
	if err := httpsRedirectsOnly(req, nil); err != nil {
		t.Errorf("https: %v", err)
	}
}

// Run polls at once and returns when its context ends (no leaked goroutine).
func TestStatusPollerRunStops(t *testing.T) {
	srv := newFeedServer(t, fixture(t, "statuspage.rss"))
	st := memstore.New()
	p, err := NewStatusPoller(StatusPollerConfig{Feeds: []VendorFeed{confluentFeed(srv.URL)}, Writer: st, Client: srv.Client(),
		Interval: 10 * time.Millisecond, Now: func() time.Time { return feedNow }, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	deadline := time.After(5 * time.Second)
	for {
		rows, _ := st.ListChanges(context.Background(), "orders", time.Time{}, feedNow.Add(time.Hour), nil, 10)
		if len(rows) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run did not poll")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// The default client dials public addresses only: a feed on a loopback,
// private or metadata address is refused before any connection, so neither
// the owners.yaml URL nor a redirect reaches an internal endpoint (SSRF).
func TestStatusFeedDefaultClientRefusesInternalAddresses(t *testing.T) {
	srv := newFeedServer(t, nil) // on 127.0.0.1
	p, err := NewStatusPoller(StatusPollerConfig{Feeds: []VendorFeed{confluentFeed(srv.URL)}, Writer: memstore.New()})
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{srv.URL + "/history.rss", "https://169.254.169.254/latest/meta-data/",
		"https://10.0.0.1/feed", "https://[::1]:8443/feed", "https://metadata.google.internal/feed"} {
		if _, err := p.fetch(context.Background(), u); !errors.Is(err, connprobe.ErrBlockedAddress) {
			t.Errorf("%s: err = %v, want connprobe.ErrBlockedAddress", u, err)
		}
	}
	tr, ok := p.cfg.Client.Transport.(*http.Transport)
	if !ok || tr.Proxy != nil || p.cfg.Client.CheckRedirect == nil || p.cfg.Client.Timeout != StatusFeedTimeout {
		t.Fatalf("default client: %+v", p.cfg.Client)
	}
}
