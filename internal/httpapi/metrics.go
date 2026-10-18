package httpapi

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// pathMetrics is the Prometheus scrape path. Like the probes it sits outside
// /api: a scraper carries no session.
const pathMetrics = "/metrics"

// durationBuckets are the upper bounds (seconds) of the request duration
// histogram. They span a fast JSON read to the 3-minute hand-off budget.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 180}

// unmatchedRoute labels requests no route matched (the web UI's static files,
// 404s), so arbitrary paths never become label values.
const unmatchedRoute = "unmatched"

// reqKey is one HTTP series. Every field comes from a bounded set: the method
// is normalised, route is a chi pattern (never a concrete path, so no room or
// user id), class is "2xx".."5xx".
type reqKey struct {
	method, route, class string
}

type histogram struct {
	counts []uint64 // per bucket, not cumulative; len(durationBuckets)+1 (the +Inf bucket)
	sum    float64
	n      uint64
}

// metrics holds the process's counters and renders them in the Prometheus text
// exposition format (version 0.0.4) with the standard library only.
type metrics struct {
	start time.Time
	g     gauges

	mu       sync.Mutex
	requests map[reqKey]uint64
	duration map[reqKey]*histogram
}

// gauges read live values at scrape time; a nil func leaves its series out.
// None of them may call the store: a scrape every few seconds must not load
// the database.
type gauges struct {
	conns     func() int                 // live WebSocket connections
	agents    func() (inUse, queued int) // agent slots in use, rooms queued for one
	openRooms func() int                 // open rooms in the room cache
	approvals func() map[string]uint64   // approval.given appended here, by grant (a counter)
}

func newMetrics(start time.Time, g gauges) *metrics {
	return &metrics{start: start, g: g, requests: map[reqKey]uint64{}, duration: map[reqKey]*histogram{}}
}

// knownMethods bounds the method label; anything else is "other".
var knownMethods = map[string]bool{
	http.MethodGet: true, http.MethodHead: true, http.MethodPost: true, http.MethodPut: true,
	http.MethodPatch: true, http.MethodDelete: true, http.MethodOptions: true,
}

func (m *metrics) observe(method, route string, status int, d time.Duration) {
	if !knownMethods[method] {
		method = "other"
	}
	if route == "" {
		route = unmatchedRoute
	}
	if status == 0 {
		status = http.StatusOK // nothing written: net/http sends 200
	}
	class := "other"
	if status >= 100 && status < 600 {
		class = strconv.Itoa(status/100) + "xx"
	}
	k := reqKey{method: method, route: route, class: class}
	secs := d.Seconds()
	i, _ := slices.BinarySearch(durationBuckets, secs)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[k]++
	h := m.duration[k]
	if h == nil {
		h = &histogram{counts: make([]uint64, len(durationBuckets)+1)}
		m.duration[k] = h
	}
	h.counts[i]++
	h.sum += secs
	h.n++
}

// middleware counts every request by its route pattern. It reads the pattern
// after the handler ran, when chi has finished routing (sub-routers included).
func (m *metrics) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// chi's wrapper keeps Hijacker and Flusher, which the WebSocket
		// upgrade and streaming exports need.
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := ""
		if rc := chi.RouteContext(r.Context()); rc != nil {
			route = rc.RoutePattern()
		}
		// The catch-all static handler matches "/*" in some setups; keep that
		// in the unmatched bucket as well.
		if route == "/*" {
			route = unmatchedRoute
		}
		m.observe(r.Method, route, ww.Status(), time.Since(start))
	})
}

// handle serves GET /metrics.
func (m *metrics) handle(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	m.write(w, time.Now())
}

func (m *metrics) write(w io.Writer, now time.Time) {
	m.mu.Lock()
	keys := make([]reqKey, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b reqKey) int {
		return strings.Compare(a.route+"\x00"+a.method+"\x00"+a.class, b.route+"\x00"+b.method+"\x00"+b.class)
	})
	reqs := make([]uint64, len(keys))
	hists := make([]histogram, len(keys))
	for i, k := range keys {
		reqs[i] = m.requests[k]
		h := m.duration[k]
		hists[i] = histogram{counts: slices.Clone(h.counts), sum: h.sum, n: h.n}
	}
	m.mu.Unlock()

	fmt.Fprintln(w, "# HELP piperoom_http_requests_total HTTP requests by method, route pattern and status class.")
	fmt.Fprintln(w, "# TYPE piperoom_http_requests_total counter")
	for i, k := range keys {
		fmt.Fprintf(w, "piperoom_http_requests_total{%s} %d\n", k.labels(), reqs[i])
	}
	fmt.Fprintln(w, "# HELP piperoom_http_request_duration_seconds HTTP request duration by method, route pattern and status class.")
	fmt.Fprintln(w, "# TYPE piperoom_http_request_duration_seconds histogram")
	for i, k := range keys {
		h := hists[i]
		var cum uint64
		for b, le := range durationBuckets {
			cum += h.counts[b]
			fmt.Fprintf(w, "piperoom_http_request_duration_seconds_bucket{%s,le=\"%s\"} %d\n", k.labels(), formatFloat(le), cum)
		}
		fmt.Fprintf(w, "piperoom_http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", k.labels(), h.n)
		fmt.Fprintf(w, "piperoom_http_request_duration_seconds_sum{%s} %s\n", k.labels(), formatFloat(h.sum))
		fmt.Fprintf(w, "piperoom_http_request_duration_seconds_count{%s} %d\n", k.labels(), h.n)
	}
	if m.g.conns != nil {
		gauge(w, "piperoom_ws_connections", "Live WebSocket connections on this replica.", m.g.conns())
	}
	if m.g.agents != nil {
		inUse, queued := m.g.agents()
		gauge(w, "piperoom_agent_slots_in_use", "Agent runs holding a slot on this replica (PIPEROOM_MAX_AGENTS caps it).", inUse)
		gauge(w, "piperoom_agent_rooms_queued", "Rooms on this replica waiting for an agent slot.", queued)
	}
	if m.g.openRooms != nil {
		gauge(w, "piperoom_rooms_open", "Open rooms in this replica's room cache.", m.g.openRooms())
	}
	if m.g.approvals != nil {
		byGrant := m.g.approvals()
		grants := make([]string, 0, len(byGrant))
		for g := range byGrant {
			grants = append(grants, g)
		}
		slices.Sort(grants)
		fmt.Fprintln(w, "# HELP piperoom_approvals_total approval.given events this replica appended, by grant.")
		fmt.Fprintln(w, "# TYPE piperoom_approvals_total counter")
		for _, g := range grants {
			fmt.Fprintf(w, "piperoom_approvals_total{grant=%s} %d\n", quoteLabel(g), byGrant[g])
		}
	}
	fmt.Fprintln(w, "# HELP piperoom_goroutines Goroutines in this process.")
	fmt.Fprintln(w, "# TYPE piperoom_goroutines gauge")
	fmt.Fprintf(w, "piperoom_goroutines %d\n", runtime.NumGoroutine())
	fmt.Fprintln(w, "# HELP piperoom_uptime_seconds Seconds since this process built its HTTP handler.")
	fmt.Fprintln(w, "# TYPE piperoom_uptime_seconds gauge")
	fmt.Fprintf(w, "piperoom_uptime_seconds %s\n", formatFloat(now.Sub(m.start).Seconds()))
}

func gauge(w io.Writer, name, help string, v int) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
}

func (k reqKey) labels() string {
	return fmt.Sprintf("method=%s,route=%s,code=%s", quoteLabel(k.method), quoteLabel(k.route), quoteLabel(k.class))
}

// quoteLabel escapes a label value per the text format: backslash, double
// quote and newline.
func quoteLabel(v string) string {
	return `"` + labelEscaper.Replace(v) + `"`
}

// labelEscaper is built once: a strings.Replacer is immutable and safe for
// concurrent use, and rebuilding it per scrape label is wasted work.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
