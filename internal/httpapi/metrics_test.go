package httpapi_test

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func scrape(t *testing.T, url string) (http.Header, string) {
	t.Helper()
	resp, err := http.Get(url + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	return resp.Header, string(b)
}

func TestMetricsTextFormat(t *testing.T) {
	srv, _ := newProbeServer(t)
	// Two concrete room ids and a junk path: none may become a label value.
	for _, p := range []string{"/api/rooms/room-secret-1", "/api/rooms/room-secret-2", "/no/such/path-xyz", "/healthz"} {
		resp, err := http.Get(srv.URL + p)
		if err != nil {
			t.Fatalf("GET %s: %v", p, err)
		}
		resp.Body.Close()
	}
	hdr, body := scrape(t, srv.URL)
	if ct := hdr.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("Content-Type = %q", ct)
	}
	for _, want := range []string{
		"# TYPE piperoom_http_requests_total counter",
		`piperoom_http_requests_total{method="GET",route="/healthz",code="2xx"} 1`,
		`piperoom_http_requests_total{method="GET",route="/api/rooms/{id}",code="4xx"} 2`,
		`piperoom_http_requests_total{method="GET",route="unmatched",code="4xx"} 1`,
		"# TYPE piperoom_http_request_duration_seconds histogram",
		`piperoom_http_request_duration_seconds_bucket{method="GET",route="/healthz",code="2xx",le="+Inf"} 1`,
		`piperoom_http_request_duration_seconds_count{method="GET",route="/api/rooms/{id}",code="4xx"} 2`,
		"# TYPE piperoom_ws_connections gauge",
		"piperoom_ws_connections 0",
		"# TYPE piperoom_goroutines gauge",
		"# TYPE piperoom_uptime_seconds gauge",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
	for _, leak := range []string{"room-secret", "path-xyz", "X-User", "arjun"} {
		if strings.Contains(body, leak) {
			t.Errorf("metrics leak %q (high-cardinality or identifying label)\n%s", leak, body)
		}
	}
}

// The histogram buckets are cumulative and end at the count, as Prometheus
// requires; a scraper rejects a series whose buckets decrease.
func TestMetricsHistogramCumulative(t *testing.T) {
	srv, _ := newProbeServer(t)
	for range 3 {
		resp, err := http.Get(srv.URL + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	_, body := scrape(t, srv.URL)
	prefix := `piperoom_http_request_duration_seconds_bucket{method="GET",route="/healthz",code="2xx",le=`
	last := -1
	n := 0
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		n++
		f := strings.Fields(line)
		v, err := strconv.Atoi(f[len(f)-1])
		if err != nil {
			t.Fatalf("bucket value: %s", line)
		}
		if v < last {
			t.Fatalf("bucket decreased: %s (previous %d)", line, last)
		}
		last = v
	}
	if n == 0 || last != 3 {
		t.Fatalf("buckets = %d, +Inf = %d, want +Inf 3\n%s", n, last, body)
	}
}

// A scraper reaches the pod by IP, so /metrics skips the Host allow-list like
// the probes; any other path on that Host still gets 421.
func TestMetricsSkipsHostGuard(t *testing.T) {
	srv, _ := newProbeServer(t)
	for path, want := range map[string]int{"/metrics": http.StatusOK, "/api/rooms": http.StatusMisdirectedRequest} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "10.1.2.3:8080"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s with Host 10.1.2.3 = %d, want %d", path, resp.StatusCode, want)
		}
	}
}
