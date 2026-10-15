package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
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

// The room gauges come from the service's in-memory state: an open room shows
// in piperoom_rooms_open, and the agent slot series are always present.
func TestMetricsRoomGauges(t *testing.T) {
	srv, svc := newServer(t)
	u, _ := svc.LookupUser("arjun")
	if _, err := svc.CreateRoom(context.Background(), u, "t", "sev2", "orders"); err != nil {
		t.Fatal(err)
	}
	_, body := scrape(t, srv.URL)
	for _, want := range []string{
		"# TYPE piperoom_rooms_open gauge\npiperoom_rooms_open 1\n",
		"# TYPE piperoom_agent_slots_in_use gauge\npiperoom_agent_slots_in_use ",
		"# TYPE piperoom_agent_rooms_queued gauge\npiperoom_agent_rooms_queued 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
}

// piperoom_ws_connections counts the room-list sockets too: the web room list
// always opens one, so a scrape with one list and one room socket reads 2.
func TestMetricsWSConnectionsCountListSockets(t *testing.T) {
	srv, svc := newServer(t)
	u, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), u, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Replace(srv.URL, "http", "ws", 1)
	for _, path := range []string{"/api/rooms/ws", "/api/rooms/" + snap.ID + "/ws?after=0"} {
		conn, _, err := websocket.Dial(context.Background(), base+path, &websocket.DialOptions{
			HTTPHeader: http.Header{"X-User": {"arjun"}},
		})
		if err != nil {
			t.Fatalf("dial %s: %v", path, err)
		}
		t.Cleanup(func() { conn.CloseNow() })
	}
	// Dial returns before the server registers the connection, so poll.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, body := scrape(t, srv.URL)
		if strings.Contains(body, "piperoom_ws_connections 2\n") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("piperoom_ws_connections never reached 2:\n%s", body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
