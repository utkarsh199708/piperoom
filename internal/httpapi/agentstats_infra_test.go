package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/httpapi"
	"piperoom/internal/store/memstore"
)

// countingSamples records each read, so a test can prove a refused request
// never reached the sample store.
type countingSamples struct {
	clients.SampleReader
	mu    sync.Mutex
	reads []string
}

func (c *countingSamples) Samples(ctx context.Context, pid, metric string, since time.Time) ([]clients.SamplePoint, error) {
	c.mu.Lock()
	c.reads = append(c.reads, pid+"/"+metric+"/"+since.UTC().Format(time.RFC3339))
	c.mu.Unlock()
	return c.SampleReader.Samples(ctx, pid, metric, since)
}

func (c *countingSamples) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.reads...)
}

// rawAgentStats fetches agent-stats and returns its body as a JSON object.
func rawAgentStats(t *testing.T, url, user string) map[string]json.RawMessage {
	t.Helper()
	resp := doJSON(t, http.MethodGet, url, user, "")
	wantStatus(t, resp, http.StatusOK)
	return decode[map[string]json.RawMessage](t, resp)
}

// E1/TC1 over the route: the infra block comes from the last 7 days of
// samples whatever ?days= is; the F11 fields are still there.
func TestAgentStatsInfraRoute(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Hour)
	start := now.Add(-48 * time.Hour)
	add := func(metric string, at time.Time, v float64) {
		t.Helper()
		if err := st.AppendSample(ctx, "orders", metric, at, v); err != nil {
			t.Fatal(err)
		}
	}
	// Older than the 7-day window: must not count (it would add 100 GB-days).
	add(clients.WatchMetricSlotWALBytes, now.Add(-8*24*time.Hour), 2400e9)
	add(clients.WatchMetricSlotWALBytes, now.Add(-8*24*time.Hour+time.Hour), 2400e9)
	// 10 GB for 24 h, then 20 GB for 24 h; the newest sample (now) holds nothing.
	for h := 0; h <= 48; h++ {
		v := 10e9
		if h >= 24 {
			v = 20e9
		}
		add(clients.WatchMetricSlotWALBytes, start.Add(time.Duration(h)*time.Hour), v)
	}
	add(clients.WatchMetricDLQRecords, now.Add(-2*time.Hour), 7)
	add(clients.WatchMetricDLQRecords, now.Add(-time.Hour), 1200)
	add(clients.WatchMetricTopicOldestUnreadAge, now.Add(-time.Hour), 5400)
	add(clients.WatchMetricGroupLag, now.Add(-time.Hour), 99) // not an infra metric
	reader := &countingSamples{SampleReader: st}
	srv := newTrendsServer(t, reader)

	body := rawAgentStats(t, srv.URL+"/api/pipelines/orders/agent-stats?days=1", "nolayers")
	var infra httpapi.AgentStatsInfra
	if err := json.Unmarshal(body["infra"], &infra); err != nil {
		t.Fatalf("infra %s: %v", body["infra"], err)
	}
	if infra.WALRetainedGBDays == nil || *infra.WALRetainedGBDays != 30 {
		t.Fatalf("GB-days: %s", body["infra"])
	}
	if infra.DLQRecords == nil || *infra.DLQRecords != 1200 || infra.OldestUnreadAgeS == nil || *infra.OldestUnreadAgeS != 5400 {
		t.Fatalf("newest values: %s", body["infra"])
	}
	if infra.Samples != 49+2+1 || !infra.From.Equal(start) || !infra.To.Equal(now) {
		t.Fatalf("window: %s", body["infra"])
	}
	if string(body["window_days"]) != "1" || string(body["pipeline_id"]) != `"orders"` {
		t.Fatalf("F11 fields: window_days=%s pipeline_id=%s", body["window_days"], body["pipeline_id"])
	}
	// Exactly the three infra metrics, each since now − 7 d.
	reads := reader.snapshot()
	if len(reads) != 3 {
		t.Fatalf("reads = %v", reads)
	}
	for _, r := range reads {
		since, err := time.Parse(time.RFC3339, r[strings.LastIndex(r, "/")+1:])
		age := time.Since(since)
		if err != nil || age < 7*24*time.Hour-time.Minute || age > 7*24*time.Hour+time.Minute {
			t.Fatalf("read %q is not the 7-day window", r)
		}
	}
}

// TC2: no samples, or no sample reader at all, is `infra: null`, not zeros.
func TestAgentStatsInfraNoHistory(t *testing.T) {
	for name, r := range map[string]clients.SampleReader{"empty store": memstore.New(), "no reader": nil} {
		t.Run(name, func(t *testing.T) {
			srv := newTrendsServer(t, r)
			body := rawAgentStats(t, srv.URL+"/api/pipelines/orders/agent-stats", "arjun")
			if got, ok := body["infra"]; !ok || string(got) != "null" {
				t.Fatalf("infra = %q (present %v)", got, ok)
			}
		})
	}
}

// E5: a sample store failure is a 500 with the generic message, never the
// store's text, and never a silent `infra: null`.
func TestAgentStatsInfraStoreError(t *testing.T) {
	srv := newTrendsServer(t, failingSamples{})
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/orders/agent-stats", "arjun", "")
	wantErr(t, resp, http.StatusInternalServerError, "internal_error")
}

// E2/E3: an unknown pipeline, another tenant's pipeline and a bad ?days= are
// refused before any sample read; an allowed read touches only its pipeline.
func TestAgentStatsInfraRefusedBeforeRead(t *testing.T) {
	reader := &countingSamples{SampleReader: memstore.New()}
	e := newTenancyEnv(t, true, httpapi.WithSamples(reader))
	e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/pa/agent-stats", "", http.StatusNotFound)
	e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/zzz/agent-stats", "", http.StatusNotFound)
	e.mustDo(t, "ann", http.MethodGet, "/api/pipelines/pa/agent-stats?days=0", "", http.StatusBadRequest)
	if reads := reader.snapshot(); len(reads) != 0 {
		t.Fatalf("refused requests read samples: %v", reads)
	}
	body := e.mustDo(t, "ann", http.MethodGet, "/api/pipelines/pa/agent-stats", "", http.StatusOK)
	if !strings.Contains(body, `"infra":null`) {
		t.Fatalf("body = %s", body)
	}
	reads := reader.snapshot()
	if len(reads) != 3 {
		t.Fatalf("reads = %v", reads)
	}
	for _, r := range reads {
		if !strings.HasPrefix(r, "pa/") {
			t.Fatalf("read another pipeline's samples: %q", r)
		}
	}
}
