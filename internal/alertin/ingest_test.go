package alertin

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func signedHeaders(t *testing.T, src Source, body []byte, now time.Time) http.Header {
	t.Helper()
	switch src {
	case Alertmanager:
		return hdr("Authorization", "Bearer am-token")
	case Grafana:
		ts := strconv.FormatInt(now.Unix(), 10)
		return hdr(DefaultGrafanaSignatureHeader, hexMAC("grafana-secret", ts, ":", string(body)), DefaultGrafanaTimestampHeader, ts)
	case PagerDuty:
		return hdr(PagerDutySignatureHeader, "v1="+hexMAC("pd-secret", string(body)))
	case Datadog:
		return hdr(DefaultDatadogTokenHeader, "dd-token")
	case Generic:
		return hdr(GenericSignatureHeader, SignGeneric("generic-secret", now, body))
	}
	t.Fatalf("no headers for %s", src)
	return nil
}

func TestIngestEverySource(t *testing.T) {
	pdTime := time.Date(2020, 10, 2, 18, 50, 0, 0, time.UTC) // 5 min after the official example
	tests := []struct {
		src     Source
		fixture string
		now     time.Time
	}{
		{Alertmanager, "alertmanager.json", testNow},
		{Grafana, "grafana.json", testNow},
		{PagerDuty, "pagerduty_triggered.json", pdTime},
		{Datadog, "datadog.json", testNow},
	}
	for _, tt := range tests {
		t.Run(string(tt.src), func(t *testing.T) {
			cfg := testConfig()
			cfg.Now = func() time.Time { return tt.now }
			body := fixture(t, tt.fixture)
			a, err := cfg.Ingest(tt.src, signedHeaders(t, tt.src, body, tt.now), body)
			if err != nil {
				t.Fatalf("ingest: %v", err)
			}
			if a.Source != tt.src || a.Status != StatusFiring || a.DedupKey == "" {
				t.Fatalf("got %+v", a)
			}
		})
	}
	t.Run("generic", func(t *testing.T) {
		body := []byte(`{"title":"lag","pipeline_id":"orders"}`)
		a, err := testConfig().Ingest(Generic, signedHeaders(t, Generic, body, testNow), body)
		if err != nil || a.PipelineID != "orders" {
			t.Fatalf("ingest: %+v, %v", a, err)
		}
	})
}

func TestIngestRejects(t *testing.T) {
	pd := fixture(t, "pagerduty_triggered.json") // occurred_at 2020-10-02T18:45:22Z
	dd := fixture(t, "datadog.json")             // date 2026-10-11T10:58:00Z
	at := func(s string) func() time.Time {
		tm, _ := time.Parse(time.RFC3339, s)
		return func() time.Time { return tm }
	}
	tests := []struct {
		name string
		src  Source
		body []byte
		h    http.Header
		now  func() time.Time
		want error
	}{
		{"pd stale", PagerDuty, pd, signedHeaders(t, PagerDuty, pd, testNow), at("2020-10-02T20:00:00Z"), ErrStale},
		{"pd from the future", PagerDuty, pd, signedHeaders(t, PagerDuty, pd, testNow), at("2020-10-02T18:30:00Z"), ErrStale},
		{"dd stale", Datadog, dd, signedHeaders(t, Datadog, dd, testNow), at("2026-10-11T12:00:00Z"), ErrStale},
		{"bad signature before parse", PagerDuty, []byte("not json"), hdr(PagerDutySignatureHeader, "v1=00"), nil, ErrUnauthenticated},
		{"too large", Generic, []byte(`"` + strings.Repeat("x", MaxBodyBytes) + `"`), hdr(), nil, ErrTooLarge},
		{"unknown source", Source("opsgenie"), []byte("{}"), hdr(), nil, ErrUnauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			if tt.now != nil {
				cfg.Now = tt.now
			}
			_, err := cfg.Ingest(tt.src, tt.h, tt.body)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestIngestIgnoredAndResolved(t *testing.T) {
	ping := []byte(`{"event":{"event_type":"pagey.ping"}}`)
	if _, err := testConfig().Ingest(PagerDuty, signedHeaders(t, PagerDuty, ping, testNow), ping); !errors.Is(err, ErrIgnored) {
		t.Fatalf("ping: err = %v, want ErrIgnored", err)
	}
	resolved := []byte(`{"groupKey":"g","status":"resolved","alerts":[{"status":"resolved"}]}`)
	a, err := testConfig().Ingest(Alertmanager, signedHeaders(t, Alertmanager, resolved, testNow), resolved)
	if err != nil || a.Status != StatusResolved {
		t.Fatalf("resolved: %+v, %v", a, err)
	}
}
