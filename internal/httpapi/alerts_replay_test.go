package httpapi_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

func newAlertServerOpts(t *testing.T, cfg alertin.Config, opts ...httpapi.Option) (*httptest.Server, *room.Service) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	api := httpapi.New(svc, st, nil, append([]httpapi.Option{httpapi.WithAlertIngest(cfg)}, opts...)...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv, svc
}

func roomEventCount(t *testing.T, svc *room.Service, id string) int {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	return len(evs)
}

// An identical copy of a verified delivery (same signature, same body) is
// not processed again: 202 ignored, no new event in the room, no snapshot.
func TestSignedAlertReplayIsIgnored(t *testing.T) {
	for _, c := range alertCases() {
		t.Run(string(c.src), func(t *testing.T) {
			srv, svc := newAlertServer(t, alertCfg(c.now))
			body := c.firing(t)
			h := c.sign(c.secret, body, c.now)
			resp := postAlert(t, srv, c.src, h, body)
			wantStatus(t, resp, http.StatusCreated)
			snap := decode[room.RoomSnapshot](t, resp)
			n := roomEventCount(t, svc, snap.ID)

			for i := 0; i < 5; i++ {
				resp = postAlert(t, srv, c.src, h, body)
				wantStatus(t, resp, http.StatusAccepted)
				got := decode[map[string]any](t, resp)
				if got["ignored"] != true || got["reason"] != "duplicate delivery" || got["id"] != nil {
					t.Fatalf("replay body = %v", got)
				}
			}
			if m := roomEventCount(t, svc, snap.ID); m != n {
				t.Fatalf("replays appended %d events", m-n)
			}
		})
	}
}

// Bearer sources sign nothing, so identical bodies are only folded together
// for DefaultBearerDedupWindow; after it a repeat joins the room again.
func TestBearerAlertRepeatAfterWindowJoins(t *testing.T) {
	c := alertCases()[0] // Alertmanager
	var mu sync.Mutex
	now := c.now
	cfg := alertCfg(c.now)
	cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	srv, _ := newAlertServer(t, cfg)
	body := c.firing(t)
	h := c.sign(c.secret, body, c.now)
	wantStatus(t, postAlert(t, srv, c.src, h, body), http.StatusCreated)
	wantStatus(t, postAlert(t, srv, c.src, h, body), http.StatusAccepted)
	mu.Lock()
	now = now.Add(alertin.DefaultBearerDedupWindow)
	mu.Unlock()
	wantStatus(t, postAlert(t, srv, c.src, h, body), http.StatusOK)
}

// Verified alerts per source are rate limited: past the limit 429
// rate_limited with Retry-After, and nothing is opened. Other sources keep
// their own budget.
func TestSignedAlertRateLimit(t *testing.T) {
	gen := alertCases()[4]
	srv, svc := newAlertServerOpts(t, alertCfg(gen.now), httpapi.WithAlertRateLimit(3, time.Minute))
	for i := 0; i < 3; i++ {
		body := []byte(fmt.Sprintf(`{"title":"t%d","pipeline_id":"orders","dedup_key":"k%d"}`, i, i))
		wantStatus(t, postAlert(t, srv, gen.src, gen.sign(gen.secret, body, gen.now), body), http.StatusCreated)
	}
	body := []byte(`{"title":"t9","pipeline_id":"orders","dedup_key":"k9"}`)
	resp := postAlert(t, srv, gen.src, gen.sign(gen.secret, body, gen.now), body)
	wantStatus(t, resp, http.StatusTooManyRequests)
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After")
	}
	if got := decode[map[string]any](t, resp); got["error"].(map[string]any)["code"] != "rate_limited" {
		t.Fatalf("429 body = %v", got)
	}
	if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 3 {
		t.Fatalf("rooms = %d, want 3", len(rooms))
	}
	am := alertCases()[0]
	amBody := am.firing(t)
	wantStatus(t, postAlert(t, srv, am.src, am.sign(am.secret, amBody, am.now), amBody), http.StatusCreated)
	// Bad signatures do not use up the budget of the real sender.
	srv2, _ := newAlertServerOpts(t, alertCfg(gen.now), httpapi.WithAlertRateLimit(1, time.Minute))
	for i := 0; i < 3; i++ {
		resp := postAlert(t, srv2, gen.src, gen.sign("wrong", body, gen.now), body)
		wantStatus(t, resp, http.StatusUnauthorized)
		resp.Body.Close()
	}
	wantStatus(t, postAlert(t, srv2, gen.src, gen.sign(gen.secret, body, gen.now), body), http.StatusCreated)
}

// A rate-limited delivery is not remembered as seen: the sender's retry of the
// same bytes after the window is processed, not dropped as a copy.
func TestRateLimitedAlertRetryIsNotDuplicate(t *testing.T) {
	gen := alertCases()[4]
	srv, _ := newAlertServerOpts(t, alertCfg(gen.now), httpapi.WithAlertRateLimit(1, time.Hour))
	first := []byte(`{"title":"a","pipeline_id":"orders","dedup_key":"a"}`)
	wantStatus(t, postAlert(t, srv, gen.src, gen.sign(gen.secret, first, gen.now), first), http.StatusCreated)
	body := []byte(`{"title":"b","pipeline_id":"orders","dedup_key":"b"}`)
	h := gen.sign(gen.secret, body, gen.now)
	wantStatus(t, postAlert(t, srv, gen.src, h, body), http.StatusTooManyRequests)
	// Still limited, but answered 429 again, not 202 duplicate.
	wantStatus(t, postAlert(t, srv, gen.src, h, body), http.StatusTooManyRequests)
}

// Headers are checked before the body is read: a wrong bearer token, or a
// missing or malformed signature header, is 401 even with a body over the
// limit (not 413).
func TestSignedAlertBadHeadersRefusedBeforeBody(t *testing.T) {
	now := time.Date(2026, 10, 11, 11, 0, 0, 0, time.UTC)
	srv, _ := newAlertServer(t, alertCfg(now))
	big := []byte(`"` + strings.Repeat("x", 2*alertin.MaxBodyBytes) + `"`)
	ts := fmt.Sprint(now.Unix())
	tests := []struct {
		name string
		src  alertin.Source
		h    http.Header
	}{
		{"wrong bearer", alertin.Alertmanager, hdrs("Authorization", "Bearer nope")},
		{"missing bearer", alertin.Alertmanager, http.Header{}},
		{"wrong datadog token", alertin.Datadog, hdrs(alertin.DefaultDatadogTokenHeader, "nope")},
		{"generic missing", alertin.Generic, http.Header{}},
		{"generic garbage", alertin.Generic, hdrs(alertin.GenericSignatureHeader, "garbage")},
		{"generic stale", alertin.Generic, hdrs(alertin.GenericSignatureHeader, "t=1,v1="+macHex("x", "y"))},
		{"grafana garbage", alertin.Grafana, hdrs(alertin.DefaultGrafanaSignatureHeader, "zz", alertin.DefaultGrafanaTimestampHeader, ts)},
		{"pagerduty missing", alertin.PagerDuty, http.Header{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := postAlert(t, srv, tt.src, tt.h, big)
			wantStatus(t, resp, http.StatusUnauthorized)
			resp.Body.Close()
		})
	}
	// A well-formed (but unverifiable until read) signature with a huge body
	// is still 413.
	gen := alertCases()[4]
	resp := postAlert(t, srv, gen.src, gen.sign(gen.secret, big, now), big)
	wantStatus(t, resp, http.StatusRequestEntityTooLarge)
	resp.Body.Close()
}
