package httpapi_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Test secrets. Production secrets come from the environment; these only have
// to match between the signer and the server config.
const (
	amToken       = "am-token"
	grafanaSecret = "grafana-secret"
	pdSecret      = "pd-secret"
	ddToken       = "dd-token"
	genericSecret = "generic-secret"
)

// alertCfg returns an alertin.Config with every source enabled and a fixed
// clock. The PagerDuty service in the alertin fixture maps to "billing".
func alertCfg(now time.Time) alertin.Config {
	return alertin.Config{
		AlertmanagerToken: amToken,
		GrafanaSecret:     grafanaSecret,
		PagerDutySecret:   pdSecret,
		DatadogToken:      ddToken,
		GenericSecret:     genericSecret,
		PagerDutyServices: map[string]string{"PF9KMXH": "billing"},
		DefaultPipeline:   "orders",
		Now:               func() time.Time { return now },
	}
}

func newAlertServer(t *testing.T, cfg alertin.Config) (*httptest.Server, *room.Service) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	api := httpapi.New(svc, st, nil, httpapi.WithAlertIngest(cfg))
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv, svc
}

func fixtureBody(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "alertin", "testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

func macHex(secret string, parts ...string) string {
	m := hmac.New(sha256.New, []byte(secret))
	for _, p := range parts {
		m.Write([]byte(p))
	}
	return hex.EncodeToString(m.Sum(nil))
}

// postAlert sends body to /api/alerts/<src> with the given headers and NO
// X-User: the signature is the auth.
func postAlert(t *testing.T, srv *httptest.Server, src alertin.Source, h http.Header, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/alerts/"+string(src), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, vs := range h {
		for _, v := range vs {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// signer builds the headers a real sender would send for src at time at, using
// secret (so a wrong secret makes a bad signature).
type signer func(secret string, body []byte, at time.Time) http.Header

func hdrs(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

// alertCase describes one source: its fixture, how it signs, when "now" is for
// the fixture to be fresh, and how to turn the firing body into a resolved one.
type alertCase struct {
	src      alertin.Source
	fixture  string // alertin testdata file; empty uses body
	body     []byte
	now      time.Time
	secret   string
	sign     signer
	resolved func([]byte) []byte
	// eventTimed: the body carries an event time, so a late server clock makes
	// it stale. Otherwise a stale request is one signed an hour ago.
	eventTimed bool
	// noStale: the source has no time check at all (Alertmanager bearer).
	noStale bool
	disable func(*alertin.Config)
}

func alertCases() []alertCase {
	fresh := time.Date(2026, 10, 11, 11, 0, 0, 0, time.UTC)
	replace := func(old, repl string) func([]byte) []byte {
		return func(b []byte) []byte { return bytes.ReplaceAll(b, []byte(old), []byte(repl)) }
	}
	return []alertCase{
		{
			src: alertin.Alertmanager, fixture: "alertmanager.json", now: fresh, secret: amToken, noStale: true,
			sign: func(secret string, _ []byte, _ time.Time) http.Header {
				return hdrs("Authorization", "Bearer "+secret)
			},
			resolved: replace(`"status": "firing"`, `"status": "resolved"`),
			disable:  func(c *alertin.Config) { c.AlertmanagerToken = "" },
		},
		{
			src: alertin.Grafana, fixture: "grafana.json", now: fresh, secret: grafanaSecret,
			sign: func(secret string, body []byte, at time.Time) http.Header {
				ts := strconv.FormatInt(at.Unix(), 10)
				return hdrs(alertin.DefaultGrafanaSignatureHeader, macHex(secret, ts, ":", string(body)),
					alertin.DefaultGrafanaTimestampHeader, ts)
			},
			resolved: replace(`"status": "firing"`, `"status": "resolved"`),
			disable:  func(c *alertin.Config) { c.GrafanaSecret = "" },
		},
		{
			// The official PagerDuty example is from 2020; "now" is 5 minutes after it.
			src: alertin.PagerDuty, fixture: "pagerduty_triggered.json", now: time.Date(2020, 10, 2, 18, 50, 0, 0, time.UTC),
			secret: pdSecret, eventTimed: true,
			sign: func(secret string, body []byte, _ time.Time) http.Header {
				return hdrs(alertin.PagerDutySignatureHeader, "v1="+macHex(secret, string(body)))
			},
			resolved: replace("incident.triggered", "incident.resolved"),
			disable:  func(c *alertin.Config) { c.PagerDutySecret = "" },
		},
		{
			src: alertin.Datadog, fixture: "datadog.json", now: fresh, secret: ddToken, eventTimed: true,
			sign: func(secret string, _ []byte, _ time.Time) http.Header {
				return hdrs(alertin.DefaultDatadogTokenHeader, secret)
			},
			resolved: replace(`"Triggered"`, `"Recovered"`),
			disable:  func(c *alertin.Config) { c.DatadogToken = "" },
		},
		{
			src: alertin.Generic, body: []byte(`{"title":"lag on orders","severity":"sev2","pipeline_id":"orders","dedup_key":"gk-1"}`),
			now: fresh, secret: genericSecret,
			sign: func(secret string, body []byte, at time.Time) http.Header {
				return hdrs(alertin.GenericSignatureHeader, alertin.SignGeneric(secret, at, body))
			},
			resolved: replace(`"dedup_key"`, `"status":"resolved","dedup_key"`),
			disable:  func(c *alertin.Config) { c.GenericSecret = "" },
		},
	}
}

func (c alertCase) firing(t *testing.T) []byte {
	if c.fixture != "" {
		return fixtureBody(t, c.fixture)
	}
	return c.body
}

func wantStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		b := new(bytes.Buffer)
		_, _ = b.ReadFrom(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d, want %d; body %s", resp.StatusCode, want, b.String())
	}
}

// A valid signature opens a room with 201, with no X-User header.
func TestSignedAlertValidOpensRoom(t *testing.T) {
	for _, c := range alertCases() {
		t.Run(string(c.src), func(t *testing.T) {
			srv, svc := newAlertServer(t, alertCfg(c.now))
			body := c.firing(t)
			resp := postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
			wantStatus(t, resp, http.StatusCreated)
			snap := decode[room.RoomSnapshot](t, resp)
			if snap.Status != "open" || snap.Title == "" {
				t.Fatalf("snapshot = %+v", snap)
			}
			if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 1 {
				t.Fatalf("want 1 room, got %d", len(rooms))
			}
		})
	}
}

// A wrong, missing or disabled credential is 401 and opens nothing, and the
// answer does not echo the secret.
func TestSignedAlertRejectsBadAuth(t *testing.T) {
	for _, c := range alertCases() {
		t.Run(string(c.src), func(t *testing.T) {
			body := c.firing(t)
			tests := []struct {
				name string
				cfg  alertin.Config
				h    http.Header
			}{
				{"bad signature", alertCfg(c.now), c.sign("not-the-secret", body, c.now)},
				{"missing signature", alertCfg(c.now), http.Header{}},
			}
			disabled := alertCfg(c.now)
			c.disable(&disabled)
			// The sender signs correctly; the server has no secret for the source.
			tests = append(tests, struct {
				name string
				cfg  alertin.Config
				h    http.Header
			}{"disabled source", disabled, c.sign(c.secret, body, c.now)})

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					srv, svc := newAlertServer(t, tt.cfg)
					resp := postAlert(t, srv, c.src, tt.h, body)
					wantStatus(t, resp, http.StatusUnauthorized)
					got := decode[map[string]any](t, resp)
					if got["error"].(map[string]any)["code"] != "unauthenticated" {
						t.Fatalf("error body = %v", got)
					}
					if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
						t.Fatalf("refused alert opened %d rooms", len(rooms))
					}
				})
			}
		})
	}
}

// A validly signed but stale request is 401: an old signed timestamp for the
// sources that sign one, an old event time inside the body for the others.
func TestSignedAlertStaleIs401(t *testing.T) {
	for _, c := range alertCases() {
		if c.noStale {
			continue
		}
		t.Run(string(c.src), func(t *testing.T) {
			body := c.firing(t)
			serverNow, signAt := c.now, c.now.Add(-time.Hour)
			if c.eventTimed {
				serverNow, signAt = c.now.Add(2*time.Hour), c.now.Add(2*time.Hour)
			}
			srv, svc := newAlertServer(t, alertCfg(serverNow))
			resp := postAlert(t, srv, c.src, c.sign(c.secret, body, signAt), body)
			wantStatus(t, resp, http.StatusUnauthorized)
			resp.Body.Close()
			if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
				t.Fatalf("stale alert opened %d rooms", len(rooms))
			}
		})
	}
}

// A resolved alert with no open room opens nothing: 202 with ignored=true.
func TestSignedAlertResolvedIs202(t *testing.T) {
	for _, c := range alertCases() {
		t.Run(string(c.src), func(t *testing.T) {
			srv, svc := newAlertServer(t, alertCfg(c.now))
			body := c.resolved(c.firing(t))
			resp := postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
			wantStatus(t, resp, http.StatusAccepted)
			if got := decode[map[string]any](t, resp); got["ignored"] != true || got["reason"] == "" {
				t.Fatalf("ignored body = %v", got)
			}
			if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
				t.Fatalf("resolved alert opened %d rooms", len(rooms))
			}
		})
	}
}

// The same alert again while its room is open joins that room: 200, same
// room, no second room. A resolved notice for that alert also joins (200) and
// leaves the room open.
func TestSignedAlertRepeatJoinsOpenRoom(t *testing.T) {
	for _, c := range alertCases() {
		t.Run(string(c.src), func(t *testing.T) {
			srv, svc := newAlertServer(t, alertCfg(c.now))
			body := c.firing(t)
			resp := postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
			wantStatus(t, resp, http.StatusCreated)
			first := decode[room.RoomSnapshot](t, resp)

			// A real repeat is a new delivery with new bytes (an identical
			// copy is a replay: TestSignedAlertReplayIsIgnored).
			repeat := append(append([]byte{}, body...), '\n')
			resp = postAlert(t, srv, c.src, c.sign(c.secret, repeat, c.now), repeat)
			wantStatus(t, resp, http.StatusOK)
			if again := decode[room.RoomSnapshot](t, resp); again.ID != first.ID {
				t.Fatalf("repeat joined room %s, want %s", again.ID, first.ID)
			}

			resolved := c.resolved(body)
			resp = postAlert(t, srv, c.src, c.sign(c.secret, resolved, c.now), resolved)
			wantStatus(t, resp, http.StatusOK)
			if got := decode[room.RoomSnapshot](t, resp); got.ID != first.ID || got.Status != "open" {
				t.Fatalf("resolved joined room %s (%s), want open room %s", got.ID, got.Status, first.ID)
			}
			if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 1 {
				t.Fatalf("want 1 room, got %d", len(rooms))
			}
		})
	}
}

// The PagerDuty service map decides the pipeline when the incident names none,
// and the alert's own fields reach the room.
func TestSignedPagerDutyServiceMapRoutesPipeline(t *testing.T) {
	c := alertCases()[2]
	body := c.firing(t)

	srv, _ := newAlertServer(t, alertCfg(c.now))
	resp := postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
	wantStatus(t, resp, http.StatusCreated)
	if snap := decode[room.RoomSnapshot](t, resp); snap.PipelineID != "billing" {
		t.Fatalf("pipeline = %q, want billing (from PagerDutyServices)", snap.PipelineID)
	}

	// Another mapping routes the same incident elsewhere.
	cfg := alertCfg(c.now)
	cfg.PagerDutyServices = map[string]string{"PF9KMXH": "payments"}
	srv, _ = newAlertServer(t, cfg)
	resp = postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
	wantStatus(t, resp, http.StatusCreated)
	if snap := decode[room.RoomSnapshot](t, resp); snap.PipelineID != "payments" {
		t.Fatalf("pipeline = %q, want payments", snap.PipelineID)
	}

	// An unmapped service falls back to the default pipeline.
	cfg.PagerDutyServices = nil
	srv, _ = newAlertServer(t, cfg)
	resp = postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
	wantStatus(t, resp, http.StatusCreated)
	if snap := decode[room.RoomSnapshot](t, resp); snap.PipelineID != "orders" {
		t.Fatalf("pipeline = %q, want orders (DefaultPipeline)", snap.PipelineID)
	}
}

// The room gets the alert's own title, severity, pipeline and raw body.
func TestSignedAlertRoomCarriesAlertFields(t *testing.T) {
	c := alertCases()[0] // Alertmanager: label pipeline=orders, severity=critical
	srv, svc := newAlertServer(t, alertCfg(c.now))
	body := c.firing(t)
	resp := postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
	wantStatus(t, resp, http.StatusCreated)
	snap := decode[room.RoomSnapshot](t, resp)
	if snap.Title != "Debezium task orders-cdc/0 FAILED" || snap.Severity != "sev1" || snap.PipelineID != "orders" {
		t.Fatalf("snapshot = title %q severity %q pipeline %q", snap.Title, snap.Severity, snap.PipelineID)
	}
	evs, _, err := svc.Events(context.Background(), snap.ID, 0, 10)
	if err != nil || len(evs) == 0 || evs[0].Type != events.RoomCreated {
		t.Fatalf("events = %v, %v", evs, err)
	}
	created, err := events.Decode[events.RoomCreatedPayload](evs[0])
	// The event log re-encodes the JSON compactly; the content must be the body.
	var want bytes.Buffer
	if err != nil || json.Compact(&want, body) != nil || !bytes.Equal(created.Alert, want.Bytes()) {
		t.Fatalf("room.created.alert is not the alert body (err %v)", err)
	}
}

// A body over alertin.MaxBodyBytes is 413, signed or not.
func TestSignedAlertBodyTooLarge(t *testing.T) {
	c := alertCases()[4] // generic
	srv, svc := newAlertServer(t, alertCfg(c.now))
	for _, size := range []int{alertin.MaxBodyBytes + 1, 2 * alertin.MaxBodyBytes} {
		body := []byte(`"` + strings.Repeat("x", size) + `"`)
		resp := postAlert(t, srv, c.src, c.sign(c.secret, body, c.now), body)
		wantStatus(t, resp, http.StatusRequestEntityTooLarge)
		resp.Body.Close()
	}
	if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
		t.Fatalf("oversized alert opened %d rooms", len(rooms))
	}
}

// A verified request that is not an alert (a PagerDuty ping) is 202 ignored; a
// verified but malformed body is 400; an unknown source is 404.
func TestSignedAlertIgnoredBadPayloadUnknownSource(t *testing.T) {
	pd := alertCases()[2]
	srv, _ := newAlertServer(t, alertCfg(pd.now))

	ping := []byte(`{"event":{"event_type":"pagey.ping"}}`)
	resp := postAlert(t, srv, pd.src, pd.sign(pd.secret, ping, pd.now), ping)
	wantStatus(t, resp, http.StatusAccepted)
	if got := decode[map[string]any](t, resp); got["ignored"] != true {
		t.Fatalf("ping body = %v", got)
	}

	am := alertCases()[0]
	bad := []byte(`{"not":"alertmanager"}`)
	resp = postAlert(t, srv, am.src, am.sign(am.secret, bad, am.now), bad)
	wantStatus(t, resp, http.StatusBadRequest)
	resp.Body.Close()

	resp = postAlert(t, srv, alertin.Source("opsgenie"), http.Header{}, bad)
	wantStatus(t, resp, http.StatusNotFound)
	resp.Body.Close()
}

// The dev webhook still wants X-User and a signed route does not accept it as
// a substitute: X-User alone is not auth on /api/alerts/{source}.
func TestAlertAuthSplit(t *testing.T) {
	srv, _ := newAlertServer(t, alertCfg(time.Now()))
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "", `{"title":"x"}`)
	wantStatus(t, resp, http.StatusUnauthorized)
	resp.Body.Close()

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/alerts/generic", "arjun", `{"title":"x"}`)
	wantStatus(t, resp, http.StatusUnauthorized)
	resp.Body.Close()

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", `{"title":"x"}`)
	wantStatus(t, resp, http.StatusCreated)
	resp.Body.Close()
}

// A source with no secret is refused before the body is read: even a body over
// the limit gets 401, not 413.
func TestSignedAlertDisabledSourceRefusedBeforeBody(t *testing.T) {
	cfg := alertCfg(time.Now())
	cfg.GenericSecret = ""
	srv, _ := newAlertServer(t, cfg)
	body := []byte(`"` + strings.Repeat("x", 2*alertin.MaxBodyBytes) + `"`)
	resp := postAlert(t, srv, alertin.Generic, http.Header{}, body)
	wantStatus(t, resp, http.StatusUnauthorized)
	resp.Body.Close()
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// Refused signed requests are logged at Warn at most once per source per
// minute; the rest are Debug, so a caller cannot flood the log.
func TestSignedAlertRefusalsAreLoggedOncePerMinute(t *testing.T) {
	var clockMu sync.Mutex
	now := time.Date(2026, 10, 11, 11, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	var mu sync.Mutex
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(lockedWriter{&mu, &logs}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	api := httpapi.New(svc, st, log, httpapi.WithAlertIngest(alertCfg(now)), httpapi.WithClock(clock))
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})

	refuse := func(src alertin.Source) {
		resp := postAlert(t, srv, src, http.Header{}, []byte(`{}`))
		wantStatus(t, resp, http.StatusUnauthorized)
		resp.Body.Close()
	}
	warns := func() int {
		mu.Lock()
		defer mu.Unlock()
		return strings.Count(logs.String(), "alerts: refused")
	}
	for i := 0; i < 5; i++ {
		refuse(alertin.Generic)
	}
	if n := warns(); n != 1 {
		t.Fatalf("5 refusals of one source logged %d warnings, want 1", n)
	}
	refuse(alertin.Grafana)
	if n := warns(); n != 2 {
		t.Fatalf("a second source logged %d warnings in total, want 2", n)
	}
	clockMu.Lock()
	now = now.Add(2 * time.Minute)
	clockMu.Unlock()
	refuse(alertin.Generic)
	if n := warns(); n != 3 {
		t.Fatalf("after a minute logged %d warnings in total, want 3", n)
	}
}

// A body Postgres jsonb cannot store (a \u0000 escape, an unpaired surrogate)
// is 400 validation on both routes and creates nothing.
func TestAlertBodyJSONBGuardIs400(t *testing.T) {
	bodies := map[string]string{
		"nul escape":         `{"title":"x\u0000y","pipeline_id":"orders","dedup_key":"k"}`,
		"unpaired surrogate": `{"title":"\ud83d","pipeline_id":"orders","dedup_key":"k"}`,
		"invalid utf-8":      "{\"title\":\"x\xffy\",\"pipeline_id\":\"orders\"}",
	}
	gen := alertCases()[4]
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			srv, svc := newAlertServer(t, alertCfg(gen.now))

			resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", body)
			wantStatus(t, resp, http.StatusBadRequest)
			if got := decode[map[string]any](t, resp); got["error"].(map[string]any)["code"] != "validation" {
				t.Fatalf("webhook error body = %v", got)
			}

			resp = postAlert(t, srv, gen.src, gen.sign(gen.secret, []byte(body), gen.now), []byte(body))
			wantStatus(t, resp, http.StatusBadRequest)
			if got := decode[map[string]any](t, resp); got["error"].(map[string]any)["code"] != "validation" {
				t.Fatalf("signed error body = %v", got)
			}

			if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
				t.Fatalf("refused bodies created %d rooms", len(rooms))
			}
		})
	}
}

// POST /api/rooms refuses a title with a NUL: 400 validation, no room.
func TestCreateRoomTitleNULIs400(t *testing.T) {
	srv, svc := newAlertServer(t, alertCfg(time.Now()))
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"a\u0000b","severity":"sev2","pipeline_id":"orders"}`)
	wantStatus(t, resp, http.StatusBadRequest)
	if got := decode[map[string]any](t, resp); got["error"].(map[string]any)["code"] != "validation" {
		t.Fatalf("error body = %v", got)
	}
	if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
		t.Fatalf("refused title created %d rooms", len(rooms))
	}
}

// With no WithAlertIngest option every signed source is disabled.
func TestSignedAlertsDisabledByDefault(t *testing.T) {
	srv, _ := newServer(t)
	for _, src := range alertin.Sources {
		resp := postAlert(t, srv, src, http.Header{}, []byte(`{}`))
		wantStatus(t, resp, http.StatusUnauthorized)
		resp.Body.Close()
	}
}
