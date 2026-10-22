package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

type routePager struct {
	mu  sync.Mutex
	n   int
	err error
}

func (p *routePager) PageTeam(context.Context, room.TeamPage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.n++
	return nil
}

// newPageServer serves a room whose agent hands its Sink to the test; a nil
// pager is the Slack bridge turned off.
func newPageServer(t *testing.T, pager room.TeamPager) (*httptest.Server, *room.Service, func() agent.Sink) {
	t.Helper()
	var mu sync.Mutex
	var sink agent.Sink
	run := func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
		mu.Lock()
		sink = d.Sink
		mu.Unlock()
		<-ctx.Done()
		return nil
	}
	own := &owners.Owners{
		Teams:     []owners.Team{{ID: "checkout", SlackChannel: "C0123ABCD", Owns: []string{"checkout-api"}}},
		Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}},
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: own, Run: run, TeamPager: pager})
	t.Cleanup(svc.Close)
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	return srv, svc, func() agent.Sink {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			s := sink
			mu.Unlock()
			if s != nil {
				return s
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("agent did not start")
		return nil
	}
}

// TC1: driver 200 with the event; non-driver 403; unknown team 400; second
// page within the hour 429; plus the 409s and 404.
func TestPageTeamRoute(t *testing.T) {
	pager := &routePager{}
	srv, svc, sink := newPageServer(t, pager)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/page-team"

	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"team":"checkout"}`), http.StatusConflict, "no_hypothesis")
	if err := sink().Emit(context.Background(), events.AgentHypothesis, events.AgentHypothesisPayload{Text: "pipeline", Confidence: "low", Scope: events.ScopePipeline}); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"team":"checkout"}`), http.StatusConflict, "not_upstream")
	if err := sink().Emit(context.Background(), events.AgentHypothesis, events.AgentHypothesisPayload{Text: "deploy", Confidence: "high", Scope: events.ScopeUpstreamApp, Team: "checkout"}); err != nil {
		t.Fatal(err)
	}

	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_nope/page-team", "arjun", `{"team":"checkout"}`), http.StatusNotFound, "not_found")
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "", `{"team":"checkout"}`), http.StatusUnauthorized, "session_expired")
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", `{"team":"checkout"}`), http.StatusForbidden, "not_driver")
	for _, body := range []string{`{}`, `{"team":"ghost"}`, `{"team":5}`, `{"team":`} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", body), http.StatusBadRequest, "validation")
	}
	pager.mu.Lock()
	pager.err = errors.New("slack down")
	pager.mu.Unlock()
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"team":"checkout"}`), http.StatusServiceUnavailable, "slack_unavailable")
	pager.mu.Lock()
	pager.err = nil
	pager.mu.Unlock()

	resp := doJSON(t, http.MethodPost, url, "arjun", `{"team":"checkout"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("driver page: status %d", resp.StatusCode)
	}
	ev := decode[events.Event](t, resp)
	p, _ := events.Decode[events.TeamPagedPayload](ev)
	if ev.Type != events.RoomTeamPaged || p.Team != "checkout" || p.HypothesisSeq == 0 {
		t.Fatalf("event = %+v %+v", ev, p)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"team":"checkout"}`), http.StatusTooManyRequests, "rate_limited")
	if pager.n != 1 {
		t.Fatalf("pages = %d, want 1", pager.n)
	}
	got, _ := svc.GetRoom(context.Background(), snap.ID)
	if len(got.TeamPages) != 1 || got.Hypothesis.Scope != events.ScopeUpstreamApp || got.Hypothesis.Team != "checkout" {
		t.Fatalf("snapshot team_pages %+v hypothesis %+v", got.TeamPages, got.Hypothesis)
	}
}

// TC4: Slack off is 409 slack_off.
func TestPageTeamRouteSlackOff(t *testing.T) {
	srv, svc, sink := newPageServer(t, nil)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err := sink().Emit(context.Background(), events.AgentHypothesis, events.AgentHypothesisPayload{Text: "msk", Confidence: "high", Scope: events.ScopeVendor}); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+snap.ID+"/page-team", "arjun", `{"team":"checkout"}`), http.StatusConflict, "slack_off")
}

// Viewers get 403 viewer (default-deny guard).
func TestOIDCViewerCannotPageTeam(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	c := e.mustLogin(t, idpViewer)
	resp := e.do(t, http.MethodPost, "/api/rooms/r_x/page-team", `{"team":"checkout"}`, ck(c))
	wantStatus(t, resp, http.StatusForbidden)
	if code, _ := errCode(t, resp); code != "viewer" {
		t.Fatalf("code %q, want viewer", code)
	}
}
