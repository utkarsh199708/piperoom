package httpapi_test

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"piperoom/evals"
	"piperoom/internal/room"
)

func TestAgentStatsRoute(t *testing.T) {
	srv, _ := newUsageServer(t, false)
	url := srv.URL + "/api/pipelines/orders/agent-stats"

	resp := doJSON(t, http.MethodGet, url, "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	got := decode[room.AgentStats](t, resp)
	if got.PipelineID != "orders" || got.WindowDays != 90 || got.Rooms != 0 || got.EnoughData || got.MedianHumanMTTRS != -1 || got.ByConfidence == nil {
		t.Fatalf("stats = %+v", got)
	}

	resp = doJSON(t, http.MethodGet, url+"?days=7", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if d := decode[room.AgentStats](t, resp).WindowDays; d != 7 {
		t.Fatalf("window_days = %d", d)
	}
	for _, bad := range []string{"0", "366", "-1", "x", "1.5", "999999999999999999999"} {
		wantErr(t, doJSON(t, http.MethodGet, url+"?days="+bad, "arjun", ""), http.StatusBadRequest, "validation")
	}
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/nope/agent-stats", "arjun", ""), http.StatusNotFound, "not_found")
}

func TestAgentStatsTenants(t *testing.T) {
	e := newTenancyEnv(t, true)
	e.mustDo(t, "ann", http.MethodGet, "/api/pipelines/pa/agent-stats", "", http.StatusOK)
	// Another tenant's pipeline answers like one that does not exist.
	other := e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/pa/agent-stats", "", http.StatusNotFound)
	missing := e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/zzz/agent-stats", "", http.StatusNotFound)
	if other != missing {
		t.Fatalf("tenant 404 differs from unknown 404: %q vs %q", other, missing)
	}
}

func TestAgentStatsViewer(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	v := e.mustLogin(t, idpViewer)
	if me := e.me(t, v); me["role"] != "viewer" {
		t.Fatalf("me = %v", me)
	}
	wantStatus(t, e.do(t, http.MethodGet, "/api/pipelines/orders/agent-stats", "", ck(v)), http.StatusOK)
}

// The eval report and the room share the names of a reliability bucket.
func TestAgentStatsBucketNamesMatchEvals(t *testing.T) {
	names := func(v any) map[string]bool {
		out := map[string]bool{}
		rt := reflect.TypeOf(v)
		for i := 0; i < rt.NumField(); i++ {
			out[strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]] = true
		}
		return out
	}
	inRoom, ev := names(room.AgentStatsBucket{}), names(evals.ReliabilityBucket{})
	for n := range inRoom {
		if !ev[n] {
			t.Errorf("agent-stats field %q is not an evals.ReliabilityBucket field", n)
		}
	}
	delete(ev, "probability") // omitted in the room: no forecast to compare with
	for n := range ev {
		if !inRoom[n] {
			t.Errorf("evals.ReliabilityBucket field %q missing from agent-stats", n)
		}
	}
}
