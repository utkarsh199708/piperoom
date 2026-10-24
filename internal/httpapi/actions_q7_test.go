package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// httpPRAction is a Layered, Deferred action (a fix PR): Layer() is "".
type httpPRAction struct{ httpAction }

func (*httpPRAction) LayerFor(owners.Pipeline, json.RawMessage) (string, error) {
	return "kafka-connect", nil
}
func (*httpPRAction) Deferred() {}

// TestActionsEndpointLayeredDeferred pins the Q7 fields of GET /api/actions:
// a Layered action shows layer "" and layer_by_params, a Deferred one
// deferred; both are omitted on every other action.
func TestActionsEndpointLayeredDeferred(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&httpAction{name: "pg_act", layer: "postgres"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(&httpPRAction{httpAction{name: "repo_open_pull_request"}}); err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Actions: reg, Run: fakeRun})
	t.Cleanup(svc.Close)
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/actions", "arjun", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("actions status: %d", resp.StatusCode)
	}
	out := decode[[]map[string]any](t, resp)
	if len(out) != 2 {
		t.Fatalf("want 2 actions, got %d", len(out))
	}
	for _, k := range []string{"layer_by_params", "deferred"} {
		if _, ok := out[0][k]; ok {
			t.Fatalf("pg_act has %s: %+v", k, out[0])
		}
	}
	if out[1]["layer"] != "" || out[1]["layer_by_params"] != true || out[1]["deferred"] != true {
		t.Fatalf("repo_open_pull_request = %+v, want layer \"\", layer_by_params and deferred true", out[1])
	}
}
