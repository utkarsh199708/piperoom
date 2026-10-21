package httpapi_test

import (
	"context"
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

// httpAction is a scoped action without a Preview for the actions/proposals
// route tests.
type httpAction struct {
	name, layer string
}

func (a *httpAction) Name() string     { return a.name }
func (a *httpAction) Layer() string    { return a.layer }
func (a *httpAction) Describe() string { return "desc " + a.name }
func (a *httpAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (a *httpAction) Validate(context.Context, json.RawMessage) error { return nil }
func (a *httpAction) CheckScope(owners.Pipeline, json.RawMessage) error {
	return nil
}
func (a *httpAction) Execute(context.Context, json.RawMessage) (string, error) { return "done", nil }

// httpPreviewAction is an httpAction with a Preview.
type httpPreviewAction struct{ httpAction }

func (a *httpPreviewAction) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	return actions.PreviewResult{Target: "t", Current: "now", Expected: "then"}, nil
}

// newActionsServer builds a server whose service has two registered actions and
// an owners config so proposals scope-check.
func newActionsServer(t *testing.T) (*httptest.Server, *room.Service) {
	t.Helper()
	st := memstore.New()
	reg := actions.NewRegistry()
	if err := reg.Register(&httpAction{name: "pg_act", layer: "postgres"}); err != nil {
		t.Fatalf("register pg_act: %v", err)
	}
	if err := reg.Register(&httpPreviewAction{httpAction{name: "wh_act", layer: "warehouse"}}); err != nil {
		t.Fatalf("register wh_act: %v", err)
	}
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Actions: reg,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		Run:    fakeRun,
	})
	h := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, svc
}

// TestActionsEndpoint pins GET /api/actions: the full registry in registration
// order, with describe, params_schema and whether the action previews.
func TestActionsEndpoint(t *testing.T) {
	srv, _ := newActionsServer(t)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/actions", "arjun", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("actions status: %d", resp.StatusCode)
	}
	out := decode[[]map[string]any](t, resp)
	if len(out) != 2 {
		t.Fatalf("want 2 actions, got %d", len(out))
	}
	if out[0]["name"] != "pg_act" || out[0]["layer"] != "postgres" || out[0]["describe"] != "desc pg_act" {
		t.Fatalf("bad first action: %+v", out[0])
	}
	if out[0]["preview"] != false {
		t.Fatalf("pg_act preview = %v, want false", out[0]["preview"])
	}
	if out[1]["name"] != "wh_act" || out[1]["preview"] != true {
		t.Fatalf("bad second action: %+v", out[1])
	}
	if _, ok := out[0]["params_schema"].(map[string]any); !ok {
		t.Fatalf("params_schema missing or not an object: %+v", out[0])
	}
}

// TestDriverProposalRoute pins POST /api/rooms/{id}/proposals: driver only,
// answered with proposal.created carrying origin "driver".
func TestDriverProposalRoute(t *testing.T) {
	srv, _ := newActionsServer(t)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create room: %d", resp.StatusCode)
	}
	roomID := decode[map[string]any](t, resp)["id"].(string)

	body := `{"title":"fix","risk":"low","rollback":"revert","steps":[{"action":"pg_act","layer":"postgres","params":{}}]}`

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/proposals", "priya", body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("non-driver proposal: want 403, got %d", resp.StatusCode)
	}
	env := decode[map[string]any](t, resp)
	if env["error"].(map[string]any)["code"] != "not_driver" {
		t.Fatalf("non-driver envelope: %+v", env)
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/proposals", "arjun", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("driver proposal: want 200, got %d", resp.StatusCode)
	}
	ev := decode[map[string]any](t, resp)
	if ev["type"] != "proposal.created" {
		t.Fatalf("want proposal.created, got %v", ev["type"])
	}
	payload, ok := ev["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload not an object: %+v", ev["payload"])
	}
	if payload["origin"] != "driver" {
		t.Fatalf("origin = %v, want driver", payload["origin"])
	}
}
