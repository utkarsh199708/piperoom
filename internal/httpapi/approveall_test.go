package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// POST /api/proposals/{pid}/approve-all over HTTP (P0 item 4, P2b-1): priya
// owns both layers of the proposal, meera only warehouse.
type approveAllEnv struct {
	srv      *httptest.Server
	st       *memstore.Store
	acts     []*testAction
	roomID   string
	pid      string
	hash     string
	shutdown func()
}

func newApproveAllEnv(t *testing.T) *approveAllEnv {
	t.Helper()
	u := func(id string) owners.Principal { return owners.Principal{Kind: owners.KindUser, Name: id} }
	own := &owners.Owners{
		Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}},
		Layers: map[string]owners.Principals{
			"postgres":  {u("priya")},
			"warehouse": {u("priya"), u("meera")},
		},
	}
	reg := actions.NewRegistry()
	acts := []*testAction{{layer: "postgres"}, {layer: "warehouse"}}
	for _, a := range acts {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: own, Actions: reg, Run: fakeRun})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			srv.Close()
			api.Close() // stops the room service and every goroutine it started
		})
	}
	t.Cleanup(shutdown)

	priya, _ := svc.LookupUser("priya")
	snap, err := svc.CreateRoom(context.Background(), priya, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := svc.CreateProposal(context.Background(), snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low risk", Rollback: "undo it",
		Steps: []events.ProposalStep{
			{Action: "fix_postgres", Layer: "postgres", Params: json.RawMessage(`{}`)},
			{Action: "fix_warehouse", Layer: "warehouse", Params: json.RawMessage(`{}`)},
		},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	view, err := svc.GetRoom(context.Background(), snap.ID)
	if err != nil || len(view.OpenProposals) != 1 {
		t.Fatalf("room %+v (err %v), want one open proposal", view.OpenProposals, err)
	}
	return &approveAllEnv{srv: srv, st: st, acts: acts, roomID: snap.ID, pid: pid, hash: view.OpenProposals[0].ParamsHash, shutdown: shutdown}
}

func (e *approveAllEnv) post(t *testing.T, user, body string) *http.Response {
	t.Helper()
	return doJSON(t, http.MethodPost, e.srv.URL+"/api/proposals/"+e.pid+"/approve-all", user, body)
}

func (e *approveAllEnv) count(t *testing.T, typ events.Type) int {
	t.Helper()
	evs, err := e.st.Since(context.Background(), e.roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ev := range evs {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

func wantAPIError(t *testing.T, resp *http.Response, status int, code, inMsg string) {
	t.Helper()
	if resp.StatusCode != status {
		resp.Body.Close()
		t.Fatalf("status %d, want %d", resp.StatusCode, status)
	}
	body := decode[struct {
		Error struct{ Code, Message string } `json:"error"`
	}](t, resp)
	if body.Error.Code != code || !strings.Contains(body.Error.Message, inMsg) {
		t.Fatalf("error %+v, want code %s and a message containing %q", body.Error, code, inMsg)
	}
}

// The owner of both layers: 200 with the two approval.given events in
// required_layers order, then the steps run and the metric counts them.
func TestHTTPApproveAllHappyPath(t *testing.T) {
	e := newApproveAllEnv(t)
	resp := e.post(t, "priya", `{"params_hash":"`+e.hash+`"}`)
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("approve-all = %d, want 200", resp.StatusCode)
	}
	evs := decode[[]events.Event](t, resp)
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(evs), evs)
	}
	for i, layer := range []string{"postgres", "warehouse"} {
		p, err := events.Decode[events.ApprovalGivenPayload](evs[i])
		if err != nil {
			t.Fatal(err)
		}
		if evs[i].Type != events.ApprovalGiven || evs[i].Actor.ID != "priya" || p.Layer != layer || p.Grant != events.GrantOwner ||
			p.ParamsHash != e.hash || p.Identity == nil || p.Identity.Grant != "user:priya" {
			t.Fatalf("event %d = %s %+v, want approval.given of %s, grant owner", i, evs[i].Type, p, layer)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.count(t, events.ActionExecuted) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("the steps never ran after approve-all")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, metrics := scrape(t, e.srv.URL)
	for _, want := range []string{
		"# TYPE piperoom_approvals_total counter\n",
		`piperoom_approvals_total{grant="owner"} 2` + "\n",
		`piperoom_approvals_total{grant="solo"} 0` + "\n",
		`piperoom_http_requests_total{method="POST",route="/api/proposals/{pid}/approve-all",code="2xx"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics lack %q\n%s", want, metrics)
		}
	}
	e.shutdown()
	for _, a := range e.acts {
		a.mu.Lock()
		runs := a.runs
		a.mu.Unlock()
		if runs != 1 {
			t.Errorf("%s ran %d times, want 1", a.Name(), runs)
		}
	}
}

// Owning one layer of two is 403 not_layer_owner naming the other, and the log
// gets no approval.
func TestHTTPApproveAllNotOwnerOfEveryLayer(t *testing.T) {
	e := newApproveAllEnv(t)
	wantAPIError(t, e.post(t, "meera", `{"params_hash":"`+e.hash+`"}`), http.StatusForbidden, "not_layer_owner", `layer "postgres"`)
	if n := e.count(t, events.ApprovalGiven); n != 0 {
		t.Fatalf("%d approval.given after a refused approve-all, want 0", n)
	}
}

// A body without params_hash, or not JSON, is 400 validation.
func TestHTTPApproveAllValidation(t *testing.T) {
	e := newApproveAllEnv(t)
	wantAPIError(t, e.post(t, "priya", `{}`), http.StatusBadRequest, "validation", "params_hash")
	wantAPIError(t, e.post(t, "priya", `{"params_hash":`), http.StatusBadRequest, "validation", "JSON")
	wantAPIError(t, e.post(t, "priya", `{"params_hash":"other"}`), http.StatusConflict, "params_mismatch", "")
	if n := e.count(t, events.ApprovalGiven); n != 0 {
		t.Fatalf("%d approval.given after refused calls, want 0", n)
	}
}
