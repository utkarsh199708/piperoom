package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// stageAction is testAction with actions.Stager: Stage waits for release
// (when set) and then reports a healthy stage.
type stageAction struct {
	testAction
	release chan struct{}
}

func (a *stageAction) Stage(ctx context.Context, _ json.RawMessage) (actions.StageResult, error) {
	if a.release != nil {
		select {
		case <-a.release:
		case <-ctx.Done():
			return actions.StageResult{}, ctx.Err()
		}
	}
	return actions.StageResult{Target: "connector orders-src", Applied: true, Healthy: true}, nil
}

type stageEnv struct {
	srv  *httptest.Server
	svc  *room.Service
	act  *stageAction
	room string
	pid  string
}

func newStageEnv(t *testing.T, staging bool, require []string) *stageEnv {
	t.Helper()
	p := owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders"}, RequireStaging: require}
	if staging {
		p.Staging = &owners.StagingSpec{Connections: owners.Connections{"connect_url": {Value: "http://connect-staging:8083"}}}
	}
	act := &stageAction{testAction: testAction{layer: "kafka-connect"}, release: make(chan struct{})}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: &owners.Owners{Pipelines: []owners.Pipeline{p}}, Actions: reg, Run: fakeRun})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() {
			select {
			case <-act.release:
			default:
				close(act.release)
			}
			srv.Close()
			api.Close()
		})
	})
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low risk", Rollback: "undo it",
		Steps: []events.ProposalStep{{Action: "fix_kafka-connect", Layer: "kafka-connect", Params: json.RawMessage(`{}`)}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	return &stageEnv{srv: srv, svc: svc, act: act, room: snap.ID, pid: pid}
}

func (e *stageEnv) url() string { return e.srv.URL + "/api/proposals/" + e.pid + "/stage" }

// roomProposal reads the proposal from GET /api/rooms/{id} as the web does.
func (e *stageEnv) roomProposal(t *testing.T) (map[string]any, map[string]any) {
	t.Helper()
	view := decode[map[string]any](t, doJSON(t, http.MethodGet, e.srv.URL+"/api/rooms/"+e.room, "arjun", ""))
	props, _ := view["open_proposals"].([]any)
	if len(props) != 1 {
		t.Fatalf("open_proposals = %v", view["open_proposals"])
	}
	mode, _ := view["mode"].(map[string]any)
	return props[0].(map[string]any), mode
}

// TC1 over HTTP: 403 for a non-driver, 202 with proposal.stage_started for
// the driver, 409 stage_in_progress while it runs, and the result in the
// room view once proposal.staged lands.
func TestStageRoute(t *testing.T) {
	e := newStageEnv(t, true, nil)
	wantAPIError(t, doJSON(t, http.MethodPost, e.url(), "priya", `{}`), http.StatusForbidden, "not_driver", "")

	resp := doJSON(t, http.MethodPost, e.url(), "arjun", `{}`)
	if resp.StatusCode != http.StatusAccepted {
		resp.Body.Close()
		t.Fatalf("stage = %d, want 202", resp.StatusCode)
	}
	ev := decode[events.Event](t, resp)
	sp, err := events.Decode[events.ProposalStageStartedPayload](ev)
	if err != nil || ev.Type != events.ProposalStageStarted || sp.ProposalID != e.pid || len(sp.Steps) != 1 || sp.Steps[0] != 0 {
		t.Fatalf("stage event %+v %+v (%v)", ev, sp, err)
	}
	p, mode := e.roomProposal(t)
	if mode["staging"] != true || p["stage_running"] == nil {
		t.Fatalf("view while running: mode=%v proposal=%v", mode, p)
	}
	wantAPIError(t, doJSON(t, http.MethodPost, e.url(), "arjun", `{}`), http.StatusConflict, "stage_in_progress", "")

	close(e.act.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		p, _ = e.roomProposal(t)
		if p["staged"] != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("staged never shown")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if p["stage_running"] != nil {
		t.Fatalf("stage_running still shown: %v", p)
	}
	staged := p["staged"].(map[string]any)
	steps := staged["steps"].([]any)
	res := steps[0].(map[string]any)["result"].(map[string]any)
	if res["healthy"] != true || res["applied"] != true || staged["params_hash"] != p["params_hash"] {
		t.Fatalf("staged = %v", staged)
	}
}

func TestStageRouteNoStaging(t *testing.T) {
	e := newStageEnv(t, false, nil)
	wantAPIError(t, doJSON(t, http.MethodPost, e.url(), "arjun", `{}`), http.StatusConflict, "no_staging", "")
	p, mode := e.roomProposal(t)
	if _, ok := mode["staging"]; ok {
		t.Fatalf("mode.staging present without staging: %v", mode)
	}
	if st, ok := p["stageable"].([]any); !ok || len(st) != 1 {
		t.Fatalf("stageable = %v, want [0]", p["stageable"])
	}
}

// TC2 over HTTP: approve and approve-all answer 409 staging_required until a
// healthy stage exists; then approve goes through.
func TestApproveNeedsStageRoute(t *testing.T) {
	e := newStageEnv(t, true, []string{"fix_kafka-connect"})
	close(e.act.release)
	p, _ := e.roomProposal(t)
	if p["staging_required"] != true {
		t.Fatalf("staging_required = %v", p["staging_required"])
	}
	hash := p["params_hash"].(string)
	approve := e.srv.URL + "/api/proposals/" + e.pid + "/approve"
	wantAPIError(t, doJSON(t, http.MethodPost, approve, "arjun", `{"layer":"kafka-connect","params_hash":"`+hash+`"}`), http.StatusConflict, "staging_required", "fix_kafka-connect")
	wantAPIError(t, doJSON(t, http.MethodPost, e.srv.URL+"/api/proposals/"+e.pid+"/approve-all", "arjun", `{"params_hash":"`+hash+`"}`), http.StatusConflict, "staging_required", "step 0")

	resp := doJSON(t, http.MethodPost, e.url(), "arjun", `{}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("stage = %d", resp.StatusCode)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		p, _ = e.roomProposal(t)
		if p["staged"] != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("staged never shown")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := p["staging_required"]; ok {
		t.Fatalf("staging_required still set: %v", p)
	}
	resp = doJSON(t, http.MethodPost, approve, "arjun", `{"layer":"kafka-connect","params_hash":"`+hash+`"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("approve after a healthy stage = %d", resp.StatusCode)
	}
}

// A viewer gets 403 viewer from the stage route (default-deny guard) and no
// stage is logged.
func TestOIDCViewerCannotStage(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	pid := e.proposal(t, snap.ID, "kafka-connect")
	viewer := e.mustLogin(t, idpViewer)
	resp := e.do(t, http.MethodPost, "/api/proposals/"+pid+"/stage", `{}`, ck(viewer))
	wantStatus(t, resp, http.StatusForbidden)
	if code, _ := errCode(t, resp); code != "viewer" {
		t.Fatalf("viewer stage: code %q, want viewer", code)
	}
	evs, _, err := e.svc.Events(context.Background(), snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		if ev.Type == events.ProposalStageStarted {
			t.Fatalf("proposal.stage_started logged: %+v", ev)
		}
	}
}
