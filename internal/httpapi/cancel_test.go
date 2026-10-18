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

// previewAction is testAction with a passing preview, so the auto-approve
// policy can schedule it.
type previewAction struct{ testAction }

func (*previewAction) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	return actions.PreviewResult{Target: "t", Current: "failed", Expected: "running"}, nil
}

// POST /api/proposals/{pid}/cancel over HTTP (P0 item 4, P2b-4): priya owns
// postgres, the proposal's one layer and is on the auto_approve: list; meera
// owns only warehouse and does not drive.
func TestCancelProposalRoute(t *testing.T) {
	u := func(id string) owners.Principal { return owners.Principal{Kind: owners.KindUser, Name: id} }
	own := &owners.Owners{
		Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}, AutoApprove: []string{"fix_postgres"}}},
		Layers: map[string]owners.Principals{
			"postgres":  {u("priya")},
			"warehouse": {u("meera")},
		},
	}
	reg := actions.NewRegistry()
	if err := reg.Register(&previewAction{testAction{layer: "postgres"}}); err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: own, Actions: reg, Run: fakeRun,
		AutoApproveDelay: time.Hour})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() {
			srv.Close()
			api.Close()
		})
	})
	ctx := context.Background()
	priya, _ := svc.LookupUser("priya")
	snap, err := svc.CreateRoom(ctx, priya, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low risk", Rollback: "undo it",
		Steps: []events.ProposalStep{{Action: "fix_postgres", Layer: "postgres", Params: json.RawMessage(`{}`)}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		view, err := svc.GetRoom(ctx, snap.ID)
		if err == nil && len(view.OpenProposals) == 1 && view.OpenProposals[0].Scheduled != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("proposal never scheduled")
		}
		time.Sleep(10 * time.Millisecond)
	}
	url := srv.URL + "/api/proposals/" + pid + "/cancel"

	wantAPIError(t, doJSON(t, http.MethodPost, url, "meera", `{}`), http.StatusForbidden, "not_layer_owner", "driver")
	wantAPIError(t, doJSON(t, http.MethodPost, url, "priya", `{"reason":"`+strings.Repeat("x", 501)+`"}`), http.StatusBadRequest, "validation", "500")

	resp := doJSON(t, http.MethodPost, url, "priya", `{"reason":"look first"}`)
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("cancel = %d, want 200", resp.StatusCode)
	}
	ev := decode[events.Event](t, resp)
	p, err := events.Decode[events.ProposalCancelledPayload](ev)
	if err != nil || ev.Type != events.ProposalCancelled || p.ProposalID != pid || p.By.ID != "priya" || p.Reason != "look first" {
		t.Fatalf("cancel event %+v %+v (%v)", ev, p, err)
	}
	wantAPIError(t, doJSON(t, http.MethodPost, url, "priya", `{}`), http.StatusConflict, "not_scheduled", "")

	view, err := svc.RoomFor(ctx, priya, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.OpenProposals) != 1 || view.OpenProposals[0].Scheduled != nil {
		t.Fatalf("open proposals after cancel = %+v, want one, unscheduled", view.OpenProposals)
	}
	if view.Mode == nil || len(view.Mode.AutoApprove) != 1 || view.Mode.AutoApprove[0] != "fix_postgres" {
		t.Fatalf("mode = %+v", view.Mode)
	}
}
