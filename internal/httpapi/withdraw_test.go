package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// gateAction is a no-op action for one layer whose Execute, when gate is set,
// signals started and waits for gate (so a test can hold a step running).
type gateAction struct {
	name, layer string
	started     chan struct{}
	gate        chan struct{}
}

func (a *gateAction) Name() string     { return a.name }
func (a *gateAction) Layer() string    { return a.layer }
func (a *gateAction) Describe() string { return a.name }
func (a *gateAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (a *gateAction) Validate(json.RawMessage) error                    { return nil }
func (a *gateAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *gateAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	if a.gate == nil {
		return "ok", nil
	}
	a.started <- struct{}{}
	select {
	case <-a.gate:
	case <-ctx.Done():
	}
	return "ok", nil
}

// newProposalServer is a dev-mode server with real proposals: an owners
// config to scope against, two actions and dana, a member with no layer.
func newProposalServer(t *testing.T, acts ...actions.Action) (*httptest.Server, *room.Service) {
	t.Helper()
	users, err := room.ParseUsers([]byte(`
users:
  - {id: arjun, name: Arjun, layers: [kafka-connect, kafka]}
  - {id: priya, name: Priya, layers: [postgres]}
  - {id: dana, name: Dana}
`))
	if err != nil {
		t.Fatal(err)
	}
	reg := actions.NewRegistry()
	for _, a := range acts {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: users, Run: fakeRun,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	t.Cleanup(svc.Close)
	return srv, svc
}

func newRoomProposal(t *testing.T, svc *room.Service, roomID, action, layer string) string {
	t.Helper()
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "restarts the task", Rollback: "set it back",
		Steps: []events.ProposalStep{{Action: action, Layer: layer, Params: json.RawMessage(`{}`)}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func snapshotHash(t *testing.T, svc *room.Service, roomID, pid string) string {
	t.Helper()
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range snap.OpenProposals {
		if p.ProposalID == pid {
			return p.ParamsHash
		}
	}
	t.Fatalf("proposal %s is not open", pid)
	return ""
}

// POST /api/rooms/{id}/proposals/{pid}/withdraw (Q0, QA G9), every answer of
// docs/api.md.
func TestWithdrawRoute(t *testing.T) {
	running := &gateAction{name: "kafka_fix", layer: "kafka", started: make(chan struct{}, 1), gate: make(chan struct{})}
	defer close(running.gate)
	srv, svc := newProposalServer(t, &gateAction{name: "pg_fix", layer: "postgres"}, running)
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	other, err := svc.CreateRoom(ctx, arjun, "t2", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid := newRoomProposal(t, svc, snap.ID, "pg_fix", "postgres")
	elsewhere := newRoomProposal(t, svc, other.ID, "pg_fix", "postgres")
	busy := newRoomProposal(t, svc, snap.ID, "kafka_fix", "kafka")
	if _, err := svc.ApproveProposal(ctx, arjun, busy, "kafka", snapshotHash(t, svc, snap.ID, busy)); err != nil {
		t.Fatal(err)
	}
	<-running.started
	url := func(roomID, pid string) string {
		return srv.URL + "/api/rooms/" + roomID + "/proposals/" + pid + "/withdraw"
	}

	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, pid), "priya", `{"reason":"x"}`), http.StatusForbidden, "not_driver")
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, "p_nope"), "arjun", ""), http.StatusNotFound, "not_found")
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, elsewhere), "arjun", ""), http.StatusNotFound, "not_found")
	wantHTTPError(t, doJSON(t, http.MethodPost, url("r_nope", pid), "arjun", ""), http.StatusNotFound, "not_found")
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, busy), "arjun", ""), http.StatusConflict, "proposal_running")
	for _, body := range []string{
		`{"reason":"` + strings.Repeat("a", 501) + `"}`,
		`{"reason":"a\u0000b"}`,
		`{"reason":`,
		`{"reason":5}`,
	} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, pid), "arjun", body), http.StatusBadRequest, "validation")
	}

	hash := snapshotHash(t, svc, snap.ID, pid)
	resp := doJSON(t, http.MethodPost, url(snap.ID, pid), "arjun", `{"reason":"`+strings.Repeat("é", 500)+`"}`)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("withdraw: %d %s", resp.StatusCode, b)
	}
	ev := decode[events.Event](t, resp)
	p, err := events.Decode[events.ProposalWithdrawnPayload](ev)
	if ev.Type != events.ProposalWithdrawn || ev.Actor.ID != "arjun" || err != nil || p.ProposalID != pid || p.Reason != strings.Repeat("é", 500) {
		t.Fatalf("event %+v payload %+v (%v)", ev, p, err)
	}
	got, _ := svc.GetRoom(ctx, snap.ID)
	for _, op := range got.OpenProposals {
		if op.ProposalID == pid {
			t.Fatal("a withdrawn proposal is still open")
		}
	}

	// A withdrawn proposal answers 409 proposal_withdrawn to withdraw, approve and reject.
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, pid), "arjun", ""), http.StatusConflict, "proposal_withdrawn")
	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/approve", "priya",
		fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hash)), http.StatusConflict, "proposal_withdrawn")
	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/reject", "priya",
		fmt.Sprintf(`{"layer":"postgres","reason":"no","params_hash":%q}`, hash)), http.StatusConflict, "proposal_withdrawn")

	// An empty body withdraws with no reason.
	pid2 := newRoomProposal(t, svc, snap.ID, "pg_fix", "postgres")
	resp = doJSON(t, http.MethodPost, url(snap.ID, pid2), "arjun", "")
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("withdraw with no body: %d %s", resp.StatusCode, b)
	}
	resp.Body.Close()

	// A closed room: 409 room_closed (the close withdrew its proposals).
	pid3 := newRoomProposal(t, svc, snap.ID, "pg_fix", "postgres")
	if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, pid3), "arjun", ""), http.StatusConflict, "room_closed")
}

// POST /api/rooms/{id}/close (Q0, QA G16): the driver or an owner of a layer
// of the room's pipeline; anyone else 403 not_driver.
func TestCloseRouteWho(t *testing.T) {
	srv, svc := newProposalServer(t, &gateAction{name: "pg_fix", layer: "postgres"})
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	for _, tc := range []struct {
		user   string
		status int
		code   string
	}{
		{"dana", http.StatusForbidden, "not_driver"},
		{"priya", http.StatusOK, ""}, // owns postgres, does not drive
		{"arjun", http.StatusOK, ""}, // the driver
	} {
		t.Run(tc.user, func(t *testing.T) {
			snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
			if err != nil {
				t.Fatal(err)
			}
			pid := newRoomProposal(t, svc, snap.ID, "pg_fix", "postgres")
			resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+snap.ID+"/close", tc.user, `{"resolution":"fixed"}`)
			if tc.code != "" {
				wantHTTPError(t, resp, tc.status, tc.code)
				return
			}
			if resp.StatusCode != tc.status {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("close: %d %s", resp.StatusCode, b)
			}
			resp.Body.Close()
			got, err := svc.GetRoom(ctx, snap.ID)
			if err != nil || got.Status != "closed" || len(got.OpenProposals) != 0 {
				t.Fatalf("after close: status %q, open proposals %+v (%v)", got.Status, got.OpenProposals, err)
			}
			evs, _, err := svc.Events(ctx, snap.ID, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			// Right after room.closed (a post-mortem draft may follow).
			for i, ev := range evs {
				if ev.Type != events.RoomClosed {
					continue
				}
				if i+1 >= len(evs) {
					t.Fatal("nothing after room.closed")
				}
				next := evs[i+1]
				wp, err := events.Decode[events.ProposalWithdrawnPayload](next)
				if next.Type != events.ProposalWithdrawn || err != nil || wp.ProposalID != pid || wp.Reason != events.ProposalWithdrawnRoomClosed {
					t.Fatalf("event after room.closed %+v, want the close's withdrawal of %s", next, pid)
				}
				return
			}
			t.Fatal("no room.closed")
		})
	}
}
