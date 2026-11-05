package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Write mode over HTTP (P0 item 1): /healthz names it, and GET
// /api/rooms/{id} carries mode and, once every layer approved with write mode
// off, open_proposals[].blocked; nothing runs then.
func TestWriteModeHealthzAndRoomView(t *testing.T) {
	for _, tc := range []struct {
		off         bool
		healthz     string
		wantBlocked string
	}{
		{true, `{"status":"ok","version":"dev","write_mode":"off"}`, "write_mode_off"},
		{false, `{"status":"ok","version":"dev","write_mode":"on"}`, ""},
	} {
		act := &testAction{layer: "postgres"}
		reg := actions.NewRegistry()
		if err := reg.Register(act); err != nil {
			t.Fatal(err)
		}
		st := memstore.New()
		svc := room.New(room.Deps{
			Store: st, Users: room.DefaultUsers(), Actions: reg, Run: fakeRun, WriteModeOff: tc.off,
			Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		})
		api := httpapi.New(svc, st, nil)
		srv := httptest.NewServer(api)

		if code, body := get(t, http.MethodGet, srv.URL+"/healthz"); code != http.StatusOK || body != tc.healthz {
			t.Errorf("off=%v: GET /healthz = %d %s, want %s", tc.off, code, body, tc.healthz)
		}

		priya, _ := svc.LookupUser("priya")
		snap, err := svc.CreateRoom(context.Background(), priya, "t", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		pid, err := svc.CreateProposal(context.Background(), snap.ID, events.ProposalCreatedPayload{
			Title: "fix", Risk: "low risk", Rollback: "undo it",
			Steps: []events.ProposalStep{{Action: "fix_postgres", Layer: "postgres", Params: json.RawMessage(`{}`)}},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		type view struct {
			Mode *struct {
				Write       bool     `json:"write"`
				Solo        bool     `json:"solo"`
				AutoApprove []string `json:"auto_approve"`
			} `json:"mode"`
			OpenProposals []struct {
				ProposalID string  `json:"proposal_id"`
				ParamsHash string  `json:"params_hash"`
				Blocked    *string `json:"blocked"`
			} `json:"open_proposals"`
		}
		getView := func() view {
			resp := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+snap.ID, "priya", "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("off=%v: GET room = %d", tc.off, resp.StatusCode)
			}
			return decode[view](t, resp)
		}
		v := getView()
		if v.Mode == nil || v.Mode.Write == tc.off || v.Mode.Solo || v.Mode.AutoApprove == nil || len(v.Mode.AutoApprove) != 0 {
			t.Fatalf("off=%v: mode = %+v", tc.off, v.Mode)
		}
		if len(v.OpenProposals) != 1 || v.OpenProposals[0].Blocked != nil {
			t.Fatalf("off=%v: before approval %+v, want one proposal, no blocked member", tc.off, v.OpenProposals)
		}
		resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/approve", "priya",
			`{"layer":"postgres","params_hash":"`+v.OpenProposals[0].ParamsHash+`"}`)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("off=%v: approve = %d", tc.off, resp.StatusCode)
		}

		if tc.off {
			v = getView()
			if len(v.OpenProposals) != 1 || v.OpenProposals[0].Blocked == nil || *v.OpenProposals[0].Blocked != tc.wantBlocked {
				t.Fatalf("after approval %+v, want blocked %q", v.OpenProposals, tc.wantBlocked)
			}
		} else {
			// The executor takes the proposal off the open list once it ran.
			deadline := time.Now().Add(5 * time.Second)
			for len(getView().OpenProposals) != 0 {
				if time.Now().After(deadline) {
					t.Fatal("the approved proposal never left open_proposals")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		srv.Close()
		api.Close() // stops the room service and every goroutine it started
		executed := 0
		evs, err := st.Since(context.Background(), snap.ID, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range evs {
			if e.Type == events.ActionExecuted {
				executed++
			}
		}
		act.mu.Lock()
		runs := act.runs
		act.mu.Unlock()
		if want := map[bool]int{true: 0, false: 1}[tc.off]; executed != want || runs != want {
			t.Fatalf("off=%v: %d action.executed, %d runs; want %d each", tc.off, executed, runs, want)
		}
	}
}
