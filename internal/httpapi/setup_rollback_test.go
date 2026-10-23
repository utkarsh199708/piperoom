package httpapi_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"piperoom/internal/room"
)

// POST /api/rooms/{id}/setup/rollback ("roll back this setup", U1-14b): the
// route is mounted always, is driver only, and answers with the proposal it
// created. The derivation itself is tested in package room
// (setup_rollback_internal_test.go); here only the wire is checked.

func TestSetupRollbackRoute(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	resp := doJSON(t, http.MethodPost, env.srv.URL+"/api/setup", "alice", setupAPIBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create setup room = %d", resp.StatusCode)
	}
	var out struct {
		Room room.RoomSnapshot `json:"room"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	path := "/api/rooms/" + out.Room.ID + "/setup/rollback"

	// A member who does not drive the room cannot roll it back.
	if status, code := env.errCode(t, "ann", http.MethodPost, path, ""); status != http.StatusForbidden || code != "not_driver" {
		t.Fatalf("non-driver = %d %s, want 403 not_driver", status, code)
	}
	// Nothing ran, so there is nothing to roll back: the refusal names that,
	// and no proposal exists afterwards.
	if status, code := env.errCode(t, "alice", http.MethodPost, path, ""); status != http.StatusBadRequest || code != "validation" {
		t.Fatalf("driver, nothing executed = %d %s, want 400 validation", status, code)
	}
	status, body := env.do(t, "alice", http.MethodGet, "/api/rooms/"+out.Room.ID, "")
	if status != http.StatusOK {
		t.Fatalf("get room = %d", status)
	}
	var snap room.RoomSnapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.OpenProposals) != 0 {
		t.Fatalf("open proposals = %+v, want none: a refused rollback creates nothing", snap.OpenProposals)
	}
}

func TestSetupRollbackRouteDisabled(t *testing.T) {
	env := newSetupAPIEnv(t, false)
	// A room route names a room that exists, so the setup check answers.
	status, body := env.do(t, "alice", http.MethodPost, "/api/rooms", `{"title":"incident","severity":"sev2","pipeline_id":"existing"}`)
	if status != http.StatusCreated {
		t.Fatalf("create room = %d %s", status, body)
	}
	var inc room.RoomSnapshot
	if err := json.Unmarshal(body, &inc); err != nil {
		t.Fatal(err)
	}
	status, code := env.errCode(t, "alice", http.MethodPost, "/api/rooms/"+inc.ID+"/setup/rollback", "")
	if status != http.StatusNotFound || code != "setup_disabled" {
		t.Fatalf("rollback route = %d %s, want 404 setup_disabled", status, code)
	}
}
