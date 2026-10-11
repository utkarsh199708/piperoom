package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

// TestDemoEndToEnd drives the whole demo story over HTTP: open a room, let the
// scripted agent propose, approve as both required owners, and see the steps run.
func TestDemoEndToEnd(t *testing.T) {
	own, err := owners.Load("../../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	areg, err := demoActionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc, err := room.New(room.Config{
		Store: st, Owners: own, Users: room.DemoUsers(), Actions: areg, Topology: demoTopology{own},
		Runner: demoRunner(0), Handoff: demoHandoff, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Shutdown)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	wsh := ws.New(ws.Config{Subscribe: st.Subscribe, Authenticate: svc.Authenticate, RoomExists: svc.RoomExists})
	srv := httptest.NewServer(httpapi.New(httpapi.Config{Service: svc, WS: wsh}))
	defer srv.Close()

	call := func(user, method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-User", user)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if ok() {
				return
			}
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	code, snap := call("kabir", "POST", "/api/rooms", `{"title":"Orders stale","severity":"sev2","pipeline_id":"orders"}`)
	if code != 200 {
		t.Fatalf("create room = %d %v", code, snap)
	}
	id := snap["id"].(string)

	var pid string
	waitFor("the agent's proposal", func() bool {
		_, s := call("kabir", "GET", "/api/rooms/"+id, "")
		props, _ := s["open_proposals"].([]any)
		if len(props) == 1 {
			pid = props[0].(map[string]any)["proposal_id"].(string)
		}
		return pid != ""
	})

	if code, _ := call("kabir", "POST", "/api/proposals/"+pid+"/approve", `{"layer":"warehouse"}`); code != 403 {
		t.Fatalf("approving a layer you do not own = %d, want 403", code)
	}
	for user, layer := range map[string]string{"priya": "schema-registry", "meera": "warehouse"} {
		if code, m := call(user, "POST", "/api/proposals/"+pid+"/approve", `{"layer":"`+layer+`"}`); code != 200 {
			t.Fatalf("%s approve = %d %v", user, code, m)
		}
	}
	waitFor("both steps to run", func() bool {
		_, page := call("kabir", "GET", "/api/rooms/"+id+"/events?after=0", "")
		n := 0
		for _, e := range page["events"].([]any) {
			if e.(map[string]any)["type"] == string(events.ActionExecuted) {
				n++
			}
		}
		return n == 2
	})

	if code, _ := call("kabir", "POST", "/api/rooms/"+id+"/close", `{"resolution":"fixed"}`); code != 200 {
		t.Fatalf("close = %d", code)
	}
	if code, m := call("kabir", "POST", "/api/rooms/"+id+"/comments", `{"text":"late"}`); code != 409 {
		t.Fatalf("comment on a closed room = %d %v, want 409", code, m)
	}
}

func TestCheckListenAddr(t *testing.T) {
	for _, c := range []struct {
		addr     string
		insecure bool
		ok       bool
	}{
		{"127.0.0.1:8090", false, true},
		{"localhost:8090", false, true},
		{"[::1]:8090", false, true},
		{"0.0.0.0:8090", false, false},
		{":8090", false, false},
		{"10.0.0.5:8090", false, false},
		{"0.0.0.0:8090", true, true},
		{"nonsense", false, false},
	} {
		if err := checkListenAddr(c.addr, c.insecure); (err == nil) != c.ok {
			t.Errorf("checkListenAddr(%q, %v) = %v, want ok=%v", c.addr, c.insecure, err, c.ok)
		}
	}
}
