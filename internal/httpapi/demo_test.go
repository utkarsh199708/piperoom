package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// The scripted demo's presenter controls (KAN-398): mounted only with
// WithDemoControls (cmd/server --demo), driver only, open rooms only.

type demoStub struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (d *demoStub) record(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, name)
	return d.err
}

func (d *demoStub) Skip(string) error  { return d.record("skip") }
func (d *demoStub) Pause(string) error { return d.record("pause") }
func (d *demoStub) Resume(string) error {
	return d.record("resume")
}
func (d *demoStub) Reset(string) error { return d.record("reset") }

func (d *demoStub) recorded() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func newDemoControlServer(t *testing.T, c httpapi.DemoControls) *httptest.Server {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	h := httpapi.New(svc, st, nil, httpapi.WithDemoControls(c))
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		h.Close()
	})
	return srv
}

var demoVerbs = []string{"skip", "pause", "resume", "reset"}

// TC1: without --demo the four routes do not exist.
func TestDemoRoutesAbsentWithoutControls(t *testing.T) {
	srv, _ := newServer(t)
	for _, verb := range demoVerbs {
		resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_x/demo/"+verb, "priya", "")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("POST demo/%s without controls = %d, want 404", verb, resp.StatusCode)
		}
	}
}

// With --demo each route calls its control for the driver.
func TestDemoControlsAreDriverOnly(t *testing.T) {
	stub := &demoStub{}
	srv := newDemoControlServer(t, stub)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "priya", `{"title":"demo","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create room: %d", resp.StatusCode)
	}
	roomID := decode[map[string]any](t, resp)["id"].(string)
	base := srv.URL + "/api/rooms/" + roomID + "/demo/"

	for _, verb := range demoVerbs {
		t.Run(verb, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, base+verb, "priya", "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("POST demo/%s = %d", verb, resp.StatusCode)
			}
			body := decode[map[string]any](t, resp)
			if body["control"] != verb || body["room_id"] != roomID {
				t.Fatalf("body = %+v", body)
			}
		})
	}
	if got := strings.Join(stub.recorded(), ","); got != "skip,pause,resume,reset" {
		t.Fatalf("controls called %q", got)
	}

	// A non-driver is refused and nothing is called.
	resp = doJSON(t, http.MethodPost, base+"skip", "arjun", "")
	body := decode[map[string]any](t, resp)
	if resp.StatusCode != http.StatusForbidden || body["error"].(map[string]any)["code"] != "not_driver" {
		t.Fatalf("non-driver = %d %+v", resp.StatusCode, body)
	}
	if got := len(stub.recorded()); got != 4 {
		t.Fatalf("non-driver reached the control: %d calls", got)
	}

	// A body with parameters is a clear 400.
	resp = doJSON(t, http.MethodPost, base+"skip", "priya", `{"step":3}`)
	body = decode[map[string]any](t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("parameters = %d %+v", resp.StatusCode, body)
	}
	if msg := body["error"].(map[string]any)["message"].(string); !strings.Contains(msg, "no parameters") {
		t.Fatalf("bad 400 message: %q", msg)
	}
	if got := len(stub.recorded()); got != 4 {
		t.Fatalf("invalid body reached the control: %d calls", got)
	}
}

// A closed room answers 409 room_closed before the control runs.
func TestDemoControlsRefuseClosedRoom(t *testing.T) {
	stub := &demoStub{}
	srv := newDemoControlServer(t, stub)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"demo","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/close", "arjun", `{"resolution":"fixed"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("close room: %d", resp.StatusCode)
	}
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/demo/reset", "arjun", "")
	body := decode[map[string]any](t, resp)
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "room_closed" {
		t.Fatalf("closed room = %d %+v", resp.StatusCode, body)
	}
	if len(stub.recorded()) != 0 {
		t.Fatalf("closed room reached the control: %v", stub.recorded())
	}
}

// The demo's own 409 (no active run) is carried through with its code.
func TestDemoControlErrorsAreMapped(t *testing.T) {
	stub := &demoStub{err: &room.Error{Status: 409, Code: "demo_idle", Msg: "no scripted demo run is active in this room"}}
	srv := newDemoControlServer(t, stub)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "priya", `{"title":"demo","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/demo/skip", "priya", "")
	body := decode[map[string]any](t, resp)
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "demo_idle" {
		t.Fatalf("demo_idle = %d %+v", resp.StatusCode, body)
	}
}

// An unknown control verb stays a 404 even on a demo server.
func TestDemoUnknownControlIs404(t *testing.T) {
	srv := newDemoControlServer(t, &demoStub{})
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_x/demo/step", "priya", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown verb = %d, want 404", resp.StatusCode)
	}
}

// /healthz marks a demo server so the web UI can show its control strip.
func TestHealthzDemoFlag(t *testing.T) {
	srv := newDemoControlServer(t, &demoStub{})
	resp := doJSON(t, http.MethodGet, srv.URL+"/healthz", "", "")
	body := decode[map[string]any](t, resp)
	if body["demo"] != true {
		t.Fatalf("healthz demo = %v, want true", body["demo"])
	}

	plain, _ := newServer(t)
	resp = doJSON(t, http.MethodGet, plain.URL+"/healthz", "", "")
	body = decode[map[string]any](t, resp)
	if _, ok := body["demo"]; ok {
		t.Fatalf("healthz on a plain server has demo: %v", body["demo"])
	}
}
