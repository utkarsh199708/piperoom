package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// Setup secrets and registered read credentials over HTTP (U1-5d; docs/api.md
// "Setup rooms"): the secret route is driver only, excluded from request
// logging and stored nowhere; the credentials click asks the runner and the
// answer's values go straight to PIPEROOM_SECRETS_DIR.

// createSetupOverHTTP opens a setup room over the API and returns its id and
// pairing code.
func createSetupOverHTTP(t *testing.T, env *setupAPIEnv) (roomID, code string) {
	t.Helper()
	status, body := env.do(t, "alice", http.MethodPost, "/api/setup", setupAPIBody)
	if status != http.StatusCreated {
		t.Fatalf("create setup room = %d %s", status, body)
	}
	var out struct {
		Room struct {
			ID string `json:"id"`
		} `json:"room"`
		Code string `json:"code"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Room.ID == "" || out.Code == "" {
		t.Fatalf("create response = %s", body)
	}
	return out.Room.ID, out.Code
}

// waitEvent polls the room log for one event type and returns its payload.
func waitEvent[T any](t *testing.T, env *setupAPIEnv, roomID string, typ events.Type) T {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		evs, _, err := env.svc.Events(context.Background(), roomID, 0, 1000)
		if err != nil {
			t.Fatalf("events: %v", err)
		}
		for _, e := range evs {
			if e.Type != typ {
				continue
			}
			p, err := events.Decode[T](e)
			if err != nil {
				t.Fatalf("decode %s: %v", typ, err)
			}
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %s event in the room log", typ)
	var zero T
	return zero
}

// countEvents returns how many events of one type the log holds.
func countEvents(t *testing.T, env *setupAPIEnv, roomID string, typ events.Type) int {
	t.Helper()
	evs, _, err := env.svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	n := 0
	for _, e := range evs {
		if e.Type == typ {
			n++
		}
	}
	return n
}

// TestSetupSecretRoute: a non-driver is 403; the driver's value reaches the
// paired runner in exactly one secret frame, is in no log line (the route
// writes no request line at all), in no event and in no file.
func TestSetupSecretRoute(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	ctx := context.Background()
	roomID, code := createSetupOverHTTP(t, env)
	conn, _ := dialRunner(t, ctx, env.srv.URL, code, "laptop")
	defer conn.Close(websocket.StatusNormalClosure, "")
	base := "/api/rooms/" + roomID + "/setup"

	const value = "hunter2-super-secret-value"
	secretBody := `{"name":"source_password","value":"` + value + `"}`

	// A non-driver is refused on both routes before anything reaches a runner.
	if status, errCode := env.errCode(t, "ann", http.MethodPost, base+"/secret", secretBody); status != http.StatusForbidden || errCode != "not_driver" {
		t.Fatalf("non-driver secret = %d %s, want 403 not_driver", status, errCode)
	}
	if status, errCode := env.errCode(t, "ann", http.MethodPost, base+"/credentials", ""); status != http.StatusForbidden || errCode != "not_driver" {
		t.Fatalf("non-driver credentials = %d %s, want 403 not_driver", status, errCode)
	}

	status, body := env.do(t, "alice", http.MethodPost, base+"/secret", secretBody)
	if status != http.StatusNoContent {
		t.Fatalf("secret route = %d %s, want 204", status, body)
	}

	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	f, err := readRunnerFrame(rctx, conn)
	if err != nil || f.Type != protocol.FrameSecret || f.Secret == nil {
		t.Fatalf("runner frame = %+v, %v", f, err)
	}
	if f.Secret.Name != "source_password" || f.Secret.Value != value {
		t.Fatalf("secret frame = %s", f.Secret.Name)
	}

	// The route is excluded from request logging: neither the value nor the
	// path is in any line.
	if logs := env.logs.String(); strings.Contains(logs, value) {
		t.Fatalf("the value reached a log line:\n%s", logs)
	} else if strings.Contains(logs, "/setup/secret") {
		t.Fatalf("the secret route wrote a request log line:\n%s", logs)
	}

	// The event carries the name and the driver, never the value.
	set := waitEvent[events.SetupSecretSetPayload](t, env, roomID, events.SetupSecretSet)
	if set.Name != "source_password" || set.Actor.ID != "alice" {
		t.Fatalf("setup.secret_set = %+v", set)
	}
	evs, _, err := env.svc.Events(ctx, roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		b, _ := json.Marshal(e)
		if strings.Contains(string(b), value) {
			t.Fatalf("the value is in the %s event: %s", e.Type, b)
		}
	}
	// The server stores nothing.
	if _, err := os.Stat(env.secretsDir); err == nil {
		err = filepath.Walk(env.secretsDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				t.Errorf("the server wrote %s", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// A bad name is 400 validation and no frame is sent.
	if status, errCode := env.errCode(t, "alice", http.MethodPost, base+"/secret", `{"name":"bad name","value":"`+value+`"}`); status != http.StatusBadRequest || errCode != "validation" {
		t.Fatalf("bad name = %d %s", status, errCode)
	}
}

// TestSetupCredentialsOverWS: the click sends register_credentials with the
// room's pipeline and the recorded names; the runner's credentials frame
// writes the files, emits the names only, and a second frame is refused.
func TestSetupCredentialsOverWS(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	ctx := context.Background()
	roomID, code := createSetupOverHTTP(t, env)
	conn, _ := dialRunner(t, ctx, env.srv.URL, code, "laptop")
	defer conn.Close(websocket.StatusNormalClosure, "")
	base := "/api/rooms/" + roomID + "/setup"
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const value = "read-role-password-1"
	if status, body := env.do(t, "alice", http.MethodPost, base+"/secret", `{"name":"source_password","value":"`+value+`"}`); status != http.StatusNoContent {
		t.Fatalf("secret route = %d %s", status, body)
	}
	if f, err := readRunnerFrame(rctx, conn); err != nil || f.Type != protocol.FrameSecret {
		t.Fatalf("secret frame = %+v, %v", f, err)
	}

	if status, body := env.do(t, "alice", http.MethodPost, base+"/credentials", ""); status != http.StatusAccepted {
		t.Fatalf("credentials route = %d %s, want 202", status, body)
	}
	f, err := readRunnerFrame(rctx, conn)
	if err != nil || f.Type != protocol.FrameRegisterCredentials || f.RegisterCredentials == nil {
		t.Fatalf("register frame = %+v, %v", f, err)
	}
	req := f.RegisterCredentials
	if req.PipelineID != "orders-setup" || req.RequestID == "" {
		t.Fatalf("register frame = %+v", req)
	}
	if len(req.Names) != 1 || req.Names[0] != "source_password" {
		t.Fatalf("names = %v", req.Names)
	}

	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FrameCredentials, Credentials: &protocol.Credentials{
		RequestID: req.RequestID,
		Secrets:   []protocol.Secret{{Name: "source_password", Value: value}},
	}}); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	registered := waitEvent[events.SetupCredentialsRegisteredPayload](t, env, roomID, events.SetupCredentialsRegistered)
	if registered.PipelineID != "orders-setup" || len(registered.Names) != 1 || registered.Names[0] != "source_password" {
		t.Fatalf("setup.credentials_registered = %+v", registered)
	}
	path := filepath.Join(env.secretsDir, "orders-setup", "source_password")
	if b, err := os.ReadFile(path); err != nil || string(b) != value {
		t.Fatalf("registered file = %q, %v", b, err)
	}
	if logs := env.logs.String(); strings.Contains(logs, value) {
		t.Fatalf("the value reached a log line:\n%s", logs)
	}

	// The same frame again answers setup.runner_refused {reason: credentials}
	// and registers nothing more.
	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FrameCredentials, Credentials: &protocol.Credentials{
		RequestID: req.RequestID,
		Secrets:   []protocol.Secret{{Name: "source_password", Value: value}},
	}}); err != nil {
		t.Fatalf("replay credentials: %v", err)
	}
	refusal := waitEvent[events.SetupRunnerRefusedPayload](t, env, roomID, events.SetupRunnerRefused)
	if refusal.Op != "register_credentials" || refusal.Reason != "credentials" {
		t.Fatalf("refusal = %+v", refusal)
	}
	if n := countEvents(t, env, roomID, events.SetupCredentialsRegistered); n != 1 {
		t.Fatalf("registered events = %d, want 1", n)
	}
}

// TestSetupSecretRoutesDisabled: with setup off the routes answer 404
// setup_disabled, the documented answer rather than a bare not_found.
func TestSetupSecretRoutesDisabled(t *testing.T) {
	env := newSetupAPIEnv(t, false)
	status, body := env.do(t, "alice", http.MethodPost, "/api/rooms", `{"title":"incident","severity":"sev2","pipeline_id":"existing"}`)
	if status != http.StatusCreated {
		t.Fatalf("create room = %d %s", status, body)
	}
	var inc struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &inc); err != nil {
		t.Fatal(err)
	}
	base := "/api/rooms/" + inc.ID + "/setup"
	for _, route := range []struct {
		path string
		body string
	}{
		{base + "/secret", `{"name":"source_password","value":"longenough"}`},
		{base + "/credentials", ""},
	} {
		if status, errCode := env.errCode(t, "alice", http.MethodPost, route.path, route.body); status != http.StatusNotFound || errCode != "setup_disabled" {
			t.Fatalf("%s = %d %s, want 404 setup_disabled", route.path, status, errCode)
		}
	}
}
