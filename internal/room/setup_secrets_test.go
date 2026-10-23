package room_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
)

// Setup secrets and the runner's Credentials frame (U1-5d; docs/api.md
// "Setup rooms", docs/design/setup-agent.md §2.4 and §3.7).

// secretSetupRequest is a setup request on a server without tenants: the
// tenancy rules have their own tests (setup_test.go).
func secretSetupRequest(id string) events.SetupRequest {
	req := setupRequest(id)
	req.Tenant = ""
	return req
}

// attachSetupRunner pairs a fake runner to the room and returns its frame
// capture and attachment token.
func attachSetupRunner(t *testing.T, env *setupEnv, roomID string) (*captureWriter, string) {
	t.Helper()
	creator := events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"}
	token := "tok-" + roomID
	if err := env.svc.AttachRunner(context.Background(), "CODE"+roomID, room.Runner{
		RoomID: roomID, Creator: creator, Label: "laptop", OS: "linux", Arch: "amd64",
		Token: token, TokenExpiresAt: env.now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("attach runner: %v", err)
	}
	w := &captureWriter{}
	if !env.svc.BindRunnerWriter("", roomID, "alice", token, w) {
		t.Fatal("bind writer refused")
	}
	return w, token
}

// assertNoValueInEvents fails when any event's JSON carries value. It is the
// test behind "the value appears in no event": events are compared as their
// stored JSON, not as a field they might be expected in.
func assertNoValueInEvents(t *testing.T, evs []events.Event, value string) {
	t.Helper()
	for _, e := range evs {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), value) {
			t.Fatalf("the secret value is in the %s event: %s", e.Type, b)
		}
	}
}

// framesOfType copies the captured frames of one type.
func framesOfType(w *captureWriter, typ protocol.FrameType) []protocol.Frame {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []protocol.Frame
	for _, f := range w.frames {
		if f.Type == typ {
			out = append(out, f)
		}
	}
	return out
}

// assertNoFiles fails when anything was written under dir.
func assertNoFiles(t *testing.T, dir string) {
	t.Helper()
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			t.Errorf("the server wrote %s", path)
		}
		return nil
	})
}

// TestSetupSecretForwardsOnceAndKeepsNothing: POST the secret route's service
// call once, and check the value is in exactly one secret frame, in no event
// and in no file; the event carries the name and the driver.
func TestSetupSecretForwardsOnceAndKeepsNothing(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID
	w, _ := attachSetupRunner(t, env, roomID)

	const value = "hunter2-not-in-any-event"
	if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", value); err != nil {
		t.Fatalf("SetSetupSecret: %v", err)
	}

	frames := framesOfType(w, protocol.FrameSecret)
	if len(frames) != 1 || frames[0].Secret == nil {
		t.Fatalf("secret frames = %d, want exactly 1", len(frames))
	}
	if frames[0].Secret.Name != "source_password" || frames[0].Secret.Value != value {
		t.Fatalf("frame = %s", frames[0].Secret.Name)
	}

	set := eventsOf[events.SetupSecretSetPayload](t, env.svc, roomID, events.SetupSecretSet)
	if len(set) != 1 || set[0].Name != "source_password" || set[0].Actor.ID != "alice" {
		t.Fatalf("setup.secret_set = %+v", set)
	}
	evs := roomEvents(t, env.svc, roomID)
	assertNoValueInEvents(t, evs, value)
	snap, err := env.svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	snapJSON, _ := json.Marshal(snap)
	if strings.Contains(string(snapJSON), value) {
		t.Fatal("the secret value is in the room snapshot")
	}
	// The route stores nothing: the server's secrets directory stays empty.
	assertNoFiles(t, env.secretsDir)

	// The driver's lease is renewed by the click.
	if snap.Driver == nil || !snap.Driver.LeaseUntil.After(env.now) {
		t.Fatalf("driver lease = %+v", snap.Driver)
	}
}

// TestSetupSecretValidation: name and value run the rule before anything is
// written, and the value is never echoed.
func TestSetupSecretValidation(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID
	w, _ := attachSetupRunner(t, env, roomID)
	cases := []struct {
		name, secretName, value string
	}{
		{"empty name", "", "longenough"},
		{"name is a path segment", "../etc/passwd", "longenough"},
		{"name with a slash", "a/b", "longenough"},
		{"name with a space", "source password", "longenough"},
		{"name with a dot", "source.password", "longenough"},
		{"short value", "source_password", "short"},
		{"empty value", "source_password", ""},
		{"long value", "source_password", strings.Repeat("x", maxValueForTest+1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := env.svc.SetSetupSecret(ctx, alice(), roomID, c.secretName, c.value)
			wantSetupError(t, err, 400, "validation")
			if c.value != "" && strings.Contains(err.Error(), c.value) {
				t.Fatalf("the refusal echoes the value: %v", err)
			}
		})
	}
	if n := len(framesOfType(w, protocol.FrameSecret)); n != 0 {
		t.Fatalf("%d secret frames reached the runner, want 0", n)
	}
	if n := len(eventsOf[events.SetupSecretSetPayload](t, env.svc, roomID, events.SetupSecretSet)); n != 0 {
		t.Fatalf("%d setup.secret_set events, want 0", n)
	}
}

// maxValueForTest is the documented maximum secret length (docs/api.md).
const maxValueForTest = 4 << 10

// TestSetupSecretRefusals: a non-driver is 403, a room without a runner is
// 409 no_runner with no event, and setup off is 404 setup_disabled.
func TestSetupSecretRefusals(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID

	// No runner yet: the value must not be claimed in the log.
	err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", "longenough")
	wantSetupError(t, err, 409, "no_runner")
	if n := len(eventsOf[events.SetupSecretSetPayload](t, env.svc, roomID, events.SetupSecretSet)); n != 0 {
		t.Fatalf("%d setup.secret_set events, want 0", n)
	}

	_, _ = attachSetupRunner(t, env, roomID)
	// A member who does not drive is refused before anything moves.
	err = env.svc.SetSetupSecret(ctx, ann(), roomID, "source_password", "longenough")
	wantSetupError(t, err, 403, "not_driver")
}

// TestSetupCredentialsWritesFiles: the register frame reaches the runner with
// the room's pipeline id and the recorded names; the Credentials frame writes
// <pipeline>/<name> 0600 through the directory, emits the names only, and is
// accepted once.
func TestSetupCredentialsWritesFiles(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID
	w, token := attachSetupRunner(t, env, roomID)

	const password = "read-role-password"
	const apiKey = "api-key-value-1"
	for _, s := range []struct{ name, value string }{
		{"source_password", password}, {"api_key", apiKey},
	} {
		if err := env.svc.SetSetupSecret(ctx, alice(), roomID, s.name, s.value); err != nil {
			t.Fatalf("SetSetupSecret %s: %v", s.name, err)
		}
	}
	if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
		t.Fatalf("RegisterSetupCredentials: %v", err)
	}
	frames := framesOfType(w, protocol.FrameRegisterCredentials)
	if len(frames) != 1 || frames[0].RegisterCredentials == nil {
		t.Fatalf("register frames = %d, want exactly 1", len(frames))
	}
	req := frames[0].RegisterCredentials
	if req.PipelineID != "orders-setup" {
		t.Fatalf("pipeline id = %q, want the room's", req.PipelineID)
	}
	if req.RequestID == "" {
		t.Fatal("request id is empty")
	}
	if len(req.Names) != 2 || req.Names[0] != "source_password" || req.Names[1] != "api_key" {
		t.Fatalf("names = %v, want the recorded names in order", req.Names)
	}

	// The answer carries the requested set in the other order: a set, not a
	// sequence.
	creds := protocol.Credentials{RequestID: req.RequestID, Secrets: []protocol.Secret{
		{Name: "api_key", Value: apiKey},
		{Name: "source_password", Value: password},
	}}
	env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, creds)

	registered := eventsOf[events.SetupCredentialsRegisteredPayload](t, env.svc, roomID, events.SetupCredentialsRegistered)
	if len(registered) != 1 {
		t.Fatalf("setup.credentials_registered = %d, want 1", len(registered))
	}
	got := registered[0]
	if got.PipelineID != "orders-setup" || got.RunnerLabel != "laptop" {
		t.Fatalf("registered = %+v", got)
	}
	if len(got.Names) != 2 || got.Names[0] != "source_password" || got.Names[1] != "api_key" {
		t.Fatalf("registered names = %v", got.Names)
	}
	evs := roomEvents(t, env.svc, roomID)
	assertNoValueInEvents(t, evs, password)
	assertNoValueInEvents(t, evs, apiKey)

	for name, want := range map[string]string{"source_password": password, "api_key": apiKey} {
		path := filepath.Join(env.secretsDir, "orders-setup", name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(b) != want {
			t.Fatalf("%s content = %q", name, b)
		}
		if runtime.GOOS != "windows" {
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if perm := fi.Mode().Perm(); perm != 0o600 {
				t.Fatalf("%s mode = %o, want 600", name, perm)
			}
		}
	}

	// The request is answered: the same frame again is refused, and a second
	// registration needs a new request.
	before := len(eventsOf[events.SetupRunnerRefusedPayload](t, env.svc, roomID, events.SetupRunnerRefused))
	env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, creds)
	refusals := eventsOf[events.SetupRunnerRefusedPayload](t, env.svc, roomID, events.SetupRunnerRefused)
	if len(refusals) != before+1 {
		t.Fatalf("second frame: refusals = %d, want %d", len(refusals), before+1)
	}
	last := refusals[len(refusals)-1]
	if last.Op != "register_credentials" || last.Reason != "credentials" {
		t.Fatalf("refusal = %+v", last)
	}
	if n := len(eventsOf[events.SetupCredentialsRegisteredPayload](t, env.svc, roomID, events.SetupCredentialsRegistered)); n != 1 {
		t.Fatalf("registered events = %d after a replay, want 1", n)
	}
	if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
		t.Fatalf("a new request after the answer: %v", err)
	}
}

// TestSetupCredentialsRefusals: every frame that does not answer the pending
// request exactly is setup.runner_refused {reason: "credentials"} and leaves
// no file behind.
func TestSetupCredentialsRefusals(t *testing.T) {
	ctx := context.Background()
	const value = "read-role-password"

	type testCase struct {
		name string
		// setup prepares the request and returns the frames to send.
		frames func(t *testing.T, env *setupEnv, roomID, token, requestID string) []protocol.Credentials
		// wantRegistered is 1 only for the pipeline-id case, which is a valid
		// frame that must be written under the room's pipeline.
		wantRegistered int
	}
	cases := []testCase{
		{name: "unknown request id", frames: func(_ *testing.T, _ *setupEnv, _, _, _ string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: "cred_never-issued", Secrets: []protocol.Secret{{Name: "source_password", Value: value}}}}
		}},
		{name: "missing name", frames: func(_ *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: id}}
		}},
		{name: "extra name", frames: func(_ *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: id, Secrets: []protocol.Secret{
				{Name: "source_password", Value: value}, {Name: "not_requested", Value: value},
			}}}
		}},
		{name: "renamed", frames: func(_ *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: id, Secrets: []protocol.Secret{{Name: "other_name", Value: value}}}}
		}},
		{name: "duplicate name", frames: func(_ *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: id, Secrets: []protocol.Secret{
				{Name: "source_password", Value: value}, {Name: "source_password", Value: value},
			}}}
		}},
		{name: "short value", frames: func(_ *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: id, Secrets: []protocol.Secret{{Name: "source_password", Value: "short"}}}}
		}},
		{name: "unsafe name", frames: func(_ *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			return []protocol.Credentials{{RequestID: id, Secrets: []protocol.Secret{{Name: "../escape", Value: value}}}}
		}},
		{name: "pipeline id in the frame is the room's", wantRegistered: 1, frames: func(t *testing.T, _ *setupEnv, _, _, id string) []protocol.Credentials {
			// A peer's unknown pipeline_id field is ignored: the frame decodes
			// and the room's pipeline is the one that gets the file.
			raw := `{"request_id":"` + id + `","pipeline_id":"another-pipeline","secrets":[{"name":"source_password","value":"` + value + `"}]}`
			var frame protocol.Credentials
			if err := json.Unmarshal([]byte(raw), &frame); err != nil {
				t.Fatalf("decode crafted frame: %v", err)
			}
			return []protocol.Credentials{frame}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newSetupEnv(t, setupOptions{})
			res := createSetup(t, env, secretSetupRequest("orders-setup"))
			roomID := res.Room.ID
			w, token := attachSetupRunner(t, env, roomID)
			if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", value); err != nil {
				t.Fatalf("SetSetupSecret: %v", err)
			}
			if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
				t.Fatalf("RegisterSetupCredentials: %v", err)
			}
			regs := framesOfType(w, protocol.FrameRegisterCredentials)
			if len(regs) != 1 || regs[0].RegisterCredentials == nil {
				t.Fatalf("register frames = %d", len(regs))
			}
			for _, frame := range c.frames(t, env, roomID, token, regs[0].RegisterCredentials.RequestID) {
				env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, frame)
			}
			registered := len(eventsOf[events.SetupCredentialsRegisteredPayload](t, env.svc, roomID, events.SetupCredentialsRegistered))
			if registered != c.wantRegistered {
				t.Fatalf("setup.credentials_registered = %d, want %d", registered, c.wantRegistered)
			}
			if c.wantRegistered == 0 {
				refusals := eventsOf[events.SetupRunnerRefusedPayload](t, env.svc, roomID, events.SetupRunnerRefused)
				if len(refusals) != 1 || refusals[0].Op != "register_credentials" || refusals[0].Reason != "credentials" {
					t.Fatalf("refusals = %+v, want one register_credentials/credentials", refusals)
				}
				if _, err := os.Stat(filepath.Join(env.secretsDir, "orders-setup")); !os.IsNotExist(err) {
					t.Fatalf("the refused frame wrote %s: %v", "orders-setup", err)
				}
			}
		})
	}

	// The crafted frame in the pipeline case never writes another pipeline's
	// directory: a fresh case to check that side effect.
	t.Run("no other pipeline directory", func(t *testing.T) {
		env := newSetupEnv(t, setupOptions{})
		res := createSetup(t, env, secretSetupRequest("orders-setup"))
		roomID := res.Room.ID
		w, token := attachSetupRunner(t, env, roomID)
		if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", value); err != nil {
			t.Fatal(err)
		}
		if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
			t.Fatal(err)
		}
		id := framesOfType(w, protocol.FrameRegisterCredentials)[0].RegisterCredentials.RequestID
		var frame protocol.Credentials
		raw := `{"request_id":"` + id + `","pipeline_id":"another-pipeline","secrets":[{"name":"source_password","value":"` + value + `"}]}`
		if err := json.Unmarshal([]byte(raw), &frame); err != nil {
			t.Fatal(err)
		}
		env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, frame)
		if _, err := os.Stat(filepath.Join(env.secretsDir, "another-pipeline")); !os.IsNotExist(err) {
			t.Fatalf("a frame's pipeline id created a directory: %v", err)
		}
		if _, err := os.Stat(filepath.Join(env.secretsDir, "orders-setup", "source_password")); err != nil {
			t.Fatalf("the room's pipeline has no file: %v", err)
		}
	})
}

// TestSetupCredentialsOnePending: a second click while a request is pending is
// 409 already_decided, and a room without a runner is 409 no_runner.
func TestSetupCredentialsOnePending(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID

	err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID)
	wantSetupError(t, err, 409, "no_runner")

	w, _ := attachSetupRunner(t, env, roomID)
	if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", "longenough"); err != nil {
		t.Fatal(err)
	}
	if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
		t.Fatalf("register: %v", err)
	}
	err = env.svc.RegisterSetupCredentials(ctx, alice(), roomID)
	wantSetupError(t, err, 409, "already_decided")
	if n := len(framesOfType(w, protocol.FrameRegisterCredentials)); n != 1 {
		t.Fatalf("register frames = %d, want 1", n)
	}

	// A non-driver cannot ask either.
	err = env.svc.RegisterSetupCredentials(ctx, ann(), roomID)
	wantSetupError(t, err, 403, "not_driver")
}

// TestSetupCredentialsNoRecordedNames: without a recorded secret name there is
// nothing to ask for, and no frame is sent.
func TestSetupCredentialsNoRecordedNames(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	w, _ := attachSetupRunner(t, env, res.Room.ID)
	err := env.svc.RegisterSetupCredentials(ctx, alice(), res.Room.ID)
	wantSetupError(t, err, 400, "validation")
	if n := len(framesOfType(w, protocol.FrameRegisterCredentials)); n != 0 {
		t.Fatalf("register frames = %d, want 0", n)
	}
}

// TestSetupCredentialsDirectoryAtNameRefused: a directory where the file
// belongs is refused like a symlink; nothing is written into it. It runs
// where symlink creation is not available (Windows without the privilege),
// so the non-regular-entry check is covered on every OS.
func TestSetupCredentialsDirectoryAtNameRefused(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID
	w, token := attachSetupRunner(t, env, roomID)
	const value = "read-role-password"
	if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", value); err != nil {
		t.Fatal(err)
	}
	if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
		t.Fatal(err)
	}
	id := framesOfType(w, protocol.FrameRegisterCredentials)[0].RegisterCredentials.RequestID

	pipelineDir := filepath.Join(env.secretsDir, "orders-setup")
	if err := os.MkdirAll(filepath.Join(pipelineDir, "source_password"), 0o700); err != nil {
		t.Fatal(err)
	}
	env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, protocol.Credentials{
		RequestID: id, Secrets: []protocol.Secret{{Name: "source_password", Value: value}},
	})
	refusals := eventsOf[events.SetupRunnerRefusedPayload](t, env.svc, roomID, events.SetupRunnerRefused)
	if len(refusals) != 1 || refusals[0].Reason != "credentials" {
		t.Fatalf("refusals = %+v", refusals)
	}
	entries, err := os.ReadDir(filepath.Join(pipelineDir, "source_password"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the value was written into the directory: %v", entries)
	}
}

// TestSetupCredentialsSymlinkRefused: the target file is a symlink, and the
// value is refused rather than written through it. The target is untouched.
func TestSetupCredentialsSymlinkRefused(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID
	w, token := attachSetupRunner(t, env, roomID)
	const value = "read-role-password"
	if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", value); err != nil {
		t.Fatal(err)
	}
	if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
		t.Fatal(err)
	}
	id := framesOfType(w, protocol.FrameRegisterCredentials)[0].RegisterCredentials.RequestID

	pipelineDir := filepath.Join(env.secretsDir, "orders-setup")
	if err := os.MkdirAll(pipelineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(outside, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(pipelineDir, "source_password")); err != nil {
		t.Skipf("the OS refused to create a symlink: %v", err)
	}
	env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, protocol.Credentials{
		RequestID: id, Secrets: []protocol.Secret{{Name: "source_password", Value: value}},
	})
	if n := len(eventsOf[events.SetupCredentialsRegisteredPayload](t, env.svc, roomID, events.SetupCredentialsRegistered)); n != 0 {
		t.Fatalf("registered = %d, want 0", n)
	}
	refusals := eventsOf[events.SetupRunnerRefusedPayload](t, env.svc, roomID, events.SetupRunnerRefused)
	if len(refusals) != 1 || refusals[0].Reason != "credentials" {
		t.Fatalf("refusals = %+v", refusals)
	}
	if b, err := os.ReadFile(outside); err != nil || string(b) != "untouched" {
		t.Fatalf("the symlink target changed: %q, %v", b, err)
	}
	// The symlink itself was not replaced with a regular file either: os.Root
	// with a rename would replace the link, and a later registration could
	// then write a path the runner believes points elsewhere.
	fi, err := os.Lstat(filepath.Join(pipelineDir, "source_password"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatal("the symlink was replaced")
	}
}

// TestSetupCredentialsSymlinkDirRefused: a symlink where the pipeline
// directory belongs is refused too, not followed.
func TestSetupCredentialsSymlinkDirRefused(t *testing.T) {
	ctx := context.Background()
	env := newSetupEnv(t, setupOptions{})
	res := createSetup(t, env, secretSetupRequest("orders-setup"))
	roomID := res.Room.ID
	w, token := attachSetupRunner(t, env, roomID)
	const value = "read-role-password"
	if err := env.svc.SetSetupSecret(ctx, alice(), roomID, "source_password", value); err != nil {
		t.Fatal(err)
	}
	if err := env.svc.RegisterSetupCredentials(ctx, alice(), roomID); err != nil {
		t.Fatal(err)
	}
	id := framesOfType(w, protocol.FrameRegisterCredentials)[0].RegisterCredentials.RequestID

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(env.secretsDir, "orders-setup")); err != nil {
		t.Skipf("the OS refused to create a symlink: %v", err)
	}
	env.svc.RunnerCredentials(ctx, "", roomID, "alice", token, protocol.Credentials{
		RequestID: id, Secrets: []protocol.Secret{{Name: "source_password", Value: value}},
	})
	refusals := eventsOf[events.SetupRunnerRefusedPayload](t, env.svc, roomID, events.SetupRunnerRefused)
	if len(refusals) != 1 || refusals[0].Reason != "credentials" {
		t.Fatalf("refusals = %+v", refusals)
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the value was written through the directory symlink: %v", entries)
	}
}
