package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/room"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/forward"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// Every forwarding tool is KindRead and registers in a read-only registry;
// the KindRoom tools do not.
func TestReadToolsRegisterReadOnly(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	if err := RegisterReads(reg, Config{Executor: &fakeRunner{}, RoomID: "r_1"}); err != nil {
		t.Fatalf("RegisterReads into a read-only registry: %v", err)
	}
	cat, err := catalog.Builtin()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tl := range reg.All() {
		names = append(names, tl.Name())
		if tl.Kind() != tools.KindRead {
			t.Errorf("%s is %q, want read", tl.Name(), tl.Kind())
		}
		schema := tl.InputSchema()
		if schema["type"] != "object" || schema["additionalProperties"] != false {
			t.Errorf("%s schema = %v", tl.Name(), schema)
		}
		if _, ok := schema["required"]; !ok {
			t.Errorf("%s schema has no required list", tl.Name())
		}
	}
	if len(names) != len(cat.Reads()) {
		t.Fatalf("registered %v, want one per T0 op (%d)", names, len(cat.Reads()))
	}
	for _, s := range cat.Writes() {
		if slices.Contains(names, s.Name) {
			t.Errorf("the write op %s is a read tool", s.Name)
		}
	}
	if !slices.Contains(names, "runner_net_probe") || slices.Contains(names, "net_probe") {
		t.Errorf("net_probe must register as runner_net_probe (Q6a keeps net_probe): %v", names)
	}

	plan, err := NewPlanTool(PlanConfig{Sink: &recordingSink{}})
	if err != nil {
		t.Fatal(err)
	}
	verify, err := NewVerificationTool(VerifyConfig{Requester: &fakeRequester{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range []tools.Tool{plan, verify} {
		if tl.Kind() != tools.KindRoom {
			t.Errorf("%s is %q, want room", tl.Name(), tl.Kind())
		}
		if err := reg.Register(tl); !errors.Is(err, tools.ErrNotReadOnly) {
			t.Errorf("read-only registry took %s: %v", tl.Name(), err)
		}
		if err := tools.NewRegistry().Register(tl); err != nil {
			t.Errorf("a room registry refused %s: %v", tl.Name(), err)
		}
	}
}

func readToolNamed(t *testing.T, cfg Config, name string) tools.Tool {
	t.Helper()
	ts, err := ReadTools(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range ts {
		if tl.Name() == name {
			return tl
		}
	}
	t.Fatalf("no tool %s", name)
	return nil
}

func TestReadToolForwardsOneT0Op(t *testing.T) {
	f := &fakeRunner{res: okResult("notes.txt: 2 lines", map[string]any{"path": "notes.txt", "content": "a\nb\n"})}
	tl := readToolNamed(t, Config{Executor: f, RoomID: "r_1"}, "file_read")

	ctx := remoteclients.WithClass(context.Background(), protocol.ClassShared)
	res, err := tl.Run(ctx, json.RawMessage(`{"path":"notes.txt"}`))
	if err != nil {
		t.Fatal(err)
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("sent %d requests, want 1", len(reqs))
	}
	req := reqs[0]
	if req.Op != "file_read" || req.Tier != protocol.TierT0 || req.Class != protocol.ClassShared ||
		req.ProposalID != "" || req.StepIndex != nil || req.ParamsHash != "" || len(req.Proposal) != 0 {
		t.Fatalf("request = %+v", req)
	}
	if string(req.Params) != `{"path":"notes.txt"}` || req.ID == "" || req.Deadline.IsZero() {
		t.Fatalf("request params/id/deadline = %s %q %v", req.Params, req.ID, req.Deadline)
	}
	if f.rooms[0] != "r_1" {
		t.Fatalf("room = %q", f.rooms[0])
	}
	if res.Summary != "notes.txt: 2 lines" || !strings.Contains(string(res.Data.(json.RawMessage)), `"content"`) {
		t.Fatalf("result = %+v", res)
	}
	if len(res.Provenance) != 1 || res.Provenance[0].Kind != tools.CallFile || res.Provenance[0].Target != "notes.txt" {
		t.Fatalf("provenance = %+v", res.Provenance)
	}

	// The class defaults to local for a read (the runner raises it), and the
	// room comes from the call when the tools are not bound to one.
	f2 := &fakeRunner{res: okResult("ok", map[string]any{})}
	unbound := readToolNamed(t, Config{Executor: f2}, "env_detect")
	if _, err := unbound.Run(room.WithRoomID(context.Background(), "r_9"), nil); err != nil {
		t.Fatal(err)
	}
	if got := f2.requests()[0]; got.Class != protocol.ClassLocal || string(got.Params) != `{}` || f2.rooms[0] != "r_9" {
		t.Fatalf("request = %+v room %q", got, f2.rooms[0])
	}
}

func TestReadToolErrors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		cfg    Config
		ctx    context.Context
		tool   string
		input  string
		want   error
		noSend bool
	}{
		{"unknown field", Config{Executor: &fakeRunner{}, RoomID: "r"}, ctx, "file_read", `{"path":"a","rm":true}`, tools.ErrBadInput, true},
		{"missing field", Config{Executor: &fakeRunner{}, RoomID: "r"}, ctx, "file_read", `{}`, tools.ErrBadInput, true},
		{"folded keys", Config{Executor: &fakeRunner{}, RoomID: "r"}, ctx, "file_read", `{"path":"a","Path":"b"}`, tools.ErrBadInput, true},
		{"not an object", Config{Executor: &fakeRunner{}, RoomID: "r"}, ctx, "env_detect", `"x"`, tools.ErrBadInput, true},
		{"no executor", Config{RoomID: "r"}, ctx, "env_detect", `{}`, protocol.ErrNoRunner, true},
		{"no room", Config{Executor: &fakeRunner{}}, ctx, "env_detect", `{}`, protocol.ErrNoRunner, true},
		{"another room", Config{Executor: &fakeRunner{}, RoomID: "r_1"}, room.WithRoomID(ctx, "r_2"), "env_detect", `{}`, protocol.ErrNoRunner, true},
		{"runner gone", Config{Executor: &fakeRunner{err: protocol.ErrNoRunner}, RoomID: "r"}, ctx, "env_detect", `{}`, protocol.ErrNoRunner, false},
		{"runner refused the schema", Config{Executor: &fakeRunner{err: &protocol.RefusedError{Refusal: protocol.Refusal{Op: "file_read", Reason: protocol.RefuseSchema}}}, RoomID: "r"}, ctx, "file_read", `{"path":"../x"}`, tools.ErrBadInput, false},
		{"op failed", Config{Executor: &fakeRunner{res: protocol.OpResult{OK: false, Summary: "refused file_read: path refused"}}, RoomID: "r"}, ctx, "file_read", `{"path":".git/config"}`, forward.ErrOpFailed, false},
		{"oversized data", Config{Executor: &fakeRunner{res: protocol.OpResult{OK: true, Data: json.RawMessage(`"` + strings.Repeat("a", protocol.MaxDataBytes) + `"`)}}, RoomID: "r"}, ctx, "env_detect", `{}`, forward.ErrBadResult, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tl := readToolNamed(t, tc.cfg, tc.tool)
			_, err := tl.Run(tc.ctx, json.RawMessage(tc.input))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if f, ok := tc.cfg.Executor.(*fakeRunner); ok && tc.noSend && len(f.requests()) != 0 {
				t.Fatalf("sent %d requests for a call that must send none", len(f.requests()))
			}
		})
	}
}

// The tools against the real runner catalogue: what the runner's ops accept
// is what the tools forward.
func TestReadToolsAgainstRunnerCatalogue(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := ops.New(ops.Config{ProjectDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	f := &fakeRunner{exec: func(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
		res, err := r.Execute(ctx, req)
		if err != nil && res.OK {
			t.Fatalf("OK with an error: %v", err)
		}
		return res, nil
	}}
	cfg := Config{Executor: f, RoomID: "r_1"}
	res, err := readToolNamed(t, cfg, "file_read").Run(context.Background(), json.RawMessage(`{"path":"notes.txt"}`))
	if err != nil || !strings.Contains(string(res.Data.(json.RawMessage)), "hello") {
		t.Fatalf("file_read = %+v, %v", res, err)
	}
	if _, err := readToolNamed(t, cfg, "dir_list").Run(context.Background(), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("dir_list: %v", err)
	}
	// The runner's own confinement still refuses what the schema allows.
	if _, err := readToolNamed(t, cfg, "file_read").Run(context.Background(), json.RawMessage(`{"path":"../etc/passwd"}`)); !errors.Is(err, forward.ErrOpFailed) {
		t.Fatalf("an escaping path: %v", err)
	}
}
