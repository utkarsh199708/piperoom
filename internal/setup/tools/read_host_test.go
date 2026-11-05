package setuptools_test

// Round trip of the host target's read tools (row U8f): the params a tool
// forwards must pass the host driver's own Validate, so the tool-side mirror
// of the driver's rules (hostServiceNameRE, hostUnitRE, the worker file enum)
// cannot drift into sending params the runner would refuse. This file is an
// external test because the package itself must not import the host target
// (the agent imports the package; the host target reaches the agent through
// ops, connprobe and wiring).

import (
	"context"
	"encoding/json"
	"errors"

	"sync"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/host"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// recordingRunner keeps every OpRequest and answers OK.
type recordingRunner struct {
	mu   sync.Mutex
	reqs []protocol.OpRequest
}

func (r *recordingRunner) Execute(_ context.Context, _ string, req protocol.OpRequest) (protocol.OpResult, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok", Data: json.RawMessage(`{"ok":true}`)}, nil
}

func (r *recordingRunner) last(t *testing.T) protocol.OpRequest {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.reqs) == 0 {
		t.Fatal("no op reached the runner")
	}
	return r.reqs[len(r.reqs)-1]
}

func TestHostReadToolParamsRoundTrip(t *testing.T) {
	hostReg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = hostReg.Close() })
	if _, err := host.Register(hostReg, host.Options{}); err != nil {
		t.Fatalf("host.Register: %v", err)
	}

	rn := &recordingRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := setuptools.Register(reg, setuptools.Deps{
		Runner:   rn,
		RoomID:   "r_setup",
		CallRoom: func(context.Context) string { return "r_setup" },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	good := map[string]json.RawMessage{
		"service_status":  json.RawMessage(`{"name":"connect"}`),
		"properties_read": json.RawMessage(`{"file":"kafka/config/connect-distributed.properties"}`),
		"journal_tail":    json.RawMessage(`{"unit":"piperoom-connect.service","user":true}`),
	}
	for name, input := range good {
		tool, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s did not register", name)
		}
		if _, err := tool.Run(context.Background(), input); err != nil {
			t.Fatalf("%s: Run = %v", name, err)
		}
		req := rn.last(t)
		if req.Op != name {
			t.Fatalf("%s forwarded op %q", name, req.Op)
		}
		op, ok := hostReg.Get(req.Op)
		if !ok {
			t.Fatalf("%s: not in the host catalogue", req.Op)
		}
		// On an OS with no service manager (macOS) the driver refuses the
		// service ops before reading their params.
		if err := op.Validate(req.Params, ops.Env{}); err != nil && !errors.Is(err, host.ErrUnsupported) {
			t.Errorf("%s: the host driver refuses the forwarded params: %v", name, err)
		}
	}

	bad := map[string]json.RawMessage{
		"service_status":  json.RawMessage(`{"name":"Not A Name"}`),
		"properties_read": json.RawMessage(`{"file":"../etc/passwd"}`),
		"journal_tail":    json.RawMessage(`{"unit":"sshd.service; rm -rf /"}`),
	}
	for name, input := range bad {
		tool, _ := reg.Get(name)
		before := len(rn.reqs)
		if _, err := tool.Run(context.Background(), input); err == nil {
			t.Errorf("%s: accepted %s", name, input)
		}
		if len(rn.reqs) != before {
			t.Errorf("%s: a refused input still reached the runner", name)
		}
	}
}
