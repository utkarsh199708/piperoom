package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// call is one request a fake runner received.
type call struct {
	roomID string
	req    protocol.OpRequest
}

// fakeRunner records every request and answers from answer (default: OK with
// an empty result).
type fakeRunner struct {
	mu     sync.Mutex
	calls  []call
	answer func(protocol.OpRequest) (protocol.OpResult, error)
}

func (f *fakeRunner) Execute(_ context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, call{roomID: roomID, req: req})
	answer := f.answer
	f.mu.Unlock()
	if answer != nil {
		return answer(req)
	}
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok", Data: json.RawMessage(`{"ok":true}`)}, nil
}

func (f *fakeRunner) recorded() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

// readInputs are valid inputs for every read tool.
func readInputs() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"env_detect":        json.RawMessage(`{}`),
		"java_version":      json.RawMessage(`{}`),
		"jar_list":          json.RawMessage(`{"path":"connect-plugins"}`),
		"file_read":         json.RawMessage(`{"path":"compose.yaml"}`),
		"dir_list":          json.RawMessage(`{"path":"."}`),
		"docker_info":       json.RawMessage(`{}`),
		"compose_ps":        json.RawMessage(`{"services":["connect"]}`),
		"compose_config":    json.RawMessage(`{"file":"compose.yaml"}`),
		"container_inspect": json.RawMessage(`{"service":"connect"}`),
		"container_logs":    json.RawMessage(`{"service":"connect"}`),
	}
}

// readOps pins every tool to its one runner op. A tool cannot grow a second
// op or take one from its input without failing this table.
var readOps = map[string]string{
	"env_detect":        "env_detect",
	"java_version":      "java_version",
	"jar_list":          "jar_list",
	"file_read":         "file_read",
	"dir_list":          "dir_list",
	"docker_info":       "docker_info",
	"compose_ps":        "compose_ps",
	"compose_config":    "compose_config",
	"container_inspect": "container_inspect",
	"container_logs":    "container_logs",
}

func TestRegisterReadToolsInReadOnlyRegistry(t *testing.T) {
	f := &fakeRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r_setup"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !reg.ReadOnly() {
		t.Fatal("the registry is not read-only")
	}
	all := reg.All()
	if len(all) != len(readOps) {
		t.Fatalf("registered %d tools, want %d", len(all), len(readOps))
	}
	names := ReadToolNames()
	for i, tool := range all {
		if tool.Name() != names[i] {
			t.Errorf("tool %d = %q, want %q", i, tool.Name(), names[i])
		}
		if got := tool.Kind(); got != tools.KindRead {
			t.Errorf("%s kind = %q, want %q", tool.Name(), got, tools.KindRead)
		}
		schema := tool.InputSchema()
		if schema["type"] != "object" {
			t.Errorf("%s schema type = %v, want object", tool.Name(), schema["type"])
		}
		if ap, _ := schema["additionalProperties"].(bool); ap {
			t.Errorf("%s schema is open (additionalProperties true)", tool.Name())
		}
		if _, ok := schema["required"].([]string); !ok {
			t.Errorf("%s schema has no required list", tool.Name())
		}
	}
}

func TestRegisterRefusesAPlainRegistry(t *testing.T) {
	err := Register(tools.NewRegistry(), Deps{Runner: &fakeRunner{}})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Register(plain registry) = %v, want a read-only refusal", err)
	}
	if err := Register(nil, Deps{}); err == nil {
		t.Fatal("Register(nil) = nil, want an error")
	}
}

func TestReadToolsForwardOnlyTheirWhitelistedOp(t *testing.T) {
	inputs := readInputs()
	for name, op := range readOps {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{}
			reg := tools.NewReadOnlyRegistry()
			if err := Register(reg, Deps{Runner: f, RoomID: "r_setup", Class: protocol.ClassLocal}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			tool, ok := reg.Get(name)
			if !ok {
				t.Fatalf("%s did not register", name)
			}
			res, err := tool.Run(context.Background(), inputs[name])
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Summary == "" {
				t.Fatal("Run returned an empty summary")
			}
			calls := f.recorded()
			if len(calls) != 1 {
				t.Fatalf("sent %d ops, want exactly 1", len(calls))
			}
			req := calls[0].req
			if req.Op != op {
				t.Fatalf("op = %q, want %q", req.Op, op)
			}
			if req.Tier != protocol.TierT0 {
				t.Fatalf("tier = %q, want T0", req.Tier)
			}
			if req.ProposalID != "" || req.StepIndex != nil || req.ParamsHash != "" {
				t.Fatalf("a read carried proposal fields: %+v", req)
			}
			if req.Class != protocol.ClassLocal {
				t.Fatalf("class = %q, want local", req.Class)
			}
			if calls[0].roomID != "r_setup" {
				t.Fatalf("room = %q, want r_setup", calls[0].roomID)
			}
			if req.Deadline.IsZero() {
				t.Fatal("the read has no deadline")
			}
			if !json.Valid(req.Params) {
				t.Fatalf("params are not JSON: %s", req.Params)
			}
		})
	}
}

func TestReadToolsRefuseUnknownInputFields(t *testing.T) {
	cases := map[string]json.RawMessage{
		// No tool may take an op name from the model: there is no generic
		// passthrough.
		"env_detect":        json.RawMessage(`{"op":"compose_up"}`),
		"java_version":      json.RawMessage(`{"path":"."}`),
		"file_read":         json.RawMessage(`{"path":"a","op":"file_delete"}`),
		"jar_list":          json.RawMessage(`{"path":"a","extra":true}`),
		"container_inspect": json.RawMessage(`{"service":"connect","command":"rm -rf /"}`),
		// compose_config renders the whole project; its schema has only file.
		"compose_config": json.RawMessage(`{"file":"compose.yaml","services":["connect"]}`),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{}
			reg := tools.NewReadOnlyRegistry()
			if err := Register(reg, Deps{Runner: f}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			tool, _ := reg.Get(name)
			if _, err := tool.Run(context.Background(), input); !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("Run(%s) = %v, want ErrBadInput", input, err)
			}
			if got := len(f.recorded()); got != 0 {
				t.Fatalf("a refused input sent %d ops, want 0", got)
			}
		})
	}
}

func TestReadToolsRefuseBadInput(t *testing.T) {
	cases := []struct {
		tool  string
		input json.RawMessage
	}{
		{"file_read", json.RawMessage(`{}`)},
		{"file_read", json.RawMessage(`{"path":""}`)},
		{"container_inspect", json.RawMessage(`{"service":""}`)},
		{"container_logs", json.RawMessage(`{}`)},
		{"compose_ps", json.RawMessage(`{"services":"connect"}`)},
		{"dir_list", json.RawMessage(`{"path":42}`)},
		{"compose_config", json.RawMessage(`[]`)},
	}
	for _, tc := range cases {
		t.Run(tc.tool+string(tc.input), func(t *testing.T) {
			f := &fakeRunner{}
			reg := tools.NewReadOnlyRegistry()
			if err := Register(reg, Deps{Runner: f}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			tool, _ := reg.Get(tc.tool)
			if _, err := tool.Run(context.Background(), tc.input); !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("Run(%s) = %v, want ErrBadInput", tc.input, err)
			}
			if got := len(f.recorded()); got != 0 {
				t.Fatalf("bad input sent %d ops, want 0", got)
			}
		})
	}
}

func TestReadToolsWithoutARunner(t *testing.T) {
	inputs := readInputs()
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for name := range readOps {
		tool, _ := reg.Get(name)
		if _, err := tool.Run(context.Background(), inputs[name]); !errors.Is(err, ErrNoRunner) {
			t.Errorf("%s without a runner = %v, want ErrNoRunner", name, err)
		}
	}
}

func TestReadToolRefusesAnotherRoomsCall(t *testing.T) {
	f := &fakeRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r_one"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("env_detect")
	ctx := room.WithRoomID(context.Background(), "r_two")
	if _, err := tool.Run(ctx, json.RawMessage(`{}`)); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("Run for another room = %v, want ErrNoRunner", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("a cross-room read sent %d ops, want 0", got)
	}
}

func TestReadToolTakesTheRoomsIDFromContext(t *testing.T) {
	f := &fakeRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("docker_info")
	ctx := room.WithRoomID(context.Background(), "r_ctx")
	if _, err := tool.Run(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := f.recorded()
	if len(calls) != 1 || calls[0].roomID != "r_ctx" {
		t.Fatalf("calls = %+v, want one to r_ctx", calls)
	}
}

func TestReadToolTurnsARefusalIntoAnError(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "the plugin dir is flat"}, nil
	}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("jar_list")
	_, err := tool.Run(context.Background(), json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "flat") {
		t.Fatalf("Run = %v, want the runner's refusal", err)
	}
}

func TestReadToolTruncatesALongSummary(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Summary: strings.Repeat("x", tools.MaxSummaryBytes+100)}, nil
	}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("env_detect")
	res, err := tool.Run(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Summary) > tools.MaxSummaryBytes || !res.Truncated {
		t.Fatalf("summary is %d bytes, truncated %v; want it cut and marked", len(res.Summary), res.Truncated)
	}
}

func TestReadToolWithholdsCutData(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Summary: "partial", Data: json.RawMessage(`{"truncated":true}`), Truncated: true}, nil
	}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("compose_ps")
	res, err := tool.Run(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Truncated {
		t.Fatal("a cut result is not marked truncated")
	}
	if res.Data != nil {
		t.Fatalf("cut data was returned: %v", res.Data)
	}
}

func TestReadToolMapsErrorCodesToSentinels(t *testing.T) {
	for code, want := range map[string]error{
		"out_of_scope": tools.ErrOutOfScope,
		"not_found":    clients.ErrNotFound,
	} {
		t.Run(code, func(t *testing.T) {
			f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
				return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused", Data: json.RawMessage(`{"error_code":"` + code + `"}`)}, nil
			}}
			reg := tools.NewReadOnlyRegistry()
			if err := Register(reg, Deps{Runner: f, RoomID: "r"}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			tool, _ := reg.Get("file_read")
			_, err := tool.Run(context.Background(), json.RawMessage(`{"path":"compose.yaml"}`))
			if !errors.Is(err, want) {
				t.Fatalf("Run with error_code %q = %v, want %v", code, err, want)
			}
		})
	}
	// An unknown code is still an error, with no sentinel.
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused", Data: json.RawMessage(`{"error_code":"weird"}`)}, nil
	}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("file_read")
	_, err := tool.Run(context.Background(), json.RawMessage(`{"path":"compose.yaml"}`))
	if err == nil || errors.Is(err, tools.ErrOutOfScope) || errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Run with an unknown error_code = %v, want a plain error", err)
	}
}
