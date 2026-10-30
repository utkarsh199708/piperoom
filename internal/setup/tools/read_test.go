package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/clients"
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
		"service_status":    json.RawMessage(`{"name":"connect"}`),
		"properties_read":   json.RawMessage(`{"file":"connect-distributed.properties"}`),
		"journal_tail":      json.RawMessage(`{"unit":"piperoom-connect.service"}`),
		"project_inspect":   json.RawMessage(`{}`),
		"embedded_log_read": json.RawMessage(`{"path":"piperoom-reference/run.log","nonce":"4f2a9c71e0b85d36"}`),

		"k8s_get":                 json.RawMessage(`{"kind":"KafkaConnector","name":"orders-src"}`),
		"k8s_events":              json.RawMessage(`{"involved":"orders-connect"}`),
		"k8s_logs":                json.RawMessage(`{"pod":"orders-connect-connect-0","tail":50}`),
		"strimzi_build_status":    json.RawMessage(`{}`),
		"preview_k8s_apply":       json.RawMessage(`{"kind":"KafkaTopic","template":"canary-topic","params":{"topic_prefix":"pg","schema":"public","table":"piperoom_canary"}}`),
		"msk_describe_cluster":    json.RawMessage(`{}`),
		"msk_list_plugins":        json.RawMessage(`{"name_prefix":"debezium"}`),
		"msk_describe_connector":  json.RawMessage(`{"name":"orders-src"}`),
		"msk_vpc_check":           json.RawMessage(`{"subnets":["subnet-0123456789abcdef0"],"security_groups":["sg-0123456789abcdef0"],"database_ip":"10.0.1.5","database_port":5432}`),
		"msk_role_check":          json.RawMessage(`{"role_arn":"arn:aws:iam::123456789012:role/msk-connect"}`),
		"msk_plugin_object_check": json.RawMessage(`{"bucket":"orders-plugins","key":"debezium/pg-3.2.0.zip","expected_size":1024}`),
		"msk_wait":                json.RawMessage(`{"kind":"connector","arn":"arn:aws:kafkaconnect:eu-west-1:123456789012:connector/orders-src/abcd-1234"}`),
		"ccloud_list_plugins":     json.RawMessage(`{}`),
		"ccloud_connector_status": json.RawMessage(`{"name":"orders-src"}`),
		"ccloud_cluster":          json.RawMessage(`{}`),
		"aiven_service_get":       json.RawMessage(`{"service":"kafka-main"}`),
		"aiven_list_plugins":      json.RawMessage(`{}`),
		"aiven_connector_status":  json.RawMessage(`{"name":"orders-src"}`),
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
	"service_status":    "service_status",
	"properties_read":   "properties_read",
	"journal_tail":      "journal_tail",
	"project_inspect":   "project_inspect",
	"embedded_log_read": "embedded_log_read",

	"k8s_get":                 "k8s_get",
	"k8s_events":              "k8s_events",
	"k8s_logs":                "k8s_logs",
	"strimzi_build_status":    "strimzi_build_status",
	"preview_k8s_apply":       "preview_k8s_apply",
	"msk_describe_cluster":    "msk_describe_cluster",
	"msk_list_plugins":        "msk_list_plugins",
	"msk_describe_connector":  "msk_describe_connector",
	"msk_vpc_check":           "msk_vpc_check",
	"msk_role_check":          "msk_role_check",
	"msk_plugin_object_check": "msk_plugin_object_check",
	"msk_wait":                "msk_wait",
	"ccloud_list_plugins":     "ccloud_list_plugins",
	"ccloud_connector_status": "ccloud_connector_status",
	"ccloud_cluster":          "ccloud_cluster",
	"aiven_service_get":       "aiven_service_get",
	"aiven_list_plugins":      "aiven_list_plugins",
	"aiven_connector_status":  "aiven_connector_status",
}

func TestRegisterReadToolsInReadOnlyRegistry(t *testing.T) {
	f := &fakeRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r_setup", CallRoom: callRoomFromCtx}); err != nil {
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

func TestRegisterRequiresTheBoundRoom(t *testing.T) {
	for _, id := range []string{"", "   "} {
		reg := tools.NewReadOnlyRegistry()
		err := Register(reg, Deps{Runner: &fakeRunner{}, RoomID: id, CallRoom: callRoomFromCtx})
		if err == nil || !strings.Contains(err.Error(), "RoomID") {
			t.Fatalf("Register(RoomID %q) = %v, want a refusal naming RoomID", id, err)
		}
		if got := len(reg.All()); got != 0 {
			t.Fatalf("a refused Register added %d tools, want 0", got)
		}
	}
}

func TestReadToolsForwardOnlyTheirWhitelistedOp(t *testing.T) {
	inputs := readInputs()
	for name, op := range readOps {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{}
			reg := tools.NewReadOnlyRegistry()
			if err := Register(reg, Deps{Runner: f, RoomID: "r_setup", Class: protocol.ClassLocal, CallRoom: callRoomFromCtx}); err != nil {
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
			if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
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
			if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
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
	if err := Register(reg, Deps{RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for name := range readOps {
		tool, _ := reg.Get(name)
		if _, err := tool.Run(context.Background(), inputs[name]); !errors.Is(err, ErrNoRunner) {
			t.Errorf("%s without a runner = %v, want ErrNoRunner", name, err)
		}
	}
}

// ctxRoomKey stands in for the room package's context key: CallRoom is how
// wiring hands this package room.RoomID without an import.
type ctxRoomKey struct{}

func callRoomFromCtx(ctx context.Context) string {
	id, _ := ctx.Value(ctxRoomKey{}).(string)
	return id
}

func TestRegisterRequiresTheCallRoomReader(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	err := Register(reg, Deps{Runner: &fakeRunner{}, RoomID: "r"})
	if err == nil || !strings.Contains(err.Error(), "CallRoom") {
		t.Fatalf("Register without CallRoom = %v, want a refusal naming CallRoom", err)
	}
	if got := len(reg.All()); got != 0 {
		t.Fatalf("a refused Register added %d tools, want 0", got)
	}
}

func TestZeroCallerFailsClosed(t *testing.T) {
	f := &fakeRunner{}
	c := &caller{runner: f, roomID: "r"}
	if _, err := c.call(context.Background(), "env_detect", nil); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("call without a room reader = %v, want ErrNoRunner", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("sent %d ops, want 0", got)
	}
}

func TestReadToolRefusesAnotherRoomsCall(t *testing.T) {
	f := &fakeRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r_one", CallRoom: callRoomFromCtx}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("env_detect")
	ctx := context.WithValue(context.Background(), ctxRoomKey{}, "r_two")
	if _, err := tool.Run(ctx, json.RawMessage(`{}`)); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("Run for another room = %v, want ErrNoRunner", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("a cross-room read sent %d ops, want 0", got)
	}
}

func TestReadToolSendsToTheBoundRoom(t *testing.T) {
	cases := map[string]context.Context{
		"no room in ctx":   context.Background(),
		"same room in ctx": context.WithValue(context.Background(), ctxRoomKey{}, "r_bound"),
	}
	for name, ctx := range cases {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{}
			reg := tools.NewReadOnlyRegistry()
			if err := Register(reg, Deps{Runner: f, RoomID: "r_bound", CallRoom: callRoomFromCtx}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			tool, _ := reg.Get("docker_info")
			if _, err := tool.Run(ctx, json.RawMessage(`{}`)); err != nil {
				t.Fatalf("Run: %v", err)
			}
			calls := f.recorded()
			if len(calls) != 1 || calls[0].roomID != "r_bound" {
				t.Fatalf("calls = %+v, want one to r_bound", calls)
			}
		})
	}
}

func TestReadToolTurnsARefusalIntoAnError(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "the plugin dir is flat"}, nil
	}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
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
	if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
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
	if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
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
			if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
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
	if err := Register(reg, Deps{Runner: f, RoomID: "r", CallRoom: callRoomFromCtx}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, _ := reg.Get("file_read")
	_, err := tool.Run(context.Background(), json.RawMessage(`{"path":"compose.yaml"}`))
	if err == nil || errors.Is(err, tools.ErrOutOfScope) || errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Run with an unknown error_code = %v, want a plain error", err)
	}
}
