package evals

import (
	"context"
	"encoding/json"
	"errors"
	"piperoom/internal/agent"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/runbooks"
)

func TestReplayLookup(t *testing.T) {
	recs := []Recording{
		{Tool: "kafka_find_by_key", Summary: "wildcard"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t"}, Summary: "topic"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t", "key": "42"}, Summary: "topic+key"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t", "key": "42"}, Summary: "later duplicate"},
		{Tool: "schema_registry_diff", Input: map[string]any{"subject": "s", "to_version": 2}, Summary: "v2"},
		{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "c"},
	}
	rp, err := newReplayer(recs)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, tool, input string
		want              string // "" = miss
	}{
		{"most specific wins", "kafka_find_by_key", `{"topic":"t","key":"42","since":"2026-10-11T10:00:00Z"}`, "topic+key"},
		{"extra call fields ignored", "kafka_find_by_key", `{"topic":"t","key":"7","since":"x"}`, "topic"},
		{"wildcard fallback", "kafka_find_by_key", `{"topic":"u","key":"7","since":"x"}`, "wildcard"},
		{"yaml int equals json number", "schema_registry_diff", `{"subject":"s","to_version":2}`, "v2"},
		{"value differs", "schema_registry_diff", `{"subject":"s","to_version":3}`, ""},
		{"field absent", "schema_registry_diff", `{"subject":"s"}`, ""},
		{"no wildcard means miss", "connect_status", `{"connector":"other"}`, ""},
		{"unknown tool", "pg_row", `{}`, ""},
		{"bad json", "connect_status", `{`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, ok := rp.lookup(tt.tool, json.RawMessage(tt.input))
			if tt.want == "" {
				if ok {
					t.Errorf("got %q, want miss", rec.Summary)
				}
				return
			}
			if !ok || rec.Summary != tt.want {
				t.Errorf("got %q (ok %v), want %q", rec.Summary, ok, tt.want)
			}
		})
	}
}

func TestReplayToolRun(t *testing.T) {
	rp, err := newReplayer([]Recording{
		{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "ok", Data: map[string]any{"tasks": []any{map[string]any{"id": 0}}}},
		{Tool: "connect_status", Input: map[string]any{"connector": "down"}, Error: "connect: 503"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := rp.registry(defs)
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.All()) != len(defs) {
		t.Fatalf("registry has %d tools, want %d", len(reg.All()), len(defs))
	}
	tool, _ := reg.Get("connect_status")
	var live tools.Tool
	for _, d := range defs {
		if d.Name() == "connect_status" {
			live = d
		}
	}
	if tool.Description() != live.Description() || tool.Kind() != live.Kind() {
		t.Error("replay tool does not present the live definition")
	}

	res, err := tool.Run(context.Background(), json.RawMessage(`{"connector":"c"}`))
	if err != nil || res.Summary != "ok" {
		t.Fatalf("run = %+v, %v", res, err)
	}
	if b, _ := json.Marshal(res.Data); string(b) != `{"tasks":[{"id":0}]}` {
		t.Errorf("data = %s", b)
	}
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"connector":"down"}`)); err == nil || err.Error() != "connect: 503" {
		t.Errorf("error recording: %v", err)
	}
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"connector":"x"}`)); !errors.Is(err, errNoRecording) {
		t.Errorf("miss: %v", err)
	}
	if m := rp.Misses(); len(m) != 1 || m[0].Tool != "connect_status" || string(m[0].Input) != `{"connector":"x"}` {
		t.Errorf("misses = %+v", m)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Run(ctx, json.RawMessage(`{"connector":"c"}`)); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled ctx: %v", err)
	}
}

func TestReplayRejectsUnmarshalableData(t *testing.T) {
	if _, err := newReplayer([]Recording{{Tool: "x", Summary: "s", Data: map[string]any{"f": func() {}}}}); err == nil {
		t.Fatal("want error for data that cannot become JSON")
	}
}

func TestLiveActionsValidateButNeverWrite(t *testing.T) {
	reg, wl, err := liveActions(Resources{Connectors: []string{"pg-orders-connector"}, Subjects: []string{"pg.public.orders-value"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	restart, _ := reg.Get("connect_restart_task")
	if err := restart.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)); err != nil {
		t.Errorf("valid params refused: %v", err)
	}
	if err := restart.Validate(t.Context(), json.RawMessage(`{"connector":"nope","task":0}`)); err == nil {
		t.Error("unknown connector accepted")
	}
	// Execute reaches the replay client, which records the write and does
	// nothing; what Execute makes of the recorded status afterwards does not
	// matter here.
	_, _ = restart.Execute(ctx, json.RawMessage(`{"connector":"pg-orders-connector","task":0}`))
	compat, _ := reg.Get("schema_set_compatibility")
	if err := compat.Validate(t.Context(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`)); err != nil {
		t.Errorf("valid params refused: %v", err)
	}
	_, _ = compat.Execute(ctx, json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	var methods []string
	for _, w := range wl.snapshot() {
		methods = append(methods, w.Method)
	}
	if strings.Join(methods, ",") != "RestartTask,SetCompatibility" {
		t.Errorf("recorded writes = %v, want RestartTask,SetCompatibility", methods)
	}
}

func TestRecordingActionKeepsScope(t *testing.T) {
	live, err := LiveActions(Resources{Connectors: []string{"pg-orders-connector"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	log := &attemptLog{}
	reg, err := log.wrap(live)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("connect_restart_task")
	scoped, ok := a.(actions.Scoped)
	if !ok {
		t.Fatal("wrapped action lost actions.Scoped")
	}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}}
	if err := scoped.CheckScope(p, json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)); err != nil {
		t.Errorf("in-scope refused: %v", err)
	}
	if err := scoped.CheckScope(p, json.RawMessage(`{"connector":"billing","task":0}`)); err == nil {
		t.Error("out-of-scope accepted")
	}
	_ = a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":0}`))
	_ = a.Validate(t.Context(), json.RawMessage(`{"connector":"nope","task":0}`))
	got := log.snapshot()
	if len(got) != 2 || got[0].Error != "" || got[1].Error == "" || got[1].Action != "connect_restart_task" {
		t.Errorf("attempts = %+v", got)
	}
}

// runbook_lookup is pure (embedded text, no I/O), so an eval runs it for real:
// a call gets runbook text and is never counted as a replay miss.
func TestRunbookLookupRunsLive(t *testing.T) {
	rp, err := newReplayer(nil)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := rp.registry(defs)
	if err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get(runbooks.ToolName)
	if !ok {
		t.Fatal("runbook_lookup not registered")
	}
	if _, wrapped := tool.(*replayTool); wrapped {
		t.Fatal("runbook_lookup is wrapped in a replay tool")
	}
	res, err := tool.Run(context.Background(), json.RawMessage(`{"query":"replication slot WAL growth"}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Summary == "" {
		t.Fatal("empty runbook summary")
	}
	if m := rp.Misses(); len(m) != 0 {
		t.Fatalf("misses = %+v, want none", m)
	}
	// Tools that read live systems are still replayed.
	if ct, _ := reg.Get("connect_status"); ct == nil {
		t.Fatal("connect_status missing")
	} else if _, wrapped := ct.(*replayTool); !wrapped {
		t.Fatal("connect_status must stay replayed")
	}
}

type lookupMark struct{}

// The agent's own room-start lookup is not a call the model made: it is no
// miss, does not count toward after_calls, and does not consume a sequence.
func TestReplayPastIncidentsLookupIsNotCounted(t *testing.T) {
	rp, err := newPhasedReplayer([]Recording{
		{Tool: "similar_incidents", Input: map[string]any{"pipeline_id": "orders"}, Sequence: []Observation{{Summary: "first"}, {Summary: "second"}}},
	}, []string{"a", "b"}, []Transition{{From: "a", To: "b", On: TransitionOn{AfterCalls: &AfterCalls{Tool: "similar_incidents", N: 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	rp.isLookup = func(ctx context.Context) bool { return ctx.Value(lookupMark{}) != nil }
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := rp.registry(defs)
	if err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("similar_incidents")
	lookup := context.WithValue(context.Background(), lookupMark{}, true)

	// A recorded lookup is answered without moving the sequence or the phase.
	if res, err := tool.Run(lookup, json.RawMessage(`{"pipeline_id":"orders"}`)); err != nil || res.Summary != "first" {
		t.Fatalf("lookup = %+v, %v", res, err)
	}
	// An unrecorded one gets the usual error but is not counted as a miss.
	if _, err := tool.Run(lookup, json.RawMessage(`{"pipeline_id":"payments"}`)); !errors.Is(err, errNoRecording) {
		t.Fatalf("unrecorded lookup: %v", err)
	}
	if m := rp.Misses(); len(m) != 0 {
		t.Fatalf("lookup counted as a miss: %+v", m)
	}
	if rp.phase != "a" {
		t.Fatalf("lookup fired the after_calls transition: phase %q", rp.phase)
	}

	// The model's own calls still count: the sequence starts at "first",
	// the miss is recorded and the transition fires after the first call.
	if res, err := tool.Run(context.Background(), json.RawMessage(`{"pipeline_id":"orders"}`)); err != nil || res.Summary != "first" {
		t.Fatalf("model call = %+v, %v", res, err)
	}
	if rp.phase != "b" {
		t.Errorf("model call did not fire the transition: phase %q", rp.phase)
	}
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"pipeline_id":"payments"}`)); !errors.Is(err, errNoRecording) {
		t.Fatalf("model miss: %v", err)
	}
	if m := rp.Misses(); len(m) != 1 {
		t.Errorf("misses = %+v, want the model's one", m)
	}
}

// w4cUpdateHeartbeatConfig is the config step every idle-wal-growth run of
// 39994729845 tried (action_attempts, run 3's exact params).
const w4cUpdateHeartbeatConfig = `{"connector":"pg-orders-connector","changes":{"heartbeat.interval.ms":"60000","heartbeat.action.query":"INSERT INTO public.debezium_heartbeat (id, ts) VALUES (1, NOW()) ON CONFLICT (id) DO UPDATE SET ts = NOW()"}}`

// validateUpdateConfig validates w4cUpdateHeartbeatConfig against res and the
// scenario's recordings.
func validateUpdateConfig(t *testing.T, sc Scenario, res Resources) error {
	t.Helper()
	reg, err := LiveActions(res, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get("connect_update_connector_config")
	if !ok {
		t.Fatal("no action connect_update_connector_config")
	}
	return a.Validate(tools.WithPipeline(context.Background(), sc.Pipeline), json.RawMessage(w4cUpdateHeartbeatConfig))
}

// TestReplayServesDeclaredConnectorConfig: a connector declared in
// resources.connector_configs has its config served to the actions.Register
// actions too (the connect_config recording merged with the declared keys),
// so connect_update_connector_config validates in replay.
func TestReplayServesDeclaredConnectorConfig(t *testing.T) {
	sc, err := LoadScenario("scenarios/idle-wal-growth.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sc.Resources.ConnectorConfigs["pg-orders-connector"]; !ok {
		t.Fatal("idle-wal-growth declares no connector_configs for pg-orders-connector")
	}
	if err := validateUpdateConfig(t, sc, sc.Resources); err != nil {
		t.Fatalf("Validate with a declared config = %v, want nil", err)
	}
	c := replayConnect{res: sc.Resources, recs: sc.Recordings}
	cfg, err := c.Config(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatal(err)
	}
	// From the connect_config recording, and the declared key.
	if cfg["slot.name"] != "debezium_orders" || cfg["publication.autocreate.mode"] != "filtered" {
		t.Errorf("served config = %v", cfg)
	}
}

// TestReplayUndeclaredConnectorConfigNotReplayed: without the declaration the
// registered actions' Config read is not replayed, exactly as before, even
// when a connect_config recording exists.
func TestReplayUndeclaredConnectorConfigNotReplayed(t *testing.T) {
	sc, err := LoadScenario("scenarios/idle-wal-growth.yaml")
	if err != nil {
		t.Fatal(err)
	}
	res := sc.Resources
	res.ConnectorConfigs = nil
	err = validateUpdateConfig(t, sc, res)
	const want = "actions: read config of pg-orders-connector: evals: not available in replay"
	if err == nil || err.Error() != want || !errors.Is(err, errNotReplayed) {
		t.Fatalf("Validate without a declared config = %v, want %q", err, want)
	}
	if _, err := (replayConnect{res: res, recs: sc.Recordings}).Config(context.Background(), "pg-orders-connector"); !errors.Is(err, errNotReplayed) {
		t.Errorf("Config = %v, want errNotReplayed", err)
	}
}

type otherMark struct{}

// agentOwnRead is the production isLookup: a context the runner did not mark
// (the model's own calls, or any other value) is never the agent's own read.
// The marked cases (past-incidents, running versions, replan) cannot be built
// outside internal/agent, so they are covered by that package's tests of
// IsReplanRead and friends.
func TestAgentOwnReadUnmarkedContexts(t *testing.T) {
	for _, tt := range []struct {
		name string
		ctx  context.Context
	}{
		{"background", context.Background()},
		{"unrelated value", context.WithValue(context.Background(), otherMark{}, true)},
		{"cancelled", func() context.Context {
			c, cancel := context.WithCancel(context.Background())
			cancel()
			return c
		}()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if agentOwnRead(tt.ctx) {
				t.Error("a model call counted as the agent's own read")
			}
			if agent.IsReplanRead(tt.ctx) {
				t.Error("IsReplanRead true on an unmarked context")
			}
		})
	}
}
