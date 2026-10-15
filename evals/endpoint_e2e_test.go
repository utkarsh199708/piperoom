package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// The whole production path with only the model faked: room.Service starts
// the Endpoint that wiring.NewAgentEndpoint builds (the constructor
// cmd/server uses), whose Anthropic client streams from a local fake Messages
// server through the model tap. No network, no key.

// fakeTurn is one scripted model turn: a tool call, or a closing text.
type fakeTurn struct {
	tool  string
	input map[string]any
	// cite fills based_on_steps with every step id the request's tool
	// results carry, as a model that read them would.
	cite bool
	text string
}

// fakeModel is a Messages API server that answers each request with the
// next scripted turn, streamed, and records the request bodies.
type fakeModel struct {
	*httptest.Server
	mu       sync.Mutex
	turns    []fakeTurn
	requests []string
}

var stepIDs = regexp.MustCompile(`s_[0-9A-Z]{26}`)

func newFakeModel(t *testing.T, turns []fakeTurn) *fakeModel {
	t.Helper()
	m := &fakeModel{turns: turns}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.requests = append(m.requests, string(b))
		n := len(m.requests)
		turn := fakeTurn{text: "done"}
		if n <= len(m.turns) {
			turn = m.turns[n-1]
		}
		m.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseTurn(n, turn, string(b)))
	}))
	t.Cleanup(m.Close)
	return m
}

func (m *fakeModel) bodies() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.requests...)
}

// sseTurn streams one assistant turn in the Messages API's event format.
func sseTurn(n int, turn fakeTurn, request string) string {
	var b strings.Builder
	ev := func(name string, v any) {
		j, _ := json.Marshal(v)
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", name, j)
	}
	ev("message_start", map[string]any{"type": "message_start", "message": map[string]any{
		"id": fmt.Sprintf("msg_%d", n), "type": "message", "role": "assistant", "model": "claude-test",
		"content": []any{}, "stop_reason": nil, "usage": map[string]any{"input_tokens": 100, "output_tokens": 1},
	}})
	stop := "end_turn"
	if turn.tool == "" {
		ev("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}})
		ev("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": turn.text}})
	} else {
		stop = "tool_use"
		in := map[string]any{}
		for k, v := range turn.input {
			in[k] = v
		}
		if turn.cite {
			seen := map[string]bool{}
			var cited []string
			for _, id := range stepIDs.FindAllString(request, -1) {
				if !seen[id] {
					seen[id] = true
					cited = append(cited, id)
				}
			}
			in["based_on_steps"] = cited
		}
		raw, _ := json.Marshal(in)
		ev("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{
			"type": "tool_use", "id": fmt.Sprintf("toolu_%d", n), "name": turn.tool, "input": map[string]any{}}})
		ev("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "input_json_delta", "partial_json": string(raw)}})
	}
	ev("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	ev("message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": stop}, "usage": map[string]any{"output_tokens": 20}})
	ev("message_stop", map[string]any{"type": "message_stop"})
	return b.String()
}

// goodSeedTurns is a correct investigation of the seed as model turns.
func goodSeedTurns(hypothesis string) []fakeTurn {
	return []fakeTurn{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders", "why": "start from the map"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector", "why": "the failing connector"}},
		{tool: "pg_recent_ddl", input: map[string]any{"since": "2026-10-11T09:00:00Z", "why": "recent schema changes"}},
		{tool: "update_hypothesis", input: map[string]any{"text": hypothesis, "confidence": "high"}, cite: true},
		{tool: "propose_fix", input: map[string]any{
			"title": "Allow the new schema, then restart task 0", "risk": "NONE lets the new amount type through to the warehouse",
			"rollback": "set compatibility back to BACKWARD",
			"steps": []map[string]any{
				{"action": "schema_set_compatibility", "params": setCompatV, "description": "let v2 register"},
				{"action": "connect_restart_task", "params": restartT0, "description": "restart the failed task"},
			},
		}},
		{text: "Proposed the fix; waiting for approval."},
	}
}

// endpointConfig runs evals through a tapped Endpoint on fake.
func endpointConfig(t *testing.T, fake *fakeModel) Config {
	t.Helper()
	tap, err := NewModelTap(fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tap.Close() })
	t.Setenv("ANTHROPIC_BASE_URL", tap.URL())
	t.Setenv("ANTHROPIC_API_KEY", "test-key-not-real")
	run, err := NewEndpointRunner(context.Background(), config.Config{}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		Runner: run, Tap: tap, Commit: "abc123", Logger: quietLogger(),
		Options: agent.Options{Model: "claude-test", Effort: "low", MaxTurns: 40},
	}
}

func TestEndpointThroughRoomPasses(t *testing.T) {
	fake := newFakeModel(t, goodSeedTurns(seedCause))
	cfg := endpointConfig(t, fake)
	var tr Transcript
	cfg.OnRun = func(_ RunResult, got Transcript) { tr = got }
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	if !r.Pass {
		t.Fatalf("good model run fails: %v (error %q)", r.Failures, r.Error)
	}
	// Three reads, then the agent's own baseline read after proposing
	// (agent verify.go): the room runs it, a direct call of the scripted
	// steps never would.
	if r.ToolCalls != 4 || r.Proposals != 1 || r.TimeToCause == nil || r.TimeToCause.ToolCalls != 3 {
		t.Errorf("tool_calls=%d proposals=%d ttc=%+v", r.ToolCalls, r.Proposals, r.TimeToCause)
	}
	// Provenance: the commit given, the model the requests named, the hash
	// of what the model was sent.
	if r.Commit != "abc123" || r.Model != "claude-test" || strings.Join(r.ModelsUsed, ",") != "claude-test" {
		t.Errorf("provenance = %+v", r.Provenance)
	}
	if !strings.HasPrefix(r.PromptHash, "sha256:") || len(r.PromptHashes) != 0 {
		t.Errorf("prompt hash = %q %v", r.PromptHash, r.PromptHashes)
	}
	if got := rep.PromptHashes; len(got) != 1 || got[0] != r.PromptHash {
		t.Errorf("report prompt hashes = %v", got)
	}
	// The hash is of what the fake received.
	var req tapRequest
	if err := json.Unmarshal([]byte(fake.bodies()[0]), &req); err != nil {
		t.Fatal(err)
	}
	if h := PromptHash(req.System, req.Tools); h != r.PromptHash {
		t.Errorf("hash %s, request hashes to %s", r.PromptHash, h)
	}
	// The room built the alert: its id, the scenario's map.
	if tr.Room == nil || tr.Room.Alert.RoomID != tr.Room.RoomID || !strings.Contains(fake.bodies()[0], "Room: "+tr.Room.RoomID) {
		t.Errorf("room = %+v", tr.Room)
	}
}

// The room's own proposal checks reach the model. A task number no double
// holds exactly passes the agent's propose_fix (an integer) and the action's
// Validate, but room.Service refuses to hash it; the parallel harness this
// replaces stored the proposal and scored the fix as found.
func TestEndpointRoomRefusesWhatOnlyTheRoomChecks(t *testing.T) {
	turns := goodSeedTurns(seedCause)
	turns[4].input["steps"] = []map[string]any{
		{"action": "schema_set_compatibility", "params": setCompatV, "description": "let v2 register"},
		{"action": "connect_restart_task", "params": `{"connector":"pg-orders-connector","task":9007199254740993}`, "description": "restart"},
	}
	fake := newFakeModel(t, turns)
	// The task is listed, so Validate (Q6 U6) lets it through and the refusal
	// can only come from the room.
	sc := withListedTasks(t, loadSeed(), "pg-orders-connector", 9007199254740993)
	rep, err := Evaluate(context.Background(), []Scenario{sc}, endpointConfig(t, fake))
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	if r.Pass || r.Proposals != 0 || !hasFailure(r, "expected fix not proposed") {
		t.Errorf("pass=%v proposals=%d failures=%v", r.Pass, r.Proposals, r.Failures)
	}
	bodies := fake.bodies()
	if len(bodies) < 6 || !strings.Contains(bodies[5], "cannot be written exactly as a double") {
		t.Errorf("the room's refusal did not reach the model")
	}
}

// A steer reaches the model through the room: the driver steers on the
// agent's idle and the next request carries the text.
func TestEndpointSteerReachesModel(t *testing.T) {
	sc := loadSeed()
	sc.Inputs = []Input{{Kind: "steer", Text: "Check the registry compatibility before restarting anything."}}
	fake := newFakeModel(t, goodSeedTurns(seedCause))
	rep, err := Evaluate(context.Background(), []Scenario{sc}, endpointConfig(t, fake))
	if err != nil {
		t.Fatal(err)
	}
	if r := rep.Results[0]; r.Idles != 2 {
		t.Errorf("idles = %d, want 2 (the steer, then the end)", r.Idles)
	}
	bodies := fake.bodies()
	last := bodies[len(bodies)-1]
	if !strings.Contains(last, "Check the registry compatibility before restarting anything.") {
		t.Errorf("the steer is not in the request after the idle")
	}
	for _, b := range bodies[:len(bodies)-1] {
		if strings.Contains(b, "Check the registry compatibility") {
			t.Errorf("the steer reached the model before the idle")
		}
	}
}

// The watcher-started room on the Endpoint: the model's first request
// carries the watcher's title, not an alert's.
func TestEndpointWatcherStart(t *testing.T) {
	turns := []fakeTurn{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector", "why": "the failed task"}},
		{tool: "pg_replication_slots", input: map[string]any{"why": "is the slot still there"}},
		{tool: "update_hypothesis", input: map[string]any{"text": watcherCause, "confidence": "high"}, cite: true},
		{tool: "propose_fix", input: map[string]any{
			"title": "Restart task 0", "risk": "the task re-reads from the slot; no data is skipped",
			"rollback": "nothing to roll back; the task fails again if Postgres is still down",
			"steps":    []map[string]any{{"action": "connect_restart_task", "params": restartT0, "description": "restart the failed task"}},
		}},
		{text: "Proposed restarting task 0."},
	}
	fake := newFakeModel(t, turns)
	rep, err := Evaluate(context.Background(), []Scenario{loadScenario(t, "scenarios/watcher-start.yaml")}, endpointConfig(t, fake))
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	if !r.Pass || r.Entry != StartWatcher {
		t.Fatalf("pass=%v entry=%s failures=%v error=%q", r.Pass, r.Entry, r.Failures, r.Error)
	}
	if first := fake.bodies()[0]; !strings.Contains(first, watcherTitle) {
		t.Errorf("the first request lacks the watcher's title")
	}
}
