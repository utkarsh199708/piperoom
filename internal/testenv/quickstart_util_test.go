package testenv

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

func TestQuickstartEnv(t *testing.T) {
	in := []string{
		"PATH=/usr/bin", "CONNECT_URL=http://localhost:8083", "USERS_FILE=dev/users.yaml", "OWNERS_FILE=x.yaml",
		"PIPEROOM_WRITE_MODE=on", "PIPEROOM_HTTP_ADDR=0.0.0.0:8090", "PIPEROOM_SOLO_USER=arjun",
		"PIPEROOM_WATCH_MODE=rooms", "ANTHROPIC_API_KEY=sk-test", "ANTHROPIC_BASE_URL=http://proxy",
		"PIPEROOM_MODEL_PROVIDER=bedrock", "PIPEROOM_STACK_OWNER=ci",
	}
	tests := []struct {
		name    string
		withKey bool
		want    []string
		absent  []string
	}{
		{
			name:    "with key keeps the model settings",
			withKey: true,
			want: []string{"PATH=/usr/bin", "CONNECT_URL=http://localhost:8083", "ANTHROPIC_API_KEY=sk-test",
				"ANTHROPIC_BASE_URL=http://proxy", "PIPEROOM_MODEL_PROVIDER=bedrock", "PIPEROOM_STACK_OWNER=ci",
				"PIPEROOM_HTTP_ADDR=127.0.0.1:0", "PIPEROOM_WATCH_MODE=off"},
			absent: []string{"USERS_FILE", "OWNERS_FILE", "PIPEROOM_WRITE_MODE", "PIPEROOM_SOLO_USER"},
		},
		{
			name: "without key points the model at a closed port",
			want: []string{"PATH=/usr/bin", "PIPEROOM_STACK_OWNER=ci", "PIPEROOM_HTTP_ADDR=127.0.0.1:0",
				"PIPEROOM_WATCH_MODE=off", "ANTHROPIC_BASE_URL=" + deadModelURL},
			absent: []string{"ANTHROPIC_API_KEY", "PIPEROOM_MODEL_PROVIDER", "USERS_FILE", "PIPEROOM_WRITE_MODE"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := quickstartEnv(in, tc.withKey)
			for _, w := range tc.want {
				if !slices.Contains(got, w) {
					t.Errorf("missing %q in %v", w, got)
				}
			}
			for _, kv := range got {
				k, _, _ := strings.Cut(kv, "=")
				if slices.Contains(tc.absent, k) {
					t.Errorf("%s should be dropped: %v", k, got)
				}
			}
			// Exactly one value per forced key, so no inherited one wins.
			for _, k := range []string{"PIPEROOM_HTTP_ADDR", "PIPEROOM_WATCH_MODE", "ANTHROPIC_BASE_URL"} {
				n := 0
				for _, kv := range got {
					if strings.HasPrefix(kv, k+"=") {
						n++
					}
				}
				if n > 1 {
					t.Errorf("%s set %d times", k, n)
				}
			}
		})
	}
}

func TestParseQuickstartURL(t *testing.T) {
	tests := []struct {
		line, want string
		ok         bool
	}{
		{"http://127.0.0.1:41233/\n", "http://127.0.0.1:41233", true},
		{"http://[::1]:8090/", "http://[::1]:8090", true},
		{"http://localhost:8090", "http://localhost:8090", true},
		{"http://0.0.0.0:8090/", "", false},
		{"http://10.0.0.5:8090/", "", false},
		{"http://127.0.0.1:0/", "", false},
		{"http://127.0.0.1/", "", false},
		{"https://127.0.0.1:8090/", "", false},
		{"time=... level=INFO msg=starting", "", false},
		{"", "", false},
		{"http://%zz", "", false},
	}
	for _, tc := range tests {
		got, err := parseQuickstartURL(tc.line)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("parseQuickstartURL(%q) = %q, %v; want %q, ok=%v", tc.line, got, err, tc.want, tc.ok)
		}
	}
}

// loadOwners writes body to a temp owners.yaml and loads it with the real
// loader, so the shapes below are ones the server accepts.
func loadOwners(t *testing.T, body string) *owners.Owners {
	t.Helper()
	p := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(p)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

const ownersPipeline = `
  - id: pg-orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: []
    sink_ts_column: ""
`

func TestCheckOwnersShape(t *testing.T) {
	topLevel := loadOwners(t, "pipelines:"+ownersPipeline+`layers:
  postgres: user:it_qs_1
  kafka-connect: user:it_qs_1
  schema-registry: user:it_qs_1
  kafka: user:it_qs_1
  consumer: user:it_qs_1
`)
	shape, err := checkOwnersShape(topLevel, "it_qs_1")
	if err != nil {
		t.Fatalf("top-level layers: %v", err)
	}
	if !strings.Contains(shape, "top-level layers: [consumer kafka kafka-connect postgres schema-registry]") {
		t.Errorf("shape = %q", shape)
	}
	if _, err := checkOwnersShape(topLevel, "someone_else"); !errors.Is(err, errOwnersShape) {
		t.Errorf("a user the file does not name: err = %v, want errOwnersShape", err)
	}
	if got := topLayers(topLevel); !sameLayers(got, []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer"}) {
		t.Errorf("topLayers = %v", got)
	}
	id, err := pipelineFor(topLevel, DevConnector)
	if err != nil || id != "pg-orders" {
		t.Errorf("pipelineFor = %q, %v", id, err)
	}
	if _, err := pipelineFor(topLevel, "other-connector"); err == nil {
		t.Error("pipelineFor of an unknown connector: want an error")
	}
}

func TestCheckOwnersShapePerPipelineOnly(t *testing.T) {
	// The shape LEAD-ARCH flagged on #600: layers assigned only in the
	// pipeline's owners: block. --quickstart's users file would list no
	// layer, so the check must fail and say why.
	body := `pipelines:
  - id: pg-orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: []
    sink_ts_column: ""
    owners:
      postgres: {approvers: [user:it_qs_1]}
      kafka-connect: {approvers: [user:it_qs_1]}
      schema-registry: {approvers: [user:it_qs_1]}
      kafka: {approvers: [user:it_qs_1]}
      consumer: {approvers: [user:it_qs_1]}
`
	o := loadOwners(t, body)
	shape, err := checkOwnersShape(o, "it_qs_1")
	if !errors.Is(err, errOwnersShape) {
		t.Fatalf("per-pipeline owners only: err = %v, want errOwnersShape", err)
	}
	if !strings.Contains(shape, "pipelines with their own owners: block: [pg-orders]") || !strings.Contains(err.Error(), "no top-level layers") {
		t.Errorf("shape %q, err %v: want both to name the per-pipeline block", shape, err)
	}
}

func ev(typ events.Type, payload any) events.Event {
	b, _ := json.Marshal(payload)
	return events.Event{Type: typ, Payload: b}
}

func TestEventHelpers(t *testing.T) {
	evs := []events.Event{
		ev(events.RoomCreated, map[string]string{"title": "x"}),
		ev(events.AgentStatus, events.AgentStatusPayload{State: "working"}),
		ev(events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: "model request failed"}),
		ev(events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: "second"}),
	}
	if d, ok := agentErrored(evs); !ok || d != "model request failed" {
		t.Errorf("agentErrored = %q, %v", d, ok)
	}
	if _, ok := agentErrored(evs[:2]); ok {
		t.Error("agentErrored on a working agent: want false")
	}
	if agentRan(evs) {
		t.Error("agentRan without a tool call: want false")
	}
	evs = append(evs, ev(events.AgentToolCall, map[string]string{"tool": "connect_status"}))
	if !agentRan(evs) {
		t.Error("agentRan with a tool call: want true")
	}
	if _, ok := firstOfType(evs, events.ActionExecuted); ok {
		t.Error("firstOfType found an action.executed that is not there")
	}
	if got := typeCounts(evs); got != "room.created=1 agent.status=3 agent.tool_call=1" {
		t.Errorf("typeCounts = %q", got)
	}
	// A malformed payload is skipped, not a match.
	bad := []events.Event{{Type: events.AgentStatus, Payload: json.RawMessage(`{`)}}
	if _, ok := agentErrored(bad); ok {
		t.Error("agentErrored on a malformed payload: want false")
	}
}

func TestTailBuffer(t *testing.T) {
	b := newTailBuffer(8)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = b.Write([]byte("abc")) }()
	}
	wg.Wait()
	if got := b.String(); len(got) != 8 {
		t.Errorf("len = %d (%q), want 8", len(got), got)
	}
	_, _ = b.Write([]byte("0123456789"))
	if got := b.String(); got != "23456789" {
		t.Errorf("tail = %q, want 23456789", got)
	}
}

func TestSameLayers(t *testing.T) {
	if !sameLayers([]string{"kafka", "postgres"}, []string{"postgres", "kafka"}) {
		t.Error("same set in another order: want true")
	}
	if sameLayers([]string{"kafka"}, []string{"kafka", "postgres"}) {
		t.Error("subset: want false")
	}
	if got := exeName("server"); got != "server" && got != "server.exe" {
		t.Errorf("exeName = %q", got)
	}
}
