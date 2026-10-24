package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
	setuptools "piperoom/internal/setup/tools"
)

// The setup prompt frame (docs/design/setup-agent.md §3.1) is pinned as a
// snapshot like the CDC and batch prompts: a change to it is reviewed as a
// diff of testdata/system_prompt_setup.txt and
// testdata/setup_first_message.txt.

// TestSetupPromptSnapshot pins the setup system prompt and its content: the
// goal is a green ladder, the rules state plan-before-propose, re-plan after
// a failure, request-not-report verification, never ask a person to run a
// command or type a secret, and stop-and-ask after the bounds. Every tool it
// names is one a setup room really offers (the KindRoom tools and the setup
// read tools).
func TestSetupPromptSnapshot(t *testing.T) {
	got := systemPromptForSetup(events.PipelineKindCDC)
	checkPromptSnapshot(t, "system_prompt_setup.txt", got)

	for _, want := range []string{
		"setup.verified",
		"setup_plan",
		"setup_request_verification",
		"re-plan",
		"Never report a verification yourself",
		"Never ask the person to run a command",
		"bounds",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("setup prompt does not say %q", want)
		}
	}
	// The prompt only names tools a setup room's tool list holds: the room
	// tools every kind has, the two setup room tools, the setup read tools,
	// and the Q tools of the rules block every kind shares (the wiring
	// registers them for a CDC room, conditionally like net_probe and
	// similar_incidents, exactly as the CDC prompt assumes).
	allowed := map[string]bool{}
	for _, n := range RoomToolNames() {
		allowed[n] = true
	}
	for _, n := range setuptools.RoomToolNames() {
		allowed[n] = true
	}
	for _, n := range setuptools.ReadToolNames() {
		allowed[n] = true
	}
	for _, n := range []string{"what_changed", "runbook_lookup", "similar_incidents", "net_probe"} {
		allowed[n] = true
	}
	for name, ok := range promptToolNames(got) {
		if ok && !allowed[name] {
			t.Errorf("the setup prompt names %q, which is not in a setup room's tool list", name)
		}
	}
}

// TestSetupFirstMessageSnapshot pins the opening user turn of a setup room:
// the request the person filled (untrusted, masked like an alert) and the
// run's bounds with their numbers.
func TestSetupFirstMessageSnapshot(t *testing.T) {
	raw := []byte(`{
		"pipeline_id": "orders",
		"tenant": "",
		"title": "orders pipeline",
		"source_kind": "postgres",
		"target": "db.acme:5432",
		"topic_prefix": "pg",
		"tables": ["public.orders", "public.items"],
		"connectors": ["pg-orders"],
		"targets": [
			{"kind": "postgres", "address": "db.acme:5432", "class": "local"},
			{"kind": "connect", "address": "connect.acme:8083", "class": "shared"}
		]
	}`)
	alert := Alert{Mode: ModeSetup, RoomID: "r_setup", PipelineID: "orders", Raw: raw}
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	s.decodeRequest(alert)
	if !s.reqOK {
		t.Fatal("the fixed request must decode")
	}
	got := setupFirstMessage(alert, s)
	checkPromptSnapshot(t, "setup_first_message.txt", got)

	for _, want := range []string{
		"orders pipeline",
		"db.acme:5432",
		"public.orders, public.items",
		"(class local)",
		"45m0s of wall clock",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("setup first message does not carry %q", want)
		}
	}
	// The bounds line carries the design's numbers.
	if !strings.Contains(got, "3 failed attempts") || !strings.Contains(got, "12 executed steps") {
		t.Errorf("setup first message does not state the attempt and step bounds: %q", got)
	}
}

// TestSetupFirstMessageWithoutRequest: a room whose request did not decode (a
// harness) still opens, saying no request was recorded.
func TestSetupFirstMessageWithoutRequest(t *testing.T) {
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	got := setupFirstMessage(Alert{Mode: ModeSetup}, s)
	if !strings.Contains(got, "none recorded") {
		t.Fatalf("setup first message without a request = %q, want it to say none was recorded", got)
	}
}

// TestSetupPromptCacheable: the system prompt depends only on the kind, so it
// stays byte-identical across a room's requests (prompt caching).
func TestSetupPromptCacheable(t *testing.T) {
	if systemPromptForSetup(events.PipelineKindCDC) != systemPromptForSetup(events.PipelineKindCDC) {
		t.Fatal("trivially different")
	}
	// The first message, not the system prompt, carries the request: a room
	// with a different request gets the same system prompt.
	if strings.Contains(systemPromptForSetup(events.PipelineKindCDC), "topic prefix") {
		t.Fatal("the system prompt carries request fields")
	}
}

// promptToolNames finds the names of the prompt's tool mentions. It is a
// small helper, not a full parser: the prompt spells tool names with
// parentheses ("setup_plan (phases of ...)") and lists ("env_detect,
// dir_list, ..."), so the known set of names is looked up in the text.
func promptToolNames(prompt string) map[string]bool {
	names := []string{
		"update_hypothesis", "propose_fix", "post_message",
		"setup_plan", "setup_request_verification",
		"env_detect", "dir_list", "docker_info", "compose_config",
		"java_version", "jar_list", "file_read", "compose_ps", "container_inspect", "container_logs",
		"what_changed", "runbook_lookup", "similar_incidents", "net_probe",
	}
	out := map[string]bool{}
	for _, n := range names {
		out[n] = strings.Contains(prompt, n)
	}
	return out
}
