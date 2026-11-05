package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
)

// hookCfg parses c the way wiring does (knobs and triage pass) into an
// EndpointConfig.
func hookCfg(t *testing.T, c config.Config) EndpointConfig {
	t.Helper()
	tp, err := config.ParseTriagePass(c)
	if err != nil {
		t.Fatal(err)
	}
	return EndpointConfig{Provider: c.ModelProvider, AWSRegion: c.AWSRegion, TriageModel: c.TriageModel,
		Knobs: knobsOf(t, c), TriagePass: tp.On, TriagePassModel: tp.Model}
}

// wrapRecorder counts WrapModel calls by role and marks the models it wraps.
type wrapRecorder struct {
	mu    sync.Mutex
	calls map[string]int
}

type tappedModel struct {
	llm.Model
	role string
}

func (w *wrapRecorder) wrap(role string, m llm.Model) llm.Model {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.calls == nil {
		w.calls = map[string]int{}
	}
	w.calls[role]++
	return tappedModel{Model: m, role: role}
}

// mixedCfg: main and triage on an ollama host, first look and judge on the
// main client, the summary writer on Anthropic with its own client.
func mixedCfg(t *testing.T) EndpointConfig {
	c := ollamaCfg("http://127.0.0.1:1")
	c.TriageModel = "qwen3:8b"
	c.SummaryProvider, c.SummaryModel = "anthropic", "claude-opus-5-5"
	return hookCfg(t, c)
}

func TestWrapModelSeesEveryRoleOnce(t *testing.T) {
	var w wrapRecorder
	ec := mixedCfg(t)
	ec.WrapModel = w.wrap
	ep, err := newEndpoint(context.Background(), ec, envOf(map[string]string{"ANTHROPIC_API_KEY": "sk-ant-test"}), testLoaders(nil), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	// main once; the summary's own client once; triage, first look and judge
	// share the main client and are not wrapped again.
	if want := map[string]int{roleMain: 1, roleSummary: 1}; !reflect.DeepEqual(w.calls, want) {
		t.Fatalf("wrap calls = %v, want %v", w.calls, want)
	}
	if m, ok := ep.model.(tappedModel); !ok || m.role != roleMain {
		t.Fatalf("main client = %T, want the wrapped main", ep.model)
	}
	if m, ok := ep.summaryClient().(tappedModel); !ok || m.role != roleSummary {
		t.Fatalf("summary client = %T, want the wrapped summary", ep.summaryClient())
	}
	for name, m := range map[string]model{"judge": ep.judgeClient(), "first look": ep.firstLookModel()} {
		if tm, ok := m.(tappedModel); !ok || tm.role != roleMain {
			t.Fatalf("%s client = %T, want the shared wrapped main", name, m)
		}
	}
	// The wrap is outermost: the neutraliser sits inside it.
	if _, ok := ep.model.(tappedModel).Model.(neutralisingModel); !ok {
		t.Fatalf("inside the wrap = %T, want the neutraliser", ep.model.(tappedModel).Model)
	}
}

func TestWrapModelClaudeOnlyWrapsTheOneClient(t *testing.T) {
	for _, ec := range []EndpointConfig{{}, hookCfg(t, config.Config{})} {
		var w wrapRecorder
		ec.WrapModel = w.wrap
		ep, err := newEndpoint(context.Background(), ec, envOf(nil), testLoaders(nil), discardLogger())
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]int{roleMain: 1}; !reflect.DeepEqual(w.calls, want) {
			t.Fatalf("knobs=%v: wrap calls = %v, want %v", ec.Knobs != nil, w.calls, want)
		}
		if ep.summary.m != nil || ep.judge.m != nil || ep.firstLook.m != nil {
			t.Fatal("a Claude-only endpoint built a second client")
		}
	}
}

func TestRolesWithKnobs(t *testing.T) {
	ep, err := newEndpoint(context.Background(), mixedCfg(t), envOf(map[string]string{"ANTHROPIC_API_KEY": "sk-ant-test"}), testLoaders(nil), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	got := ep.Roles()
	var names []string
	for _, r := range got {
		names = append(names, r.Name)
	}
	if want := []string{roleMain, roleTriage, roleFirstLook, roleSummary, roleJudge}; !reflect.DeepEqual(names, want) {
		t.Fatalf("role order = %v, want %v", names, want)
	}
	byName := map[string]RoleInfo{}
	for _, r := range got {
		byName[r.Name] = r
	}
	if r := byName[roleMain]; r != (RoleInfo{Name: roleMain, Provider: "openai", Flavor: "ollama", Model: "qwen3:32b", Region: "127.0.0.1:1"}) {
		t.Fatalf("main = %+v", r)
	}
	if r := byName[roleTriage]; r.Model != "qwen3:8b" || r.Provider != "openai" {
		t.Fatalf("triage = %+v", r)
	}
	if r := byName[roleSummary]; r != (RoleInfo{Name: roleSummary, Provider: "anthropic", Model: "claude-opus-5-5", PromptCache: true}) {
		t.Fatalf("summary = %+v", r)
	}
	if r := byName[roleJudge]; r.Provider != "openai" || r.Model != "qwen3:32b" || r.PromptCache {
		t.Fatalf("judge = %+v", r)
	}
	if r := byName[roleFirstLook]; r.Provider != "openai" || r.Model == "" {
		t.Fatalf("first look = %+v", r)
	}
	// Stable: a second call returns the same list.
	if again := ep.Roles(); !reflect.DeepEqual(again, got) {
		t.Fatalf("Roles changed between calls: %+v vs %+v", again, got)
	}
}

func TestRolesClaudeOnlyNoKnobs(t *testing.T) {
	tests := []struct {
		cfg  EndpointConfig
		want RoleInfo
	}{
		{EndpointConfig{}, RoleInfo{Name: roleMain, Provider: "anthropic", PromptCache: true}},
		{EndpointConfig{Provider: "bedrock", AWSRegion: "eu-west-1"}, RoleInfo{Name: roleMain, Provider: "bedrock", Region: "eu-west-1", PromptCache: true}},
		{EndpointConfig{Provider: "vertex", VertexProject: "p", VertexRegion: "global"}, RoleInfo{Name: roleMain, Provider: "vertex", Region: "global", PromptCache: true}},
	}
	for _, tt := range tests {
		ep, err := newEndpoint(context.Background(), tt.cfg, envOf(nil), testLoaders(nil), discardLogger())
		if err != nil {
			t.Fatal(err)
		}
		if got := ep.Roles(); !reflect.DeepEqual(got, []RoleInfo{tt.want}) {
			t.Fatalf("%s: Roles = %+v, want [%+v]", tt.cfg.Provider, got, tt.want)
		}
	}
}

// TestGetenvOverrideAnthropicKey: with Getenv set, the anthropic client
// sends the override's key and never the process's, and refuses to start
// when the override has no key.
func TestGetenvOverrideAnthropicKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-process")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")
	var mu sync.Mutex
	var gotKey, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotKey, gotAuth = r.Header.Get("X-Api-Key"), r.Header.Get("Authorization")
		mu.Unlock()
		http.Error(w, `{"type":"error","error":{"type":"invalid_request_error","message":"test"}}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	for _, tt := range []struct {
		env            map[string]string
		wantKey, wantA string
	}{
		{map[string]string{"ANTHROPIC_API_KEY": "sk-ant-picker", "ANTHROPIC_BASE_URL": srv.URL}, "sk-ant-picker", ""},
		{map[string]string{"ANTHROPIC_AUTH_TOKEN": "tok-picker", "ANTHROPIC_BASE_URL": srv.URL}, "", "Bearer tok-picker"},
	} {
		ep, err := NewEndpoint(context.Background(), EndpointConfig{Getenv: envOf(tt.env)}, discardLogger())
		if err != nil {
			t.Fatal(err)
		}
		_, _ = ep.model.Turn(context.Background(), llm.Request{Model: "claude-opus-5-5", MaxTokens: 16,
			Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: "hi"}}}}})
		mu.Lock()
		k, a := gotKey, gotAuth
		mu.Unlock()
		if k != tt.wantKey || a != tt.wantA {
			t.Fatalf("sent X-Api-Key=%q Authorization=%q, want %q %q", k, a, tt.wantKey, tt.wantA)
		}
	}

	_, err := NewEndpoint(context.Background(), EndpointConfig{Getenv: envOf(nil)}, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "ANTHROPIC_API_KEY") {
		t.Fatalf("no key in the override: err = %v, want one naming ANTHROPIC_API_KEY", err)
	}
	if strings.Contains(err.Error(), "sk-ant-process") {
		t.Fatal("the error names a key")
	}
}

// TestGetenvOverrideOpenAIAndGeminiKeys: the hosted flavors' and gemini's
// keys come from the override, not the process.
func TestGetenvOverrideOpenAIAndGeminiKeys(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("GEMINI_API_KEY", "")
	t.Setenv(config.EnvModelAPIKey, "")
	for _, c := range []config.Config{
		{ModelProvider: "openai", ModelFlavor: "openai", Model: "gpt-5-2026-08-01"},
		{ModelProvider: "gemini", Model: "gemini-3-pro"},
	} {
		ec := hookCfg(t, c)
		if _, err := NewEndpoint(context.Background(), ec, discardLogger()); err == nil {
			t.Fatalf("%s: built without any key", c.ModelProvider)
		}
		ec.Getenv = envOf(map[string]string{"OPENAI_API_KEY": "sk-picker", "GEMINI_API_KEY": "g-picker"})
		if _, err := NewEndpoint(context.Background(), ec, discardLogger()); err != nil {
			t.Fatalf("%s with the override's key: %v", c.ModelProvider, err)
		}
	}
}
