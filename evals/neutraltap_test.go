package evals

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/converse"
	"piperoom/internal/agent/llm/gemini"
	"piperoom/internal/agent/llm/openai"
	llmretry "piperoom/internal/agent/llm/retry"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/events"
)

// The in-process tap of the non-Anthropic adapters (V11): a report from each
// adapter, driven through a local fake of its wire format, records the
// models called and a neutral-sha256 prompt hash. No network, no key.

const (
	tapSystem      = "You investigate CDC pipeline incidents. Use read-only tools."
	tapTriageModel = "small-model"
)

// fakeLLM is a scripted llm.Model for the tap's unit tests.
type fakeLLM struct {
	provider string
	err      error
	mu       sync.Mutex
	got      []llm.Request
}

func (f *fakeLLM) Turn(_ context.Context, req llm.Request) (llm.Response, error) {
	f.mu.Lock()
	f.got = append(f.got, req)
	f.mu.Unlock()
	if f.err != nil {
		return llm.Response{}, f.err
	}
	return llm.Response{Message: llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartText, Text: "ok"}}}, Stop: llm.StopEndTurn}, nil
}

func (f *fakeLLM) Capabilities(string) llm.Capabilities {
	return llm.Capabilities{ToolCalls: true, ContextTokens: 4242}
}

func (f *fakeLLM) Provider() string { return f.provider }

func roomRequest(model, room string, tools []llm.ToolDef) llm.Request {
	return llm.Request{
		Model:  model,
		System: tapSystem,
		Tools:  tools,
		Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText,
			Text: "Investigate this pipeline incident.\n\nAlert: <untrusted>x</untrusted>\nRoom: " + room + "\n"}}}},
	}
}

var sampleTools = []llm.ToolDef{
	{Name: "connect_status", Description: "Connector and task state.", Schema: map[string]any{
		"type": "object", "properties": map[string]any{"connector": map[string]any{"type": "string"}, "why": map[string]any{"type": "string"}},
		"required": []any{"connector", "why"},
	}},
	{Name: "pipeline_map", Description: "The pipeline's dependency graph.", Schema: map[string]any{"type": "object"}, Strict: true},
}

func TestNeutralPromptHash(t *testing.T) {
	h := NeutralPromptHash(tapSystem, sampleTools)
	// Pinned: the canonical form is the hash's definition (design section
	// 12); a change here makes every neutral baseline's hash stale.
	if want := "neutral-sha256:" + neutralHashOf(t, tapSystem, sampleTools); h != want || h != "neutral-sha256:9a888d1bc14b7a6c" {
		t.Fatalf("hash %s, want %s (pinned neutral-sha256:9a888d1bc14b7a6c)", h, want)
	}
	if !strings.HasPrefix(h, "neutral-sha256:") || len(h) != len("neutral-sha256:")+16 {
		t.Fatalf("hash %q", h)
	}
	if PromptHashKind([]string{h}) != PromptKindNeutral || PromptHashKind([]string{h, "sha256:0123456789abcdef"}) != PromptKindMixed {
		t.Fatal("the kind of a neutral hash")
	}
	// The same prompt built again, its schema maps in another insertion
	// order, hashes the same.
	again := []llm.ToolDef{
		{Name: "connect_status", Description: "Connector and task state.", Schema: map[string]any{
			"required": []any{"connector", "why"}, "properties": map[string]any{"why": map[string]any{"type": "string"}, "connector": map[string]any{"type": "string"}},
			"type": "object",
		}},
		{Name: "pipeline_map", Description: "The pipeline's dependency graph.", Schema: map[string]any{"type": "object"}, Strict: true},
	}
	if NeutralPromptHash(tapSystem, again) != h {
		t.Fatal("same prompt, different hash")
	}
	if NeutralPromptHash(tapSystem, nil) != NeutralPromptHash(tapSystem, []llm.ToolDef{}) {
		t.Fatal("no tools: nil and empty differ")
	}
	// What the model is told changes it.
	for name, other := range map[string]string{
		"system":      NeutralPromptHash(tapSystem+" ", sampleTools),
		"description": NeutralPromptHash(tapSystem, withTool(sampleTools, 0, func(d *llm.ToolDef) { d.Description += "!" })),
		"schema":      NeutralPromptHash(tapSystem, withTool(sampleTools, 1, func(d *llm.ToolDef) { d.Schema = map[string]any{"type": "object", "x": 1} })),
		"strict":      NeutralPromptHash(tapSystem, withTool(sampleTools, 1, func(d *llm.ToolDef) { d.Strict = false })),
		"order":       NeutralPromptHash(tapSystem, []llm.ToolDef{sampleTools[1], sampleTools[0]}),
		"one less":    NeutralPromptHash(tapSystem, sampleTools[:1]),
		// The boundary between system and tools is part of the hash.
		"boundary": NeutralPromptHash("", sampleTools),
	} {
		if other == h {
			t.Errorf("%s: the hash did not change", name)
		}
	}
	// A schema JSON cannot encode still hashes, and apart.
	bad := withTool(sampleTools, 1, func(d *llm.ToolDef) { d.Schema = map[string]any{"c": make(chan int)} })
	if b := NeutralPromptHash(tapSystem, bad); b == h || !strings.HasPrefix(b, "neutral-sha256:") {
		t.Errorf("unencodable schema: %q", b)
	}
}

// neutralHashOf restates the definition independently of the code: sha256 of
// json(system) NUL json([{name,description,schema,strict}]) NUL.
func neutralHashOf(t *testing.T, system string, tools []llm.ToolDef) string {
	t.Helper()
	sys, _ := json.Marshal(system)
	var b strings.Builder
	b.WriteString("[")
	for i, d := range tools {
		if i > 0 {
			b.WriteString(",")
		}
		n, _ := json.Marshal(d.Name)
		desc, _ := json.Marshal(d.Description)
		s, _ := json.Marshal(d.Schema)
		fmt.Fprintf(&b, `{"name":%s,"description":%s,"schema":%s,"strict":%t}`, n, desc, s, d.Strict)
	}
	b.WriteString("]")
	full := string(sys) + "\x00" + b.String() + "\x00"
	return sha256Hex(full)[:16]
}

func withTool(tools []llm.ToolDef, i int, f func(*llm.ToolDef)) []llm.ToolDef {
	out := slices.Clone(tools)
	f(&out[i])
	return out
}

func TestNeutralTapPassesThroughAndAttributes(t *testing.T) {
	tap := NewNeutralTap()
	if tap.Wrap("main", nil) != nil {
		t.Fatal("a nil model wrapped into a non-nil one")
	}
	inner := &fakeLLM{provider: llm.ProviderGemini}
	m := tap.Wrap("main", inner)
	if m.Provider() != llm.ProviderGemini || m.Capabilities("x").ContextTokens != 4242 {
		t.Fatal("provider or capabilities not passed through")
	}
	req := roomRequest("gemini-2.5-pro", "r_1", sampleTools)
	resp, err := m.Turn(context.Background(), req)
	if err != nil || resp.Message.Parts[0].Text != "ok" {
		t.Fatalf("turn: %v %+v", err, resp)
	}
	// Unchanged on the way through.
	if len(inner.got) != 1 || inner.got[0].System != req.System || inner.got[0].Model != req.Model || len(inner.got[0].Tools) != 2 {
		t.Fatalf("request changed: %+v", inner.got)
	}
	_, _ = m.Turn(context.Background(), roomRequest(tapTriageModel, "r_1", sampleTools))
	_, _ = m.Turn(context.Background(), roomRequest("gemini-2.5-pro", "r_1", sampleTools))
	// Not an investigation: a hand-off, the judge, an empty history, an
	// assistant first message, a room line after the first text part.
	_, _ = m.Turn(context.Background(), llm.Request{Model: "summary", System: "write a hand-off", Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: "events"}}}}})
	_, _ = m.Turn(context.Background(), llm.Request{Model: "nothing"})
	_, _ = m.Turn(context.Background(), llm.Request{Model: "asst", Messages: []llm.Message{{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartText, Text: "Room: r_2"}}}}})
	_, _ = m.Turn(context.Background(), llm.Request{Model: "late", Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: "hi"}, {Kind: llm.PartText, Text: "Room: r_3"}}}}})

	want := NeutralPromptHash(tapSystem, sampleTools)
	got := tap.Room("r_1")
	if !slices.Equal(got.Models, []string{"gemini-2.5-pro", tapTriageModel}) || !slices.Equal(got.PromptHashes, []string{want}) || got.Requests != 3 {
		t.Fatalf("room = %+v", got)
	}
	for _, id := range []string{"r_2", "r_3"} {
		if r := tap.Room(id); r.Requests != 0 {
			t.Errorf("%s attributed: %+v", id, r)
		}
	}
	if !slices.Equal(tap.PromptHashes(), []string{want}) {
		t.Fatalf("hashes %v", tap.PromptHashes())
	}
	// A failed call is recorded and its error comes back unchanged.
	apiErr := &llm.APIError{Kind: llm.ErrRateLimited, Status: 429}
	inner.err = apiErr
	if _, err := m.Turn(context.Background(), roomRequest("gemini-2.5-pro", "r_4", nil)); !errors.Is(err, apiErr) {
		t.Fatalf("err %v", err)
	}
	if r := tap.Room("r_4"); r.Requests != 1 || len(tap.PromptHashes()) != 2 {
		t.Fatalf("failed call: %+v %v", r, tap.PromptHashes())
	}
}

func TestNeutralTapConcurrent(t *testing.T) {
	tap := NewNeutralTap()
	m := tap.Wrap("main", &fakeLLM{provider: llm.ProviderOpenAI})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 20 {
				_, _ = m.Turn(context.Background(), roomRequest(fmt.Sprintf("m%d", j%2), fmt.Sprintf("r_%d", i), sampleTools))
				_ = tap.Room(fmt.Sprintf("r_%d", i))
				_ = tap.PromptHashes()
			}
		}()
	}
	wg.Wait()
	for i := range 8 {
		if r := tap.Room(fmt.Sprintf("r_%d", i)); r.Requests != 20 || len(r.Models) != 2 {
			t.Fatalf("room %d: %+v", i, r)
		}
	}
}

func TestNeutralTapProvider(t *testing.T) {
	for p, want := range map[string]bool{
		"": false, "anthropic": false, "bedrock": false, "vertex": false,
		"openai": true, " OpenAI ": true, "gemini": true, "bedrock-converse": true, "mystery": false,
	} {
		if NeutralTapProvider(p) != want {
			t.Errorf("%q: want %v", p, want)
		}
	}
}

// adapterFake is one adapter's local fake: a server that answers its wire
// format and counts the requests, and the real adapter pointed at it.
type adapterFake struct {
	name, provider, model string
	build                 func(t *testing.T) (llm.Model, *int)
}

// serve answers every request with body and counts it.
func serve(t *testing.T, tls bool, contentType string, body []byte) (*httptest.Server, *int) {
	t.Helper()
	var mu sync.Mutex
	n := new(int)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		*n++
		mu.Unlock()
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
	})
	var srv *httptest.Server
	if tls {
		srv = httptest.NewTLSServer(h)
	} else {
		srv = httptest.NewServer(h)
	}
	t.Cleanup(srv.Close)
	return srv, n
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func baseURL(t *testing.T, u string) *config.BaseURL {
	t.Helper()
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", u, false)
	if err != nil {
		t.Fatal(err)
	}
	return &b
}

func openAIFake(flavor, model string) adapterFake {
	return adapterFake{name: "openai/" + flavor, provider: llm.ProviderOpenAI, model: model, build: func(t *testing.T) (llm.Model, *int) {
		srv, n := serve(t, false, "application/json", mustJSON(t, map[string]any{
			"id": "chatcmpl-tap", "object": "chat.completion", "model": model,
			"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
			"usage":   map[string]any{"prompt_tokens": 20000, "completion_tokens": 2, "total_tokens": 20002},
		}))
		key := ""
		if flavor != openai.FlavorOllama {
			key = "test-not-a-key"
		}
		m, err := openai.New(openai.Options{Flavor: flavor, BaseURL: baseURL(t, srv.URL+"/v1"), Key: netsec.Secret(key), Retry: &llmretry.Policy{}})
		if err != nil {
			t.Fatal(err)
		}
		return m, n
	}}
}

func geminiFake() adapterFake {
	return adapterFake{name: "gemini", provider: llm.ProviderGemini, model: "gemini-2.5-pro", build: func(t *testing.T) (llm.Model, *int) {
		srv, n := serve(t, false, "application/json", mustJSON(t, map[string]any{
			"candidates":    []any{map[string]any{"finishReason": "STOP", "content": map[string]any{"role": "model", "parts": []any{map[string]any{"text": "ok"}}}}},
			"usageMetadata": map[string]any{"promptTokenCount": 20000, "candidatesTokenCount": 2, "totalTokenCount": 20002},
			"modelVersion":  "gemini-2.5-pro",
		}))
		m, err := gemini.New(context.Background(), gemini.Options{APIKey: netsec.Secret("test-not-a-key"), BaseURL: baseURL(t, srv.URL), Retry: &llmretry.Policy{}})
		if err != nil {
			t.Fatal(err)
		}
		return m, n
	}}
}

func converseFake() adapterFake {
	const model = "amazon.nova-pro-v1:0"
	return adapterFake{name: "bedrock-converse", provider: llm.ProviderBedrockConverse, model: model, build: func(t *testing.T) (llm.Model, *int) {
		// The adapter takes https only; the test server's own client
		// trusts its certificate and uses no proxy.
		srv, n := serve(t, true, "application/json", mustJSON(t, map[string]any{
			"output":     map[string]any{"message": map[string]any{"role": "assistant", "content": []any{map[string]any{"text": "ok"}}}},
			"stopReason": "end_turn",
			"usage":      map[string]any{"inputTokens": 20000, "outputTokens": 2, "totalTokens": 20002},
			"metrics":    map[string]any{"latencyMs": 3},
		}))
		creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDTAPTEST", SecretAccessKey: "tap-test-not-a-secret", Source: "test"}, nil
		})
		m, err := converse.New(converse.Config{
			AWS:          &aws.Config{Region: "us-east-1", Credentials: creds, HTTPClient: srv.Client()},
			BaseEndpoint: srv.URL,
			Models:       []string{model, tapTriageModel},
			Getenv:       func(string) string { return "" },
		})
		if err != nil {
			t.Fatal(err)
		}
		return m, n
	}}
}

// tapRunner is an agent.Runner that sends what an investigation sends
// through a wrapped model: a triage turn, then a main turn, both with the
// room line, the system prompt and the room's tools; then a hand-off-like
// call without the room line. Then it records the seed's hypothesis.
func tapRunner(m llm.Model, hypothesis string) agent.Runner {
	return func(ctx context.Context, alert agent.Alert, d agent.Deps, opts agent.Options) error {
		var defs []llm.ToolDef
		for _, tl := range d.Tools.All() {
			defs = append(defs, llm.ToolDef{Name: tl.Name(), Description: tl.Description(), Schema: tl.InputSchema()})
		}
		for _, model := range []string{tapTriageModel, opts.Model} {
			if _, err := m.Turn(ctx, roomRequest(model, alert.RoomID, defs)); err != nil {
				return err
			}
		}
		if _, err := m.Turn(ctx, llm.Request{Model: opts.Model, System: "Write the hand-off.",
			Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: "room events"}}}}}); err != nil {
			return err
		}
		return d.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{Text: hypothesis, Confidence: "high"})
	}
}

func TestNeutralTapReportPerAdapter(t *testing.T) {
	var hashes []string
	for _, a := range []adapterFake{
		openAIFake(openai.FlavorOpenAI, "gpt-4.1"),
		openAIFake(openai.FlavorOllama, "qwen3:32b"),
		openAIFake(openai.FlavorDeepSeek, "deepseek-chat"),
		geminiFake(),
		converseFake(),
	} {
		t.Run(a.name, func(t *testing.T) {
			inner, served := a.build(t)
			tap := NewNeutralTap()
			m := tap.Wrap("main", inner)
			if m.Provider() != a.provider {
				t.Fatalf("provider %q", m.Provider())
			}
			rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, Config{
				Runner: tapRunner(m, seedCause), Tap: tap, Commit: "abc123", Logger: quietLogger(),
				Options: agent.Options{Model: a.model, Effort: "low", MaxTurns: 5},
			})
			if err != nil {
				t.Fatal(err)
			}
			r := rep.Results[0]
			if r.Error != "" {
				t.Fatalf("run error %q", r.Error)
			}
			if *served != 3 {
				t.Fatalf("the fake served %d requests, want 3", *served)
			}
			if want := []string{tapTriageModel, a.model}; !slices.Equal(r.ModelsUsed, want) || r.Model != a.model {
				t.Errorf("models used %v (configured %q), want %v", r.ModelsUsed, r.Model, want)
			}
			if !strings.HasPrefix(r.PromptHash, PromptKindNeutral+":") || len(r.PromptHashes) != 0 {
				t.Errorf("prompt hash %q %v", r.PromptHash, r.PromptHashes)
			}
			if !slices.Equal(rep.PromptHashes, []string{r.PromptHash}) {
				t.Errorf("report hashes %v", rep.PromptHashes)
			}
			if k := PromptHashKind(rep.PromptHashes); k != PromptKindNeutral {
				t.Errorf("kind %q", k)
			}
			hashes = append(hashes, r.PromptHash)
		})
	}
	// One prompt, one neutral hash, whatever adapter carries it.
	for _, h := range hashes {
		if h != hashes[0] {
			t.Fatalf("hashes differ across adapters: %v", hashes)
		}
	}
}

// An untapped run stays "untapped": no tap in the config.
func TestNoTapStaysUntapped(t *testing.T) {
	m := &fakeLLM{provider: llm.ProviderOpenAI}
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, Config{
		Runner: tapRunner(m, seedCause), Logger: quietLogger(), Options: agent.Options{Model: "gpt-4.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r := rep.Results[0]; r.PromptHash != "untapped" || len(r.ModelsUsed) != 0 || PromptHashKind(rep.PromptHashes) != PromptKindUntapped {
		t.Fatalf("%+v %v", r.Provenance, rep.PromptHashes)
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
