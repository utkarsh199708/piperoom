package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// knobsOf parses c the way wiring does.
func knobsOf(t *testing.T, c config.Config) *config.ModelKnobs {
	t.Helper()
	k, err := config.ParseModelKnobs(c)
	if err != nil {
		t.Fatalf("ParseModelKnobs: %v", err)
	}
	return &k
}

// probeServer is an OpenAI-compatible host that answers every request with
// reply(requestBytes) and counts the requests.
type probeServer struct {
	*httptest.Server
	calls atomic.Int32
}

func newProbeServer(t *testing.T, reply func(reqBytes int) map[string]any) *probeServer {
	t.Helper()
	s := &probeServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply(len(b)))
	}))
	t.Cleanup(s.Close)
	return s
}

// chatReply is a chat completion holding message, with prompt tokens a
// share of the request bytes (bytes / 3 is an honest server).
func chatReply(message map[string]any, finish string, promptTokens func(int) int) func(int) map[string]any {
	return func(n int) map[string]any {
		message["role"] = "assistant"
		return map[string]any{
			"id": "c", "object": "chat.completion", "model": "m",
			"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}},
			"usage":   map[string]any{"prompt_tokens": promptTokens(n), "completion_tokens": 5},
		}
	}
}

func probeCall(args string) map[string]any {
	return map[string]any{"content": nil, "tool_calls": []any{map[string]any{
		"id": "call_1", "type": "function", "function": map[string]any{"name": probeToolName, "arguments": args}}}}
}

func honest(n int) int { return n / 3 }

// startSets are tool sets with no read tool: the room tools alone.
func startSets() []StartToolSet {
	return []StartToolSet{{Kind: events.PipelineKindCDC, Tools: tools.NewReadOnlyRegistry()}}
}

func ollamaCfg(base string) config.Config {
	return config.Config{ModelProvider: "openai", ModelFlavor: "ollama", Model: "qwen3:32b", ModelBaseURL: base, ModelUnverified: "on"}
}

// TestStartFailures is the table of design section 9's start failures: each
// refuses start, and the error names what to change.
func TestStartFailures(t *testing.T) {
	good := newProbeServer(t, chatReply(probeCall(`{"status":"ready"}`), "tool_calls", honest))
	noCall := newProbeServer(t, chatReply(map[string]any{"content": "I am ready."}, "stop", honest))
	badArgs := newProbeServer(t, chatReply(probeCall(`{"status":"go"}`), "tool_calls", honest))
	truncating := newProbeServer(t, chatReply(probeCall(`{"status":"ready"}`), "tool_calls", func(int) int { return 200 }))

	tests := []struct {
		name string
		cfg  config.Config
		env  map[string]string
		want []string
	}{
		{"unknown flavor is refused by the parser", config.Config{ModelProvider: "openai", ModelFlavor: "nope"}, nil, []string{"PIPEROOM_MODEL_FLAVOR"}},
		{"groq without GROQ_API_KEY", config.Config{ModelProvider: "openai", ModelFlavor: "groq", Model: "openai/gpt-oss-120b", ModelUnverified: "on"}, nil,
			[]string{"PIPEROOM_MODEL_PROVIDER=openai", "flavor groq", "needs an API key", "GROQ_API_KEY"}},
		{"summary on groq reads only GROQ_API_KEY", config.Config{Model: "claude-opus-5-5", ModelFlavor: "groq", SummaryProvider: "openai", SummaryModel: "openai/gpt-oss-120b"},
			map[string]string{"PIPEROOM_MODEL_API_KEY": "main-key-value"}, []string{"PIPEROOM_SUMMARY_PROVIDER=openai", "GROQ_API_KEY"}},
		{"deepseek with its key but without the acknowledgement", config.Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat", ModelUnverified: "on"},
			map[string]string{"DEEPSEEK_API_KEY": "sk-deepseek-value"}, []string{"api.deepseek.com", "PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes"}},
		{"missing main key", config.Config{ModelProvider: "openai", Model: "gpt-x"}, nil, []string{"PIPEROOM_MODEL_PROVIDER=openai", "OPENAI_API_KEY", "PIPEROOM_MODEL_API_KEY"}},
		{"a role reads its own provider's key, never the main one", config.Config{ModelProvider: "openai", Model: "gpt-x", JudgeProvider: "gemini", JudgeModel: "gemini-3-pro"},
			map[string]string{"PIPEROOM_MODEL_API_KEY": "main-key"}, []string{"PIPEROOM_JUDGE_PROVIDER=gemini", "GEMINI_API_KEY"}},
		{"summary on openai without its key", config.Config{Model: "claude-opus-5-5", SummaryProvider: "openai", SummaryModel: "gpt-x"},
			nil, []string{"PIPEROOM_SUMMARY_PROVIDER=openai", "OPENAI_API_KEY"}},
		{"bad base URL", config.Config{ModelProvider: "openai", ModelFlavor: "generic", ModelBaseURL: "http://169.254.169.254/v1"}, nil, []string{"PIPEROOM_MODEL_BASE_URL"}},
		{"new-provider model not listed", config.Config{ModelProvider: "openai", ModelFlavor: "ollama", Model: "qwen3:32b", ModelBaseURL: good.URL}, nil,
			[]string{"PIPEROOM_MODEL=\"qwen3:32b\"", "supported.json", "PIPEROOM_MODEL_UNVERIFIED=on"}},
		{"unverified model fails the probe: no tool call", ollamaCfg(noCall.URL), nil, []string{"probe", "did not call"}},
		{"unverified model fails the probe: bad arguments", ollamaCfg(badArgs.URL), nil, []string{"probe", "do not validate"}},
		{"ollama truncates the prompt in the probe", ollamaCfg(truncating.URL), nil, []string{"truncated", "OLLAMA_CONTEXT_LENGTH"}},
		{"context window below the minimum", func() config.Config {
			c := ollamaCfg(good.URL)
			c.ModelContextTokens = "32768"
			return c
		}(), nil, []string{"32768", "128000"}},
		{"China-operated endpoint without the acknowledgement", config.Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat"}, nil,
			[]string{"PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes"}},
		{"triage model on another provider", func() config.Config {
			c := ollamaCfg(good.URL)
			c.TriageModel = "claude-haiku-5-5"
			return c
		}(), nil, []string{"PIPEROOM_TRIAGE_MODEL", "another provider"}},
		{"bedrock-converse is not built yet", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}, nil,
			[]string{"bedrock-converse", "not built yet"}},
		{"prompt variant is reserved", config.Config{ModelPromptVariant: "small"}, nil, []string{"PIPEROOM_MODEL_PROMPT_VARIANT"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := startEndpoint(t, tt.cfg, tt.env, nil)
			if err == nil {
				t.Fatal("want a start-up error")
			}
			for _, s := range tt.want {
				if !strings.Contains(err.Error(), s) {
					t.Fatalf("error %q does not contain %q", err, s)
				}
			}
			for _, k := range tt.env {
				if strings.Contains(err.Error(), k) {
					t.Fatalf("error carries a key: %q", err)
				}
			}
		})
	}
	if n := good.calls.Load(); n != 0 {
		t.Fatalf("a refused start sent %d requests to the good host", n)
	}
}

// startEndpoint parses cfg, builds the endpoint and runs the start checks
// with startSets and list (nil: the embedded supported.json), as wiring does.
func startEndpoint(t *testing.T, c config.Config, env map[string]string, list []llm.SupportedModel) error {
	_, err := startEndpointLog(t, c, env, list, discardLogger())
	return err
}

func startEndpointLog(t *testing.T, c config.Config, env map[string]string, list []llm.SupportedModel, log *slog.Logger) (*Endpoint, error) {
	t.Helper()
	k, err := config.ParseModelKnobs(c)
	if err != nil {
		return nil, err
	}
	tp, err := config.ParseTriagePass(c)
	if err != nil {
		return nil, err
	}
	ec := EndpointConfig{Provider: c.ModelProvider, AWSRegion: c.AWSRegion, TriageModel: c.TriageModel, Knobs: &k,
		TriagePass: tp.On, TriagePassModel: tp.Model}
	ep, err := newEndpoint(context.Background(), ec, envOf(env), testLoaders(nil), log)
	if err != nil {
		return nil, err
	}
	if list == nil {
		if list, err = llm.SupportedModels(); err != nil {
			t.Fatal(err)
		}
	}
	return ep, ep.checkStart(context.Background(), startSets(), list, time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC))
}

func TestStartUnverifiedOllamaPassesTheProbeAndLogsItsRole(t *testing.T) {
	var sent atomic.Int64
	s := newProbeServer(t, func(n int) map[string]any {
		sent.Store(int64(n))
		return chatReply(probeCall(`{"status":"ready"}`), "tool_calls", honest)(n)
	})
	var buf bytes.Buffer
	ep, err := startEndpointLog(t, ollamaCfg(s.URL), nil, nil, slog.New(slog.NewTextHandler(&buf, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// One probe for main, first look and summary and judge together: they
	// share the main client and model.
	if n := s.calls.Load(); n != 1 {
		t.Fatalf("probe requests = %d, want 1", n)
	}
	// The local probe carries the real system prompt and tools.
	if sent.Load() < int64(len(systemPromptFor(events.PipelineKindCDC))) {
		t.Fatalf("probe sent %d bytes, less than the system prompt", sent.Load())
	}
	log := buf.String()
	for _, w := range []string{"unverified model", "start probe", `msg="model role" role=main`, "provider=openai", "flavor=ollama",
		"region=127.0.0.1:", "prompt_cache=no", "gate=unverified", "no prompt cache", "dropped=effort,thinking,strict_tools,output_schema,server_compaction"} {
		if !strings.Contains(log, w) {
			t.Errorf("log lacks %q:\n%s", w, log)
		}
	}
	if ep.Provider() != llm.ProviderOpenAI {
		t.Fatalf("provider = %q", ep.Provider())
	}
}

func TestStartListedHostedModelSendsNoProbe(t *testing.T) {
	s := newProbeServer(t, chatReply(probeCall(`{"status":"ready"}`), "tool_calls", honest))
	c := config.Config{ModelProvider: "openai", ModelFlavor: "openai", ModelBaseURL: s.URL, Model: "gpt-5-2026-08-01"}
	list := []llm.SupportedModel{{Provider: "openai", Flavor: "openai", Model: "gpt-5-2026-08-01", Status: llm.GateSupported, Baseline: "b", GatedAt: "2026-10-01"}}
	if err := startEndpoint(t, c, map[string]string{"OPENAI_API_KEY": "sk-test"}, list); err != nil {
		t.Fatal(err)
	}
	if n := s.calls.Load(); n != 0 {
		t.Fatalf("a listed hosted model was probed (%d requests)", n)
	}
}

func TestStartSunsetModel(t *testing.T) {
	s := newProbeServer(t, chatReply(probeCall(`{"status":"ready"}`), "tool_calls", honest))
	c := config.Config{ModelProvider: "openai", ModelFlavor: "openai", ModelBaseURL: s.URL, Model: "gpt-old"}
	env := map[string]string{"OPENAI_API_KEY": "sk-test"}
	row := llm.SupportedModel{Provider: "openai", Flavor: "openai", Model: "gpt-old", Status: llm.GateSupported, Baseline: "b", GatedAt: "2026-01-01"}

	row.Sunset = "2026-10-01" // passed on 2026-10-23
	err := startEndpoint(t, c, env, []llm.SupportedModel{row})
	if err == nil || !strings.Contains(err.Error(), "sunset date 2026-10-01") {
		t.Fatalf("a model past its sunset: err = %v", err)
	}

	row.Sunset = "2026-11-10" // within 30 days
	var buf bytes.Buffer
	if _, err := startEndpointLog(t, c, env, []llm.SupportedModel{row}, slog.New(slog.NewTextHandler(&buf, nil))); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "sunset date soon") {
		t.Fatalf("no sunset warning:\n%s", buf.String())
	}
}

func TestCheckFitNamesRoleModelAndNumbers(t *testing.T) {
	sp := roleSpec{name: roleMain, env: "PIPEROOM_MODEL", role: config.ModelRole{Provider: "openai", Flavor: "vllm", Model: "small"}}
	defs := []llm.ToolDef{{Name: "t", Schema: map[string]any{"type": "object", "description": strings.Repeat("x", 300_000)}}}
	err := checkFit(sp, events.PipelineKindCDC, strings.Repeat("s", 30_000), defs, maxTokens, 128_000)
	if err == nil {
		t.Fatal("want a context-fit error")
	}
	for _, w := range []string{"PIPEROOM_MODEL=\"small\"", "main (cdc room) role", "128000"} {
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("error %q lacks %q", err, w)
		}
	}
	if err := checkFit(sp, "", "short", nil, maxTokens, 128_000); err != nil {
		t.Fatalf("a small prompt: %v", err)
	}
}

// fakeConverse is a ModelFactory standing in for V5's adapter.
type fakeConverse struct{ got config.ModelRole }

func (f *fakeConverse) NewModel(_ context.Context, r config.ModelRole, _ bool) (llm.Model, error) {
	f.got = r
	return capsModel{provider: llm.ProviderBedrockConverse}, nil
}

// capsModel answers Capabilities and Provider only.
type capsModel struct {
	provider string
	caps     llm.Capabilities
}

func (m capsModel) Turn(context.Context, llm.Request) (llm.Response, error) {
	return llm.Response{}, errors.New("not used")
}
func (m capsModel) Capabilities(string) llm.Capabilities { return m.caps }
func (m capsModel) Provider() string                     { return m.provider }

func TestConverseHookBuildsTheRole(t *testing.T) {
	c := config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}
	f := &fakeConverse{}
	ep, err := newEndpoint(context.Background(), EndpointConfig{AWSRegion: "us-east-1", Knobs: knobsOf(t, c), Converse: f}, envOf(nil), testLoaders(nil), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if f.got.Model != "amazon.nova-pro-v1:0" || ep.Provider() != llm.ProviderBedrockConverse {
		t.Fatalf("factory got %+v, provider %q", f.got, ep.Provider())
	}
}

// TestPricingRule is design 8.2 and decision 4 under a daily cost cap.
func TestPricingRule(t *testing.T) {
	price := ModelPrice{Input: 1, Output: 2, CacheWrite: 1, CacheRead: 0.1}
	s := newProbeServer(t, chatReply(probeCall(`{"status":"ready"}`), "tool_calls", honest))
	claudeSummaryOnOpenAI := config.Config{Model: "claude-opus-5-5", SummaryProvider: "openai", SummaryModel: "gpt-x", SummaryBaseURL: s.URL, ModelUnverified: "on"}
	tests := []struct {
		name   string
		cfg    config.Config
		strict string
		prices Prices
		want   string // "" = SetSpend succeeds
	}{
		{"Claude unpriced first look keeps today's warning by default", config.Config{}, "", Prices{"claude-opus-5-5": price}, ""},
		{"Claude unpriced first look refuses with all", config.Config{}, config.PricingStrictAll, Prices{"claude-opus-5-5": price},
			`PIPEROOM_TRIAGE_PASS="claude-haiku-5-5"`},
		{"unpriced new-provider role refuses by default", claudeSummaryOnOpenAI, "", Prices{"claude-opus-5-5": price, "claude-haiku-5-5": price},
			`PIPEROOM_SUMMARY="gpt-x" on openai (flavor openai)`},
		{"a provider-qualified key prices it", claudeSummaryOnOpenAI, "", Prices{"claude-opus-5-5": price, "claude-haiku-5-5": price, "openai:gpt-x": price}, ""},
		{"a local model priced at zero is priced", ollamaCfg(s.URL), "", Prices{"qwen3:32b": {}}, ""},
		{"an unpriced local model refuses", ollamaCfg(s.URL), "", Prices{"other": price}, `"ollama:qwen3:32b"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.cfg
			c.ModelPricingStrict = tt.strict
			env := map[string]string{"OPENAI_API_KEY": "sk-test"}
			ep, err := startEndpointLog(t, c, env, nil, discardLogger())
			if err != nil {
				t.Fatal(err)
			}
			err = ep.SetSpend(SpendOptions{Ledger: &memLedger{}, PipelineDailyCostUSD: 5, Prices: tt.prices})
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("SetSpend: %v", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Fatalf("err = %v, want one containing %s", err, tt.want)
			}
			// Without a cost cap nothing is refused.
			ep2, _ := startEndpointLog(t, c, env, nil, discardLogger())
			if err := ep2.SetSpend(SpendOptions{Prices: tt.prices}); err != nil {
				t.Fatalf("no cap: %v", err)
			}
		})
	}
}

// memLedger is a SpendLedger that adds in memory.
type memLedger struct{ t SpendTotals }

func (l *memLedger) AddSpend(_ context.Context, c SpendChange) (SpendTotals, error) {
	l.t.RoomTokens += c.Tokens
	l.t.RoomCostMicros += c.CostMicros
	l.t.PipelineTokens += c.Tokens
	l.t.PipelineCostMicros += c.CostMicros
	return l.t, nil
}

func TestQualifiedPriceWins(t *testing.T) {
	bare, groq, oai := ModelPrice{Input: 1}, ModelPrice{Input: 2}, ModelPrice{Input: 3}
	p := Prices{"openai/gpt-oss-120b": bare, "groq:openai/gpt-oss-120b": groq, "openai:openai/gpt-oss-120b": oai,
		"arn:aws:bedrock:us-east-1:1:application-inference-profile/x": bare}
	if got, _ := p.PriceFor("openai", "groq", "openai/gpt-oss-120b"); got != groq {
		t.Fatalf("groq flavor: %+v, want the groq: price", got)
	}
	if got, _ := p.PriceFor("openai", "together", "openai/gpt-oss-120b"); got != oai {
		t.Fatalf("together flavor: %+v, want the openai: price", got)
	}
	if got, _ := p.PriceFor("gemini", "", "openai/gpt-oss-120b"); got != bare {
		t.Fatalf("other provider: %+v, want the bare price", got)
	}
	if _, ok := p.PriceFor("bedrock", "", "arn:aws:bedrock:us-east-1:1:application-inference-profile/x"); !ok {
		t.Fatal("an ARN key with colons must still price itself")
	}
	if len(p) != 4 {
		t.Fatal("the table was changed")
	}
}

func TestClaudeOnlyNeedsNoStartChecks(t *testing.T) {
	for _, c := range []config.Config{
		{},
		{ModelProvider: "bedrock", AWSRegion: "us-east-1", Model: "us.anthropic.claude-opus-5-5-v1:0"},
		{ModelProvider: "bedrock", AWSRegion: "us-east-1", Model: "arn:aws:bedrock:us-east-1:123:application-inference-profile/abc"},
		{ModelProvider: "bedrock", AWSRegion: "us-east-1", Model: "claude-unlisted-9"},
		{ModelProvider: "vertex", VertexProject: "p", VertexRegion: "global", Model: "claude-opus-5-5@20261001"},
	} {
		k := knobsOf(t, c)
		ec := EndpointConfig{Provider: c.ModelProvider, AWSRegion: c.AWSRegion, VertexProject: c.VertexProject, VertexRegion: c.VertexRegion, Knobs: k, TriagePass: true}
		ep, err := newEndpoint(context.Background(), ec, envOf(nil), testLoaders(nil), discardLogger())
		if err != nil {
			t.Fatalf("%s %q: %v", c.ModelProvider, c.Model, err)
		}
		if ep.NeedsStartChecks() {
			t.Fatalf("%s %q needs start checks", c.ModelProvider, c.Model)
		}
		// Every role shares the one Claude client, and no role overrides
		// the request's model.
		if ep.summary.m != nil || ep.judge.m != nil || ep.firstLook.m != nil || ep.summary.modelID != "" || ep.judge.modelID != "" {
			t.Fatalf("%s: a Claude-only deployment built a second client or a model override", c.ModelProvider)
		}
		if err := ep.checkStart(context.Background(), nil, nil, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSupportedListParses(t *testing.T) {
	list, err := llm.SupportedModels()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d supported models", len(list))
	bad := []string{
		`{"models":[{"provider":"anthropic","model":"claude-opus-5-5","status":"supported","baseline":"b","gated_at":"2026-10-01"}]}`,
		`{"models":[{"provider":"openai","model":"x","status":"supported","baseline":"b","gated_at":"2026-10-01"}]}`,
		`{"models":[{"provider":"gemini","model":"x-latest","status":"supported","alias":true,"baseline":"b","gated_at":"2026-10-01"}]}`,
		`{"models":[{"provider":"gemini","model":"x","status":"supported","baseline":"b","gated_at":"2026-10-01","sunset":"soon"}]}`,
		`{"models":[{"provider":"gemini","model":"x","status":"great","baseline":"b","gated_at":"2026-10-01"}]}`,
		`{"models":[{"provider":"gemini","model":"x","status":"supported","baseline":"b","gated_at":"2026-10-01","extra":1}]}`,
		`{"models":[{"provider":"gemini","model":"x","status":"supported","baseline":"b","gated_at":"2026-10-01"},{"provider":"gemini","model":"x","status":"experimental","baseline":"b","gated_at":"2026-10-01"}]}`,
	}
	for _, b := range bad {
		if _, err := llm.ParseSupported([]byte(b)); err == nil {
			t.Errorf("accepted %s", b)
		}
	}
}

// TestChinaEndpointRefusedByTheAdaptersAnswer: the acknowledgement is checked
// again on llm/openai's ChinaOperated, so an EndpointConfig whose knobs did
// not come through config.ParseModelKnobs cannot skip it; with the
// acknowledgement the endpoint builds without sending anything.
func TestChinaEndpointRefusedByTheAdaptersAnswer(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  config.Config
		env  map[string]string
		host string
	}{
		{"deepseek default host", config.Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat"},
			map[string]string{"DEEPSEEK_API_KEY": "sk-deepseek-value"}, "api.deepseek.com"},
		{"qwen on the DashScope China endpoint", config.Config{ModelProvider: "openai", ModelFlavor: "qwen", Model: "qwen-plus",
			ModelBaseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1"},
			map[string]string{"DASHSCOPE_API_KEY": "sk-dashscope-value"}, "dashscope.aliyuncs.com"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.cfg
			c.ModelAckCNEndpoint = "yes"
			k, err := config.ParseModelKnobs(c)
			if err != nil {
				t.Fatal(err)
			}
			ec := EndpointConfig{Provider: c.ModelProvider, Knobs: &k}
			if _, err := newEndpoint(context.Background(), ec, envOf(tt.env), testLoaders(nil), discardLogger()); err != nil {
				t.Fatalf("with the acknowledgement: %v", err)
			}
			k.AckCNEndpoint = false
			_, err = newEndpoint(context.Background(), ec, envOf(tt.env), testLoaders(nil), discardLogger())
			if err == nil {
				t.Fatal("want a start-up error without the acknowledgement")
			}
			for _, s := range []string{tt.host, "PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes", "PIPEROOM_MODEL_PROVIDER=openai"} {
				if !strings.Contains(err.Error(), s) {
					t.Fatalf("error %q does not contain %q", err, s)
				}
			}
			for _, v := range tt.env {
				if strings.Contains(err.Error(), v) {
					t.Fatalf("error carries a key: %q", err)
				}
			}
		})
	}
}
