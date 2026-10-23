// Package openai is the llm.Model adapter for OpenAI-compatible chat
// completions hosts (provider "openai"): OpenAI, Azure OpenAI, Ollama, vLLM
// and any other compatible server (design docs/design/model-providers.md
// sections 3 and 7, row V3a). It speaks POST {base}/chat/completions over
// net/http with no SDK and no streaming (design 3.1).
//
// A flavor sets the defaults and the quirks (auth header, token field,
// reasoning, strict tools, structured output); it never changes the prompt
// or the tools. The hosted flavors deepseek, qwen, mistral, groq, together,
// openrouter and xai arrive in V3b; New refuses them until then.
//
// Every request goes through one *http.Client whose dialer refuses the
// metadata deny list (config.ModelAddrDenied) at dial time, which never
// follows a redirect, ignores the proxy variables unless Options.Proxy, and
// sends the key only to the configured host (transport.go). Retries and the
// per-endpoint limits are the shared policy of internal/agent/llm/retry.
package openai

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// Flavors this adapter serves (PIPEROOM_MODEL_FLAVOR).
const (
	FlavorOpenAI  = "openai"
	FlavorAzure   = "azure"
	FlavorOllama  = "ollama"
	FlavorVLLM    = "vllm"
	FlavorGeneric = "generic"
)

// TurnTimeout bounds one model turn, retries and rate-limit waits included
// (design 3.1: the 10 min deadline of the Claude turn).
const TurnTimeout = 10 * time.Minute

// maxResponseBytes caps a response body (AGENTS.md rule 7); a model turn is
// far smaller.
const maxResponseBytes = 4 << 20

// toolNameMax and maxTools are the documented limits of the chat completions
// API (design 7.1), which compatible hosts copy.
const (
	toolNameMax = 64
	maxTools    = 128
)

// flavor is one flavor's quirks.
type flavor struct {
	name string
	// keyEnv is the standard key variable; "" when the flavor has none.
	keyEnv string
	// needsKey refuses New without a key.
	needsKey bool
	// azureAuth sends the key as api-key instead of a bearer token.
	azureAuth bool
	// hosted knows its models: strict tools, json_schema, automatic prompt
	// cache, reasoning by model id, max_completion_tokens. The local and
	// generic flavors get these only through PIPEROOM_MODEL_CAPS (and never
	// strict tools: a cap cannot lower a check).
	hosted bool
}

var flavors = map[string]flavor{
	FlavorOpenAI:  {name: FlavorOpenAI, keyEnv: "OPENAI_API_KEY", needsKey: true, hosted: true},
	FlavorAzure:   {name: FlavorAzure, keyEnv: "AZURE_OPENAI_API_KEY", needsKey: true, azureAuth: true, hosted: true},
	FlavorOllama:  {name: FlavorOllama},
	FlavorVLLM:    {name: FlavorVLLM, keyEnv: "PIPEROOM_MODEL_API_KEY"},
	FlavorGeneric: {name: FlavorGeneric, keyEnv: "PIPEROOM_MODEL_API_KEY"},
}

// Flavors returns the flavors New accepts, sorted.
func Flavors() []string {
	return []string{FlavorAzure, FlavorGeneric, FlavorOllama, FlavorOpenAI, FlavorVLLM}
}

// KeyEnv is flavor's standard key variable, "" when it has none (ollama) or
// the flavor is unknown. PIPEROOM_MODEL_API_KEY, when set, is the main
// role's key on any flavor (LoadKey).
func KeyEnv(flavorName string) string { return flavors[flavorName].keyEnv }

// Options builds a Model.
type Options struct {
	// Flavor is one of the Flavor* values; empty means openai.
	Flavor string
	// BaseURL is the endpoint (config.ParseBaseURL); nil means the flavor's
	// default (config.FlavorDefaultBaseURL). azure and generic have none.
	// New validates it again, so a hand-made value cannot skip the rules.
	BaseURL *config.BaseURL
	// AllowPrivateHTTP is PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP: plain http to
	// RFC 1918 and ULA hosts besides loopback.
	AllowPrivateHTTP bool
	// Proxy is PIPEROOM_MODEL_PROXY: off ignores HTTPS_PROXY and friends; on
	// uses them, refusing a target whose name or any address is on the deny
	// list before the CONNECT.
	Proxy bool
	// Key is the API key; empty sends no auth header (ollama, a keyless
	// vllm). It is sent only to BaseURL's host.
	Key netsec.Secret
	// Caps is PIPEROOM_MODEL_CAPS for the ollama, vllm and generic flavors:
	// parallel_tools, structured_output, reasoning. Ignored on hosted flavors.
	Caps []string
	// ContextTokens is the context window of every model this Model serves
	// (PIPEROOM_MODEL_CONTEXT_TOKENS for the main role); 0 = the adapter's
	// table, else unknown.
	ContextTokens int64
	// Retry is the retry policy; nil means retry.Default(). A zero Policy
	// never retries.
	Retry *retry.Policy
	// Limiter is the endpoint's shared limiter (retry.Limiters.For); nil
	// limits nothing.
	Limiter *retry.Limiter
	// Log gets the debug lines (the redacted error body) and the long-wait
	// warning; nil means slog.Default().
	Log *slog.Logger
}

// Model is the OpenAI-compatible adapter. It is safe for concurrent use.
type Model struct {
	flavor   flavor
	endpoint string // {base}/chat/completions
	origin   string // scheme://host[:port], the form that may be logged
	key      netsec.Secret
	caps     map[string]bool
	ctxToks  int64
	client   *http.Client
	policy   retry.Policy
	limiter  *retry.Limiter
	log      *slog.Logger
}

var _ llm.Model = (*Model)(nil)

// New returns the adapter for o. It makes no network call. The error names
// the setting, never the key or the base URL's path.
func New(o Options) (*Model, error) {
	return newModel(o, hooks{})
}

func newModel(o Options, h hooks) (*Model, error) {
	name := strings.ToLower(strings.TrimSpace(o.Flavor))
	if name == "" {
		name = FlavorOpenAI
	}
	fl, ok := flavors[name]
	if !ok {
		if config.FlavorDefaultBaseURL(name) != "" {
			return nil, fmt.Errorf("llm/openai: flavor %q is not served yet (V3b)", name)
		}
		return nil, fmt.Errorf("llm/openai: unknown flavor %q", name)
	}
	base, err := baseURL(fl, o)
	if err != nil {
		return nil, err
	}
	if fl.needsKey && o.Key == "" {
		return nil, fmt.Errorf("llm/openai: flavor %s needs an API key (%s or PIPEROOM_MODEL_API_KEY)", fl.name, fl.keyEnv)
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	policy := retry.Default()
	if o.Retry != nil {
		policy = *o.Retry
	}
	m := &Model{
		flavor:   fl,
		endpoint: strings.TrimSuffix(base.String(), "/") + "/chat/completions",
		origin:   base.Redacted(),
		key:      o.Key,
		caps:     map[string]bool{},
		ctxToks:  o.ContextTokens,
		policy:   policy,
		limiter:  o.Limiter,
		log:      log,
	}
	if !fl.hosted {
		for _, c := range o.Caps {
			m.caps[strings.ToLower(strings.TrimSpace(c))] = true
		}
	}
	m.client = newClient(base, o.Proxy, o.Key, fl.azureAuth, h)
	return m, nil
}

// baseURL validates o.BaseURL, or the flavor's default, by the rules of
// design 10.3.
func baseURL(fl flavor, o Options) (config.BaseURL, error) {
	raw := config.FlavorDefaultBaseURL(fl.name)
	if o.BaseURL != nil {
		raw = o.BaseURL.String()
	}
	if raw == "" {
		return config.BaseURL{}, fmt.Errorf("llm/openai: flavor %s needs a base URL (PIPEROOM_MODEL_BASE_URL)", fl.name)
	}
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", raw, o.AllowPrivateHTTP)
	if err != nil {
		return config.BaseURL{}, fmt.Errorf("llm/openai: %w", err)
	}
	return b, nil
}

// Provider is always llm.ProviderOpenAI; the flavor is a setting of it.
func (m *Model) Provider() string { return llm.ProviderOpenAI }

// Flavor reports the flavor this Model was built for.
func (m *Model) Flavor() string { return m.flavor.name }

// Capabilities reports what the flavor supports for model, from the
// flavor's table and Options; it never makes a network call.
func (m *Model) Capabilities(model string) llm.Capabilities {
	c := llm.Capabilities{
		ToolCalls:     true,
		ContextTokens: m.contextTokens(model),
		MaxTools:      maxTools,
		ToolNameMax:   toolNameMax,
	}
	if m.flavor.hosted {
		c.ParallelToolCalls = true
		c.StrictTools = true // per tool: only schemas in the strict subset (strictSchema)
		c.StructuredOutput = true
		c.PromptCache = true // automatic prefix caching, reported as cached_tokens
		c.Reasoning = reasoningModel(model)
		c.Effort = c.Reasoning
		return c
	}
	c.ParallelToolCalls = m.caps["parallel_tools"]
	c.StructuredOutput = m.caps["structured_output"]
	c.Reasoning = m.caps["reasoning"]
	c.Effort = c.Reasoning
	return c
}

// contextTokens is Options.ContextTokens, else the table below for a hosted
// flavor, else 0 (the agent assumes 128000 and the truncation check guards).
func (m *Model) contextTokens(model string) int64 {
	if m.ctxToks > 0 {
		return m.ctxToks
	}
	if !m.flavor.hosted {
		return 0
	}
	id := strings.ToLower(model)
	for _, e := range contextTable {
		if strings.HasPrefix(id, e.prefix) {
			return e.tokens
		}
	}
	return 0
}

// contextTable is the context window of OpenAI model families by id prefix
// (longest first), as OpenAI documents them in 2026-10. On Azure the model
// is a deployment name, which often but not always repeats the model id.
var contextTable = []struct {
	prefix string
	tokens int64
}{
	{"gpt-4.1", 1_047_576},
	{"gpt-4o", 128_000},
	{"gpt-5", 400_000},
	{"o4", 200_000},
	{"o3", 200_000},
	{"o1", 200_000},
}

// reasoningModel reports an OpenAI reasoning model by id: the o-series and
// gpt-5, except the chat variants. It decides the developer role and
// reasoning_effort.
func reasoningModel(model string) bool {
	id := strings.ToLower(model)
	if strings.Contains(id, "-chat") {
		return false
	}
	for _, p := range []string{"o1", "o3", "o4", "gpt-5"} {
		if id == p || strings.HasPrefix(id, p+"-") || strings.HasPrefix(id, p+".") {
			return true
		}
	}
	return false
}

// Turn encodes req, sends it with the shared retry policy and limits, and
// decodes the answer. A failed request returns an error wrapping an
// *llm.APIError, with the partial response when one was billed (a
// truncated prompt); one that never left the process wraps llm.ErrNotSent.
func (m *Model) Turn(ctx context.Context, req llm.Request) (llm.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, TurnTimeout)
	defer cancel()

	body, err := m.encode(req)
	if err != nil {
		// The encoder's text can name request content, so it stays in Err.
		ae := &llm.APIError{Provider: llm.ProviderOpenAI, Kind: llm.ErrBadRequest, Err: fmt.Errorf("%w: %w", llm.ErrNotSent, err)}
		return llm.Response{}, &classified{api: ae, text: ae.Error() + ": the request could not be encoded"}
	}
	estimate := estimateTokens(len(body))
	var resp llm.Response
	err = m.policy.Do(ctx, func(ctx context.Context) (retry.Outcome, error) {
		r, out, err := m.attempt(ctx, req, body, estimate)
		resp = r
		return out, err
	})
	return resp, err
}

// estimateTokens is the over-estimate of design 8.3: bytes / 3, rounded up.
func estimateTokens(n int) int64 { return int64(math.Ceil(float64(n) / 3)) }

// truncated reports a reported prompt below half the lower-bound estimate
// of the request (bytes / 4): the server cut the prompt to fit its context
// (design 6.2).
func truncated(reqBytes int, u llm.Usage) bool {
	if u.Estimated {
		return false
	}
	reported := u.Input + u.CacheRead + u.CacheWrite
	return float64(reported) < float64(reqBytes)/4/2
}

// attempt sends body once.
func (m *Model) attempt(ctx context.Context, req llm.Request, body []byte, estimate int64) (llm.Response, retry.Outcome, error) {
	release, err := m.limiter.Acquire(ctx, estimate)
	if err != nil {
		kind := llm.ErrNetwork
		if errors.Is(err, retry.ErrWaitPastDeadline) {
			kind = llm.ErrRateLimited
		}
		ae := &llm.APIError{Provider: llm.ProviderOpenAI, Kind: kind, Err: fmt.Errorf("%w: %w", llm.ErrNotSent, err)}
		return llm.Response{}, retry.Outcome{}, &classified{api: ae, text: ae.Error() + ": " + networkHint(err, kind)}
	}
	status, header, raw, err := m.send(ctx, body)
	if err != nil {
		release(0)
		ae := &llm.APIError{Provider: llm.ProviderOpenAI, Status: status, Kind: llm.ErrNetwork, Err: err}
		m.logFailure(ae, err)
		return llm.Response{}, retry.Outcome{Retry: status == 0 && retry.ConnReset(err)}, &classified{api: ae, text: ae.Error() + ": " + networkHint(err, llm.ErrNetwork)}
	}
	m.limiter.Observe(header)
	if status < 200 || status > 299 {
		release(0)
		if resp, ok := m.filteredPrompt(status, raw, len(body)); ok {
			return resp, retry.Outcome{}, nil
		}
		ae, text := m.classify(status, raw)
		return llm.Response{}, retry.Outcome{
			Retry:      retry.RetryableStatus(status) && ae.Kind != llm.ErrBilling,
			RetryAfter: retry.After(header, time.Now()),
		}, &classified{api: ae, text: text}
	}
	resp, err := m.decode(req, raw, len(body))
	if err != nil {
		release(0)
		ae := &llm.APIError{Provider: llm.ProviderOpenAI, Status: status, Kind: llm.ErrOverloaded, Err: err}
		m.logFailure(ae, err)
		return llm.Response{}, retry.Outcome{}, &classified{api: ae, text: ae.Error() + ": the provider's answer could not be read"}
	}
	u := resp.Usage
	release(u.Input + u.CacheRead + u.CacheWrite + u.Output)
	if truncated(len(body), u) {
		ae := &llm.APIError{Provider: llm.ProviderOpenAI, Status: status, Kind: llm.ErrTruncated}
		m.log.Debug("llm/openai: prompt truncated by the server", "flavor", m.flavor.name, "endpoint", m.origin,
			"reported_prompt_tokens", u.Input+u.CacheRead, "request_bytes", len(body))
		return resp, retry.Outcome{}, &classified{api: ae, text: ae.Error() + ": " + kindHint(llm.ErrTruncated)}
	}
	m.log.Debug("llm/openai: turn complete", "flavor", m.flavor.name, "finish_reason", resp.ProviderStop,
		"input_tokens", u.Input, "cached_tokens", u.CacheRead, "output_tokens", u.Output, "estimated", u.Estimated)
	return resp, retry.Outcome{}, nil
}

// String names the flavor and the endpoint origin only: fmt's %v of the
// struct would print the key, whose Secret type cannot hide it from
// reflection on an unexported field.
func (m *Model) String() string { return "openai(" + m.flavor.name + " " + m.origin + ")" }

// GoString is String, for %#v.
func (m *Model) GoString() string { return m.String() }

// LogValue is String, for log/slog.
func (m *Model) LogValue() slog.Value { return slog.StringValue(m.String()) }
