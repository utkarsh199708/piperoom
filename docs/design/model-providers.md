# Model providers: design (main @ 48a974f4, 2026-10-19)

Owner of the design: LEAD-ARCH. Board: `tasks/V-board.md` (the V wave). There is no separate plan file: the owner's
decision of 2026-10-19, recorded in §0, is the plan. Contract changes land in V0 (§4). The Go shapes below are
proposals in the style of `tasks/P0-contracts.md`. Where this text and merged code differ, the code and its doc
comments win.

## 0. Decision recorded (owner, 2026-10-19)

Piperoom must run on every major model provider, not only Claude: OpenAI, Azure OpenAI, Google Gemini, DeepSeek,
Qwen (Alibaba DashScope), Mistral, Groq, Together, OpenRouter, xAI, local Ollama and vLLM, and the non-Claude models
on AWS Bedrock. Claude (Anthropic API, Bedrock, Vertex) stays fully supported and remains the **reference model**:
every other model is measured against its eval baseline. Timing: the design and the board are written now. Code
rows start after P3 closes, because the P3 Claude eval baseline (`P3-W2e`, `evals/baseline/default.json`) is the
reference score.

The agreed shape is four adapters behind one model interface in `internal/agent`:

| # | Adapter | Providers it serves | Today |
|---|---|---|---|
| 1 | Anthropic Messages (`anthropic-sdk-go`) | `anthropic`, `bedrock` (Claude on Bedrock), `vertex` (Claude on Vertex) | exists: `internal/agent/endpoint.go`, `model.go` |
| 2 | OpenAI-compatible chat completions with tool calling | `openai`: OpenAI, Azure OpenAI, DeepSeek, Qwen, Mistral, Groq, Together, OpenRouter, xAI, Ollama, vLLM (base URL + key + model) | new |
| 3 | Gemini native (`generateContent`) | `gemini`: Gemini API (key) and Gemini on Vertex AI (Google ADC) | new |
| 4 | Bedrock Converse | `bedrock-converse`: non-Claude Bedrock models (Nova, Llama, Mistral, DeepSeek, Qwen on Bedrock, and others) | new |

## 1. Goals and non-goals

**Goals**

1. An operator picks any supported provider and model with environment variables only, and Piperoom runs the
   same investigation loop, room tools, proposals, approvals, hand-off, post-mortem, first look and judge on it.
2. **Claude does not change.** With `PIPEROOM_MODEL_PROVIDER` in `anthropic | bedrock | vertex`, the request bytes
   sent to the API are identical to today's (a golden test, §10), `agent.PromptHash()` and the evals prompt hash
   are unchanged, and every existing test passes without a loosened assertion.
3. One system prompt, one tool list, one untrusted-data wrapper for every provider. Differences between providers
   live only in the adapters, so an eval score compares models, not prompts.
4. Every model is admitted by evidence: an eval gate per model (§5), a committed baseline per model, and a published
   pass-rate table.
5. Spend limits work for any model: tokens are normalised, prices come from config, and an unknown price fails safe
   (§8).
6. Free local runs: Ollama or vLLM on the developer's machine, with the dev stack, for development and evals.

**Non-goals**

- No per-provider system prompts or tool descriptions in this wave. A model that needs a different prompt to pass
  the gate is not supported yet (open decision 3).
- No mixing of providers inside one investigation history: the main and the in-loop triage model
  (`PIPEROOM_TRIAGE_MODEL`) are on the same provider. Single-shot roles (first look, summary, judge) may use another
  provider (§9).
- No fine-tuning, embeddings, or provider-hosted tools (web search, code execution, file search).
- No OpenAI Responses API, no Gemini explicit context caching (`cachedContents`) and no streaming in the new
  adapters in this wave (§3.1); each is a later row if the evals show a need.
- No change to who may approve what. The model choice changes nothing in the approval model, write mode, or the
  read-only guarantees of tools (`internal/wiring/readonly*.go`).
- No new runtime dependency where the standard library does: adapters 2 and 3 are `net/http` + `encoding/json`
  (AGENTS.md §4 rule 10). Only adapter 4 adds a module (`github.com/aws/aws-sdk-go-v2/service/bedrockruntime`; the
  AWS SDK core is already in `go.mod`).

## 2. What is Claude-specific today (code read at 48a974f4)

Every non-test file that imports `github.com/anthropics/anthropic-sdk-go`, and what it uses:

| File | Owner | What it uses | Where it goes |
|---|---|---|---|
| `internal/agent/model.go` | A4 | the `model` seam: `Turn(ctx, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error)`; streaming through `client.Beta.Messages.NewStreaming` + `Accumulate`; the `anthropic-workspace-id` header from `ANTHROPIC_WORKSPACE_ID`; a 10 min turn timeout | the seam becomes `llm.Model` (§4.1); the streaming code moves into the Anthropic adapter unchanged |
| `internal/agent/endpoint.go` | A4 | provider switch `anthropic \| bedrock \| vertex`; `bedrock.WithConfig`, `vertex.WithCredentials`; startup credential check | builds one `llm.Model` per role from the provider (§9) |
| `internal/agent/run.go` | A4 | the request: Beta API with `compact-2026-01-12`, system with `cache_control`, top-level `cache_control`, `output_config.effort`, adaptive `thinking`, `tool_choice: auto`, `context_management` compaction with `compactionInstructions()`; stop reasons `refusal`, `end_turn`, `stop_sequence`, `model_context_window_exceeded`, `pause_turn`, `max_tokens`; `resp.ToParam()` append-only history; `StopDetails` for refusals; tool results as `tool_result` blocks | neutral `llm.Request`/`llm.Response`; provider state kept as native parts (§4.2) |
| `internal/agent/toolparams.go` | A4 | tools as `BetaToolUnionParam`: `strict` on up to 8 tools (`maxStrictTools`, `maxStrictSchemaBytes` measured live), `eager_input_streaming`, a cache breakpoint on the last tool, the injected `why` property | neutral `llm.ToolDef{Strict}`; the strict caps become Anthropic adapter capabilities |
| `internal/agent/triage.go` | A4 | triage model switch; `*anthropic.Error` body matched for `perRunMarkers` / `modelRejectionMarkers`; `callsWriteProposal` over content blocks | `llm.APIError.Kind` (§4.3) |
| `internal/agent/triage_pass.go` | A4 | first look: `effort: low`, tools, then a verdict with `output_config.format` (JSON schema) | `llm.Request.OutputSchema` |
| `internal/agent/handoff.go`, `postmortem.go` | A4 | summary request: cached system, `effort`, adaptive thinking, JSON schema output; one retry with the violations (`retryNote`) | `OutputSchema` + the same retry |
| `internal/agent/handoff_judge_claude.go` (`-tags claude`) | A4 | hand-off judge: JSON schema output, `judgeEffort` | provider-neutral judge (V1b) |
| `internal/agent/verify.go`, `examples.go` | A4 | text blocks only (`NewBetaTextBlock`) | `llm.Part` text |
| `internal/agent/usage.go`, `budget.go`, `spend.go` | A4 | `Usage.InputTokens`, `CacheCreationInputTokens`, `CacheReadInputTokens`, `OutputTokens`, `Usage.Iterations` of type `compaction` | `llm.Usage` (§4.1), same four counters |
| `internal/room/service.go` | A5 | `*anthropic.Error` in `isUnretryable` / `modelRefused` (4xx other than 408/409/413/429, 400 "too long") | `llm.APIError.Kind` (V1c) |
| `evals/infra.go` | C6 | `*anthropic.Error` in `APIRefusal` (401, 403, 400 with "credit balance") | `llm.APIError.Kind` (V9) |
| `evals/judge_claude.go` (`-tags claude`) | C6 | stable Messages API, `effort: low`, JSON schema output, stop reasons | provider-neutral judge (V9) |
| `evals/modeltap.go` | C6 | a reverse proxy in front of `ANTHROPIC_BASE_URL` that hashes system + tools as sent | kept for Anthropic; an in-process tap for other providers (V11) |

Configuration (`internal/config/config.go`, a contract file): `PIPEROOM_MODEL` (default `claude-opus-5-5`),
`PIPEROOM_EFFORT` (default `high`), `PIPEROOM_MODEL_PROVIDER` (`anthropic | bedrock | vertex`), `AWS_REGION`,
`VERTEX_PROJECT_ID`, `VERTEX_REGION`, `PIPEROOM_TRIAGE_MODEL`, `PIPEROOM_TRIAGE_PASS_MODEL` (default
`claude-haiku-5-5`), `PIPEROOM_POSTMORTEM_MODEL` (a **mode**, `auto | off`, not a model id),
`PIPEROOM_MODEL_PRICES` (a JSON price table, four required prices per model), `PIPEROOM_ROOM_TOKEN_BUDGET`,
`PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET`, `PIPEROOM_PIPELINE_DAILY_COST_USD`. Keys are never in `Config`: the SDK reads
`ANTHROPIC_API_KEY` and `AWS_BEARER_TOKEN_BEDROCK` itself.

Spend (`docs/design/cost.md`): the token caps count input + cache writes + cache reads + output, compaction passes
included. A model missing from the price table has its cost omitted (never guessed); with a daily cost cap set, its
requests are not counted against the cost cap and a warning is logged once per model (`spend.go`
`warnUnpriced`). §8 changes that to fail-safe for every new provider.

Security pieces that every provider must keep: `wrapToolOutput` (`run.go`: `<tool_output tool=".." step=".."
untrusted="true">…</tool_output>`, with `escapeToolText`), the injection flag (`injectflag.go`, `internal/injectscan`),
secret redaction of untrusted text (`internal/agent/redact.go`, `internal/redact`), the alert-text masking
(`alerttext.go`), the past-incident wrapper (`examples.go`), and the compaction instructions that keep untrusted text
as data (`prompt.go` `compactionInstructions`).

Evals (`evals/cmd/evalrun`): `-model` (default `PIPEROOM_MODEL`), `-judge terms | claude | auto`, `-judge-model`
(default the configured model), `-prices`; the live judge is linked only with `-tags claude`; one baseline,
`evals/baseline/default.json` (`model: claude-opus-5-5`, `effort: high`, prompt hash `sha256:c0d50b2f69c8959f`).
The `evals-live` workflow reads `ANTHROPIC_API_KEY` from the `evals` environment and compares against that baseline.

## 3. Provider capability matrix

Facts below are as each provider documents them in 2026-10. They are the starting values of each adapter's
capability table (`llm.Capabilities`, §4.1); the adapter's table, proven by its recorded fixtures, wins over this
page. A cell marked *per model* means the adapter decides by model id (with a config override, §9).

| | Anthropic (API, Bedrock, Vertex) | OpenAI-compatible | Gemini | Bedrock Converse |
|---|---|---|---|---|
| Tool calling | yes, `tool_use` / `tool_result` blocks | `tools[].function`, `tool_calls`, `role: tool` messages; quality varies by model (gate, §5) | `functionDeclarations`, `functionCall` / `functionResponse` parts | `toolConfig.tools[].toolSpec`, `toolUse` / `toolResult` blocks; *per model* |
| Parallel tool calls | yes | yes on OpenAI, Azure, most hosts (`parallel_tool_calls`); *per model* on local servers | yes (several `functionCall` parts) | *per model* |
| Tool call ids | yes | yes | not on every model: the adapter makes `call_<turn>_<n>` and maps back by position | yes |
| Strict tool schemas | yes, capped (8 tools, ~5.4 KB summed schema, measured) | OpenAI and Azure (`strict: true`, a schema subset: every property required, `additionalProperties: false`); others *per flavor* or no | no strict flag; `parametersJsonSchema` on newer models, else an OpenAPI 3.0 subset | no |
| Structured output | `output_config.format` JSON schema | `response_format: json_schema` on OpenAI, Azure and some hosts; `json_object` or nothing elsewhere | `responseMimeType: application/json` + `responseJsonSchema` / `responseSchema` | none in general: forced tool call (§6) |
| Streaming | used today (required by the SDK for long requests) | SSE; not used in this wave | SSE; not used | `ConverseStream`; not used |
| Prompt caching | explicit `cache_control` breakpoints; cache write and read priced | automatic prefix caching on OpenAI, Azure, DeepSeek, some others; reported as `prompt_tokens_details.cached_tokens` (DeepSeek: `prompt_cache_hit_tokens`); no write premium; none on local servers | implicit caching on current models, reported as `cachedContentTokenCount` | `cachePoint` blocks on models that support it; `cacheReadInputTokens` / `cacheWriteInputTokens` |
| Reasoning / effort | adaptive thinking + `effort` low…max | `reasoning_effort` on reasoning models (OpenAI, Azure, Groq, xAI *per model*); DeepSeek and Qwen pick reasoning by model or flag; dropped elsewhere | `thinkingConfig` (`thinkingLevel` on Gemini 3, `thinkingBudget` on 2.5) | `additionalModelRequestFields` *per model*, else dropped |
| Server-side compaction | yes (beta `compact-2026-01-12`) | no: client-side compaction (§6.1) | no: client-side | no: client-side |
| Context window | 200k (1M on some) | *per model* (128k typical; less on local models unless configured) | 1M on current models | *per model* |
| Usage report | `input_tokens` (uncached), `cache_creation_input_tokens`, `cache_read_input_tokens`, `output_tokens`, compaction `iterations` | `prompt_tokens` (cached included), `completion_tokens` (reasoning included), cached tokens in details; some local servers report nothing | `promptTokenCount` (cached included), `candidatesTokenCount`, `thoughtsTokenCount` (billed as output), `cachedContentTokenCount` | `inputTokens`, `outputTokens`, `cacheReadInputTokens`, `cacheWriteInputTokens` |
| Refusal signal | `stop_reason: refusal` + `stop_details` | `finish_reason: content_filter`, a `message.refusal` string; Azure answers a filtered prompt with HTTP 400 `content_filter` | `finishReason` `SAFETY`, `PROHIBITED_CONTENT`, `BLOCKLIST`, `SPII`, `RECITATION`; `promptFeedback.blockReason` | `stopReason` `guardrail_intervened`, `content_filtered` |
| Auth | `ANTHROPIC_API_KEY`; AWS chain or `AWS_BEARER_TOKEN_BEDROCK`; Google ADC | bearer key (`api-key` header on Azure); none on local servers | `x-goog-api-key` (Gemini API) or Google ADC (Vertex) | AWS chain or Bedrock API key |

**OpenAI-compatible flavors.** One adapter, one `flavor` that sets the defaults and the quirks; the operator can
always override the base URL. A flavor never changes the prompt or the tools.

| Flavor | Default base URL | Key variable | Quirks the adapter handles |
|---|---|---|---|
| `openai` | `https://api.openai.com/v1` | `OPENAI_API_KEY` | `developer` role for reasoning models; `max_completion_tokens`; `reasoning_effort`; strict tools and `json_schema` |
| `azure` | none (required: `https://<resource>.openai.azure.com/openai/v1`) | `AZURE_OPENAI_API_KEY` | `api-key` header; model = deployment name; prompt filter answers 400 `content_filter` → refusal |
| `deepseek` | `https://api.deepseek.com/v1` | `DEEPSEEK_API_KEY` | reasoning by model id; `reasoning_content` handled by the rule the V3b fixtures prove (sent back or not); cache usage fields |
| `qwen` | `https://dashscope-intl.aliyuncs.com/compatible-mode/v1` | `DASHSCOPE_API_KEY` | `enable_thinking` per model; the China endpoint only by explicit base URL (§7.4) |
| `mistral` | `https://api.mistral.ai/v1` | `MISTRAL_API_KEY` | tool call ids of 9 alphanumeric characters (the adapter keeps the model's own) |
| `groq` | `https://api.groq.com/openai/v1` | `GROQ_API_KEY` | `reasoning_effort` on reasoning models only |
| `together` | `https://api.together.xyz/v1` | `TOGETHER_API_KEY` | — |
| `openrouter` | `https://openrouter.ai/api/v1` | `OPENROUTER_API_KEY` | `provider` routing object with `data_collection: deny` always sent (§7.4) |
| `xai` | `https://api.x.ai/v1` | `XAI_API_KEY` | `reasoning_effort` *per model* |
| `ollama` | `http://localhost:11434/v1` | none | loopback http allowed; no usage on some versions → estimated (§8.3); context from `PIPEROOM_MODEL_CONTEXT_TOKENS` |
| `vllm` | `http://localhost:8000/v1` | `PIPEROOM_MODEL_API_KEY` if the server has one | as `ollama`; tool calling needs the server's tool parser enabled (doc note) |
| `generic` | none (required) | `PIPEROOM_MODEL_API_KEY` | no reasoning, no strict, no `json_schema` unless `PIPEROOM_MODEL_CAPS` turns them on (§9) |

### 3.1 Why no streaming in the new adapters

The room shows a model turn when it is complete (the loop emits `agent.message` after the turn), so streaming buys
no UI. A non-streaming request reports usage reliably on every compatible host (in a stream, usage needs
`stream_options.include_usage`, which several hosts ignore), and the code is a quarter of the size. The turn keeps
the 10 min deadline (`modelTimeout`). If a host is found to cut long non-streaming requests, streaming becomes a
flavor quirk in a later row.

## 4. The model interface (contract change list for V0)

The neutral types go in a **new contract file, `internal/agent/llm/llm.go`** (package `llm`), written by the leads
in V0 and added to the contract list in AGENTS.md §2 and to the leads' folders in `docs/process/ownership.json`
(done in this PR, so A4 cannot edit it before V0). It is a sub-package, not `contract.go`, because the adapters
(`internal/agent/llm/<adapter>/`) must import the types and `internal/agent` must import the adapters; putting the
types in package `agent` would be an import cycle. `internal/room` and `evals` import `llm` for error kinds and the
judge. Everything else under `internal/agent/llm/**` is A4's.

### 4.1 Types

```go
// Package llm is the provider-neutral model interface of the agent. Adapters
// translate it to one provider's wire format; nothing above them sees a
// provider type.
package llm

// Provider values of PIPEROOM_MODEL_PROVIDER (and PIPEROOM_<ROLE>_PROVIDER).
const (
	ProviderAnthropic       = "anthropic"        // Anthropic API (adapter 1)
	ProviderBedrock         = "bedrock"          // Claude on Bedrock (adapter 1)
	ProviderVertex          = "vertex"           // Claude on Vertex AI (adapter 1)
	ProviderOpenAI          = "openai"           // any OpenAI-compatible chat completions host (adapter 2)
	ProviderGemini          = "gemini"           // Gemini API or Gemini on Vertex AI (adapter 3)
	ProviderBedrockConverse = "bedrock-converse" // non-Claude Bedrock models (adapter 4)
)

// Model sends one request and returns one complete assistant turn.
// Implementations are safe for concurrent use and honour ctx's deadline.
type Model interface {
	Turn(ctx context.Context, req Request) (Response, error)
	// Capabilities reports what the provider supports for model; it never
	// makes a network call.
	Capabilities(model string) Capabilities
	// Provider is one of the Provider* values.
	Provider() string
}

// Request is one model call.
type Request struct {
	Model    string
	System   string
	Tools    []ToolDef // stable order: the prompt cache depends on it
	Messages []Message
	// MaxTokens bounds the output (thinking included where the provider counts it).
	MaxTokens int64
	// Effort is low | medium | high | xhigh | max; the adapter maps it or
	// drops it (Capabilities.Effort).
	Effort string
	// Thinking asks for the provider's adaptive reasoning when it has one.
	Thinking bool
	// ToolChoice is auto (default), none, or required.
	ToolChoice ToolChoice
	// OutputSchema, when set, asks for a JSON object that matches it. An
	// adapter without structured output uses a forced tool call or plain
	// text; the caller always validates (Capabilities.StructuredOutput).
	OutputSchema map[string]any
	// Cache marks where the provider may cache a prefix: after System, after
	// Tools, and at the end of Messages. Ignored by providers that cache on
	// their own or not at all.
	Cache CacheHints
	// Compaction, when set, asks for server-side compaction; adapters without
	// it ignore it and the agent compacts client-side (Capabilities.Compaction).
	Compaction *Compaction
}

type ToolChoice string

const (
	ToolChoiceAuto     ToolChoice = "" // the default
	ToolChoiceNone     ToolChoice = "none"
	ToolChoiceRequired ToolChoice = "required"
)

type CacheHints struct{ System, Tools, History bool }

type Compaction struct{ Instructions string }

// ToolDef is one tool as the model sees it. Name is the Piperoom tool name;
// an adapter that must rename it on the wire maps it back (§7.1).
type ToolDef struct {
	Name        string
	Description string
	Schema      map[string]any
	// Strict asks the provider to enforce Schema. The agent validates every
	// input in code either way.
	Strict bool
}

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// Message is one turn of history. History is append-only: an assistant
// Message goes back exactly as the Response returned it.
type Message struct {
	Role  Role
	Parts []Part
}

type PartKind string

const (
	PartText       PartKind = "text"
	PartToolCall   PartKind = "tool_call"
	PartToolResult PartKind = "tool_result"
	// PartReasoning is provider state with no text for Piperoom: thinking
	// with its signature, a compaction block, a Gemini thought signature,
	// DeepSeek reasoning content. Only Native carries it.
	PartReasoning PartKind = "reasoning"
)

// Part is one block of a Message.
type Part struct {
	Kind       PartKind
	Text       string      // PartText
	ToolCall   *ToolCall   // PartToolCall
	ToolResult *ToolResult // PartToolResult
	// Native is the provider's own encoding of this part, set by the adapter
	// that produced it. The same adapter sends Native back verbatim (byte
	// identity keeps the prompt cache and thinking signatures valid); any other
	// adapter ignores it and uses the neutral fields, and drops a
	// PartReasoning whose Native is not its own.
	Native *Native
}

type Native struct {
	Provider string          // Provider* of the adapter that made it
	Model    string          // the model that made it (signatures are model-bound)
	Raw      json.RawMessage // the provider's block, as received
}

type ToolCall struct {
	ID    string          // the provider's id, or one the adapter made
	Name  string          // the Piperoom tool name (mapped back)
	Input json.RawMessage // the model's arguments, unparsed; the agent validates
}

type ToolResult struct {
	CallID  string
	Name    string // needed by Gemini's functionResponse
	Content string // already wrapped by the agent (wrapToolOutput); adapters never alter it
	IsError bool
}

type StopReason string

const (
	StopEndTurn         StopReason = "end_turn"         // includes stop_sequence
	StopToolUse         StopReason = "tool_use"         // any turn whose Parts hold a tool call
	StopMaxTokens       StopReason = "max_tokens"
	StopPause           StopReason = "pause"            // Anthropic pause_turn
	StopRefusal         StopReason = "refusal"          // refusal, safety or content filter, prompt or output
	StopContextExceeded StopReason = "context_exceeded"
	StopOther           StopReason = "other"            // continue like tool_use; logged with ProviderStop
)

// Response is one complete assistant turn.
type Response struct {
	Message      Message    // Role is RoleAssistant
	Stop         StopReason
	StopDetail   string     // refusal category and explanation, or the filter reason; never logged in full
	ProviderStop string     // the raw stop reason, for the debug log
	Usage        Usage
	Model        string     // the model id the provider says served the turn
}

// Usage is normalised: Input excludes cached tokens on every provider.
type Usage struct {
	Input      int64 // uncached input tokens
	CacheWrite int64 // tokens written to a cache at a write price (Anthropic, Converse)
	CacheRead  int64 // tokens read from a cache
	Output     int64 // output, reasoning included
	Reasoning  int64 // the part of Output that was reasoning, when reported; informational
	// Compaction is a server-side compaction pass inside this request (Anthropic).
	Compaction *Usage
	// Estimated is true when the provider reported no usage and the adapter
	// estimated it from the request and response sizes (§8.3).
	Estimated bool
}

// Capabilities is what a provider supports for one model. Zero values mean
// "not supported"; the agent degrades by §6.
type Capabilities struct {
	ToolCalls         bool
	ParallelToolCalls bool
	StrictTools       bool
	MaxStrictTools    int   // 0 = no cap
	MaxStrictBytes    int   // summed schema bytes, 0 = no cap
	StructuredOutput  bool
	PromptCache       bool  // any cache that lowers price, automatic or explicit
	Reasoning         bool
	Effort            bool
	Compaction        bool  // server-side
	ContextTokens     int64 // 0 = unknown: the agent assumes 128000
	MaxTools          int   // 0 = no cap
	ToolNameMax       int   // longest tool name the provider accepts
}
```

### 4.2 Errors

```go
// ErrorKind classifies a provider error so the room, the triage switch and
// the evals decide without reading provider bodies.
type ErrorKind string

const (
	ErrAuth          ErrorKind = "auth"           // 401, 403: bad, revoked or unauthorised key
	ErrBilling       ErrorKind = "billing"        // no credit, quota exhausted (OpenAI insufficient_quota, Anthropic "credit balance")
	ErrModelNotFound ErrorKind = "model_not_found"
	ErrBadRequest    ErrorKind = "bad_request"    // request shape: a model that rejects a parameter
	ErrTooLong       ErrorKind = "too_long"       // 413, or a 400 about the context length
	ErrRateLimited   ErrorKind = "rate_limited"   // 429 after the adapter's retries
	ErrOverloaded    ErrorKind = "overloaded"     // 5xx, 529 after retries
	ErrNetwork       ErrorKind = "network"        // no HTTP answer
)

// APIError is every adapter's error for a failed request. Err keeps the
// SDK's own error (for example *anthropic.Error), so errors.As on it still
// works during the switch-over.
type APIError struct {
	Provider string
	Status   int
	Kind     ErrorKind
	Body     string // redacted (internal/redact) and cut to 2 KiB; never holds the key
	Err      error
}

func (e *APIError) Error() string
func (e *APIError) Unwrap() error

// Unretryable reports an error a fresh run repeats: auth, billing,
// model_not_found and bad_request. too_long, rate_limited, overloaded and
// network are not.
func Unretryable(err error) bool
```

`room.modelRefused` becomes `llm.Unretryable`; `evals.APIRefusal` becomes `Kind ∈ {auth, billing}`; the triage
switch's `modelRejection` becomes `Kind ∈ {bad_request, model_not_found}` with the same "the main model accepted the
same request" rule. The Anthropic adapter maps today's markers (`perRunMarkers`, `modelRejectionMarkers`, "credit
balance", "too long") into kinds, so behaviour is identical (V1a test: every existing marker case maps to the kind
that gives today's decision).

### 4.3 Other contract changes in V0

| File | Change |
|---|---|
| `internal/events/events.go` | `AgentUsagePayload` gains `Provider string \`json:"provider,omitempty"\`` and `Estimated bool \`json:"estimated,omitempty"\``; `ShadowRun` and the usage rows keep `model` |
| `internal/config/config.go` | the variables of §9 and their parse functions (`ParseModelRoles`, `ParseBaseURL`) |
| `internal/agent/contract.go` | no change: `Options.Model` and `Options.Effort` keep their meaning for the main model |
| `docs/api.md` | `agent.usage` fields `provider`, `estimated`; `GET /api/models` and, for standalone only, `PUT /api/models` (§11) |
| AGENTS.md §2 | `internal/agent/llm/llm.go` in the contract list (this PR) |

New methods go on new types; every existing fake and the eval replay compile unchanged, because the old `model`
seam is unexported and the refactor (V1a) changes it in one PR with its fakes.

## 5. Minimum requirements and the eval gate

A model is **supported** when it passes the gate; **experimental** when it passes the hard gates only; **refused**
otherwise. The server refuses to start on a model that is not in the supported list
(`internal/agent/llm/supported.json`, §5.3) unless `PIPEROOM_MODEL_UNVERIFIED=on`, which logs a warning at start
and shows "unverified model" in the room header and the web model panel. Claude models listed today are supported
by definition (the reference).

### 5.1 Hard requirements (all must hold; one failure means refused)

1. **Tool calling with JSON arguments**, several tools in one request, and a tool result fed back. Checked by the
   adapter's conformance suite (`internal/agent/llm/llmtest`, V1a) against recorded fixtures, then live.
2. **Reliable tool calls**: over the CDC eval set (3 runs per scenario), at least 98% of tool calls name a real
   tool and pass input validation the first time (the report's `rejected_attempts` over tool calls).
3. **Safety**: zero forbidden actions and zero forbidden params (`evals/forbidden.go`) over the set; zero failures
   on the injection scenarios (`evals/injection_test.go` set and the V15 scenarios).
4. **Context window** ≥ 128k tokens (declared or configured) and **max output** ≥ 8k tokens.
5. **System prompt** honoured as a system or developer message (not folded into the first user turn).
6. **Summaries parse**: ≥ 95% of hand-off and first-look verdicts validate against their schema within the one
   retry (§6).

### 5.2 Quality floor (supported vs experimental)

Against the Claude reference baseline of the same scenario set (P3-W2e's `default.json`, the reference model at
`effort: high`), on 3 runs per scenario: root-cause rate ≥ 0.8 × reference, layer rate ≥ 0.8 × reference, pass rate
≥ 0.7 × reference, and no scenario that the reference passes 3/3 failed 3/3. The ratios are the owner's to change
(open decision 1).

### 5.3 How the gate is run and recorded

- `evalrun -provider P -model M -runs 3 -gate evals/baseline/default.json` (V10) prints each gate with the measured
  value and exits 4 on a hard-gate failure, 5 on a quality-floor miss.
- A passing run is committed as `evals/baseline/<provider>-<model>.json` (C6, V10) by the same PR flow as the
  reference baseline (workflow artifact, reviewed diff).
- A4 then adds the model to `internal/agent/llm/supported.json` (`{provider, model, status, baseline, gated_at}`)
  in a one-line row (V13). A CI check (C1, V12) fails when an entry has no baseline file or its baseline misses the
  gate.
- A supported model is re-gated whenever the reference baseline is re-baselined after a prompt change; one that
  then fails drops to experimental in the same PR as the new reference.

## 6. Degradation rules

The rule: **a missing feature never weakens a safety check; it only costs money or a retry, and the cost is
reported.**

| Missing | What the agent does | How it is reported |
|---|---|---|
| Prompt caching | sends the same prompt; history is re-billed at the input price each turn | `agent.usage` has `cache_read_input_tokens: 0`; the startup log says "no prompt cache: each turn re-sends the full history at the input price"; `GET /api/models` has `prompt_cache: false`; the per-model eval report shows cost per run |
| Strict tool schemas | sends the schema without `strict` | nothing to report: every tool input is validated in code before `Run`, room tools included (V2a makes this universal; today read tools are validated and `propose_fix` checks its steps). An invalid input becomes an error `tool_result` with the validation message, and counts as a rejected attempt |
| Structured output | (a) forced tool call: one tool `emit_<what>` whose schema is the output schema, `tool_choice: required`; else (b) plain text with the schema in the prompt, the first JSON object parsed | a parse or schema failure gets the existing one retry (`handoff.go` `retryNote`); a second failure is the existing failure path (`postmortem.failed`, triage `reason: model_error`, judge verdict unjudged) |
| Effort / reasoning | the adapter maps the effort it can (§3) and drops the rest | the startup log names the dropped parameter once |
| Server-side compaction | client-side compaction (§6.1) | an `agent.usage` event with role `compaction` for the summary call |
| Parallel tool calls | none: the loop already handles one call per turn | — |
| Usage report | estimate (§8.3) | `estimated: true` on the event |
| Refusal detail | `StopDetail` empty | "model refused the request", as today |

### 6.1 Client-side compaction

For a provider without server compaction, before each turn the agent estimates the request's input tokens (the
last turn's reported input plus the bytes added since, at 3 bytes per token). Above 70% of
`Capabilities.ContextTokens` it makes one summary call on the same model with `compactionInstructions()` as the
system prompt and the oldest messages as input, keeping the last 6 messages and every message after the latest
human input verbatim. The summary replaces the oldest messages as one user message wrapped as
`<compacted_history untrusted="true">…</compacted_history>`: the model wrote it from text that held untrusted tool
output, so it is data, never instructions (the same rule the compaction instructions give Claude). Step ids,
hypotheses, proposals and driver instructions must survive (a test over a recorded long room). A
`context_exceeded` stop after compaction keeps today's behaviour: report and wait for a human.

## 7. Tool-call translation and limits

### 7.1 Names

Piperoom tool names are lowercase snake case under 40 characters, which every provider accepts:

| Provider | Name rule (documented) | Max tools in one request |
|---|---|---|
| Anthropic | `^[a-zA-Z0-9_-]{1,64}$` (128 on newer models) | no small cap |
| OpenAI and most compatible hosts | `^[a-zA-Z0-9_-]{1,64}$` | 128 |
| Gemini | starts with a letter or `_`; letters, digits, `_ . : -`; at most 64 | per model (V4 records it) |
| Bedrock Converse | `^[a-zA-Z0-9_-]{1,64}$` | per model |

Each adapter has a test over the full production registry plus room tools (`wiring` full registry, as
`TestLiveStrictToolCap` builds it) that every name passes its rule and the count is under its cap; the server
refuses to start otherwise. If a future tool breaks a rule, the adapter maps the name reversibly (a table built from
the registry, never from model output) instead of renaming the tool.

### 7.2 Schemas

The agent sends each tool's `InputSchema()` plus the injected `why` property, as today. Adapters convert:

- **OpenAI strict**: only on flavors with `StrictTools`, only for tools whose schema already meets the subset
  (every property required, `additionalProperties: false`, no unsupported keyword); others go non-strict. No
  adapter rewrites optional fields into nullable ones: that would change the meaning the code validates.
- **Gemini**: `parametersJsonSchema` where the model takes it; else a converter to the OpenAPI subset that drops
  `additionalProperties`, `$schema`, `pattern`-only constraints it cannot express, and fails start on a construct it
  cannot convert (`oneOf` over objects). Dropped constraints are still enforced by the code validation.
- **Converse**: `inputSchema.json` as is.

### 7.3 Messages

| Neutral | OpenAI-compatible | Gemini | Converse |
|---|---|---|---|
| `System` | `system` message (`developer` for OpenAI reasoning models) | `systemInstruction` | `system` blocks |
| assistant `PartText` + `PartToolCall` | one `assistant` message with `content` and `tool_calls` (arguments as a JSON string) | `model` content with `text` and `functionCall` parts | `assistant` with `text` and `toolUse` |
| user `PartToolResult` × n then `PartText` | n `tool` messages (`tool_call_id`) first, then one `user` message with the text | one `user` content with n `functionResponse` parts (`response: {"content": <wrapped text>}`, `name` from `ToolResult.Name`) then `text` parts | `user` with n `toolResult` (status `error` when `IsError`) then `text` |
| `PartReasoning` | sent back only for the flavor and model that made it, by the fixture-proven rule | Gemini thought signatures returned on their parts (required on current models for function calling) | dropped |
| `ToolResult.IsError` | the wrapped text already starts `error:`; no wire flag exists | as OpenAI | `status: error` |

A tool call whose arguments are not valid JSON is not dropped: it becomes an error `tool_result` ("arguments are not
valid JSON") so the call and its result stay paired, which every provider requires.

## 8. Pricing and spend limits

### 8.1 The price table

`PIPEROOM_MODEL_PRICES` keeps its format (`docs/design/cost.md` 2.1: four required prices per model, USD per
million tokens, unknown fields refused). Two changes, both A4 (V6):

- A key may be **provider-qualified**: `"groq:openai/gpt-oss-120b"`. Lookup tries `<provider>:<model>` first, then
  `<model>`. Model ids repeat across hosts at different prices; the qualified key removes the ambiguity. Claude
  tables keep working unchanged.
- How each provider's usage maps onto the four prices: `input` is the uncached input price; `cache_read` the cached
  input price (equal to `input` where there is no discount); `cache_write` the cache write price, equal to `input`
  where there is no write premium (OpenAI-compatible, Gemini implicit); `output` the output price, which also prices
  reasoning tokens. A local model is priced explicitly at zero (`{"input": 0, "output": 0, "cache_write": 0,
  "cache_read": 0}`): zero is a price, a missing entry is not.

### 8.2 Unknown prices fail safe

Today a missing price under a daily cost cap logs a warning and the cap does not count that model's requests. The
new rule (V6): when `PIPEROOM_PIPELINE_DAILY_COST_USD` is set, the server **refuses to start** if any configured
model of any role (main, triage, first look, summary, judge) has no price. Without a cost cap, a missing price keeps
today's behaviour (cost omitted, never guessed). Token caps (`PIPEROOM_ROOM_TOKEN_BUDGET`,
`PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET`) need no price and work on every provider, since usage is normalised. This
also applies to Claude, which is a behaviour change for a deployment that sets a cost cap and leaves a role's model
out of the table (open decision 4; until it is decided, V6 applies it only to non-Claude providers).

### 8.3 Missing usage

A host that returns no usage (some Ollama and vLLM versions, some proxies) gets an estimate: input = request JSON
bytes / 3, output = response JSON bytes / 3, rounded up, `Estimated: true`. The estimate is charged against every
cap like real usage. Over-estimating is the safe side; the 3 bytes per token ratio over-counts English and JSON.

## 9. Configuration (contract change for `internal/config/config.go`)

| Variable | Default | Meaning |
|---|---|---|
| `PIPEROOM_MODEL_PROVIDER` | `anthropic` | `anthropic \| bedrock \| vertex \| openai \| gemini \| bedrock-converse` |
| `PIPEROOM_MODEL` | `claude-opus-5-5` | main model id, as the provider names it (deployment name on Azure) |
| `PIPEROOM_EFFORT` | `high` | unchanged; mapped per provider (§3) |
| `PIPEROOM_MODEL_FLAVOR` | `openai` | `openai` only: one of the flavors of §3 |
| `PIPEROOM_MODEL_BASE_URL` | the flavor's default | `openai` and `gemini` (an override, for example a regional or proxy endpoint); validated (§10.3) |
| `PIPEROOM_MODEL_API_KEY` | — | the main provider's key when its standard variable is not used; **read by the adapter at construction, never stored in `Config`**, never logged. `PIPEROOM_MODEL_API_KEY_FILE` reads it from a file (a mounted secret) |
| standard key variables | — | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `GEMINI_API_KEY`, `DEEPSEEK_API_KEY`, `DASHSCOPE_API_KEY`, `MISTRAL_API_KEY`, `GROQ_API_KEY`, `TOGETHER_API_KEY`, `OPENROUTER_API_KEY`, `XAI_API_KEY`, `AWS_BEARER_TOKEN_BEDROCK`; used when `PIPEROOM_MODEL_API_KEY` is empty |
| `PIPEROOM_MODEL_CONTEXT_TOKENS` | the adapter's table, else 128000 | the main model's context window, for client-side compaction |
| `PIPEROOM_MODEL_CAPS` | — | comma list that turns capabilities on for a `generic`, `ollama` or `vllm` host the adapter cannot know: `parallel_tools,structured_output,reasoning`. It cannot turn on anything that lowers a check |
| `PIPEROOM_MODEL_UNVERIFIED` | `off` | `on` allows a model not in `supported.json` (§5) |
| `AWS_REGION` | — | `bedrock` and `bedrock-converse` |
| `VERTEX_PROJECT_ID`, `VERTEX_REGION` | — | `vertex`, and `gemini` on Vertex (`PIPEROOM_GEMINI_BACKEND=vertex`; default `api`) |
| `PIPEROOM_TRIAGE_MODEL` | — | unchanged: in-loop triage turns, **same provider as the main model** (they share one history) |
| `PIPEROOM_TRIAGE_PASS_PROVIDER`, `PIPEROOM_TRIAGE_PASS_MODEL` | main provider; `claude-haiku-5-5` on Claude providers, else the main model | first look (single-shot) |
| `PIPEROOM_SUMMARY_PROVIDER`, `PIPEROOM_SUMMARY_MODEL` | main provider and model | hand-off and post-mortem writer. `PIPEROOM_POSTMORTEM_MODEL` keeps its meaning (`auto \| off`, a mode); it is not renamed in this wave (open decision 5) |
| `PIPEROOM_JUDGE_PROVIDER`, `PIPEROOM_JUDGE_MODEL` | main provider and model | the hand-off judge and the evals judge (`-judge-provider`, `-judge-model` override) |
| `PIPEROOM_<ROLE>_BASE_URL` | the role provider's default | only when the role's provider differs from the main one; `ROLE` ∈ `TRIAGE_PASS`, `SUMMARY`, `JUDGE` |

A role on another provider reads that provider's standard key variable (never `PIPEROOM_MODEL_API_KEY`, which
belongs to the main provider). Start fails, with the variable named, on: an unknown provider or flavor, a missing key
or credentials for any configured role, an invalid base URL, a model not supported (without `UNVERIFIED`), a role
whose tool count or names break its adapter's limits, and an unpriced model under a cost cap (§8.2).

`cmd/investigate` and `evals/cmd/evalrun` read the same variables; `evalrun` adds `-provider` and
`-judge-provider` flags (V10).

## 10. Security

### 10.1 The same untrusted-data rules on every provider

- The agent builds every tool result, alert text, past incident and compacted history string **before** the adapter
  sees it: `wrapToolOutput` with `escapeToolText`, the injection flag, `redact`, `alerttext` masking. Adapters
  receive finished strings and must not alter them (the conformance suite asserts byte equality of every
  `ToolResult.Content` and `PartText` from the neutral request to the wire body, per adapter).
- Untrusted text is never placed in a `system`, `developer` or `systemInstruction` field, and never as a JSON key
  or tool name: only in a tool result or a user text part, inside its wrapper (conformance test).
- Local and open-weight models are served through chat templates with control tokens. Untrusted text holding those
  tokens (`<|im_start|>`, `<|im_end|>`, `<|eot_id|>`, `<start_of_turn>`, `[INST]`, `<|tool_call|>` and the like)
  could break the template on a server that does not escape them. `internal/injectscan` gains a rule that flags them
  (N6, V16), and the agent neutralises them in untrusted text for every provider (the tokens never matter to
  Claude, so the Claude request changes only for text that holds them; open decision 6).
- Tool arguments from any model are untrusted input: validated against the schema in code (§6), and the pipeline
  scope comes from the alert, never from the model (as today, `run.go` `toolScope`).

### 10.2 Keys

Keys come only from the environment or a mounted file; they are never in `Config`, a log line, an event, an error
(`APIError.Body` is redacted and the request is never logged), the web, or an eval transcript. Adapters hold the key
as `netsec.Secret` (`internal/clients/netsec`), whose `String`, `GoString`, `LogValue` and `MarshalText` redact. A
test per adapter makes a request against `httptest` that fails, and greps the error, the log output (a capturing
`slog` handler) and the event payloads for the key.

### 10.3 Base URL validation (no SSRF through a configurable base URL)

The base URL is operator configuration, but it can be set from the standalone web picker (§11) and it decides where
the key is sent, so it is validated at start and on every dial:

- scheme `https`; `http` only when the host is a loopback name or address (`localhost`, `127.0.0.0/8`, `::1`), for
  Ollama and vLLM on the same machine;
- no user info, no query, no fragment; the path is kept (Azure and proxies need it);
- the dialer (one `net.Dialer` with a `Control` hook, as U1-2 does for the runner) refuses link-local
  (`169.254.0.0/16`, `fe80::/10`, which covers the cloud metadata services), multicast and unspecified addresses for
  the address actually dialed, so a name that resolves to the metadata service is refused;
- redirects are not followed (`CheckRedirect` returns an error), so a 3xx cannot send the key to another host;
- the key is sent only to the configured host;
- a private-range `http` vLLM inside a cluster is refused by default (open decision 2).

Every adapter keeps AGENTS.md §4 rule 7: one `*http.Client` with a timeout, bodies closed, status checked, responses
capped (`io.LimitReader`, 4 MB; a model turn is far smaller).

### 10.4 Where incident data goes

Each provider receives what Claude receives today: the system prompt, the alert text (masked), tool outputs
(redacted for secrets, but holding topic names, connector configs, schema fields and, from `trace_record`, record
values), steers and comments. Choosing a provider makes it a processor of that data. So:

- the model panel (§11) and the room header show `provider / model` and, where known, the region;
- `docs/pilot/data-handling.md` gets a per-provider table (C14, V14): who processes the data, where, retention and
  training terms as the provider publishes them, and the setting that keeps data out of training where one exists;
- Bedrock, Vertex and Azure keep data in the chosen region; the Gemini API, OpenAI, Groq, Together, xAI and
  OpenRouter process it where the provider decides; DeepSeek's API is operated in the People's Republic of China;
  DashScope has an international endpoint (the `qwen` default) and a China endpoint (explicit base URL only);
  Mistral's API is in the EU; Ollama and vLLM on the operator's own machine send nothing out;
- OpenRouter forwards to third parties: the adapter always sends `provider: {data_collection: "deny"}`;
- no provider is a default other than Anthropic; nothing switches provider on its own (no automatic failover
  between providers, since that would move incident data to a processor the operator did not choose).

## 11. The model picker (web) and the docs

- **Team server** (any non-standalone deployment): the web shows a read-only **Model** panel (settings and the room
  header popover): provider, model and region per role, gate status (supported, experimental, unverified), prompt
  cache yes or no, and the price per million tokens when priced. Data from `GET /api/models` (A5, V7): never a key,
  never a full base URL (host only). Changing the model there is an operator task (environment, Helm values).
- **Standalone and quickstart** (`piperoom-setup --standalone`, `cmd/server --quickstart`; loopback only, one user):
  when no usable model is configured, the server starts in a *no model* state and the web opens the picker before
  anything else, because the setup agent itself needs a model. The picker lists the supported models with their pass
  rate and cost per run from the committed baselines (`supported.json` served through `GET /api/models`), then the
  experimental ones behind a warning. The key goes in a secret field to `PUT /api/models` (loopback, standalone or
  quickstart only, the solo user only; the key is excluded from request logging and never echoed), which validates
  provider, flavor, base URL and model, writes `.piperoom/model.env` 0600 through `os.Root` (U1-4's store rules),
  rebuilds the endpoint and returns the panel. A team server answers `PUT` with `404`.
- **Docs**: `docs/pilot/models.md` (C14, V14) is the published table: provider, model, status, pass rate and
  root-cause rate against the reference, cost per run, gate date, the environment block to copy, and the data
  residency line; `README.md` and `docs/runbook.md` (C1, V12) link it; `evals/README.md` (C6) explains per-model
  baselines and the gate.

## 12. Evals

- **A provider-neutral judge** (C6, V9): `evals.NewLLMJudge(llm.Model, model)` replaces `NewClaudeJudge`; the
  verdict schema goes through `OutputSchema` with the forced-tool and retry fallbacks (§6). `-judge claude` stays as
  an alias of `-judge llm` with an Anthropic model. The judge cache entry records `provider:model`; the key is
  unchanged (a verdict stays valid across models, as today). The build tag `claude` that keeps paid judges out of
  default builds is renamed `livejudge`, with `claude` kept as an alias until C1 switches the workflow.
- **One baseline per model** at `evals/baseline/<provider>-<model>.json`, the model id with `/` and `:` replaced by
  `_`. `default.json` stays the Claude reference until the workflow reads the new names, then becomes
  `anthropic-claude-opus-5-5.json` in one C6 + C1 pair of PRs. A report records `provider`, `flavor`, `model`,
  `effort` and the prompt hash.
- **Prompt hash**: for the Anthropic adapter the existing tap hash (system + tools as sent) is unchanged. Other
  providers hash the neutral `System` and `Tools` (canonical JSON), prefixed `neutral-sha256:` so the two kinds never
  compare; `agent.PromptHash()` (system text only) is the same for every provider and is the value that proves the
  prompt did not change between models.
- **A published pass-rate table**: `evalrun -models-table evals/baseline` prints the markdown table that
  `docs/pilot/models.md` embeds; the reference row is first.
- **evals-live per provider** (C1, V12): `workflow_dispatch` inputs `provider`, `flavor`, `model`; one secret per
  provider in the `evals` environment (required reviewers, as today); a matrix job over `supported.json` for the
  nightly when it is turned back on; compare against that model's own baseline; artifacts named
  `baseline-<provider>-<model>`.
- **Free local runs**: `dev/docker-compose.yml` gains an `llm` profile with Ollama (A1, V17); `evalrun -provider
  openai -flavor ollama -model <id>` runs the whole set at no cost. A local run's report is valid for development,
  not for the gate unless the model is the one being admitted.

## 13. Rollout

1. **V0** (leads): the contract file and config, events and API fields. No behaviour change: nothing uses them.
2. **V1a/V1b** (A4): the refactor onto `llm` with the Anthropic adapter. Proven by: every existing test in
   `internal/agent`, `internal/room`, `internal/wiring`, `evals` passing with no assertion loosened; a golden test
   that the marshalled Anthropic request for a recorded room (main turn, triage turn, first look, hand-off, judge)
   is byte-identical to the one main sends today (golden captured on main before the refactor); `agent.PromptHash()`
   and the evals tap hash unchanged; the CDC pass rate on one live `evals-live` run within noise of the reference.
3. **V1c** (A5) and V9's error part (C6): the room and evals classify by `llm.APIError`.
4. **V2a/V2b** (A4): universal code validation, structured-output fallback, client-side compaction (no effect on
   Claude: compaction stays server-side there; code validation already passes every valid Claude input).
5. Adapters **V3a/V3b, V4, V5**, each behind its provider value; nothing selects them by default.
6. **V6** per-role selection and the start checks; **V7/V8** API and picker; **V9 to V12** evals and CI.
7. Per model: a gated baseline (V10 run), then the `supported.json` line (V13), then the docs table row (V14).

Claude stays the default and the reference throughout; a deployment that sets nothing new sees no change.

## 14. Risks

| Risk | Mitigation |
|---|---|
| The refactor changes Claude's behaviour subtly (block order, cache breakpoints, thinking signatures) | native parts sent back verbatim; the golden byte test; one live eval run before merge |
| Other models call tools badly or loop | the hard gate (98% valid calls); `MaxTurns` and the token caps apply unchanged; invalid calls cost a retry, never an action |
| A weaker model proposes a wrong fix | nothing changes in the approval model: every write still needs the layer owners; the gate requires zero forbidden actions |
| Prompt injection is easier on some models | the same wrapper and scan; injection scenarios are a hard gate; template control tokens neutralised |
| Costs higher than expected (no cache) | reported per event and per eval run; token caps work everywhere; unpriced under a cost cap refuses start |
| Incident data sent to a processor the customer did not approve | no default other than Anthropic; region shown in the UI; the data-handling table; no automatic cross-provider failover |
| SSRF or key leak through a configurable base URL | §10.3; key only to the configured host; no redirects |
| Provider APIs drift (field names, reasoning rules) | recorded fixtures per flavor; capability tables in code; a live smoke run per supported model in `evals-live` |
| The supported list goes stale after a prompt change | re-gate rule (§5.3) and the CI check (V12) |
| One new module (`bedrockruntime`) | V5 is built outside the ship workflow under the §7 two-lead review, since `go.mod` is not in any agent's folders in `ownership.json` |

## 15. Open owner decisions

1. **Gate thresholds**: 98% valid tool calls, root-cause and layer rates ≥ 0.8 × reference, pass rate ≥ 0.7 ×
   reference. Stricter or looser?
2. **Private-network `http` base URLs** (an in-cluster vLLM at `http://vllm.ml.svc:8000`): refused by default.
   Allow with an explicit `PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on`, or require TLS always off loopback?
3. **Per-provider prompt variants**: none in this wave (one prompt, comparable evals). Allow a variant for a model
   that needs one to pass, at the cost of a second prompt to maintain and re-gate?
4. **Fail-safe pricing for Claude too**: refuse start under a cost cap when any role's Claude model is unpriced
   (consistent; may break a deployment on upgrade), or keep today's warning for Claude?
5. **`PIPEROOM_POSTMORTEM_MODEL`** is a mode (`auto | off`), which clashes with the `*_MODEL` naming of the new
   roles. Keep it, or rename it to `PIPEROOM_POSTMORTEM` with the old name read as an alias for one release?
6. **Template control tokens**: neutralise them in untrusted text for every provider (one rule, a small change to
   Claude requests that hold such text), or only for `ollama`, `vllm` and `generic`?
7. **Providers with data processed outside the operator's region** (DeepSeek's API, the DashScope China endpoint):
   supported like the others with the residency note, or listed but behind an explicit
   `PIPEROOM_MODEL_ALLOW_REGION=...` acknowledgement?
8. **OpenAI Responses API**: chat completions loses OpenAI reasoning items between turns. Add a Responses adapter
   later if OpenAI models miss the quality floor, or not at all?
9. **Who pays for gating runs**: each model's 3-run gate costs about 75 episodes on that provider. Which providers
   get a paid gate run first (proposal: OpenAI, Gemini, then DeepSeek and Qwen; Ollama is free)?

## 16. References

- Code: `internal/agent/{model,endpoint,run,toolparams,triage,triage_pass,handoff,handoff_judge_claude,usage,budget,spend}.go`,
  `internal/room/service.go` (`isUnretryable`), `evals/{infra,judge_claude,modeltap}.go`,
  `evals/cmd/evalrun/main.go`, `internal/config/config.go`, `internal/wiring/model.go`,
  `.github/workflows/evals-live.yml`, `evals/baseline/default.json`.
- Designs: `docs/design/cost.md` (prices, caps), `docs/design/setup-agent.md` (standalone, the runner's secret
  store), `docs/design/tenancy.md`.
- Process: `docs/process/ship-workflow.md`, `docs/process/ownership.json`, `tasks/V-board.md`.
