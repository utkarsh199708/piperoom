package openai

import "strings"

// Flavors this adapter serves (PIPEROOM_MODEL_FLAVOR).
const (
	FlavorOpenAI     = "openai"
	FlavorAzure      = "azure"
	FlavorDeepSeek   = "deepseek"
	FlavorQwen       = "qwen"
	FlavorMistral    = "mistral"
	FlavorGroq       = "groq"
	FlavorTogether   = "together"
	FlavorOpenRouter = "openrouter"
	FlavorXAI        = "xai"
	FlavorOllama     = "ollama"
	FlavorVLLM       = "vllm"
	FlavorGeneric    = "generic"
)

// flavor is one flavor's quirks. The default base URLs live in
// config.FlavorDefaultBaseURL (V0), so config and the adapter cannot
// disagree; flavor_test.go checks both against design section 3.
type flavor struct {
	name string
	// keyEnv is the standard key variable; "" when the flavor has none.
	keyEnv string
	// needsKey refuses New without a key.
	needsKey bool
	// azureAuth sends the key as api-key instead of a bearer token.
	azureAuth bool
	// hosted knows its models (models, contexts). The local and generic
	// flavors learn what they support only through PIPEROOM_MODEL_CAPS (and
	// never strict tools: a cap cannot lower a check).
	hosted bool
	// models is a hosted flavor's capabilities for one model id.
	models func(model string) modelCaps
	// contexts is the context window by lowercase model id prefix, most
	// specific first; a model it does not name reports 0 (unknown).
	contexts []contextEntry
	// completionTokens sends max_completion_tokens; every other flavor
	// sends max_tokens, which the V3b hosts document.
	completionTokens bool
	// effort maps the agent's effort for model to reasoning_effort when
	// models(model).effort; nil is reasoningEffort.
	effort func(model, effort string) string
}

// modelCaps is what a hosted flavor supports for one model.
type modelCaps struct {
	parallel    bool // several tool calls in one turn
	strictTools bool // strict: true on a tool whose schema is in the strict subset
	structured  bool // response_format json_schema
	// strictOutput sets strict on the response_format schema when it is
	// in the strict subset.
	strictOutput bool
	cache        bool // an automatic prompt cache that lowers the price
	reasoning    bool // the model reasons before it answers
	effort       bool // the model takes reasoning_effort
}

type contextEntry struct {
	prefix string
	tokens int64
}

var flavors = map[string]flavor{
	FlavorOpenAI: {name: FlavorOpenAI, keyEnv: "OPENAI_API_KEY", needsKey: true, hosted: true,
		models: openAIModel, contexts: openAIContexts, completionTokens: true},
	FlavorAzure: {name: FlavorAzure, keyEnv: "AZURE_OPENAI_API_KEY", needsKey: true, azureAuth: true, hosted: true,
		models: openAIModel, contexts: openAIContexts, completionTokens: true},
	FlavorDeepSeek: {name: FlavorDeepSeek, keyEnv: "DEEPSEEK_API_KEY", needsKey: true, hosted: true,
		models: deepSeekModel, contexts: []contextEntry{{"deepseek-", 128_000}}},
	FlavorQwen: {name: FlavorQwen, keyEnv: "DASHSCOPE_API_KEY", needsKey: true, hosted: true,
		models: qwenModel, contexts: []contextEntry{{"qwen3-max", 262_144}, {"qwen-plus", 131_072}}},
	FlavorMistral: {name: FlavorMistral, keyEnv: "MISTRAL_API_KEY", needsKey: true, hosted: true,
		models: mistralModel, contexts: []contextEntry{
			{"codestral", 256_000}, {"mistral-large", 131_072}, {"mistral-medium", 131_072}, {"mistral-small", 131_072},
		}},
	FlavorGroq: {name: FlavorGroq, keyEnv: "GROQ_API_KEY", needsKey: true, hosted: true,
		models: groqModel, completionTokens: true, contexts: []contextEntry{
			{"openai/gpt-oss", 131_072}, {"llama-3.3-70b", 131_072}, {"qwen/qwen3-32b", 131_072},
		}},
	FlavorTogether: {name: FlavorTogether, keyEnv: "TOGETHER_API_KEY", needsKey: true, hosted: true,
		models: plainHostedModel},
	FlavorOpenRouter: {name: FlavorOpenRouter, keyEnv: "OPENROUTER_API_KEY", needsKey: true, hosted: true,
		models: plainHostedModel},
	FlavorXAI: {name: FlavorXAI, keyEnv: "XAI_API_KEY", needsKey: true, hosted: true,
		models: xaiModel, effort: xaiEffort, contexts: []contextEntry{
			{"grok-4-fast", 2_000_000}, {"grok-4", 256_000}, {"grok-code-fast", 256_000}, {"grok-3", 131_072},
		}},
	FlavorOllama:  {name: FlavorOllama},
	FlavorVLLM:    {name: FlavorVLLM, keyEnv: "PIPEROOM_MODEL_API_KEY"},
	FlavorGeneric: {name: FlavorGeneric, keyEnv: "PIPEROOM_MODEL_API_KEY"},
}

// Flavors returns the flavors New accepts, sorted.
func Flavors() []string {
	return []string{
		FlavorAzure, FlavorDeepSeek, FlavorGeneric, FlavorGroq, FlavorMistral, FlavorOllama,
		FlavorOpenAI, FlavorOpenRouter, FlavorQwen, FlavorTogether, FlavorVLLM, FlavorXAI,
	}
}

// KeyEnv is flavor's standard key variable, "" when it has none (ollama) or
// the flavor is unknown. PIPEROOM_MODEL_API_KEY, when set, is the main
// role's key on any flavor (LoadKey).
func KeyEnv(flavorName string) string { return flavors[flavorName].keyEnv }

// openAIContexts is the context window of OpenAI model families by id
// prefix, as OpenAI documents them in 2026-10. On Azure the model is a
// deployment name, which often but not always repeats the model id.
var openAIContexts = []contextEntry{
	{"gpt-4.1", 1_047_576},
	{"gpt-4o", 128_000},
	{"gpt-5", 400_000},
	{"o4", 200_000},
	{"o3", 200_000},
	{"o1", 200_000},
}

// openAIModel is OpenAI and Azure: strict tools and json_schema on every
// current model, automatic prefix caching, reasoning_effort on the
// reasoning models.
func openAIModel(model string) modelCaps {
	r := reasoningModel(model)
	return modelCaps{parallel: true, strictTools: true, structured: true, strictOutput: true, cache: true, reasoning: r, effort: r}
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

// deepSeekModel is DeepSeek's API: reasoning by model id (deepseek-reasoner
// thinks, deepseek-chat does not; no reasoning_effort), the automatic disk
// cache reported as prompt_cache_hit_tokens, no json_schema (json_object
// only) and strict tools only on its beta endpoint, so neither is claimed.
func deepSeekModel(model string) modelCaps {
	return modelCaps{parallel: true, cache: true, reasoning: deepSeekReasoner(model)}
}

// deepSeekReasoner reports a DeepSeek thinking model: deepseek-reasoner and
// the R1 ids some deployments still route.
func deepSeekReasoner(model string) bool {
	id := strings.ToLower(model)
	return strings.Contains(id, "reasoner") || strings.HasPrefix(id, "deepseek-r1")
}

// qwenModel is Qwen on DashScope's compatible mode. Thinking is a per
// request flag (enable_thinking) that DashScope accepts only on streamed
// calls; this adapter does not stream (design 3.1), so it turns thinking off
// on the hybrid models and refuses the thinking-only ones (qwenThinkingOnly).
// The implicit context cache is reported as cached_tokens. Parallel tool
// calls are off unless the request asks (parallel_tool_calls), which the
// adapter does.
func qwenModel(string) modelCaps {
	return modelCaps{parallel: true, cache: true}
}

// qwenThinkingOnly reports a Qwen model that only answers in thinking mode
// (QwQ, the -thinking releases): DashScope serves it only streamed.
func qwenThinkingOnly(model string) bool {
	id := strings.ToLower(model)
	return strings.HasPrefix(id, "qwq") || strings.Contains(id, "-thinking")
}

// mistralModel is Mistral's API: json_schema structured output, no strict
// tools claimed, no reported cache; Magistral reasons (its thinking comes
// back as content chunks of type thinking, which the decoder drops).
func mistralModel(model string) modelCaps {
	return modelCaps{parallel: true, structured: true, reasoning: strings.HasPrefix(strings.ToLower(model), "magistral")}
}

// groqModel is Groq: reasoning_effort only on the gpt-oss models, json_schema
// on them too; qwen3-32b reasons and gets reasoning_format hidden (its
// default would put <think> in the content); prompt caching on gpt-oss,
// reported as cached_tokens.
func groqModel(model string) modelCaps {
	oss := groqGPTOSS(model)
	return modelCaps{parallel: true, structured: oss, cache: oss, reasoning: oss || groqQwen3(model), effort: oss}
}

func groqGPTOSS(model string) bool { return strings.Contains(strings.ToLower(model), "gpt-oss") }

func groqQwen3(model string) bool { return strings.HasPrefix(strings.ToLower(model), "qwen/qwen3") }

// plainHostedModel is Together and OpenRouter: many models behind one API,
// so nothing per model is claimed beyond tool calls in parallel; usage and
// cached tokens are read when the host reports them.
func plainHostedModel(string) modelCaps { return modelCaps{parallel: true} }

// xaiModel is xAI: grok-4 and grok-3-mini reason (the -non-reasoning ids do
// not); only grok-3-mini takes reasoning_effort (grok-4 rejects it);
// json_schema on current models; automatic caching reported as
// cached_tokens.
func xaiModel(model string) modelCaps {
	id := strings.ToLower(model)
	reasoning := !strings.Contains(id, "non-reasoning") &&
		(strings.HasPrefix(id, "grok-4") || strings.HasPrefix(id, "grok-3-mini") || strings.HasPrefix(id, "grok-code-fast"))
	return modelCaps{parallel: true, structured: true, cache: true, reasoning: reasoning,
		effort: strings.HasPrefix(id, "grok-3-mini")}
}

// xaiEffort maps the agent's effort to grok-3-mini's two values.
func xaiEffort(_ string, e string) string {
	switch e {
	case "low":
		return "low"
	case "medium", "high", "xhigh", "max":
		return "high"
	}
	return ""
}
