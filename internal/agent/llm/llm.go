// Package llm is the provider-neutral model interface of the agent. Adapters
// (internal/agent/llm/<adapter>) translate it to one provider's wire format;
// nothing above them sees a provider type. Design:
// docs/design/model-providers.md section 4.
//
// CONTRACT FILE — owned by the leads. Do not edit; request changes in your PR.
// Everything else under internal/agent/llm/** is owned by agent A4.
package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

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
	// Turn sends req and returns the whole assistant turn. A failed request
	// returns an *APIError.
	Turn(ctx context.Context, req Request) (Response, error)
	// Capabilities reports what the provider supports for model; it never
	// makes a network call.
	Capabilities(model string) Capabilities
	// Provider is one of the Provider* values.
	Provider() string
}

// Request is one model call.
type Request struct {
	// Model is the model id as the provider names it (a deployment name on Azure).
	Model string
	// System is the system prompt; adapters send it as a system or developer
	// message, never folded into a user turn. It never holds untrusted text.
	System string
	// Tools are the tools the model may call, in a stable order: the prompt
	// cache depends on it.
	Tools []ToolDef
	// Messages is the history, oldest first.
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

// ToolChoice says whether the model may, must or must not call a tool.
type ToolChoice string

// ToolChoice values.
const (
	ToolChoiceAuto     ToolChoice = ""         // the default: the model decides
	ToolChoiceNone     ToolChoice = "none"     // no tool call
	ToolChoiceRequired ToolChoice = "required" // at least one tool call
)

// CacheHints marks the prefixes a provider with explicit caching may cache.
type CacheHints struct {
	System  bool // cache through the system prompt
	Tools   bool // cache through the tool definitions
	History bool // cache through the end of Messages
}

// Compaction asks for server-side compaction with these instructions.
type Compaction struct {
	// Instructions is the compaction prompt (agent compactionInstructions).
	Instructions string
}

// ToolDef is one tool as the model sees it. Name is the Piperoom tool name;
// an adapter that must rename it on the wire maps it back (design 7.1).
type ToolDef struct {
	Name        string
	Description string
	// Schema is the tool's JSON Schema input, the injected "why" included.
	Schema map[string]any
	// Strict asks the provider to enforce Schema. The agent validates every
	// input in code either way.
	Strict bool
}

// Role is the author of a Message.
type Role string

// Role values.
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

// PartKind says which field of a Part is set.
type PartKind string

// PartKind values.
const (
	PartText       PartKind = "text"        // Part.Text
	PartToolCall   PartKind = "tool_call"   // Part.ToolCall
	PartToolResult PartKind = "tool_result" // Part.ToolResult
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

// Native is a provider's own encoding of one Part.
type Native struct {
	Provider string          // Provider* of the adapter that made it
	Model    string          // the model that made it (signatures are model-bound)
	Raw      json.RawMessage // the provider's block, as received
}

// ToolCall is one tool call the model made.
type ToolCall struct {
	ID    string          // the provider's id, or one the adapter made
	Name  string          // the Piperoom tool name (mapped back)
	Input json.RawMessage // the model's arguments, unparsed; the agent validates
}

// ToolResult answers one ToolCall.
type ToolResult struct {
	CallID  string // the ToolCall.ID it answers
	Name    string // the tool name; needed by Gemini's functionResponse
	Content string // already wrapped by the agent (wrapToolOutput); adapters never alter it
	IsError bool
}

// StopReason is why the model ended its turn, normalised across providers.
type StopReason string

// StopReason values.
const (
	StopEndTurn         StopReason = "end_turn"         // includes stop_sequence
	StopToolUse         StopReason = "tool_use"         // any turn whose Parts hold a tool call
	StopMaxTokens       StopReason = "max_tokens"       // the output hit MaxTokens
	StopPause           StopReason = "pause"            // Anthropic pause_turn
	StopRefusal         StopReason = "refusal"          // refusal, safety or content filter, prompt or output
	StopContextExceeded StopReason = "context_exceeded" // the history no longer fits the context window
	StopOther           StopReason = "other"            // continue like tool_use; logged with ProviderStop
)

// Response is one complete assistant turn.
type Response struct {
	Message      Message    // Role is RoleAssistant
	Stop         StopReason // normalised stop reason
	StopDetail   string     // refusal category and explanation, or the filter reason; never logged in full
	ProviderStop string     // the raw stop reason, for the debug log
	Usage        Usage
	Model        string // the model id the provider says served the turn
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
	// estimated it from the request and response sizes (design 8.3).
	Estimated bool
}

// Capabilities is what a provider supports for one model. Zero values mean
// "not supported"; the agent degrades by design section 6.
type Capabilities struct {
	ToolCalls         bool
	ParallelToolCalls bool
	StrictTools       bool
	MaxStrictTools    int // 0 = no cap
	MaxStrictBytes    int // summed schema bytes, 0 = no cap
	StructuredOutput  bool
	PromptCache       bool // any cache that lowers price, automatic or explicit
	Reasoning         bool
	Effort            bool
	Compaction        bool  // server-side
	ContextTokens     int64 // 0 = unknown: the agent assumes 128000
	MaxTools          int   // 0 = no cap
	ToolNameMax       int   // longest tool name the provider accepts
}

// ErrorKind classifies a provider error so the room, the triage switch and
// the evals decide without reading provider bodies.
type ErrorKind string

// ErrorKind values.
const (
	ErrAuth          ErrorKind = "auth"            // 401, 403: bad, revoked or unauthorised key
	ErrBilling       ErrorKind = "billing"         // no credit, quota exhausted (OpenAI insufficient_quota, Anthropic "credit balance")
	ErrModelNotFound ErrorKind = "model_not_found" // the provider does not know the model id
	ErrBadRequest    ErrorKind = "bad_request"     // request shape: a model that rejects a parameter
	ErrTooLong       ErrorKind = "too_long"        // 413, or a 400 about the context length
	ErrRateLimited   ErrorKind = "rate_limited"    // 429 after the adapter's retries
	ErrOverloaded    ErrorKind = "overloaded"      // 5xx, 529 after retries
	ErrNetwork       ErrorKind = "network"         // no HTTP answer
	ErrTruncated     ErrorKind = "truncated"       // the server cut the prompt to fit its context (design 6.2)
)

// APIError is every adapter's error for a failed request. Err keeps the
// SDK's own error (for example *anthropic.Error), so errors.As on it still
// works during the switch-over.
type APIError struct {
	Provider string    // Provider* of the adapter
	Status   int       // HTTP status; 0 when there was no HTTP answer
	Kind     ErrorKind // the classification callers decide on
	// Body is the provider's error body, redacted (internal/redact) and cut
	// to 2 KiB; it never holds the key. It can echo prompt content, so it is
	// logged at debug level only and never put in an event or a room status.
	Body string
	Err  error // the underlying error, if any
}

// Error names the provider, the kind and the status; never the body, which
// can echo prompt content (design 10.2).
func (e *APIError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("llm: %s: %s (status %d)", e.Provider, e.Kind, e.Status)
	}
	return fmt.Sprintf("llm: %s: %s", e.Provider, e.Kind)
}

// Unwrap returns the underlying error.
func (e *APIError) Unwrap() error { return e.Err }

// Unretryable reports an error a fresh run repeats: auth, billing,
// model_not_found, bad_request and truncated. too_long, rate_limited,
// overloaded and network are not, and neither is an error that is not an
// *APIError.
func Unretryable(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.Kind {
	case ErrAuth, ErrBilling, ErrModelNotFound, ErrBadRequest, ErrTruncated:
		return true
	}
	return false
}
