package converse

import (
	"strings"

	"piperoom/internal/agent/llm"
)

// toolNameMax is the longest tool name Converse accepts
// (^[a-zA-Z0-9_-]{1,64}$, design 7.1).
const toolNameMax = 64

// family is what the adapter knows about a group of Bedrock models. Design 3
// marks most Converse features "per model"; the table holds only what AWS
// documents for the family, and everything else stays off: the agent
// degrades by design 6 (no cache costs money, no parallel calls costs turns),
// and the V6 start probe and the V10 gate decide whether a model runs at all.
type family struct {
	// cacheSystem and cacheMessages: the model takes cachePoint blocks in the
	// system prompt and in messages (Bedrock prompt caching).
	cacheSystem, cacheMessages bool
	// cacheTools: the model takes a cachePoint after the tool definitions.
	cacheTools bool
	// toolStatus: the model takes toolResult.status. AWS documents the field
	// for Amazon Nova (and Claude, refused here); elsewhere the agent's
	// wrapped text already starts "error:" (design 7.3, as OpenAI).
	toolStatus bool
	// contextTokens is the documented context window; 0 = unknown (the agent
	// assumes 128000; PIPEROOM_MODEL_CONTEXT_TOKENS overrides, V6).
	contextTokens int64
}

// families are matched by the longest prefix of the model id with any
// region prefix and ARN removed (baseModel).
var families = []struct {
	prefix string
	f      family
}{
	{"amazon.nova-premier", family{cacheSystem: true, cacheMessages: true, toolStatus: true, contextTokens: 1_000_000}},
	{"amazon.nova-pro", family{cacheSystem: true, cacheMessages: true, toolStatus: true, contextTokens: 300_000}},
	{"amazon.nova-lite", family{cacheSystem: true, cacheMessages: true, toolStatus: true, contextTokens: 300_000}},
	{"amazon.nova-micro", family{cacheSystem: true, cacheMessages: true, toolStatus: true, contextTokens: 128_000}},
	{"amazon.nova", family{cacheSystem: true, cacheMessages: true, toolStatus: true}},
}

// familyOf returns the family of model, or the zero family (nothing beyond
// tool calls) for a model the table does not know.
func familyOf(model string) family {
	base := baseModel(model)
	best, found := -1, family{}
	for _, e := range families {
		if strings.HasPrefix(base, e.prefix) && len(e.prefix) > best {
			best, found = len(e.prefix), e.f
		}
	}
	return found
}

// caps is the family's llm.Capabilities.
func (f family) caps() llm.Capabilities {
	return llm.Capabilities{
		ToolCalls:     true,
		PromptCache:   f.cacheSystem || f.cacheMessages || f.cacheTools,
		ContextTokens: f.contextTokens,
		ToolNameMax:   toolNameMax,
	}
}

// regionPrefixes are the cross-region inference profile prefixes
// ("us.amazon.nova-pro-v1:0").
var regionPrefixes = []string{"us-gov.", "us.", "eu.", "apac.", "jp.", "au.", "ca.", "global."}

// baseModel is the foundation model id inside id: the last path segment of an
// ARN, without a cross-region prefix, lower-cased.
func baseModel(id string) string {
	id = strings.ToLower(strings.TrimSpace(id))
	if strings.HasPrefix(id, "arn:") {
		if i := strings.LastIndexByte(id, '/'); i >= 0 {
			id = id[i+1:]
		}
	}
	for _, p := range regionPrefixes {
		if strings.HasPrefix(id, p) {
			return id[len(p):]
		}
	}
	return id
}

// isClaude reports a Claude model id in any of its Bedrock spellings.
func isClaude(id string) bool {
	base := baseModel(id)
	return strings.HasPrefix(base, "anthropic.") || strings.Contains(base, "claude")
}
