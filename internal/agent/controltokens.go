package agent

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
)

// Chat-template control tokens (design 10.1, decision 6). Local and
// open-weight models are served through chat templates whose special tokens
// are plain strings in the prompt; a server that does not escape them lets
// untrusted text holding one close the user turn and open a system or
// assistant turn of its own. The agent rewrites every such token in the text
// it sends as (ctl:NAME): still readable as evidence, never a token.
//
// Three families cover the templates in use:
//   - pipe tokens <|name|>, with the ASCII bar or DeepSeek's fullwidth bar
//     (ChatML <|im_start|>, Llama 3 <|eot_id|>, Phi, Granite, GPT-OSS,
//     Command-R, DeepSeek <｜User｜>, <|tool_call|>, reserved tokens);
//   - named tags: <s>, </s>, <bos>, <eos>, Gemma's <start_of_turn>, Qwen and
//     Hermes <tool_call>/<tool_response>, reasoning <think>, Llama 2 <<SYS>>;
//   - Mistral's bracket tokens: [INST], [/INST], [SYSTEM_PROMPT],
//     [TOOL_CALLS], [TOOL_RESULTS], [AVAILABLE_TOOLS] and the like.
//
// Angle tokens are matched raw and in the form escapeToolText leaves them
// (&lt;|im_start|&gt;): the escape already keeps them from tokenising, and
// rewriting both forms gives the model one spelling whichever path the text
// took. Matching is case-sensitive, as tokenizers are, so ordinary log text
// such as "[info]" is left alone.
var (
	pipeToken    = regexp.MustCompile(`(?:<|&lt;)[|｜]([A-Za-z0-9_▁.\-]{1,64})[|｜](?:>|&gt;)`)
	namedToken   = regexp.MustCompile(`(?:<|&lt;)(/?)(s|bos|eos|unk|pad|start_of_turn|end_of_turn|start_of_image|end_of_image|tool_call|tool_response|think)(?:>|&gt;)`)
	sysToken     = regexp.MustCompile(`(?:<|&lt;)(?:<|&lt;)(/?)SYS(?:>|&gt;)(?:>|&gt;)`)
	bracketToken = regexp.MustCompile(`\[(/?)(INST|SYSTEM_PROMPT|TOOL_CALLS|TOOL_RESULTS|TOOL_CONTENT|AVAILABLE_TOOLS|THINK|ARGS|CALL_ID|IMG|IMG_BREAK|IMG_END|PREFIX|MIDDLE|SUFFIX)\]`)
)

// neutraliseControlTokens rewrites the chat-template control tokens in s as
// (ctl:NAME). Text without one comes back unchanged, byte for byte, so a
// request that holds none is the request of before V2a. The replacement holds
// no <, [ or &, so it can neither re-form a token nor break a wrapper.
func neutraliseControlTokens(s string) string {
	if !strings.ContainsAny(s, "<&[") {
		return s
	}
	s = sysToken.ReplaceAllString(s, "(ctl:${1}SYS)")
	s = pipeToken.ReplaceAllString(s, "(ctl:$1)")
	s = namedToken.ReplaceAllString(s, "(ctl:$1$2)")
	return bracketToken.ReplaceAllString(s, "(ctl:$1$2)")
}

// isClaudeProvider reports the providers that serve Claude (adapter 1). Their
// API takes structured messages, not a chat template, so a control token there
// is ordinary text; PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS=non-claude exempts
// them.
func isClaudeProvider(p string) bool {
	switch p {
	case llm.ProviderAnthropic, llm.ProviderBedrock, llm.ProviderVertex:
		return true
	}
	return false
}

// parseNeutraliseMode checks a PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS value
// with config.ParseModelKnobs's rules: all (the default when empty) or
// non-claude; there is no off.
func parseNeutraliseMode(v string) (string, error) {
	switch m := strings.ToLower(strings.TrimSpace(v)); m {
	case "", config.NeutraliseAll:
		return config.NeutraliseAll, nil
	case config.NeutraliseNonClaude:
		return config.NeutraliseNonClaude, nil
	default:
		return "", fmt.Errorf("agent: PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS=%q: want all or non-claude (there is no off)", v)
	}
}

// withNeutraliser returns m with control tokens neutralised in every request,
// unless mode exempts m's provider. mode is a parseNeutraliseMode result; an
// unknown mode neutralises, the safe side.
func withNeutraliser(m llm.Model, mode string) llm.Model {
	if m == nil || (mode == config.NeutraliseNonClaude && isClaudeProvider(m.Provider())) {
		return m
	}
	return neutralisingModel{inner: m}
}

// neutralisingModel rewrites the text a request carries from outside the
// agent before the adapter sees it: every text part of a user turn (alert
// text, past incidents, steers, suggestions, comments, action results, the
// room's events in a hand-off) and every tool result. The adapter then sends
// those strings unaltered (design 10.1). It leaves alone what the agent
// itself wrote for the model (System, the tool definitions, the output
// schema) and the assistant turns, which go back exactly as the provider
// returned them (llm.Message). User text is rewritten whole, not only inside
// the untrusted wrappers: a token breaks a template wherever it sits, and the
// agent's own framing holds none.
type neutralisingModel struct {
	inner llm.Model
}

var _ llm.Model = neutralisingModel{}

// Turn sends req with its user text and tool results neutralised. The
// caller's history is never mutated: a message that changes is copied.
func (n neutralisingModel) Turn(ctx context.Context, req llm.Request) (llm.Response, error) {
	req.Messages = neutraliseMessages(req.Messages)
	return n.inner.Turn(ctx, req)
}

// Capabilities is the inner model's.
func (n neutralisingModel) Capabilities(model string) llm.Capabilities {
	return n.inner.Capabilities(model)
}

// Provider is the inner model's.
func (n neutralisingModel) Provider() string { return n.inner.Provider() }

// neutraliseMessages returns msgs with the user text and tool results
// neutralised. It returns msgs itself when nothing changes, and copies only
// the messages and parts that do, so the caller's slices stay as they were.
func neutraliseMessages(msgs []llm.Message) []llm.Message {
	var out []llm.Message
	for i, msg := range msgs {
		if msg.Role != llm.RoleUser {
			continue
		}
		parts, changed := neutraliseParts(msg.Parts)
		if !changed {
			continue
		}
		if out == nil {
			out = append([]llm.Message(nil), msgs...)
		}
		out[i] = llm.Message{Role: msg.Role, Parts: parts}
	}
	if out == nil {
		return msgs
	}
	return out
}

// neutraliseParts is neutraliseMessages for one user message. A part with
// Native is provider state that its adapter sends verbatim, so it is left as
// it is.
func neutraliseParts(parts []llm.Part) ([]llm.Part, bool) {
	var out []llm.Part
	set := func(i int, p llm.Part) {
		if out == nil {
			out = append([]llm.Part(nil), parts...)
		}
		out[i] = p
	}
	for i, p := range parts {
		if p.Native != nil {
			continue
		}
		switch p.Kind {
		case llm.PartText:
			if t := neutraliseControlTokens(p.Text); t != p.Text {
				p.Text = t
				set(i, p)
			}
		case llm.PartToolResult:
			if p.ToolResult == nil {
				continue
			}
			if c := neutraliseControlTokens(p.ToolResult.Content); c != p.ToolResult.Content {
				tr := *p.ToolResult
				tr.Content = c
				p.ToolResult = &tr
				set(i, p)
			}
		}
	}
	return out, out != nil
}
