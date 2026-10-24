package evals

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
	"piperoom/internal/wiring"
)

// PromptTap is what a run's provenance reads from a model tap: per room, the
// models called and the prompt hashes seen, and every prompt hash of the
// evaluation. *ModelTap (the Anthropic HTTP tap) and *NeutralTap (the
// in-process tap of the other adapters) implement it.
type PromptTap interface {
	Room(roomID string) TapRoom
	PromptHashes() []string
}

var (
	_ PromptTap = (*ModelTap)(nil)
	_ PromptTap = (*NeutralTap)(nil)
)

// ModelWrap wraps one role's model client before the agent uses it. role is
// the agent's role name (main, first_look, summary, judge); a wrap must
// return a model that behaves exactly like m.
type ModelWrap func(role string, m llm.Model) llm.Model

// NeutralTap is the in-process model tap of the providers whose requests the
// Anthropic HTTP tap cannot see (openai, gemini, bedrock-converse; V11,
// docs/design/model-providers.md section 12). It wraps each role's
// llm.Model, passes every call through unchanged, and reads only the neutral
// request's model, system prompt, tool definitions and the room line of the
// first user message. Per room it records the models the agent called (the
// triage model too) and NeutralPromptHash of the system prompt and tools, as
// the agent handed them to the adapter.
//
// Its hashes carry the "neutral-sha256:" prefix, so they never compare with
// the Anthropic tap's "sha256:" hashes of the provider's wire body (the
// adapters encode the same neutral request differently). It holds no key and
// never reads a response.
type NeutralTap struct {
	mu     sync.Mutex
	rooms  map[string]*tapRoom
	hashes []string // every prompt hash of an attributed request, in first-seen order
}

// NewNeutralTap returns an empty tap.
func NewNeutralTap() *NeutralTap {
	return &NeutralTap{rooms: map[string]*tapRoom{}}
}

// Wrap returns m with every Turn observed by the tap. It has the ModelWrap
// signature; the role is not recorded (the room line attributes a request).
func (t *NeutralTap) Wrap(_ string, m llm.Model) llm.Model {
	if m == nil {
		return nil
	}
	return tappedModel{next: m, tap: t}
}

// tappedModel is one role's model behind the tap.
type tappedModel struct {
	next llm.Model
	tap  *NeutralTap
}

// Turn records req, then sends it unchanged. A request is recorded even when
// the call then fails, as the HTTP tap records a request the API refused.
func (m tappedModel) Turn(ctx context.Context, req llm.Request) (llm.Response, error) {
	m.tap.observe(req)
	return m.next.Turn(ctx, req)
}

func (m tappedModel) Capabilities(model string) llm.Capabilities { return m.next.Capabilities(model) }

func (m tappedModel) Provider() string { return m.next.Provider() }

func (t *NeutralTap) observe(req llm.Request) {
	if len(req.Messages) == 0 {
		return
	}
	roomID := firstRoomIDNeutral(req.Messages[0])
	if roomID == "" {
		return // not an investigation request (a hand-off, the judge)
	}
	hash := NeutralPromptHash(req.System, req.Tools)
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.rooms[roomID]
	if r == nil {
		r = &tapRoom{}
		t.rooms[roomID] = r
	}
	r.requests++
	if req.Model != "" && !slices.Contains(r.models, req.Model) {
		r.models = append(r.models, req.Model)
	}
	if !slices.Contains(r.hashes, hash) {
		r.hashes = append(r.hashes, hash)
	}
	if !slices.Contains(t.hashes, hash) {
		t.hashes = append(t.hashes, hash)
	}
}

// firstRoomIDNeutral reads the room id from the first text part of the
// first message, where the agent writes its "Room: <id>" line
// (agent.firstUserMessage), as firstRoomID does for the Anthropic body.
func firstRoomIDNeutral(msg llm.Message) string {
	if msg.Role != llm.RoleUser {
		return ""
	}
	for _, p := range msg.Parts {
		if p.Kind != llm.PartText {
			continue
		}
		if m := roomLine.FindStringSubmatch(p.Text); m != nil {
			return m[1]
		}
		return ""
	}
	return ""
}

// Room returns what the tap saw for one room.
func (t *NeutralTap) Room(roomID string) TapRoom {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.rooms[roomID]
	if r == nil {
		return TapRoom{}
	}
	return TapRoom{Models: slices.Clone(r.models), PromptHashes: slices.Clone(r.hashes), Requests: r.requests}
}

// PromptHashes are every prompt hash the tap saw in investigation requests.
func (t *NeutralTap) PromptHashes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.hashes)
}

// neutralTool is the canonical form of one llm.ToolDef in the hash.
type neutralTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Schema      map[string]any `json:"schema"`
	Strict      bool           `json:"strict"`
}

// NeutralPromptHash is the prompt hash of a provider-neutral request (design
// section 12): sha256 over the canonical JSON of the system prompt (a JSON
// string), a zero byte, the canonical JSON of the tool definitions (an array
// of {name, description, schema, strict} in the request's order; [] when
// there are none) and a zero byte, as "neutral-sha256:" and the first 16 hex
// digits, the length of PromptHash's. Canonical JSON is encoding/json's
// output: no whitespace, object keys of the schemas sorted, struct fields in
// the order above. The tool order is kept because the agent sends tools in a
// stable order and a reorder changes what the model is told. Any change to
// the prompt text, a tool description or schema, or the actions in
// propose_fix changes it; the model id, the history and the other request
// fields do not.
func NeutralPromptHash(system string, tools []llm.ToolDef) string {
	defs := make([]neutralTool, 0, len(tools))
	for _, d := range tools {
		defs = append(defs, neutralTool{Name: d.Name, Description: d.Description, Schema: d.Schema, Strict: d.Strict})
	}
	h := sha256.New()
	for _, v := range []any{system, defs} {
		b, err := json.Marshal(v)
		if err != nil {
			// A schema JSON cannot encode (a NaN, a channel) is never one
			// the agent sends; hash its printed form so the tap still
			// tells two prompts apart instead of failing the run.
			b = []byte(fmt.Sprintf("%#v", v))
		}
		h.Write(b)
		h.Write([]byte{0})
	}
	return PromptKindNeutral + ":" + hex.EncodeToString(h.Sum(nil))[:16]
}

// NeutralTapProvider reports whether provider's runs are tapped in process
// (NeutralTap) rather than by the Anthropic HTTP tap: the adapters of the V
// wave. The Claude providers keep what they had: anthropic through the HTTP
// tap, bedrock and vertex untapped, so a Claude report's hash kind never
// changes.
func NeutralTapProvider(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case config.ModelProviderOpenAI, config.ModelProviderGemini, config.ModelProviderBedrockConverse:
		return true
	}
	return false
}

// NewTappedEndpointRunner builds the agent runner of NewEndpointRunner with
// every role's model wrapped by wrap (wiring.WithModelWrap, the A4 hook), and
// reports that the wrap reached the role models. A nil wrap builds the plain
// Endpoint and reports false, so the caller does not hand Evaluate a tap that
// saw no request.
func NewTappedEndpointRunner(ctx context.Context, cfg config.Config, log *slog.Logger, wrap ModelWrap) (agent.Runner, bool, error) {
	if wrap == nil {
		run, err := NewEndpointRunner(ctx, cfg, log)
		return run, false, err
	}
	ep, err := wiring.NewAgentEndpointWith(ctx, cfg, log, nil, wiring.WithModelWrap(wrap))
	if err != nil {
		return nil, false, fmt.Errorf("evals: %w", err)
	}
	return ep.Run, true, nil
}
