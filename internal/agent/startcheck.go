package agent

// The start checks of the new providers (V6; docs/design/model-providers.md
// sections 5, 6.2 and 9): the supported-model list, the hard requirements a
// table can show, the tool limits of each adapter, the context fit, and the
// one-request tool-call probe. None of them runs for a role on anthropic,
// bedrock or vertex: Claude starts exactly as before (design 5).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/gemini"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

const (
	// minContextTokens and assumedContextTokens: hard requirement 4 (design
	// 5.1), and the window assumed when the adapter does not know it
	// (llm.Capabilities.ContextTokens 0).
	minContextTokens     = 128_000
	assumedContextTokens = 128_000
	// fitHeadroom is the room left for the opening message (design 6.2).
	fitHeadroom = 16_000
	// fitBytesPerToken over-counts tokens on purpose (design 6.1 and 6.2).
	fitBytesPerToken = 3
	// probeTimeout bounds the start probe; a local model reading the real
	// system prompt and tools on a CPU is slow, so it is generous.
	probeTimeout = 3 * time.Minute
	// probeMaxTokens leaves a reasoning model room to think before the call.
	probeMaxTokens = 2048
	// probeToolName is the one tool the probe must call.
	probeToolName = "piperoom_start_probe"
)

// StartToolSet is the read tools and write actions a room of one pipeline
// kind is offered: what the context fit and the tool limits are measured on.
type StartToolSet struct {
	Kind    string // cdc or batch
	Tools   *tools.Registry
	Actions *actions.Registry
}

// NeedsStartChecks reports whether any configured role runs on a new
// provider (openai, gemini, bedrock-converse). Without one, CheckStart does
// nothing, and the caller need not build StartToolSets.
func (e *Endpoint) NeedsStartChecks() bool {
	for _, sp := range e.specs {
		if newProvider(sp.role.Provider) {
			return true
		}
	}
	return false
}

// CheckStart runs the start checks of every role on a new provider and logs
// the role lines. Any failure is a start-up error naming the variable to
// change. The probe sends one request per role that is not in supported.json
// (allowed by PIPEROOM_MODEL_UNVERIFIED=on) and per role on an ollama, vllm
// or generic host, listed or not; nothing else calls a model.
func (e *Endpoint) CheckStart(ctx context.Context, sets []StartToolSet) error {
	list, err := llm.SupportedModels()
	if err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	return e.checkStart(ctx, sets, list, time.Now())
}

func (e *Endpoint) checkStart(ctx context.Context, sets []StartToolSet, list []llm.SupportedModel, now time.Time) error {
	if !e.NeedsStartChecks() {
		return nil
	}
	e.gates = map[string]llm.Gate{}
	type probeKey struct {
		m  model
		id string
	}
	type probeJob struct {
		sp     roleSpec
		system string
		defs   []llm.ToolDef
	}
	probed := map[probeKey]bool{}
	var probes []probeJob
	for _, sp := range e.specs {
		if sp.name == roleTriage && newProvider(sp.role.Provider) {
			if err := triageOnMainProvider(sp); err != nil {
				return err
			}
		}
	}
	// Every check that sends nothing runs for every role first, so a
	// configuration error never costs a probe request.
	for _, sp := range e.specs {
		if !newProvider(sp.role.Provider) {
			continue
		}
		g := llm.LookupGate(list, sp.role.Provider, sp.role.Flavor, sp.role.Model, now)
		e.gates[sp.name] = g
		if g.Status == llm.GateUnverified {
			why := "is not in internal/agent/llm/supported.json"
			if g.SunsetPassed {
				why = fmt.Sprintf("is past its sunset date %s in internal/agent/llm/supported.json", g.Entry.Sunset)
			}
			if !e.unverified {
				return fmt.Errorf("agent: %s=%q on %s %s: set PIPEROOM_MODEL_UNVERIFIED=on to run it unverified (the start checks and a tool-call probe still run), or choose a listed model",
					sp.env, sp.role.Model, providerLabel(sp.role), why)
			}
			e.log.Warn("agent: unverified model: it has passed no eval gate; PIPEROOM_MODEL_UNVERIFIED=on", "setting", sp.env, "model", sp.role.Model, "provider", sp.role.Provider, "flavor", sp.role.Flavor)
		}
		if g.SunsetSoon {
			e.log.Warn("agent: model reaches its sunset date soon; after it the model counts as not listed", "setting", sp.env, "model", sp.role.Model, "sunset", g.Entry.Sunset)
		}
		caps := sp.m.Capabilities(sp.role.Model)
		window, err := checkCaps(sp, caps)
		if err != nil {
			return err
		}
		var probeSystem string
		var probeDefs []llm.ToolDef
		if sp.loop {
			for _, set := range sets {
				system, defs, err := startRequest(set, caps)
				if err != nil {
					return err
				}
				if err := checkToolLimits(sp, caps, defs); err != nil {
					return err
				}
				if err := checkFit(sp, set.Kind, system, defs, maxTokens, window); err != nil {
					return err
				}
				if probeSystem == "" {
					probeSystem, probeDefs = system, defs
				}
			}
		} else if err := checkFit(sp, "", handoffPrompt(), nil, handoffMaxTokens, window); err != nil {
			return err
		}
		local := localFlavor(sp.role)
		k := probeKey{sp.m, sp.role.Model}
		if (g.Status == llm.GateUnverified || local) && !probed[k] {
			probed[k] = true
			if !local {
				// A hosted model gets the small probe: one tool, a fixed prompt.
				probeSystem, probeDefs = "", nil
			}
			probes = append(probes, probeJob{sp, probeSystem, probeDefs})
		}
	}
	for _, p := range probes {
		if err := e.probe(ctx, p.sp, p.system, p.defs); err != nil {
			return err
		}
	}
	e.logRoles()
	return nil
}

// triageOnMainProvider refuses an in-loop triage model that names another
// provider's model: the triage turns share the investigation's history, so
// they run on the main provider (design 9).
func triageOnMainProvider(sp roleSpec) error {
	id := strings.ToLower(sp.role.Model)
	other := strings.HasPrefix(id, "claude-") || strings.HasPrefix(id, "anthropic.") || strings.HasPrefix(id, "arn:")
	if p, _, ok := strings.Cut(id, ":"); ok && p != sp.role.Provider && p != sp.role.Flavor && (isClaudeProvider(p) || newProvider(p) && slices.Contains(knownQualifiers, p)) {
		other = true
	}
	if other {
		return fmt.Errorf("agent: PIPEROOM_TRIAGE_MODEL=%q names a model of another provider: the triage turns share the investigation's history and run on PIPEROOM_MODEL_PROVIDER (%s); set a %s model or unset it",
			sp.role.Model, providerLabel(sp.role), sp.role.Provider)
	}
	return nil
}

// knownQualifiers are the provider and flavor names a model id may be
// qualified with.
var knownQualifiers = []string{
	llm.ProviderOpenAI, llm.ProviderGemini, llm.ProviderBedrockConverse,
	"azure", "deepseek", "qwen", "mistral", "groq", "together", "openrouter", "xai", "ollama", "vllm", "generic",
}

// localFlavor reports a host whose tool behaviour depends on its own chat
// template and tool parser: the probe runs whether or not the model is listed.
func localFlavor(r config.ModelRole) bool {
	return r.Provider == llm.ProviderOpenAI && (r.Flavor == openai.FlavorOllama || r.Flavor == openai.FlavorVLLM || r.Flavor == openai.FlavorGeneric)
}

// checkCaps applies the hard requirements a capability table shows (design
// 5.1 items 1 and 4) and returns the context window to fit in.
func checkCaps(sp roleSpec, caps llm.Capabilities) (int64, error) {
	if !caps.ToolCalls {
		return 0, fmt.Errorf("agent: %s=%q on %s cannot call tools; the agent needs tool calling", sp.env, sp.role.Model, providerLabel(sp.role))
	}
	window := caps.ContextTokens
	if window <= 0 {
		window = assumedContextTokens
	}
	if window < minContextTokens {
		return 0, fmt.Errorf("agent: %s=%q on %s has a %d-token context window; the agent needs at least %d (PIPEROOM_MODEL_CONTEXT_TOKENS sets the main model's)",
			sp.env, sp.role.Model, providerLabel(sp.role), window, minContextTokens)
	}
	return window, nil
}

// startRequest is the system prompt and tool list the loop sends in a room of
// set's kind (newRunnerWith and runner.run), with no room state.
func startRequest(set StartToolSet, caps llm.Capabilities) (string, []llm.ToolDef, error) {
	kind, err := RoomKind(set.Kind)
	if err != nil {
		return "", nil, err
	}
	reads, err := ToolsForKind(set.Tools, kind)
	if err != nil {
		return "", nil, err
	}
	acts, err := ActionsForKind(set.Actions, kind)
	if err != nil {
		return "", nil, err
	}
	room := roomToolsForKind(newRoomToolsWithCauses(Deps{Actions: acts}, newProposalTracker(), newStepLedger(), newReadFailures(), newExternalCauses(Alert{}, time.Time{})), kind, acts)
	tl := buildToolListWithCap(reads, room, strictCap(caps))
	return systemPromptForMode(kind, RoomMode{Write: true}), tl.defs, nil
}

// toolNameRe is the name rule every adapter of the V wave shares (design 7.1).
var toolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// checkToolLimits refuses a tool list the role's adapter cannot send: too
// many tools, a name too long or with a character the API refuses, or (on
// Gemini) a schema that does not convert.
func checkToolLimits(sp roleSpec, caps llm.Capabilities, defs []llm.ToolDef) error {
	if caps.MaxTools > 0 && len(defs) > caps.MaxTools {
		return fmt.Errorf("agent: %s=%q on %s takes at most %d tools in a request; the agent sends %d",
			sp.env, sp.role.Model, providerLabel(sp.role), caps.MaxTools, len(defs))
	}
	for _, d := range defs {
		if !toolNameRe.MatchString(d.Name) || (caps.ToolNameMax > 0 && len(d.Name) > caps.ToolNameMax) {
			return fmt.Errorf("agent: %s on %s: tool name %q breaks the provider's rule ^[a-zA-Z0-9_-]{1,%d}$",
				sp.env, providerLabel(sp.role), d.Name, caps.ToolNameMax)
		}
	}
	if sp.role.Provider == llm.ProviderGemini {
		if err := gemini.CheckTools(sp.role.Model, defs); err != nil {
			return fmt.Errorf("agent: %s=%q on gemini: %w", sp.env, sp.role.Model, err)
		}
	}
	return nil
}

// checkFit is the start half of design 6.2: the system prompt, the tool
// definitions (at 3 bytes per token), the output bound and a 16k headroom for
// the opening message must fit the context window.
func checkFit(sp roleSpec, kind, system string, defs []llm.ToolDef, maxOut, window int64) error {
	n := len(system)
	if len(defs) > 0 {
		b, err := json.Marshal(defs)
		if err != nil {
			return fmt.Errorf("agent: %s: measure the tool definitions: %w", sp.env, err)
		}
		n += len(b)
	}
	need := int64((n+fitBytesPerToken-1)/fitBytesPerToken) + maxOut + fitHeadroom
	if need <= window {
		return nil
	}
	room := sp.name
	if kind != "" {
		room += " (" + kind + " room)"
	}
	return fmt.Errorf("agent: %s=%q on %s does not fit the %s role: it needs %d tokens (prompt, tools, output and a %d headroom), the context window is %d; raise the window (PIPEROOM_MODEL_CONTEXT_TOKENS, the server's context length) or choose a larger model",
		sp.env, sp.role.Model, providerLabel(sp.role), room, need, fitHeadroom, window)
}

// probeTool is the one tool the start probe asks for.
func probeTool() llm.ToolDef {
	return llm.ToolDef{
		Name:        probeToolName,
		Description: "Start-up check: call this tool once with status \"ready\".",
		Schema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"status": map[string]any{"type": "string", "enum": []any{"ready"}}},
			"required":             []any{"status"},
			"additionalProperties": false,
		},
	}
}

// probeSystemPrompt is the hosted probe's system prompt; a local host gets
// the real one, so the probe measures a prompt of the real size.
const probeSystemPrompt = "You are the start-up check of a tool-calling agent. Call the tool the user names, with valid arguments, and write nothing else."

// probe sends the one-request tool-call probe (design 5 and 6.2): the call
// must name the probe tool with arguments that validate, and the reported
// prompt must not be below half of the lower bound of what was sent (a
// server that silently truncates the prompt). system and defs are the real
// prompt and tools for a local host, empty for a hosted one.
func (e *Endpoint) probe(ctx context.Context, sp roleSpec, system string, defs []llm.ToolDef) error {
	if system == "" {
		system = probeSystemPrompt
	}
	pt := probeTool()
	toolDefs := append(slices.Clip(defs), pt)
	req := llm.Request{
		Model:      sp.role.Model,
		System:     system,
		Tools:      toolDefs,
		Messages:   []llm.Message{userMessage(textPart(`Start-up check: call the tool ` + probeToolName + ` once with {"status": "ready"}. Call no other tool.`))},
		MaxTokens:  probeMaxTokens,
		Effort:     "low",
		ToolChoice: llm.ToolChoiceRequired,
	}
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	start := time.Now()
	resp, err := sp.m.Turn(pctx, req)
	what := fmt.Sprintf("agent: %s=%q on %s: the start tool-call probe", sp.env, sp.role.Model, providerLabel(sp.role))
	if err != nil {
		var ae *llm.APIError
		if errors.As(err, &ae) && ae.Kind == llm.ErrTruncated {
			return fmt.Errorf("%s: the model server truncated the prompt; raise its context length (Ollama OLLAMA_CONTEXT_LENGTH, vLLM --max-model-len): %w", what, err)
		}
		return fmt.Errorf("%s failed: %w", what, err)
	}
	u := resp.Usage
	e.log.Info("agent: start probe", "role", sp.name, "provider", sp.role.Provider, "flavor", sp.role.Flavor, "model", sp.role.Model,
		"input_tokens", u.Input, "cache_read_input_tokens", u.CacheRead, "output_tokens", u.Output, "estimated", u.Estimated,
		"latency_ms", time.Since(start).Milliseconds())
	if !u.Estimated {
		sent := len(system)
		if b, err := json.Marshal(toolDefs); err == nil {
			sent += len(b)
		}
		if reported := u.Input + u.CacheRead + u.CacheWrite; float64(reported) < float64(sent)/4/2 {
			return fmt.Errorf("%s: the server reported %d prompt tokens for at least %d bytes sent: it truncated the prompt; raise its context length (Ollama OLLAMA_CONTEXT_LENGTH, vLLM --max-model-len)", what, reported, sent)
		}
	}
	for _, p := range resp.Message.Parts {
		if p.Kind != llm.PartToolCall || p.ToolCall == nil || p.ToolCall.Name != probeToolName {
			continue
		}
		if err := validateInput(pt.Schema, p.ToolCall.Input); err != nil {
			return fmt.Errorf("%s: the tool call's arguments do not validate: %w", what, err)
		}
		return nil
	}
	return fmt.Errorf("%s: the model did not call %s (stop %s); its tool calling does not work on this host", what, probeToolName, resp.Stop)
}

// logRoles writes one line per role: provider, model, region, gate, prompt
// cache and the request parameters its adapter drops (design 6 and 10.4).
// It runs only when a role is on a new provider; a Claude-only deployment
// keeps its one "model endpoint ready" line.
func (e *Endpoint) logRoles() {
	for _, sp := range e.specs {
		caps := sp.m.Capabilities(sp.role.Model)
		gate := "claude"
		if g, ok := e.gates[sp.name]; ok {
			gate = g.Status
		}
		cache := "yes"
		if !caps.PromptCache {
			cache = "no"
		}
		e.log.Info("model role", "role", sp.name, "setting", sp.env, "provider", sp.role.Provider, "flavor", sp.role.Flavor,
			"model", sp.role.Model, "region", sp.region, "gate", gate, "prompt_cache", cache, "dropped", strings.Join(droppedParams(caps), ","))
		if !caps.PromptCache {
			e.log.Info("no prompt cache: each turn re-sends the full history at the input price", "role", sp.name, "model", sp.role.Model)
		}
	}
}

// droppedParams names the request parameters the agent sends that the
// role's adapter maps away (design 6).
func droppedParams(c llm.Capabilities) []string {
	var out []string
	if !c.Effort {
		out = append(out, "effort")
	}
	if !c.Reasoning {
		out = append(out, "thinking")
	}
	if !c.StrictTools {
		out = append(out, "strict_tools")
	}
	if !c.StructuredOutput {
		out = append(out, "output_schema")
	}
	if !c.Compaction {
		out = append(out, "server_compaction")
	}
	return out
}
