// Package agent runs the investigation loop with Claude.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

const (
	// toolTimeout bounds a single read tool call. The turn continues with the
	// other results if one tool times out.
	toolTimeout = 10 * time.Second
	// maxParallelTools caps how many read tools run at once.
	maxParallelTools = 4
	// maxTokens bounds one model turn.
	maxTokens = 16000
)

// runner carries the per-investigation state.
type runner struct {
	alert Alert
	deps  Deps
	opts  Options
	log   *slog.Logger
	model model

	toolList toolList
	system   string

	emitMu sync.Mutex
}

// Run investigates a pipeline incident: it calls Claude, executes read-only
// tools, reads human input between turns, proposes fixes, and turns everything
// into room events. It runs until ctx ends or MaxTurns is reached.
func Run(ctx context.Context, alert Alert, deps Deps, opts Options) error {
	r, err := newRunner(alert, deps, opts)
	if err != nil {
		return err
	}
	return r.run(ctx)
}

// newRunner validates dependencies and builds the loop state. Tests call it
// and then swap in a scripted model.
func newRunner(alert Alert, deps Deps, opts Options) (*runner, error) {
	opts = opts.withDefaults()
	if deps.Tools == nil {
		return nil, errors.New("agent: Deps.Tools is nil")
	}
	if deps.Sink == nil {
		return nil, errors.New("agent: Deps.Sink is nil")
	}
	if deps.Inbox == nil {
		return nil, errors.New("agent: Deps.Inbox is nil")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &runner{
		alert: alert,
		deps:  deps,
		opts:  opts,
		log:   deps.Logger,
		model: newModel(opts, deps.Logger),
	}, nil
}

func (r *runner) run(ctx context.Context) error {
	r.toolList = buildToolList(r.deps.Tools, newRoomTools(r.deps))
	r.system = systemPrompt()

	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})

	// Drain anything already waiting before the first turn so an early steer is
	// part of the opening prompt.
	messages := []anthropic.MessageParam{r.initialMessage(r.drain(ctx))}

	for turn := 0; turn < r.opts.MaxTurns; turn++ {
		if err := ctx.Err(); err != nil {
			return stopErr(err)
		}

		resp, err := r.model.Turn(ctx, r.request(messages))
		if err != nil {
			return fmt.Errorf("agent: model turn: %w", stopErr(err))
		}
		if resp.StopReason == anthropic.StopReasonRefusal {
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
				State:  "error",
				Detail: "model refused the request",
			})
			return errors.New("agent: model refused the request")
		}

		// History is append-only: keep the assistant turn exactly as returned.
		messages = append(messages, resp.ToParam())

		texts, uses := splitContent(resp.Content)
		resultBlocks := r.runToolUses(ctx, uses)

		// The agent's own words follow the tool steps, matching the loop order.
		for _, t := range texts {
			r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: t})
		}

		switch resp.StopReason {
		case anthropic.StopReasonEndTurn, anthropic.StopReasonStopSequence:
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
			if err := r.deps.Inbox.Wait(ctx); err != nil {
				return stopErr(err)
			}
			if inputs := r.drain(ctx); len(inputs) > 0 {
				messages = append(messages, r.humanMessage(inputs))
			}
		case anthropic.StopReasonModelContextWindowExceeded:
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
				State:  "error",
				Detail: "model context window exceeded",
			})
			return errors.New("agent: model context window exceeded")
		default:
			// tool_use, max_tokens and pause_turn all continue: the next user
			// message carries the tool results plus any human input that arrived
			// while we were running.
			blocks := append([]anthropic.ContentBlockParamUnion(nil), resultBlocks...)
			blocks = append(blocks, r.humanBlocks(r.drain(ctx))...)
			if len(blocks) == 0 {
				blocks = append(blocks, anthropic.NewTextBlock("Continue."))
			}
			messages = append(messages, anthropic.NewUserMessage(blocks...))
		}
	}

	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
		State:  "done",
		Detail: "max turns reached",
	})
	return nil
}

// request assembles one model request. The system prompt and tool list are
// stable so the prompt cache stays warm.
func (r *runner) request(messages []anthropic.MessageParam) anthropic.MessageNewParams {
	return anthropic.MessageNewParams{
		Model:     r.opts.Model,
		MaxTokens: maxTokens,
		System:    []anthropic.TextBlockParam{{Text: r.system, CacheControl: anthropic.NewCacheControlEphemeralParam()}},
		Tools:     r.toolList.params,
		Messages:  messages,
		OutputConfig: anthropic.OutputConfigParam{
			Effort: anthropic.OutputConfigEffort(r.opts.effort()),
		},
		Thinking: anthropic.ThinkingConfigParamUnion{
			OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{},
		},
		ToolChoice: anthropic.ToolChoiceUnionParam{
			OfAuto: &anthropic.ToolChoiceAutoParam{},
		},
	}
}

func (r *runner) initialMessage(inputs []HumanInput) anthropic.MessageParam {
	blocks := []anthropic.ContentBlockParamUnion{anthropic.NewTextBlock(firstUserMessage(r.alert))}
	blocks = append(blocks, r.humanBlocks(inputs)...)
	return anthropic.NewUserMessage(blocks...)
}

func (r *runner) humanMessage(inputs []HumanInput) anthropic.MessageParam {
	return anthropic.NewUserMessage(r.humanBlocks(inputs)...)
}

func (r *runner) humanBlocks(inputs []HumanInput) []anthropic.ContentBlockParamUnion {
	blocks := make([]anthropic.ContentBlockParamUnion, 0, len(inputs))
	for _, in := range inputs {
		blocks = append(blocks, anthropic.NewTextBlock(formatHumanInput(in)))
	}
	return blocks
}

// drain reads pending human inputs without blocking.
func (r *runner) drain(ctx context.Context) []HumanInput {
	inputs, err := r.deps.Inbox.Drain(ctx)
	if err != nil {
		r.log.Warn("agent: inbox drain failed", "err", err)
		return nil
	}
	return inputs
}

// emit writes one event to the sink, serialized so parallel tool steps cannot
// interleave their payloads. Emit failures are logged, not fatal.
func (r *runner) emit(ctx context.Context, typ events.Type, payload any) {
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	if err := r.deps.Sink.Emit(ctx, typ, payload); err != nil {
		r.log.Warn("agent: emit failed", "type", typ, "err", err)
	}
}

// stopErr maps a clean shutdown (context.Canceled) to nil so the room service
// does not treat a closed room as a failure.
func stopErr(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// splitContent separates an assistant turn into text and tool_use blocks.
func splitContent(content []anthropic.ContentBlockUnion) (texts []string, uses []toolUse) {
	for _, block := range content {
		switch block.Type {
		case "text":
			if s := strings.TrimSpace(block.Text); s != "" {
				texts = append(texts, block.Text)
			}
		case "tool_use":
			uses = append(uses, toolUse{id: block.ID, name: block.Name, input: block.Input})
		}
	}
	return texts, uses
}

// toolUse is one model tool invocation.
type toolUse struct {
	id    string
	name  string
	input json.RawMessage
}

// toolResult is one tool result to send back to the model.
type toolResult struct {
	toolUseID string
	content   string
	isError   bool
}

// runToolUses executes every tool_use. Room tools run inline in order; read
// tools run in parallel (max 4). The returned blocks preserve the model's
// original order.
func (r *runner) runToolUses(ctx context.Context, uses []toolUse) []anthropic.ContentBlockParamUnion {
	results := make([]toolResult, len(uses))

	// Room tools first: they are fast and often steer the turn.
	for i, u := range uses {
		if e, ok := r.toolList.byName[u.name]; ok && e.isRoom {
			results[i] = r.runRoomTool(ctx, u)
		}
	}

	// Read tools in parallel. Unknown names also go through runReadTool, which
	// reports them as an error tool_result rather than dropping the tool_use.
	sem := make(chan struct{}, maxParallelTools)
	var wg sync.WaitGroup
	for i, u := range uses {
		if e, ok := r.toolList.byName[u.name]; ok && e.isRoom {
			continue
		}
		wg.Add(1)
		go func(i int, u toolUse) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[i] = toolResult{
					toolUseID: u.id,
					content:   wrapToolOutput(u.name, "error: "+ctx.Err().Error()),
					isError:   true,
				}
				return
			}
			results[i] = r.runReadTool(ctx, u)
		}(i, u)
	}
	wg.Wait()

	blocks := make([]anthropic.ContentBlockParamUnion, 0, len(results))
	for _, res := range results {
		blocks = append(blocks, anthropic.NewToolResultBlock(res.toolUseID, res.content, res.isError))
	}
	return blocks
}

// runReadTool executes one registry tool within a step, emitting step.started,
// tool_call and tool_result events.
func (r *runner) runReadTool(ctx context.Context, u toolUse) toolResult {
	entry, ok := r.toolList.byName[u.name]
	if !ok {
		return r.failReadTool(ctx, u, "", "unknown tool "+u.name)
	}

	cleaned, why, err := stripWhy(u.input)
	if err != nil {
		return r.failReadTool(ctx, u, u.name, "invalid tool input: "+err.Error())
	}
	if err := validateInput(entry.schema, cleaned); err != nil {
		return r.failReadTool(ctx, u, why, "invalid tool input: "+err.Error())
	}

	title := why
	if title == "" {
		title = u.name
	}
	stepID := newStepID()

	r.emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: stepID, Title: title})
	r.emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{
		StepID:    stepID,
		ToolUseID: u.id,
		Tool:      u.name,
		Input:     u.input,
	})

	tctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	start := time.Now()
	res, err := entry.tool.Run(tctx, cleaned)
	took := time.Since(start).Milliseconds()

	var data json.RawMessage
	if res.Data != nil {
		if b, mErr := json.Marshal(res.Data); mErr == nil {
			data = b
		}
	}

	if err != nil {
		msg := "error: " + err.Error()
		summary, truncated := tools.Truncate(msg)
		r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
			StepID:    stepID,
			ToolUseID: u.id,
			Tool:      u.name,
			Summary:   summary,
			IsError:   true,
			Truncated: truncated,
			TookMS:    took,
		})
		return toolResult{
			toolUseID: u.id,
			content:   wrapToolOutput(u.name, msg),
			isError:   true,
		}
	}

	r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
		StepID:    stepID,
		ToolUseID: u.id,
		Tool:      u.name,
		Summary:   res.Summary,
		Data:      data,
		IsError:   false,
		Truncated: res.Truncated,
		TookMS:    took,
	})
	return toolResult{
		toolUseID: u.id,
		content:   wrapToolOutput(u.name, res.Summary),
		isError:   false,
	}
}

// failReadTool reports a tool call that could not be validated (unknown tool,
// bad input) without panicking.
func (r *runner) failReadTool(ctx context.Context, u toolUse, title, msg string) toolResult {
	if title == "" {
		title = u.name
	}
	stepID := newStepID()
	r.emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: stepID, Title: title})
	r.emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{
		StepID:    stepID,
		ToolUseID: u.id,
		Tool:      u.name,
		Input:     u.input,
	})
	summary, truncated := tools.Truncate("error: " + msg)
	r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
		StepID:    stepID,
		ToolUseID: u.id,
		Tool:      u.name,
		Summary:   summary,
		IsError:   true,
		Truncated: truncated,
	})
	return toolResult{
		toolUseID: u.id,
		content:   wrapToolOutput(u.name, "error: "+msg),
		isError:   true,
	}
}

// runRoomTool executes one room tool (update_hypothesis, propose_fix,
// post_message). These emit their own domain events rather than step/tool_call
// events, but still return a tool_result block to the model.
func (r *runner) runRoomTool(ctx context.Context, u toolUse) toolResult {
	entry, ok := r.toolList.byName[u.name]
	if !ok {
		return toolResult{
			toolUseID: u.id,
			content:   wrapToolOutput(u.name, "error: unknown tool"),
			isError:   true,
		}
	}
	if err := validateInput(entry.schema, u.input); err != nil {
		return toolResult{
			toolUseID: u.id,
			content:   wrapToolOutput(u.name, "error: invalid input: "+err.Error()),
			isError:   true,
		}
	}
	res, err := entry.tool.Run(ctx, u.input)
	if err != nil {
		return toolResult{
			toolUseID: u.id,
			content:   wrapToolOutput(u.name, "error: "+err.Error()),
			isError:   true,
		}
	}
	return toolResult{
		toolUseID: u.id,
		content:   wrapToolOutput(u.name, res.Summary),
		isError:   false,
	}
}

// wrapToolOutput marks tool output as untrusted data. The system prompt tells
// the model to treat anything inside this wrapper as data, never instructions.
func wrapToolOutput(tool, text string) string {
	return fmt.Sprintf("<tool_output tool=%q untrusted=\"true\">%s</tool_output>", tool, text)
}
