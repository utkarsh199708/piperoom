// Package agent runs the investigation loop with Claude.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

const (
	// maxParallelTools caps how many read tools run at once.
	maxParallelTools = 4
	// maxTokens bounds one model turn.
	maxTokens = 16000
)

// clock abstracts time so tests can verify backoff behavior without waiting
// on a real timer.
type clock interface {
	// Sleep blocks for d or until ctx ends, whichever comes first, and
	// reports ctx.Err() when ctx ends first.
	Sleep(ctx context.Context, d time.Duration) error
}

// realClock sleeps for real; it is the production clock.
type realClock struct{}

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ErrInboxClosed reports that the Inbox will deliver no more input (for
// example stdin reached EOF). Run treats it as a clean shutdown once any
// pending input has been consumed.
var ErrInboxClosed = errors.New("agent: inbox closed")

// runner carries the per-investigation state.
type runner struct {
	alert Alert
	deps  Deps
	opts  Options
	log   *slog.Logger
	model model
	clk   clock
	// timing holds this runner's waits and deadlines.
	timing timing

	toolList toolList
	system   string

	// proposals remembers the proposals the agent created this run, so a later
	// action_result can be matched to its last step and verified.
	proposals *proposalTracker

	// steps remembers the steps this run opened, for update_hypothesis to check
	// the steps a hypothesis cites.
	steps *stepLedger

	// recoveredIdle is true while the pipeline is verified recovered and nothing
	// since has broken that (a failed or unknown step, or a NOT recovered
	// verdict). An episode that ends while it is set stays "done" and waits for
	// a human, instead of reporting idle.
	recoveredIdle bool

	// noneSubjects are the schema subjects a verified-recovered proposal left at
	// compatibility NONE and no verified proposal has moved away from NONE since.
	// Only a restore of such a subject is judged by its layer checks alone (see
	// verifyProposal). It is lost on restart, which only makes the verdict
	// stricter: the data-flow rule then applies.
	noneSubjects map[string]bool

	emitMu sync.Mutex

	// triageModel, when set, runs the first triageTurns turns (see triage.go).
	// triageRan counts the turns that did, and verified turns true once a
	// [verification] verdict has gone to the model.
	triageModel string
	triageTurns int
	triageRan   int
	verified    bool
	// escalated turns true once a triage turn asked for propose_fix and was
	// re-run on the main model; the run stays on the main model after that.
	escalated bool
	// triageSw is the endpoint-wide "triage is off" flag; nil outside an Endpoint.
	triageSw *triageSwitch

	// prices estimate the cost in agent.usage events; nil omits it.
	prices Prices

	// held are quiet inputs (alert updates) drained while the agent waited
	// for a person: they go out with the next batch that wakes it.
	held []HumanInput

	// past is the worked-examples block of the first user turn (examples.go),
	// "" when there is none.
	past string
}

// Run investigates a pipeline incident: it calls Claude, executes read-only
// tools, reads human input between turns, proposes fixes, and turns everything
// into room events. It runs until ctx ends or the inbox closes; MaxTurns acts
// as a per-episode runaway guard that pauses for fresh human input.
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
	return newRunnerWith(alert, deps, opts, nil)
}

// newRunnerWith is newRunner with the model injected. A nil m builds the
// production client from the environment (Anthropic API).
func newRunnerWith(alert Alert, deps Deps, opts Options, m model) (*runner, error) {
	opts = opts.withDefaults()
	if deps.Tools == nil {
		return nil, errors.New("agent: Deps.Tools is nil")
	}
	if err := checkReadOnly(deps.Tools); err != nil {
		return nil, err
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
	if m == nil {
		m = newModel(opts, deps.Logger)
	}
	steps := newStepLedger()
	steps.seedTriage(alert.Triage)
	steps.setBackground(alertBackground(alert))
	return &runner{
		alert:     alert,
		deps:      deps,
		opts:      opts,
		log:       deps.Logger,
		model:     m,
		clk:       realClock{},
		timing:    defaultTiming(),
		proposals: newProposalTracker(),
		steps:     steps,

		noneSubjects: map[string]bool{},
	}, nil
}

func (r *runner) run(ctx context.Context) error {
	r.toolList = buildToolList(r.deps.Tools, newRoomToolsWith(r.deps, r.proposals, r.steps))
	r.system = systemPrompt()

	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})

	// turns counts model turns since the last human input. It is a per-episode
	// runaway guard, not a lifetime cap: fresh human input resets it.
	turns := 0

	// Drain anything already waiting before the first turn so an early steer is
	// part of the opening prompt.
	initialInputs := r.drain(ctx)
	// The model reads the verdict of an early recovery in its first turn. Only
	// an episode that ends after a recovery parks, in pauseForHumanInput.
	initialInputs, outcome := r.handleInputs(ctx, initialInputs)
	r.past = r.pastIncidents(ctx, pastIncidentsRunBytes, pastIncidentsTimeout)
	messages := []anthropic.BetaMessageParam{r.initialMessage(initialInputs, outcome.notes)}
	for {
		if err := ctx.Err(); err != nil {
			return stopErr(err)
		}

		if turns >= r.opts.MaxTurns {
			// Runaway guard: pause and wait for a human instead of abandoning the
			// room. The room stays usable while someone is still steering it.
			recovered := r.recoveredIdle
			if recovered {
				r.emitWaiting(ctx)
			} else {
				r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
					State:  "idle",
					Detail: "max turns reached; waiting for human input",
				})
			}
			r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{
				Text: "I have reached the turn limit for this episode and am pausing. Please steer me or confirm before I continue.",
			})
			inputs, err := r.waitForInput(ctx)
			if err != nil {
				return stopErr(err)
			}
			if len(inputs) == 0 {
				return nil
			}
			inputs, outcome := r.handleInputs(ctx, inputs)
			messages = appendUser(messages, r.inputBlocks(inputs, outcome.notes)...)
			turns = 0
			if r.parked(outcome) {
				if !recovered {
					r.emitWaiting(ctx)
				}
				if stop, err := r.holdAfterRecovery(ctx, &messages, &turns); stop {
					return err
				}
			} else if !outcome.statusSet {
				// The run leaves the idle (or done) status it paused on; the
				// room's failover and the hand-off check read the last status.
				r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
			}
			continue
		}

		resp, err := r.modelTurn(ctx, messages)
		if err != nil {
			// The room's token budget is spent: stop here, between turns. Tool
			// calls of the previous turn have all finished by now.
			// The error is returned so a supervisor can tell this stop from a
			// normal end; it must treat it as a stop (no retry, no error status).
			if be, ok := asBudgetError(err); ok {
				r.stopForBudget(ctx, be)
				return fmt.Errorf("agent: stopped: %w", be)
			}
			// A cancelled context is a clean shutdown (room closed), not an error.
			if ctx.Err() != nil {
				return stopErr(err)
			}
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
				State:  "error",
				Detail: "model request failed",
			})
			return fmt.Errorf("agent: model turn: %w", err)
		}

		if resp.StopReason == anthropic.BetaStopReasonRefusal {
			reason := refusalReason(resp)
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
				State:  "error",
				Detail: reason,
			})
			r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{
				Text: "I cannot proceed with this request: " + reason,
			})
			// Do not append the refused turn; a refusal must never end the
			// investigation for everyone. Go idle and wait like end_turn.
			if stop, err := r.pauseForHumanInput(ctx, &messages, &turns, nil); stop {
				return err
			}
			continue
		}

		turns++
		// History is append-only: keep the assistant turn exactly as returned.
		messages = append(messages, resp.ToParam())

		texts, uses := splitContent(resp.Content)
		resultBlocks := r.runToolUses(ctx, uses)

		// The agent's own words follow the tool steps, matching the loop order.
		for _, t := range texts {
			r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: t})
		}

		switch resp.StopReason {
		case anthropic.BetaStopReasonEndTurn, anthropic.BetaStopReasonStopSequence:
			// resultBlocks is empty here in practice (the model does not stop
			// with end_turn while also requesting a tool), but pass it anyway:
			// if a turn's content ever did carry a tool_use with one of these
			// stop reasons, dropping its result would leave a tool_use in
			// history with no matching tool_result, which the API rejects on
			// the very next request.
			if stop, err := r.pauseForHumanInput(ctx, &messages, &turns, resultBlocks); stop {
				return err
			}
		case anthropic.BetaStopReasonModelContextWindowExceeded:
			// This must never end the room: report it and wait for a human,
			// the same as a refusal. Server-side compaction (see request())
			// should make this rare, but a single turn's input can still
			// exceed the window (for example right after a burst of human
			// steers), and the room must stay usable. The turn's tool results
			// (resultBlocks) must still go back first: the assistant turn was
			// already appended above, and any tool_use in it needs a matching
			// tool_result in the very next message or the API rejects the
			// next request outright.
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{
				State:  "error",
				Detail: "model context window exceeded",
			})
			r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{
				Text: "I've exceeded the model's context window for this turn and can't continue. Please steer me or confirm before I continue.",
			})
			if stop, err := r.pauseForHumanInput(ctx, &messages, &turns, resultBlocks); stop {
				return err
			}
		default:
			// tool_use, max_tokens and pause_turn all continue: the next user
			// message carries the tool results plus any human input that arrived
			// while we were running.
			inputs := r.drain(ctx)
			inputs, outcome := r.handleInputs(ctx, inputs)
			blocks := append([]anthropic.BetaContentBlockParamUnion(nil), resultBlocks...)
			blocks = append(blocks, r.inputBlocks(inputs, outcome.notes)...)
			if len(blocks) == 0 {
				blocks = append(blocks, anthropic.NewBetaTextBlock("Continue."))
			}
			messages = appendUser(messages, blocks...)
			if resetsTurns(inputs) {
				turns = 0
			}
			// No park here: a recovered episode keeps running until the model
			// reaches end_turn, so it can read the verification note and reply.
		}
	}
}

// callModel sends one request and reports its usage to the room as an
// agent.usage event (docs/design/cost.md): role is triage or main, the model
// the triage rules picked. A request that was never sent (a budget refused it)
// is not reported.
func (r *runner) callModel(ctx context.Context, req anthropic.BetaMessageNewParams, role string) (anthropic.BetaMessage, error) {
	start := time.Now()
	resp, err := r.model.Turn(ctx, req)
	if wasSent(err) {
		ectx, cancel := detached(ctx)
		defer cancel()
		for _, p := range usagePayloads(req.Model, role, resp, time.Since(start), r.prices) {
			r.emit(ectx, events.AgentUsage, p)
		}
	}
	return resp, err
}

// request assembles one model request. The system prompt and tool list are
// stable so the prompt cache stays warm.
//
// It asks for server-side compaction (beta compact-2026-01-12): once the
// conversation passes the server's input-token trigger, the response gets an
// extra compaction content block summarizing the oldest messages, in place of
// a stop. Run keeps history append-only either way — resp.ToParam() carries
// that block back in the next request's messages exactly like any other
// content block, never specially — which is what lets a room survive hours
// of steers and tool calls without hitting the model's context window. The
// edit carries compactionInstructions() so the summary keeps step ids,
// hypotheses, proposals and driver instructions, and keeps untrusted text
// data instead of laundering its instructions into trusted history.
func (r *runner) request(messages []anthropic.BetaMessageParam) anthropic.BetaMessageNewParams {
	return anthropic.BetaMessageNewParams{
		Model:     r.opts.Model,
		MaxTokens: maxTokens,
		Betas:     []anthropic.AnthropicBeta{anthropic.AnthropicBetaCompact2026_01_12},
		System:    []anthropic.BetaTextBlockParam{{Text: r.system, CacheControl: anthropic.NewBetaCacheControlEphemeralParam()}},
		Tools:     r.toolList.params,
		Messages:  messages,
		OutputConfig: anthropic.BetaOutputConfigParam{
			Effort: anthropic.BetaOutputConfigEffort(r.opts.effort()),
		},
		Thinking: anthropic.BetaThinkingConfigParamUnion{
			OfAdaptive: &anthropic.BetaThinkingConfigAdaptiveParam{},
		},
		ToolChoice: anthropic.BetaToolChoiceUnionParam{
			OfAuto: &anthropic.BetaToolChoiceAutoParam{},
		},
		ContextManagement: anthropic.BetaContextManagementConfigParam{
			Edits: []anthropic.BetaContextManagementConfigEditUnionParam{
				{OfCompact20260112: &anthropic.BetaCompact20260112EditParam{
					Instructions: anthropic.String(compactionInstructions()),
				}},
			},
		},
		// Top-level cache control applies a breakpoint to the last cacheable
		// block, so each turn re-reads the conversation history from cache.
		CacheControl: anthropic.NewBetaCacheControlEphemeralParam(),
	}
}

func (r *runner) initialMessage(inputs []HumanInput, notes []string) anthropic.BetaMessageParam {
	blocks := openingBlocks(r.alert, r.past)
	blocks = append(blocks, r.inputBlocks(inputs, notes)...)
	return anthropic.NewBetaUserMessage(blocks...)
}

// inputBlocks renders one batch of human input and its verification notes, in
// the order the model reads them: the inputs, then what verification found.
func (r *runner) inputBlocks(inputs []HumanInput, notes []string) []anthropic.BetaContentBlockParamUnion {
	return append(r.humanBlocks(inputs), verifyBlocks(notes)...)
}

// appendUser adds blocks to the history as a user turn. When the last message
// is already a user turn, the blocks join it, so the request never carries two
// consecutive user messages.
func appendUser(messages []anthropic.BetaMessageParam, blocks ...anthropic.BetaContentBlockParamUnion) []anthropic.BetaMessageParam {
	if n := len(messages); n > 0 && messages[n-1].Role == anthropic.BetaMessageParamRoleUser {
		messages[n-1].Content = append(messages[n-1].Content, blocks...)
		return messages
	}
	return append(messages, anthropic.NewBetaUserMessage(blocks...))
}

func (r *runner) humanBlocks(inputs []HumanInput) []anthropic.BetaContentBlockParamUnion {
	blocks := make([]anthropic.BetaContentBlockParamUnion, 0, len(inputs))
	for _, in := range inputs {
		blocks = append(blocks, anthropic.NewBetaTextBlock(formatHumanInput(in)))
	}
	return blocks
}

// drain reads pending human inputs without blocking. Quiet inputs held back
// by waitForInput come first, in the order they arrived.
func (r *runner) drain(ctx context.Context) []HumanInput {
	inputs, err := r.deps.Inbox.Drain(ctx)
	if err != nil {
		r.log.Warn("agent: inbox drain failed", "err", err)
		return r.takeHeld(nil)
	}
	return r.takeHeld(inputs)
}

// takeHeld returns the held quiet inputs followed by inputs, and forgets them.
func (r *runner) takeHeld(inputs []HumanInput) []HumanInput {
	if len(r.held) == 0 {
		return inputs
	}
	out := append(r.held, inputs...)
	r.held = nil
	return out
}

// waitForInput blocks until at least one human input is pending or the context
// ends. A spurious Wake (or a stale notify token) with nothing to drain must
// not end the loop: wait again, so the next request never ends with an
// assistant turn (which Opus rejects as a prefill with HTTP 400).
//
// If Drain itself keeps failing (for example a flaky store) while Wait keeps
// returning at once, draining again immediately would spin the loop at full
// speed; an increasing backoff between failed drains bounds that.
func (r *runner) waitForInput(ctx context.Context) ([]HumanInput, error) {
	backoff := r.timing.drainBackoffMin
	for {
		inputs, err := r.deps.Inbox.Drain(ctx)
		if err != nil {
			r.log.Warn("agent: inbox drain failed", "err", err)
			if serr := r.clk.Sleep(ctx, backoff); serr != nil {
				return nil, serr
			}
			if backoff *= 2; backoff > r.timing.drainBackoffMax {
				backoff = r.timing.drainBackoffMax
			}
			continue
		}
		if slices.ContainsFunc(inputs, func(in HumanInput) bool { return !isQuiet(in) }) {
			return r.takeHeld(inputs), nil
		}
		// Only quiet inputs (or none): they never wake an idle agent. Keep
		// them for the next batch and wait on.
		r.held = append(r.held, inputs...)
		backoff = r.timing.drainBackoffMin
		if err := r.deps.Inbox.Wait(ctx); err != nil {
			return nil, err
		}
	}
}

// pauseForHumanInput emits an idle status and waits for human input. pending
// is any tool_result (and similar) blocks still owed to the model for the
// assistant turn just appended to history — every tool_use in that turn must
// be matched by a tool_result in the very next message, or the API rejects
// the next request outright, so pending always goes out first, ahead of the
// human input, in the same user message. Pass nil when the turn had no
// pending tool calls.
//
// When input arrives, that combined message is appended and the turn counter
// resets. The bool return reports whether Run should return now (the room
// closed while waiting); when it is true, the error return is what Run
// should return. If the room closes before input arrives, pending is
// dropped along with everything else: Run is about to return, so no further
// request will ever need it.
func (r *runner) pauseForHumanInput(ctx context.Context, messages *[]anthropic.BetaMessageParam, turns *int, pending []anthropic.BetaContentBlockParamUnion) (bool, error) {
	// A recovered episode ends "done" with the reason, not "idle".
	recovered := r.recoveredIdle
	if recovered {
		r.emitWaiting(ctx)
	} else {
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
	}
	inputs, err := r.waitForInput(ctx)
	if err != nil {
		return true, stopErr(err)
	}
	if len(inputs) == 0 {
		return true, nil
	}
	inputs, outcome := r.handleInputs(ctx, inputs)
	blocks := append(append([]anthropic.BetaContentBlockParamUnion(nil), pending...), r.inputBlocks(inputs, outcome.notes)...)
	*messages = appendUser(*messages, blocks...)
	*turns = 0
	if r.parked(outcome) {
		if !recovered {
			r.emitWaiting(ctx)
		}
		return r.holdAfterRecovery(ctx, messages, turns)
	}
	if !outcome.statusSet {
		// The model runs again: leave the idle (or done) status, so the room
		// does not show an idle agent writing steps, and failover sees a
		// working run to resume.
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
	}
	return false, nil
}

// holdAfterRecovery keeps a recovered episode alive. The pipeline is fixed, so
// the model is not called until something needs an answer, but the run must
// stay alive: a later steer or comment still gets a reply. It waits on the
// Inbox exactly like the idle path and keeps the done status. It returns
// stop=false when the model should run again (a human spoke, or a step failed
// or is unknown) and stop=true when the room closed, with the error Run should
// return.
func (r *runner) holdAfterRecovery(ctx context.Context, messages *[]anthropic.BetaMessageParam, turns *int) (bool, error) {
	for {
		inputs, err := r.waitForInput(ctx)
		if err != nil {
			return true, stopErr(err)
		}
		inputs, outcome := r.handleInputs(ctx, inputs)
		*messages = appendUser(*messages, r.inputBlocks(inputs, outcome.notes)...)
		*turns = 0
		if !r.parked(outcome) {
			if !outcome.statusSet {
				r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
			}
			return false, nil
		}
	}
}

// hasHumanInput reports whether inputs contain anything a person said, as
// opposed to executor results and alert updates. A person's words always get
// a model turn.
func hasHumanInput(inputs []HumanInput) bool {
	for _, in := range inputs {
		if in.Kind != "action_result" && in.Kind != "alert_update" {
			return true
		}
	}
	return false
}

// isQuiet reports an input that rides along with the next batch but never
// starts a turn or resets the turn guard on its own: an alert update (the
// alert fired again, the watcher joined) is new numbers, not a request.
func isQuiet(in HumanInput) bool { return in.Kind == "alert_update" }

// resetsTurns reports whether a batch resets the per-episode turn guard: any
// input but a quiet one does.
func resetsTurns(inputs []HumanInput) bool {
	return slices.ContainsFunc(inputs, func(in HumanInput) bool { return !isQuiet(in) })
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

// toolDataJSON marshals a tool's structured data for its tool_result event. A
// number outside the double range (a NUMERIC column read back as 1e131072, a
// metric of 1e-16384) is a valid JSON number, but jsonb refuses some of them
// (SQLSTATE 22003, "value overflows numeric format") and the whole event would
// be lost. Such numbers are written as strings holding their exact text, so the
// event is kept and the value is still readable. The model never sees this
// data; it reads the summary.
func (r *runner) toolDataJSON(tool string, v any) (json.RawMessage, bool) {
	if v == nil {
		return nil, false
	}
	raw, n, err := encodeToolData(v)
	if err != nil {
		r.log.Warn("agent: tool data marshal failed", "tool", tool, "err", err)
		return nil, false
	}
	if n > 0 {
		r.log.Warn("agent: tool data had numbers outside the double range; kept them as strings", "tool", tool, "count", n)
	}
	out, cut := events.TruncateData(raw, r.maxToolData())
	if cut {
		r.log.Info("agent: tool data over cap; sent a preview", "tool", tool, "original_bytes", len(raw))
	}
	return out, cut
}

// defaultMaxToolDataBytes is the room's PIPEROOM_EVENT_MAX_DATA_BYTES default,
// used when Options.MaxToolDataBytes is unset.
const defaultMaxToolDataBytes = 64 << 10

// maxToolData is the cap for one tool result's Data. Zero (or less) means the
// default, never "no cap": events.TruncateData treats max <= 0 as unlimited.
func (r *runner) maxToolData() int {
	if r.opts.MaxToolDataBytes <= 0 {
		return defaultMaxToolDataBytes
	}
	return int(r.opts.MaxToolDataBytes)
}

// stopErr maps a clean shutdown (context.Canceled or a closed inbox) to nil so
// the room service does not treat a closed room as a failure.
func stopErr(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrInboxClosed) {
		return nil
	}
	return err
}

// refusalReason renders the structured refusal details into a short, stable
// reason string for the room log and the agent's message.
func refusalReason(m anthropic.BetaMessage) string {
	d := m.StopDetails
	if d.Category == "" && d.Explanation == "" {
		return "model refused the request"
	}
	if d.Category == "" {
		return d.Explanation
	}
	if d.Explanation == "" {
		return string(d.Category)
	}
	return string(d.Category) + ": " + d.Explanation
}

// splitContent separates an assistant turn into text and tool_use blocks.
func splitContent(content []anthropic.BetaContentBlockUnion) (texts []string, uses []toolUse) {
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
func (r *runner) runToolUses(ctx context.Context, uses []toolUse) []anthropic.BetaContentBlockParamUnion {
	results := make([]toolResult, len(uses))

	// Room tools first: they are fast and often steer the turn.
	for i, u := range uses {
		if e, ok := r.toolList.byName[u.name]; ok && e.isRoom {
			results[i] = r.runRoomTool(ctx, u)
		}
	}

	// Read tools in parallel. Unknown names also go through runReadTool, which
	// reports them as an error tool_result rather than dropping the tool_use.
	//
	// A panic in a read tool must not escape its goroutine: nothing there can
	// recover it, so it would end the process before Deps.Usage runs and before
	// the room supervisor sees it. Each goroutine recovers, the first panic is
	// kept (the stack goes to the log), and it is raised again on this
	// goroutine once every tool has stopped.
	sem := make(chan struct{}, maxParallelTools)
	var wg sync.WaitGroup
	var panicMu sync.Mutex
	var firstPanic any
	var panicked bool
	for i, u := range uses {
		if e, ok := r.toolList.byName[u.name]; ok && e.isRoom {
			continue
		}
		wg.Add(1)
		go func(i int, u toolUse) {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					// The value goes on to the room supervisor, which logs it and
					// puts it in the room status, so it is masked here.
					p = scrubPanic(p)
					r.log.Error("agent: read tool panicked", "tool", u.name, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
					panicMu.Lock()
					if !panicked {
						panicked, firstPanic = true, p
					}
					panicMu.Unlock()
				}
			}()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[i] = toolResult{
					toolUseID: u.id,
					content:   wrapToolOutput(u.name, "", "error: "+ctx.Err().Error()),
					isError:   true,
				}
				return
			}
			results[i] = r.runReadTool(ctx, u)
		}(i, u)
	}
	wg.Wait()
	if panicked {
		panic(firstPanic)
	}

	blocks := make([]anthropic.BetaContentBlockParamUnion, 0, len(results))
	for _, res := range results {
		blocks = append(blocks, anthropic.NewBetaToolResultBlock(res.toolUseID, res.content, res.isError))
	}
	return blocks
}

// toolScope returns ctx carrying the room's pipeline, so every read tool is
// scoped to the pipeline of the alert that opened the room and cannot be
// pointed at another pipeline by its input (security finding F11). The
// pipeline id comes from the alert, never from the model. An alert with no
// pipeline clears any pipeline the context inherited: its tools are refused
// unless the caller of Run opted out with tools.WithUnscoped.
func (r *runner) toolScope(ctx context.Context) context.Context {
	return tools.WithPipelineID(ctx, r.alert.PipelineID)
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

	tctx, cancel := context.WithTimeout(r.toolScope(ctx), r.timing.toolTimeout)
	defer cancel()
	rec := tools.NewRecorder()
	tctx = tools.WithRecorder(tctx, rec)
	start := time.Now()
	res, err := entry.tool.Run(tctx, cleaned)
	took := time.Since(start).Milliseconds()
	// An errored read still ran its queries; the receipt explains the error.
	prov := r.receipt(u.name, res.Provenance, rec)

	res, scrubErr := r.scrubToolResult(u.name, res)
	if err == nil {
		err = scrubErr
	}
	data, dataCut := r.toolDataJSON(u.name, res.Data)

	if err != nil {
		msg := "error: " + r.scrubToolError(u.name, err).Error()
		summary, truncated := tools.Truncate(msg)
		r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
			StepID:     stepID,
			ToolUseID:  u.id,
			Tool:       u.name,
			Summary:    summary,
			IsError:    true,
			Truncated:  truncated,
			TookMS:     took,
			Provenance: prov,
		})
		r.steps.addWithInput(stepID, u.name, string(cleaned), summary, true)
		return toolResult{
			toolUseID: u.id,
			content:   r.wrapReadToolOutput(u.name, stepID, summary),
			isError:   true,
		}
	}

	// Defensively re-cap the summary before it reaches the model; a tool that
	// ignores MaxSummaryBytes must not be able to bloat the request.
	summary, reTruncated := tools.Truncate(res.Summary)
	r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
		StepID:     stepID,
		ToolUseID:  u.id,
		Tool:       u.name,
		Summary:    summary,
		Data:       data,
		IsError:    false,
		Truncated:  res.Truncated || reTruncated || dataCut,
		TookMS:     took,
		Provenance: prov,
	})
	r.steps.addWithInput(stepID, u.name, string(cleaned), summary, false)
	return toolResult{
		toolUseID: u.id,
		content:   r.wrapReadToolOutput(u.name, stepID, summary),
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
	r.steps.add(stepID, u.name, summary, true)
	return toolResult{
		toolUseID: u.id,
		content:   wrapToolOutput(u.name, stepID, "error: "+msg),
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
			content:   wrapToolOutput(u.name, "", "error: unknown tool"),
			isError:   true,
		}
	}
	if err := validateInput(entry.schema, u.input); err != nil {
		return toolResult{
			toolUseID: u.id,
			content:   wrapToolOutput(u.name, "", "error: invalid input: "+err.Error()),
			isError:   true,
		}
	}
	tctx, cancel := context.WithTimeout(ctx, r.timing.roomToolTimeout)
	defer cancel()
	res, err := entry.tool.Run(tctx, u.input)
	if err != nil {
		return toolResult{
			toolUseID: u.id,
			content:   wrapToolOutput(u.name, "", "error: "+err.Error()),
			isError:   true,
		}
	}
	// A new proposal gets its creation baseline now, before anyone approves it,
	// so rows that arrive during approval and execution count as the fix's.
	if pr, ok := res.Data.(proposeFixResult); ok {
		r.snapshotBaseline(ctx, pr.ProposalID)
	}
	return toolResult{
		toolUseID: u.id,
		content:   wrapToolOutput(u.name, "", res.Summary),
		isError:   false,
	}
}

// wrapToolOutput marks tool output as untrusted data. The system prompt tells
// the model to treat anything inside this wrapper as data, never instructions.
// stepID is the room step the tool ran under (empty for room tools, which open
// no step); the model cites it when it states evidence, so it must be visible
// here. The text is escaped so a value containing </tool_output> cannot break
// out of the wrapper.
func wrapToolOutput(tool, stepID, text string) string {
	attrs := fmt.Sprintf("tool=%q", tool)
	if stepID != "" {
		attrs += fmt.Sprintf(" step=%q", stepID)
	}
	return fmt.Sprintf("<tool_output %s untrusted=\"true\">%s</tool_output>", attrs, escapeToolText(text))
}
