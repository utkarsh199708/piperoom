package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// proposalTimeout bounds the Proposals.Create call so a stalled proposal store
// cannot hang the investigation.
var proposalTimeout = 10 * time.Second

// roomToolNames are the names of the room tools newRoomTools builds, kept
// beside it so both are edited together. RoomToolNames serves callers that
// only need the names from this list, so it never constructs a tool — a room
// tool constructor may one day dereference its deps, and a name lookup must
// not panic on a zero Deps. TestRoomToolNamesMatchRoomTools keeps the list in
// sync with what the constructor really builds.
var roomToolNames = []string{"update_hypothesis", "propose_fix", "post_message"}

// newRoomTools builds the three room tools the agent itself owns. These are
// tools.KindRoom: they only write to the room log or create proposals, never
// to customer systems. track records each created proposal so the agent can
// verify recovery when its last step's action_result arrives; it may be nil
// (the tool then just skips recording).
func newRoomTools(d Deps, track *proposalTracker) []tools.Tool {
	return []tools.Tool{
		&updateHypothesisTool{sink: d.Sink},
		&proposeFixTool{actions: d.Actions, proposals: d.Proposals, sink: d.Sink, track: track},
		&postMessageTool{sink: d.Sink},
	}
}

// RoomToolNames returns the names of the room tools the agent adds on top of
// the read-tool registry, from the static list above. Exported for
// cmd/investigate's wiring test, which checks the system prompt only ever
// names tools the model can really call. The returned slice is a copy callers
// may keep or modify.
func RoomToolNames() []string {
	return slices.Clone(roomToolNames)
}

func roomObjectSchema(props map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
		"required":             required,
	}
}

// ---- update_hypothesis ----

type updateHypothesisTool struct {
	sink Sink
}

func (t *updateHypothesisTool) Name() string { return "update_hypothesis" }
func (t *updateHypothesisTool) Kind() tools.Kind {
	return tools.KindRoom
}
func (t *updateHypothesisTool) Description() string {
	return "Record your current best hypothesis about the root cause. Call this whenever your belief changes. based_on_steps lists the step ids that support it."
}
func (t *updateHypothesisTool) InputSchema() map[string]any {
	return roomObjectSchema(map[string]any{
		"text": map[string]any{
			"type":        "string",
			"description": "one short sentence describing the hypothesis",
		},
		"based_on_steps": map[string]any{
			"type":        "array",
			"items":       map[string]any{"type": "string"},
			"description": "step ids that this hypothesis is based on",
		},
		"confidence": map[string]any{
			"type": "string",
			"enum": []string{"low", "medium", "high"},
		},
	}, "text", "based_on_steps", "confidence")
}

type hypothesisInput struct {
	Text         string   `json:"text"`
	BasedOnSteps []string `json:"based_on_steps"`
	Confidence   string   `json:"confidence"`
}

func (t *updateHypothesisTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in hypothesisInput
	if err := json.Unmarshal(input, &in); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if in.Text == "" {
		return tools.Result{}, fmt.Errorf("%w: text is required", tools.ErrBadInput)
	}
	switch in.Confidence {
	case "low", "medium", "high":
	default:
		return tools.Result{}, fmt.Errorf("%w: confidence must be low, medium or high", tools.ErrBadInput)
	}
	if t.sink == nil {
		return tools.Result{}, fmt.Errorf("agent: no sink available for update_hypothesis")
	}
	if err := t.sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{
		Text:         in.Text,
		BasedOnSteps: in.BasedOnSteps,
		Confidence:   in.Confidence,
		Confirmed:    false,
	}); err != nil {
		return tools.Result{}, fmt.Errorf("agent: emit hypothesis: %w", err)
	}
	return tools.Result{Summary: "hypothesis recorded"}, nil
}

// ---- post_message ----

type postMessageTool struct {
	sink Sink
}

func (t *postMessageTool) Name() string { return "post_message" }
func (t *postMessageTool) Kind() tools.Kind {
	return tools.KindRoom
}
func (t *postMessageTool) Description() string {
	return "Post a short message to the room, for example to answer a watcher's comment. Set reply_to to the comment id when answering a comment."
}
func (t *postMessageTool) InputSchema() map[string]any {
	return roomObjectSchema(map[string]any{
		"text":     map[string]any{"type": "string", "description": "message text"},
		"reply_to": map[string]any{"type": "string", "description": "optional comment id to reply to"},
	}, "text")
}

type postMessageInput struct {
	Text    string `json:"text"`
	ReplyTo string `json:"reply_to"`
}

func (t *postMessageTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in postMessageInput
	if err := json.Unmarshal(input, &in); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if in.Text == "" {
		return tools.Result{}, fmt.Errorf("%w: text is required", tools.ErrBadInput)
	}
	if t.sink == nil {
		return tools.Result{}, fmt.Errorf("agent: no sink available for post_message")
	}
	if err := t.sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{
		Text:    in.Text,
		ReplyTo: in.ReplyTo,
	}); err != nil {
		return tools.Result{}, fmt.Errorf("agent: emit message: %w", err)
	}
	return tools.Result{Summary: "message posted"}, nil
}

// ---- propose_fix ----

type proposeFixTool struct {
	actions   *actions.Registry
	proposals Proposals
	sink      Sink
	track     *proposalTracker
}

func (t *proposeFixTool) Name() string { return "propose_fix" }
func (t *proposeFixTool) Kind() tools.Kind {
	return tools.KindRoom
}

// Description lists every available action with its params schema so the model
// proposes only real actions with valid parameters.
func (t *proposeFixTool) Description() string {
	var b strings.Builder
	b.WriteString("Propose a fix as a sequence of write actions. Nothing runs until every required layer owner approves. ")
	b.WriteString("Available actions:\n")
	acts := t.actionsList()
	if len(acts) == 0 {
		b.WriteString("(none)")
	}
	for _, a := range acts {
		fmt.Fprintf(&b, "- %s (layer %s): %s params=%s\n", a.Name(), a.Layer(), a.Describe(), compactSchema(a.ParamsSchema()))
	}
	b.WriteString("steps is an array; each step names an action, gives its params object and a short description. risk and rollback are required short text: never empty and never a placeholder such as n/a.")
	return b.String()
}

func (t *proposeFixTool) actionsList() []actions.Action {
	if t.actions == nil {
		return nil
	}
	return t.actions.All()
}

func (t *proposeFixTool) InputSchema() map[string]any {
	return roomObjectSchema(map[string]any{
		"title": map[string]any{"type": "string", "description": "short title for the proposal"},
		"steps": map[string]any{
			"type":  "array",
			"items": t.stepsItemSchema(),
		},
		"risk":     map[string]any{"type": "string", "description": "what could go wrong"},
		"rollback": map[string]any{"type": "string", "description": "how to undo this fix"},
	}, "title", "steps", "risk", "rollback")
}

// stepsItemSchema builds the schema for one proposal step. With actions
// registered it is an anyOf of one variant per action (action fixed via const,
// params taken from the action's own schema) so every nested object is strictly
// typed. Without actions it falls back to a generic strict step object.
func (t *proposeFixTool) stepsItemSchema() map[string]any {
	acts := t.actionsList()
	if len(acts) == 0 {
		return roomObjectSchema(map[string]any{
			"action":      map[string]any{"type": "string", "description": "action name"},
			"params":      map[string]any{"type": "object", "additionalProperties": false},
			"description": map[string]any{"type": "string", "description": "what this step does"},
		}, "action", "params", "description")
	}
	variants := make([]any, 0, len(acts))
	for _, a := range acts {
		params := a.ParamsSchema()
		if params == nil {
			params = map[string]any{"type": "object", "additionalProperties": false}
		}
		variants = append(variants, roomObjectSchema(map[string]any{
			"action":      map[string]any{"type": "string", "const": a.Name()},
			"params":      params,
			"description": map[string]any{"type": "string", "description": "what this step does"},
		}, "action", "params", "description"))
	}
	return map[string]any{"anyOf": variants}
}

type proposeFixInput struct {
	Title string `json:"title"`
	Steps []struct {
		Action      string          `json:"action"`
		Params      json.RawMessage `json:"params"`
		Description string          `json:"description"`
	} `json:"steps"`
	Risk     string `json:"risk"`
	Rollback string `json:"rollback"`
}

func (t *proposeFixTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in proposeFixInput
	if err := json.Unmarshal(input, &in); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if in.Title == "" {
		return tools.Result{}, fmt.Errorf("%w: title is required", tools.ErrBadInput)
	}
	if len(in.Steps) == 0 {
		return tools.Result{}, fmt.Errorf("%w: at least one step is required", tools.ErrBadInput)
	}
	if err := checkProposalText("risk", in.Risk); err != nil {
		return tools.Result{}, err
	}
	if err := checkProposalText("rollback", in.Rollback); err != nil {
		return tools.Result{}, err
	}
	if t.proposals == nil {
		return tools.Result{}, fmt.Errorf("agent: proposals are not available")
	}

	reg := t.actions
	steps := make([]events.ProposalStep, 0, len(in.Steps))
	seenLayers := map[string]bool{}
	var layers []string
	for _, s := range in.Steps {
		if reg == nil {
			return tools.Result{}, fmt.Errorf("agent: no actions are available")
		}
		a, ok := reg.Get(s.Action)
		if !ok {
			return tools.Result{}, fmt.Errorf("%w: unknown action %q", tools.ErrBadInput, s.Action)
		}
		if err := a.Validate(s.Params); err != nil {
			return tools.Result{}, fmt.Errorf("%w: action %q: %v", tools.ErrBadInput, s.Action, err)
		}
		layer := a.Layer()
		steps = append(steps, events.ProposalStep{
			Action:      s.Action,
			Layer:       layer,
			Params:      s.Params,
			Description: s.Description,
		})
		if !seenLayers[layer] {
			seenLayers[layer] = true
			layers = append(layers, layer)
		}
	}

	createCtx, cancel := context.WithTimeout(ctx, proposalTimeout)
	defer cancel()
	id, err := t.proposals.Create(createCtx, events.ProposalCreatedPayload{
		Title:          in.Title,
		Steps:          steps,
		Risk:           in.Risk,
		Rollback:       in.Rollback,
		RequiredLayers: layers,
	})
	if err != nil {
		return tools.Result{}, fmt.Errorf("agent: create proposal: %w", err)
	}
	if t.track != nil {
		t.track.record(id, steps)
	}
	return tools.Result{Summary: fmt.Sprintf("proposal %s waiting for approval from: %s", id, strings.Join(layers, ", "))}, nil
}

// placeholderTexts are the values a model sends when it has nothing real to put
// in risk or rollback. A live run stored them as "<no value>" for the approvers
// to read, so they are refused the same as an empty string.
var placeholderTexts = map[string]bool{
	"<no value>": true,
	"n/a":        true,
	"none":       true,
	"-":          true,
	"tbd":        true,
}

// checkProposalText refuses a risk or rollback that is empty, whitespace-only or
// a placeholder (compared case-insensitively). The error wraps tools.ErrBadInput
// so the model sees it as a failed tool call and retries with a real answer.
func checkProposalText(field, value string) error {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" || placeholderTexts[v] {
		return fmt.Errorf("%w: %s must describe the actual %s in words; empty or placeholder values such as n/a are refused", tools.ErrBadInput, field, field)
	}
	return nil
}

// compactSchema renders a JSON Schema object as a compact string for tool
// descriptions (the propose_fix description must list action param schemas).
func compactSchema(s map[string]any) string {
	b, err := json.Marshal(s)
	if err != nil {
		return "{}"
	}
	return string(b)
}
