package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// roomToolNames are the names of the room tools newRoomTools builds, kept
// beside it so both are edited together. RoomToolNames serves callers that
// only need the names from this list, so it never constructs a tool — a room
// tool constructor may one day dereference its deps, and a name lookup must
// not panic on a zero Deps. TestRoomToolNamesMatchRoomTools keeps the list in
// sync with what the constructor really builds.
func roomToolNames() []string {
	return []string{"update_hypothesis", "propose_fix", "post_message"}
}

// newRoomTools builds the three room tools the agent itself owns. These are
// tools.KindRoom: they only write to the room log or create proposals, never
// to customer systems. track records each created proposal so the agent can
// verify recovery when its last step's action_result arrives; it may be nil
// (the tool then just skips recording).
func newRoomTools(d Deps, track *proposalTracker) []tools.Tool {
	return newRoomToolsWith(d, track, nil)
}

// newRoomToolsWith is newRoomTools with the run's step ledger, which
// update_hypothesis checks the cited steps against. A nil ledger turns those
// checks off, for harnesses that build room tools without a runner.
func newRoomToolsWith(d Deps, track *proposalTracker, steps *stepLedger) []tools.Tool {
	return newRoomToolsWithGaps(d, track, steps, nil)
}

// newRoomToolsWithGaps is newRoomToolsWith with the run's read failures, so a
// healthy verdict is refused while a component is unreachable (KAN-408). A nil
// gaps turns the guard off, for the same harnesses as a nil ledger.
func newRoomToolsWithGaps(d Deps, track *proposalTracker, steps *stepLedger, gaps *readFailures) []tools.Tool {
	return []tools.Tool{
		&updateHypothesisTool{sink: d.Sink, steps: steps, gaps: gaps},
		&proposeFixTool{actions: d.Actions, proposals: d.Proposals, sink: d.Sink, track: track, steps: steps, log: d.Logger},
		&postMessageTool{sink: d.Sink, gaps: gaps},
	}
}

// roomToolServesKind reports whether a room of pipeline kind (already
// normalised, see RoomKind) whose propose_fix would list nActions write
// actions offers the room tool called name. A batch room with no action
// (ActionsForKind keeps no CDC action, and no action declares batch yet) is
// not offered propose_fix at all: its prompt says a person runs every fix, and
// an empty action enum with CDC examples would invite the call it forbids.
// Every other room tool serves every kind, and a CDC room keeps all three
// whatever its actions, so its tool list is unchanged.
func roomToolServesKind(name, kind string, nActions int) bool {
	return !(name == "propose_fix" && kind == events.PipelineKindBatch && nActions == 0)
}

// roomToolsForKind returns the room tools of all that a room of kind with
// acts (its kind's actions, ActionsForKind; nil is none) offers, in all's
// order, so the tool list of every room of one kind is the same.
func roomToolsForKind(all []tools.Tool, kind string, acts *actions.Registry) []tools.Tool {
	n := 0
	if acts != nil {
		n = len(acts.All())
	}
	out := make([]tools.Tool, 0, len(all))
	for _, t := range all {
		if roomToolServesKind(t.Name(), kind, n) {
			out = append(out, t)
		}
	}
	return out
}

// RoomToolNames returns the names of the room tools the agent adds on top of
// the read-tool registry, from the static list above. Exported for
// cmd/investigate's wiring test, which checks the system prompt only ever
// names tools the model can really call. The returned slice is a copy callers
// may keep or modify.
func RoomToolNames() []string {
	return roomToolNames()
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
	// steps is the run's step ledger; nil means the cited steps are not checked.
	steps *stepLedger
	// gaps is the run's failed reads; an unresolved unreachable component
	// refuses a healthy verdict (KAN-408). Nil turns the guard off.
	gaps *readFailures
}

// stepIDRe is the shape of a step id (newStepID): "s_" and 26 Crockford digits.
var stepIDRe = regexp.MustCompile(`^s_[0-9A-HJKMNP-TV-Z]{26}$`)

// maxKnownStepsListed is how many known step ids a refusal lists.
const maxKnownStepsListed = 8

// checkSteps refuses a hypothesis that cites ids that are not steps of this run, or
// that claims medium or high confidence with no step, or high confidence resting
// only on errored steps. An errored step may be cited: a tool error is a finding
// (the prompt says to cite it for what is unknown or blocked), so it is allowed
// and only a high-confidence claim needs a read that worked. The errors are
// tools.ErrBadInput: the model sees them and retries. After a restart the ledger
// is empty, so the model must re-read before it cites.
func (t *updateHypothesisTool) checkSteps(in hypothesisInput) error {
	if t.steps == nil {
		return nil
	}
	// A step the ledger knows is accepted whatever its shape (the triage pass's ids are
	// shown to the model as given); only an unknown id is told apart as malformed or stale.
	for _, id := range in.BasedOnSteps {
		if _, ok := t.steps.get(id); ok {
			continue
		}
		if !stepIDRe.MatchString(id) {
			return fmt.Errorf("%w: based_on_steps: %q is not a step id; cite the step= attribute of a tool_output in this run%s", tools.ErrBadInput, clipID(id), t.knownSteps())
		}
		return fmt.Errorf("%w: based_on_steps: %s is not a step of this run%s", tools.ErrBadInput, id, t.knownSteps())
	}
	if len(in.BasedOnSteps) == 0 && in.Confidence != "low" {
		return fmt.Errorf("%w: medium or high confidence needs at least one supporting step", tools.ErrBadInput)
	}
	if in.Confidence == "high" && len(in.BasedOnSteps) > 0 {
		allErrored := true
		for _, id := range in.BasedOnSteps {
			if f, _ := t.steps.get(id); !f.isError {
				allErrored = false
				break
			}
		}
		if allErrored {
			return fmt.Errorf("%w: based_on_steps: every cited step errored; high confidence needs a step that returned a result", tools.ErrBadInput)
		}
	}
	return nil
}

// unbacked returns the terms of the hypothesis that neither its cited steps nor
// the alert background show (see groundTerms). Without a ledger nothing is known about the steps,
// so nothing is marked.
func (t *updateHypothesisTool) unbacked(in hypothesisInput) []string {
	if t.steps == nil {
		return nil
	}
	// What backs a term: the cited steps' summaries and the inputs they were called
	// with (the model named the connector or topic there), and the alert text and
	// pipeline map it was handed up front. Naming a component from those is not
	// inventing it; the note must not push the model to drop it.
	backing := make([]string, 0, 2*len(in.BasedOnSteps)+1)
	for _, id := range in.BasedOnSteps {
		if f, ok := t.steps.get(id); ok {
			backing = append(backing, f.summary, f.input)
		}
	}
	backing = append(backing, t.steps.backgroundText())
	return groundTerms(in.Text, backing)
}

// knownSteps renders the newest known step ids for a refusal, or says there are none.
func (t *updateHypothesisTool) knownSteps() string {
	ids := t.steps.recent(maxKnownStepsListed)
	if len(ids) == 0 {
		return " (no steps recorded yet; run a read tool first)"
	}
	return " (known: " + strings.Join(ids, ", ") + ")"
}

// clipID shortens a model-supplied id for an error message.
func clipID(s string) string {
	const max = 80
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && s8(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
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
	if err := t.checkSteps(in); err != nil {
		return tools.Result{}, err
	}
	// A healthy verdict is refused while a read left a component unreachable
	// (KAN-408): the eval's scenario 13 read a registry connection error as
	// benign because the pipeline still moved data. The refusal names the gap
	// and lets "no data loss yet, risk rising" through.
	if err := checkHealthyVerdict(t.gaps, in.Text); err != nil {
		return tools.Result{}, err
	}
	if t.sink == nil {
		return tools.Result{}, fmt.Errorf("agent: no sink available for update_hypothesis")
	}
	unbacked := t.unbacked(in)
	if err := t.sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{
		Text:          in.Text,
		BasedOnSteps:  in.BasedOnSteps,
		Confidence:    in.Confidence,
		Confirmed:     false,
		UnbackedTerms: unbacked,
	}); err != nil {
		return tools.Result{}, fmt.Errorf("agent: emit hypothesis: %w", err)
	}
	t.steps.setHypothesisSteps(in.BasedOnSteps)
	return tools.Result{Summary: "hypothesis recorded" + unbackedNote(unbacked)}, nil
}

// ---- post_message ----

type postMessageTool struct {
	sink Sink
	// gaps is the run's failed reads; a message that claims a healthy verdict
	// is refused while a component is unreachable (KAN-408). Nil turns the
	// guard off.
	gaps *readFailures
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
	if err := checkHealthyVerdict(t.gaps, in.Text); err != nil {
		return tools.Result{}, err
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
	// steps is the run's step ledger, which holds the injection-flagged
	// reads the echo guard checks each step against (echoguard.go); nil
	// turns the guard off, for harnesses that build the tool without a
	// runner.
	steps *stepLedger
	// log takes the guard's one warn line per refused step; nil logs nothing.
	log *slog.Logger
}

func (t *proposeFixTool) Name() string { return "propose_fix" }
func (t *proposeFixTool) Kind() tools.Kind {
	return tools.KindRoom
}

// Description lists every available action as name(param:type, ...) with its
// layer and a one-line explanation. The description is not part of the strict
// grammar, so this is where each action's parameters live: the strict schema
// stays the same size however many actions are registered, and the params are
// checked against the action's own schema in Run. Optional params end in "?".
func (t *proposeFixTool) Description() string {
	var b strings.Builder
	b.WriteString("Propose a fix as a sequence of write actions. Nothing runs until every required layer owner approves. ")
	b.WriteString("Fix only the cause you established, with the smallest change; propose a resume or restart only after reads confirm nothing protective stopped it. ")
	b.WriteString("Available actions, as name(param:type, ...) [layer]: explanation:\n")
	acts := t.actionsList()
	if len(acts) == 0 {
		b.WriteString("(none)\n")
	}
	for _, a := range acts {
		fmt.Fprintf(&b, "- %s(%s) [%s]: %s\n", a.Name(), paramsSignature(a.ParamsSchema()), a.Layer(), a.Describe())
	}
	b.WriteString("steps is an array; each step has action, params and a short description, and may have verify (at most 200 characters): what you will check after the step runs, for example \"task 0 RUNNING within 60 s\". ")
	b.WriteString(`params is the action's parameters as JSON object text in a string, for example "{\"connector\":\"c1\"}". `)
	b.WriteString(`A param that is itself JSON, such as the Avro schema of schema_register, is escaped once more: "{\"subject\":\"orders-value\",\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}". `)
	b.WriteString("A step with a wrong or missing param is refused and nothing is stored. ")
	b.WriteString("Params are hashed for the approvers: a number above 2^53 (a Kafka offset, a large id) cannot be hashed exactly and the proposal is refused, so write such values as strings. ")
	b.WriteString("risk and rollback are required short text: never empty and never a placeholder such as n/a.")
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

// stepsItemSchema builds the schema for one proposal step. The step is the same
// closed object whatever actions are registered: action is an enum of their
// names (about one short string each), and params is a string holding the
// params object as JSON text. A strict object must be closed
// (additionalProperties:false), so an object-typed params cannot carry
// arbitrary keys; JSON text can. Run parses it and checks it against the
// chosen action's own ParamsSchema and Validate before anything is stored.
// Without actions the action is a plain string, which Run then refuses.
func (t *proposeFixTool) stepsItemSchema() map[string]any {
	action := map[string]any{"type": "string", "description": "action name"}
	if acts := t.actionsList(); len(acts) > 0 {
		names := make([]string, 0, len(acts))
		for _, a := range acts {
			names = append(names, a.Name())
		}
		action = map[string]any{"type": "string", "enum": names}
	}
	return roomObjectSchema(map[string]any{
		"action":      action,
		"params":      map[string]any{"type": "string", "description": "params as JSON object text"},
		"description": map[string]any{"type": "string", "description": "what this step does"},
		"verify":      map[string]any{"type": "string", "description": "optional: what you will check after this step runs"},
	}, "action", "params", "description")
}

// validationSchema is the schema runRoomTool checks propose_fix input against
// before Run. It differs from InputSchema in the two step fields Run checks
// itself with better errors: action is any string (Run names the registered
// actions when it is unknown) and params has no type (Run accepts the JSON
// text the model sends or an already-decoded object, then checks it against
// the action's own schema).
func (t *proposeFixTool) validationSchema() map[string]any {
	s := t.InputSchema()
	props, _ := s["properties"].(map[string]any)
	steps, _ := props["steps"].(map[string]any)
	steps["items"] = roomObjectSchema(map[string]any{
		"action":      map[string]any{"type": "string"},
		"params":      map[string]any{},
		"description": map[string]any{"type": "string"},
		"verify":      map[string]any{"type": "string"},
	}, "action", "params", "description")
	return s
}

// maxStepVerifyChars caps ProposalStep.Verify (contract: 200 characters).
const maxStepVerifyChars = 200

type proposeFixInput struct {
	Title string `json:"title"`
	Steps []struct {
		Action string `json:"action"`
		// Params is JSON text inside a string on the wire (see
		// stepsItemSchema). An already-decoded object is accepted too so
		// callers that are not the model, such as replays, need no encoding.
		Params      json.RawMessage `json:"params"`
		Description string          `json:"description"`
		Verify      string          `json:"verify"`
	} `json:"steps"`
	Risk     string `json:"risk"`
	Rollback string `json:"rollback"`
}

// decodeStepParams returns the params object a step carries: the JSON text
// inside a string, or an object given directly. It refuses anything that is
// not a JSON object, so Validate and the action schema only ever see objects.
func decodeStepParams(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("params is required: give the action's parameters as a JSON object")
	}
	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return nil, fmt.Errorf("params is not a valid string: %v", err)
		}
		trimmed = bytes.TrimSpace([]byte(text))
		if !json.Valid(trimmed) {
			return nil, fmt.Errorf("params must be valid JSON object text, for example {\"connector\":\"c1\"}")
		}
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("params must be a JSON object, not another JSON value")
	}
	// Go keeps the last of two equal keys, but the stored text keeps both and
	// an approver or another parser may read the first. Refuse the ambiguity.
	if key, dup := duplicateKey(trimmed); dup {
		return nil, fmt.Errorf("params has the field %q more than once", key)
	}
	return json.RawMessage(trimmed), nil
}

// duplicateKey reports the first object key that appears twice in the same
// object anywhere in valid JSON text.
func duplicateKey(raw []byte) (string, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	type frame struct {
		isObj  bool
		seen   map[string]bool
		expect bool // in an object: the next string token is a key
	}
	var stack []*frame
	// valueDone marks the end of a value so the enclosing object expects a key.
	valueDone := func() {
		if n := len(stack); n > 0 && stack[n-1].isObj {
			stack[n-1].expect = true
		}
	}
	for {
		tok, err := dec.Token()
		if err != nil {
			return "", false
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{':
				stack = append(stack, &frame{isObj: true, seen: map[string]bool{}, expect: true})
			case '[':
				stack = append(stack, &frame{})
			default:
				stack = stack[:len(stack)-1]
				valueDone()
			}
			continue
		}
		if n := len(stack); n > 0 && stack[n-1].isObj && stack[n-1].expect {
			key, _ := tok.(string)
			if stack[n-1].seen[key] {
				return key, true
			}
			stack[n-1].seen[key] = true
			stack[n-1].expect = false
			continue
		}
		valueDone()
	}
}

// StepObserver is implemented by an action wrapper that wants to see every
// step check, including the ones that end before Validate. propose_fix calls
// StepChecked when it refuses a step's params for a bad shape (not JSON, not
// an object, a repeated field, or a mismatch with the action's ParamsSchema),
// so a harness that records Validate calls (the evals attempt log) still sees
// an attempt it would otherwise lose. err is the reason the step was refused.
type StepObserver interface {
	StepChecked(params json.RawMessage, err error)
}

// observeStep tells a if it is a StepObserver.
func observeStep(a actions.Action, params json.RawMessage, err error) {
	if o, ok := a.(StepObserver); ok {
		o.StepChecked(params, err)
	}
}

// NewProposeFixTool returns the propose_fix room tool over reg and proposals,
// for harnesses (the evals) that drive it without a model. Proposals created
// through it go through the same checks as the agent's own tool.
func NewProposeFixTool(reg *actions.Registry, proposals Proposals) tools.Tool {
	return &proposeFixTool{actions: reg, proposals: proposals}
}

// checkStep resolves a step to its action and decoded params, or returns the
// error the model sees. Order: the action must exist, params must be a JSON
// object that fits the action's ParamsSchema (types, required fields, enums,
// no unknown fields), then the action's own Validate. None of it stores
// anything. The schema check runs first because it names the bad field, which
// the action's Validate usually does not.
func (t *proposeFixTool) checkStep(ctx context.Context, i int, action string, rawParams json.RawMessage) (actions.Action, json.RawMessage, error) {
	if t.actions == nil {
		return nil, nil, fmt.Errorf("agent: no actions are available")
	}
	a, ok := t.actions.Get(action)
	if !ok {
		return nil, nil, fmt.Errorf("%w: step %d: unknown action %q; available: %s", tools.ErrBadInput, i+1, action, t.actionNames())
	}
	params, err := decodeStepParams(rawParams)
	if err != nil {
		observeStep(a, rawParams, err)
		return nil, nil, fmt.Errorf("%w: step %d (%s): %v", tools.ErrBadInput, i+1, action, err)
	}
	if schema := a.ParamsSchema(); schema != nil {
		if err := validateInput(schema, params); err != nil {
			observeStep(a, params, err)
			return nil, nil, fmt.Errorf("%w: step %d (%s): params: %v", tools.ErrBadInput, i+1, action, err)
		}
	}
	if err := a.Validate(ctx, params); err != nil {
		return nil, nil, fmt.Errorf("%w: step %d (%s): %v", tools.ErrBadInput, i+1, action, err)
	}
	if kinds, err := t.steps.echoedInjection(i+1, action, params); err != nil {
		// The echo guard (P3-W3b): the step repeats what an injection-flagged
		// read asked for. The model sees the error; the room sees nothing
		// but this failed tool call. The log line carries no span text.
		if t.log != nil {
			t.log.Warn("agent: propose_fix step echoes injection-flagged text", "tool", t.Name(), "step", i+1, "action", action, "kinds", kinds)
		}
		observeStep(a, params, err)
		return nil, nil, err
	}
	return a, params, nil
}

func (t *proposeFixTool) actionNames() string {
	acts := t.actionsList()
	names := make([]string, 0, len(acts))
	for _, a := range acts {
		names = append(names, a.Name())
	}
	return strings.Join(names, ", ")
}

// paramsSignature renders an action's params schema as "name:type, ..." for
// the tool description. Required params come first in schema order, optional
// ones follow sorted and end in "?". An enum shows its values and a property
// description shows in brackets, so allowed values stay visible to the model
// even though they are no longer in the strict schema.
func paramsSignature(schema map[string]any) string {
	props, _ := schema["properties"].(map[string]any)
	if len(props) == 0 {
		return ""
	}
	var required []string
	switch r := schema["required"].(type) {
	case []string:
		required = r
	case []any:
		for _, x := range r {
			if s, ok := x.(string); ok {
				required = append(required, s)
			}
		}
	}
	isReq := map[string]bool{}
	var names []string
	for _, n := range required {
		if _, ok := props[n]; ok && !isReq[n] {
			isReq[n] = true
			names = append(names, n)
		}
	}
	var optional []string
	for n := range props {
		if !isReq[n] {
			optional = append(optional, n)
		}
	}
	sort.Strings(optional)
	names = append(names, optional...)

	parts := make([]string, 0, len(names))
	for _, n := range names {
		sub, _ := props[n].(map[string]any)
		s := n
		if !isReq[n] {
			s += "?"
		}
		s += ":" + schemaTypeText(sub)
		if desc, _ := sub["description"].(string); desc != "" {
			s += " [" + desc + "]"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

// schemaTypeText is the short type name of one param: its enum values joined
// with "|" when it has an enum, else its JSON type, with array<T> for arrays.
func schemaTypeText(s map[string]any) string {
	if enum := schemaEnum(s); len(enum) > 0 {
		vals := make([]string, len(enum))
		for i, v := range enum {
			vals[i] = fmt.Sprint(v)
		}
		return strings.Join(vals, "|")
	}
	typ, _ := s["type"].(string)
	switch typ {
	case "":
		return "any"
	case "array":
		if items, ok := s["items"].(map[string]any); ok {
			return "array<" + schemaTypeText(items) + ">"
		}
	case "object":
		if _, ok := s["properties"]; ok {
			return "{" + paramsSignature(s) + "}"
		}
	}
	return typ
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

	steps := make([]events.ProposalStep, 0, len(in.Steps))
	seenLayers := map[string]bool{}
	var layers []string
	for i, s := range in.Steps {
		a, params, err := t.checkStep(ctx, i, s.Action, s.Params)
		if err != nil {
			return tools.Result{}, err
		}
		layer := a.Layer()
		verify := strings.TrimSpace(s.Verify)
		if n := utf8.RuneCountInString(verify); n > maxStepVerifyChars {
			return tools.Result{}, fmt.Errorf("%w: step %d: verify is %d characters; the limit is %d", tools.ErrBadInput, i+1, n, maxStepVerifyChars)
		}
		steps = append(steps, events.ProposalStep{
			Action:      s.Action,
			Layer:       layer,
			Params:      params,
			Description: s.Description,
			Verify:      verify,
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
	return tools.Result{
		Summary: fmt.Sprintf("proposal %s waiting for approval from: %s", id, strings.Join(layers, ", ")),
		Data:    proposeFixResult{ProposalID: id},
	}, nil
}

// proposeFixResult is the structured result of a created proposal. The runner
// reads the ID to take the proposal's creation baseline.
type proposeFixResult struct {
	ProposalID string `json:"proposal_id"`
}

// isPlaceholderText reports the values a model sends when it has nothing real
// to put in risk or rollback. A live run stored them as "<no value>" for the
// approvers to read, so they are refused the same as an empty string. v is
// already trimmed and lower-cased.
func isPlaceholderText(v string) bool {
	switch v {
	case "<no value>", "n/a", "none", "-", "tbd":
		return true
	}
	return false
}

// checkProposalText refuses a risk or rollback that is empty, whitespace-only or
// a placeholder (compared case-insensitively). The error wraps tools.ErrBadInput
// so the model sees it as a failed tool call and retries with a real answer.
func checkProposalText(field, value string) error {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" || isPlaceholderText(v) {
		return fmt.Errorf("%w: %s must describe the actual %s in words; empty or placeholder values such as n/a are refused", tools.ErrBadInput, field, field)
	}
	return nil
}
