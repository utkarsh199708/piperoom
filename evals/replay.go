package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/runbooks"
	"piperoom/internal/tools/similarincidents"
)

// errNoRecording is what the model sees when it calls a tool with an input the
// scenario has no recording for. It reads like a target that returned nothing,
// so the run carries on; the miss is counted in the report so the scenario
// author can add a recording.
var errNoRecording = errors.New("no data available for this request")

// errWritesDisabled guards the replay clients: an eval must never write, and
// no write can reach a real system from here anyway.
var errWritesDisabled = errors.New("evals: writes are disabled in replay")

// errNotReplayed is returned by replay client reads that action validation
// does not need.
var errNotReplayed = errors.New("evals: not available in replay")

// Miss is one tool call the scenario had no recording for.
type Miss struct {
	Tool  string          `json:"tool"`
	Input json.RawMessage `json:"input"`
}

// LiveTools returns the read-only tools the agent gets in production, in
// production order. For tools that read live systems only the names,
// descriptions and schemas are used: the harness wraps each one and never
// calls its Run, which is why the clients can be nil here. Pure tools (see
// runsLive) are the exception and run for real.
func LiveTools() ([]tools.Tool, error) {
	reg := tools.NewRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{}); err != nil {
		return nil, fmt.Errorf("evals: kafka tool definitions: %w", err)
	}
	// dbtools refuses a nil Owners (a wiring guard); an empty one is enough
	// for definitions.
	if err := dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{}}); err != nil {
		return nil, fmt.Errorf("evals: db tool definitions: %w", err)
	}
	if err := runbooks.Register(reg); err != nil {
		return nil, fmt.Errorf("evals: runbook tool definitions: %w", err)
	}
	// Definition only (no room store): scenarios record its answers, the
	// harness never runs it. See similar_fixture_test.go.
	if err := similarincidents.Register(reg, similarincidents.Deps{}); err != nil {
		return nil, fmt.Errorf("evals: similar_incidents tool definition: %w", err)
	}
	return reg.All(), nil
}

// LiveActions returns the production write actions, backed by replay clients
// that know only res and the connector states in recs. The agent can propose
// them (their real Validate runs against that) but nothing can execute: every
// write returns an error.
func LiveActions(res Resources, recs []Recording) (*actions.Registry, error) {
	statuses, err := connectStatuses(recs)
	if err != nil {
		return nil, err
	}
	reg := actions.NewRegistry()
	if err := actions.Register(reg, actions.Deps{
		Connect:  replayConnect{connectors: res.Connectors, statuses: statuses},
		Registry: replayRegistry{subjects: res.Subjects},
	}); err != nil {
		return nil, fmt.Errorf("evals: action definitions: %w", err)
	}
	return reg, nil
}

// connectStatusData is the part of connect_status's Data that action
// validation reads (see kafkatools connectStatus: name, state, worker, tasks).
type connectStatusData struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Worker string `json:"worker"`
	Tasks  []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
	} `json:"tasks"`
}

// connectStatuses returns the connector state each connect_status recording
// with Data reports, keyed by connector. Validate of connect_resume_connector
// reads that state (the restart actions only check the connector exists), so
// a scenario that proposes a resume records it in the status data, as the real
// tool returns it. The first recording for a connector wins; for a sequence,
// its first observation. Validation keeps that recording out of later phases
// (checkStatusPhase), so proposals validate against the start state.
func connectStatuses(recs []Recording) (map[string]clients.ConnectorStatus, error) {
	out := map[string]clients.ConnectorStatus{}
	for i, r := range recs {
		data := recordingData(r)
		if r.Tool != "connect_status" || data == nil {
			continue
		}
		name, _ := r.Input["connector"].(string)
		if name == "" {
			continue
		}
		if _, dup := out[name]; dup {
			continue
		}
		b, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("evals: recordings[%d] (connect_status): data: %w", i, err)
		}
		var d connectStatusData
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, fmt.Errorf("evals: recordings[%d] (connect_status): data is not a connector status: %w", i, err)
		}
		st := clients.ConnectorStatus{Name: d.Name, State: d.State, WorkerID: d.Worker}
		if st.Name == "" {
			st.Name = name
		}
		for _, t := range d.Tasks {
			st.Tasks = append(st.Tasks, clients.TaskStatus{ID: t.ID, State: t.State})
		}
		out[name] = st
	}
	return out, nil
}

// replayer serves recordings for one run. It owns the run's replay state: the
// current phase, how often each tool was called, where each sequence is, and
// the marks the scorer reads (see ReplayTrace). One mutex guards all of it, so
// concurrent tool calls see a consistent phase and every sequence position is
// handed out exactly once.
type replayer struct {
	recs        []replayRecord // in file order
	byTool      map[string][]int
	transitions []Transition
	// events returns how many events the run has recorded so far. Each mark
	// stores it so the scorer can place the mark among the events. Nil
	// counts zero.
	events func() int

	mu    sync.Mutex
	phase string
	// from is the phase the run left on its last transition; a serve in
	// the new phase is compared with what that phase would have answered.
	from       string
	visited    []string
	calls      map[string]int
	seqPos     map[int]int // recording index -> next observation
	marks      []ReplayMark
	unexecuted []string
	misses     []Miss
}

// replayRecord is a Recording with its results already converted to JSON, so
// a result reaches the room log exactly as a real tool's Data would.
type replayRecord struct {
	Recording
	// obs holds one result for a plain recording, the sequence otherwise.
	obs []replayObs
}

// replayObs is one result a recording can give.
type replayObs struct {
	summary   string
	data      json.RawMessage
	truncated bool
	err       string
}

// ReplayMark is one replay event of a run, in order. AfterEvents is how many
// agent events had been recorded when it happened, which places it among the
// transcript's events.
type ReplayMark struct {
	AfterEvents int `json:"after_events"`
	// Kind is markServe, markExecuted or markTransition.
	Kind string `json:"kind"`
	// Tool, Recording and Observation: a serve of a phase-tagged recording
	// (index in the scenario's recordings, index in its sequence).
	Tool        string `json:"tool,omitempty"`
	Recording   *int   `json:"recording,omitempty"`
	Observation int    `json:"observation,omitempty"`
	// Phase is the recording's phase for a serve, the phase entered for a
	// transition.
	Phase string `json:"phase,omitempty"`
	From  string `json:"from,omitempty"`
	// Changed: a serve whose result differs from what the same call got
	// in the phase the run came from, so it shows the new state.
	Changed bool `json:"changed,omitempty"`
	// Action and OK: an executed proposal step. ParamsMismatch: the step's
	// params were not the scripted ones, so it failed and fired nothing.
	Action         string `json:"action,omitempty"`
	OK             *bool  `json:"ok,omitempty"`
	ParamsMismatch bool   `json:"params_mismatch,omitempty"`
	// Transition is the index of the transition that fired (in the
	// scenario's transitions); Cause names it, e.g. "transitions[0]".
	Transition *int   `json:"transition,omitempty"`
	Cause      string `json:"cause,omitempty"`
}

// Mark kinds.
const (
	// markServe: a call was answered from a recording tagged with a phase.
	// Recordings without a phase leave no mark.
	markServe = "serve"
	// markExecuted: the harness executed a proposal step.
	markExecuted = "executed"
	// markTransition: the run moved to another phase.
	markTransition = "transition"
)

// ReplayTrace is what a run's replay did, for the scorer and the transcript.
type ReplayTrace struct {
	// Phases are the phases the run was in, in order, start phase first.
	Phases []string     `json:"phases,omitempty"`
	Marks  []ReplayMark `json:"marks,omitempty"`
	// Unexecuted are scripted action results the harness skipped because no
	// proposal of the run had that action to execute.
	Unexecuted []string `json:"unexecuted_inputs,omitempty"`
}

// newReplayer replays recs as a static scenario: no phases, no transitions.
func newReplayer(recs []Recording) (*replayer, error) {
	return newPhasedReplayer(recs, nil, nil)
}

// newScenarioReplayer replays sc through its phases and transitions.
func newScenarioReplayer(sc Scenario) (*replayer, error) {
	return newPhasedReplayer(sc.Recordings, sc.Phases, sc.Transitions)
}

func newPhasedReplayer(recs []Recording, phases []string, transitions []Transition) (*replayer, error) {
	r := &replayer{
		byTool:      map[string][]int{},
		transitions: transitions,
		calls:       map[string]int{},
		seqPos:      map[int]int{},
	}
	if len(phases) > 0 {
		r.phase = phases[0]
		r.visited = []string{phases[0]}
	}
	for i, rec := range recs {
		rr := replayRecord{Recording: rec}
		results := recordingResults(rec)
		if len(results) == 0 {
			return nil, fmt.Errorf("evals: recordings[%d] (%s): sequence is empty", i, rec.Tool)
		}
		for j, o := range results {
			ob := replayObs{summary: o.Summary, truncated: o.Truncated, err: o.Error}
			if o.Data != nil {
				b, err := json.Marshal(o.Data)
				if err != nil {
					return nil, fmt.Errorf("evals: recordings[%d] (%s): data of result %d: %w", i, rec.Tool, j, err)
				}
				ob.data = b
			}
			rr.obs = append(rr.obs, ob)
		}
		r.recs = append(r.recs, rr)
		r.byTool[rec.Tool] = append(r.byTool[rec.Tool], i)
	}
	return r, nil
}

// runsLive reports whether a tool runs for real during an eval instead of
// from recordings. runbook_lookup reads only text embedded in the binary, so
// its live answer is the production answer and no scenario has to record it.
func runsLive(name string) bool { return name == runbooks.ToolName }

// registry wraps every definition in a replay tool, except pure tools, which
// are registered as they are.
func (r *replayer) registry(defs []tools.Tool) (*tools.Registry, error) {
	reg := tools.NewRegistry()
	for _, d := range defs {
		var t tools.Tool = &replayTool{def: d, r: r}
		if runsLive(d.Name()) {
			t = d
		}
		if err := reg.Register(t); err != nil {
			return nil, fmt.Errorf("evals: register replay tool: %w", err)
		}
	}
	return reg, nil
}

func (r *replayer) Misses() []Miss {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Miss(nil), r.misses...)
}

// lookup finds the recording that would answer a call now, without serving
// it: no sequence advances and no call is counted.
func (r *replayer) lookup(tool string, input json.RawMessage) (replayRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.match(tool, input)
	if !ok {
		return replayRecord{}, false
	}
	return r.recs[i], true
}

// match returns the index of the best recording for a call in the current
// phase (see Recording for the rules). The caller holds r.mu.
func (r *replayer) match(tool string, input json.RawMessage) (int, bool) {
	var call map[string]any
	if len(input) > 0 {
		if err := json.Unmarshal(input, &call); err != nil {
			return -1, false
		}
	}
	best, bestScore, bestPhased := -1, -1, false
	for _, i := range r.byTool[tool] {
		rec := r.recs[i]
		if rec.Phase != "" && rec.Phase != r.phase {
			continue
		}
		if !matches(rec.Input, call) {
			continue
		}
		// More input fields win. On a tie the current phase's recording
		// beats one that answers in every phase, so a phase can override a
		// shared answer without being more specific than it.
		score, phased := len(rec.Input), rec.Phase != ""
		if score > bestScore || (score == bestScore && phased && !bestPhased) {
			best, bestScore, bestPhased = i, score, phased
		}
	}
	return best, best >= 0
}

// serve answers one call: it finds the recording, hands out the next
// observation of its sequence, counts the call and fires any after_calls
// transition the count reaches. The call is answered before it is counted, so
// the n-th call still sees the old phase.
func (r *replayer) serve(tool string, input json.RawMessage) (replayObs, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.countCall(tool)
	i, ok := r.match(tool, input)
	if !ok {
		r.misses = append(r.misses, Miss{Tool: tool, Input: append(json.RawMessage(nil), input...)})
		return replayObs{}, false
	}
	rec := r.recs[i]
	pos := r.seqPos[i]
	if rec.Phase != "" {
		changed := r.changedFromLast(tool, input, rec.obs[pos])
		idx := i
		r.mark(ReplayMark{Kind: markServe, Tool: tool, Recording: &idx, Observation: pos, Phase: rec.Phase, Changed: changed})
	}
	if pos < len(rec.obs)-1 {
		r.seqPos[i] = pos + 1
	}
	return rec.obs[pos], true
}

// changedFromLast reports whether ob differs from what the same call would
// get in the phase the run last left, without serving that answer (no
// sequence advances). A call that phase could not answer counts as changed.
// The caller holds r.mu.
func (r *replayer) changedFromLast(tool string, input json.RawMessage, ob replayObs) bool {
	if r.from == "" {
		return false
	}
	cur := r.phase
	r.phase = r.from
	j, ok := r.match(tool, input)
	r.phase = cur
	if !ok {
		return true
	}
	old := r.recs[j].obs[r.seqPos[j]]
	return old.summary != ob.summary || old.err != ob.err || old.truncated != ob.truncated || string(old.data) != string(ob.data)
}

// countCall counts one call of tool and fires the after_calls transitions it
// completes. The caller holds r.mu.
func (r *replayer) countCall(tool string) {
	r.calls[tool]++
	for i, t := range r.transitions {
		if ac := t.On.AfterCalls; ac != nil && t.On.Action == "" && ac.Tool == tool && ac.N == r.calls[tool] && r.inFrom(t) {
			r.enter(t.To, i)
		}
	}
}

// inFrom reports whether t may fire in the current phase. The caller holds
// r.mu.
func (r *replayer) inFrom(t Transition) bool {
	return t.From == "" || t.From == r.phase
}

// executed records that the harness executed a proposal step and fires the
// first action transition that matches it. ok nil is an unknown outcome.
func (r *replayer) executed(action string, ok *bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mark(ReplayMark{Kind: markExecuted, Action: action, OK: ok})
	for i, t := range r.transitions {
		on := t.On
		if on.Action == "" || on.AfterCalls != nil || on.Action != action {
			continue
		}
		if on.OK != nil && (ok == nil || *ok != *on.OK) {
			continue
		}
		if !r.inFrom(t) {
			continue
		}
		r.enter(t.To, i)
		return
	}
}

// executedWrongParams records an executed step whose params were not the
// scripted ones: it failed, and nothing fires.
func (r *replayer) executedWrongParams(action string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	failed := false
	r.mark(ReplayMark{Kind: markExecuted, Action: action, OK: &failed, ParamsMismatch: true})
}

// enter moves the run to phase to through transition idx. The caller holds
// r.mu.
func (r *replayer) enter(to string, idx int) {
	if to == r.phase {
		return
	}
	r.mark(ReplayMark{Kind: markTransition, From: r.phase, Phase: to, Transition: &idx, Cause: fmt.Sprintf("transitions[%d]", idx)})
	r.from = r.phase
	r.phase = to
	r.visited = append(r.visited, to)
}

// mark appends m, stamped with the current event count. The caller holds
// r.mu.
func (r *replayer) mark(m ReplayMark) {
	if r.events != nil {
		m.AfterEvents = r.events()
	}
	r.marks = append(r.marks, m)
}

// skip records a scripted action result that could not be executed.
func (r *replayer) skip(why string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unexecuted = append(r.unexecuted, why)
}

// currentPhase is the phase the run is in now.
func (r *replayer) currentPhase() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.phase
}

// trace returns what the replay did, or nil when there is nothing to report
// (a static scenario that executed nothing), so static transcripts are
// unchanged.
func (r *replayer) trace() *ReplayTrace {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.visited) == 0 && len(r.marks) == 0 && len(r.unexecuted) == 0 {
		return nil
	}
	return &ReplayTrace{
		Phases:     append([]string(nil), r.visited...),
		Marks:      append([]ReplayMark(nil), r.marks...),
		Unexecuted: append([]string(nil), r.unexecuted...),
	}
}

// matches reports whether every field of want is in got with an equal value.
// Both sides come from encoding/json, so numbers are float64 on both and
// reflect.DeepEqual compares them by value.
func matches(want, got map[string]any) bool {
	for k, wv := range want {
		gv, ok := got[k]
		if !ok || !reflect.DeepEqual(normalize(wv), normalize(gv)) {
			return false
		}
	}
	return true
}

// normalize round-trips v through JSON so a value decoded from YAML (int,
// map[string]any) compares equal to the same value decoded from a call's JSON
// (float64).
func normalize(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return v
	}
	return out
}

// replayTool presents a production tool's definition to the model and answers
// from recordings.
type replayTool struct {
	def tools.Tool
	r   *replayer
}

func (t *replayTool) Name() string                { return t.def.Name() }
func (t *replayTool) Description() string         { return t.def.Description() }
func (t *replayTool) InputSchema() map[string]any { return t.def.InputSchema() }
func (t *replayTool) Kind() tools.Kind            { return t.def.Kind() }

func (t *replayTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if err := ctx.Err(); err != nil {
		return tools.Result{}, err
	}
	ob, ok := t.r.serve(t.def.Name(), input)
	if !ok {
		return tools.Result{}, errNoRecording
	}
	if ob.err != "" {
		return tools.Result{}, errors.New(ob.err)
	}
	res := tools.Result{Summary: ob.summary, Truncated: ob.truncated}
	if len(ob.data) > 0 {
		res.Data = ob.data
	}
	return res, nil
}

// replayConnect answers the Connect reads that action validation needs: the
// connector names and, where a recording gives it, each connector's status.
type replayConnect struct {
	connectors []string
	statuses   map[string]clients.ConnectorStatus
}

func (c replayConnect) ListConnectors(context.Context) ([]string, error) {
	return append([]string(nil), c.connectors...), nil
}
func (c replayConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	if st, ok := c.statuses[name]; ok {
		return st, nil
	}
	return clients.ConnectorStatus{}, errNotReplayed
}
func (replayConnect) Config(context.Context, string) (map[string]string, error) {
	return nil, errNotReplayed
}
func (replayConnect) RestartTask(context.Context, string, int) error { return errWritesDisabled }
func (replayConnect) RestartConnector(context.Context, string) error { return errWritesDisabled }

// Resume matches the connectorResumer interface that connect_resume_connector
// Execute asserts for, so a replayed resume reaches this refusal and never a
// client that does not know the method.
func (replayConnect) Resume(context.Context, string) error { return errWritesDisabled }

// replayRegistry answers the one Schema Registry read that action validation
// needs.
type replayRegistry struct{ subjects []string }

func (r replayRegistry) Subjects(context.Context) ([]string, error) {
	return append([]string(nil), r.subjects...), nil
}
func (replayRegistry) Versions(context.Context, string) ([]int, error) { return nil, errNotReplayed }
func (replayRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, errNotReplayed
}
func (replayRegistry) Compatibility(context.Context, string) (string, bool, error) {
	return "", false, errNotReplayed
}
func (replayRegistry) SetCompatibility(context.Context, string, string) error {
	return errWritesDisabled
}
func (replayRegistry) Register(context.Context, string, string) (int, error) {
	return 0, errWritesDisabled
}

// ActionAttempt is one Validate call made while the agent proposed a fix.
// propose_fix validates every step before it creates the proposal, and a
// rejected proposal leaves no event in the room log, so this log is the only
// record of what the agent tried.
type ActionAttempt struct {
	Action string          `json:"action"`
	Params json.RawMessage `json:"params"`
	Error  string          `json:"error,omitempty"` // empty when Validate passed
}

// attemptLog records every Validate call on the wrapped actions of one run.
type attemptLog struct {
	mu       sync.Mutex
	attempts []ActionAttempt
}

// wrap returns a registry whose actions record each Validate call in l and
// otherwise behave exactly like reg's.
func (l *attemptLog) wrap(reg *actions.Registry) (*actions.Registry, error) {
	out := actions.NewRegistry()
	for _, a := range reg.All() {
		if err := out.Register(&recordingAction{Action: a, log: l}); err != nil {
			return nil, fmt.Errorf("evals: wrap action: %w", err)
		}
	}
	return out, nil
}

func (l *attemptLog) snapshot() []ActionAttempt {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ActionAttempt(nil), l.attempts...)
}

var _ agent.StepObserver = (*recordingAction)(nil)

// recordingAction is an action whose Validate calls are logged.
type recordingAction struct {
	actions.Action
	log *attemptLog
}

func (a *recordingAction) Validate(params json.RawMessage) error {
	err := a.Action.Validate(params)
	a.record(params, err)
	return err
}

// StepChecked implements agent.StepObserver. propose_fix refuses params that do
// not fit the action's schema before it calls Validate, so without this a
// forbidden action sent with malformed params would leave no attempt and
// escape the forbidden_actions check.
func (a *recordingAction) StepChecked(params json.RawMessage, err error) {
	a.record(params, err)
}

func (a *recordingAction) record(params json.RawMessage, err error) {
	at := ActionAttempt{Action: a.Name(), Params: append(json.RawMessage(nil), params...)}
	if err != nil {
		at.Error = err.Error()
	}
	a.log.mu.Lock()
	a.log.attempts = append(a.log.attempts, at)
	a.log.mu.Unlock()
}

// CheckScope keeps the wrapper honest about scope: it delegates when the
// wrapped action is scoped and refuses otherwise, as the room service would.
func (a *recordingAction) CheckScope(p owners.Pipeline, params json.RawMessage) error {
	s, ok := a.Action.(actions.Scoped)
	if !ok {
		return fmt.Errorf("evals: action %q is not scope-checked", a.Name())
	}
	return s.CheckScope(p, params)
}
