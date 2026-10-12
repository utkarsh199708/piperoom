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
// tool returns it. The first recording for a connector wins.
func connectStatuses(recs []Recording) (map[string]clients.ConnectorStatus, error) {
	out := map[string]clients.ConnectorStatus{}
	for i, r := range recs {
		if r.Tool != "connect_status" || r.Data == nil {
			continue
		}
		name, _ := r.Input["connector"].(string)
		if name == "" {
			continue
		}
		if _, dup := out[name]; dup {
			continue
		}
		b, err := json.Marshal(r.Data)
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

// replayer serves recordings for one run and remembers the misses.
type replayer struct {
	byTool map[string][]replayRecord

	mu     sync.Mutex
	misses []Miss
}

// replayRecord is a Recording with its Data already converted to JSON, so the
// result reaches the room log exactly as a real tool's Data would.
type replayRecord struct {
	Recording
	data json.RawMessage
}

func newReplayer(recs []Recording) (*replayer, error) {
	r := &replayer{byTool: map[string][]replayRecord{}}
	for i, rec := range recs {
		rr := replayRecord{Recording: rec}
		if rec.Data != nil {
			b, err := json.Marshal(rec.Data)
			if err != nil {
				return nil, fmt.Errorf("evals: recordings[%d] (%s): data: %w", i, rec.Tool, err)
			}
			rr.data = b
		}
		r.byTool[rec.Tool] = append(r.byTool[rec.Tool], rr)
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

// lookup finds the best recording for a call (see Recording for the rules).
func (r *replayer) lookup(tool string, input json.RawMessage) (replayRecord, bool) {
	var call map[string]any
	if len(input) > 0 {
		if err := json.Unmarshal(input, &call); err != nil {
			return replayRecord{}, false
		}
	}
	best, bestScore := -1, -1
	recs := r.byTool[tool]
	for i, rec := range recs {
		if !matches(rec.Input, call) {
			continue
		}
		if len(rec.Input) > bestScore {
			best, bestScore = i, len(rec.Input)
		}
	}
	if best < 0 {
		return replayRecord{}, false
	}
	return recs[best], true
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
	rec, ok := t.r.lookup(t.def.Name(), input)
	if !ok {
		t.r.mu.Lock()
		t.r.misses = append(t.r.misses, Miss{Tool: t.def.Name(), Input: append(json.RawMessage(nil), input...)})
		t.r.mu.Unlock()
		return tools.Result{}, errNoRecording
	}
	if rec.Error != "" {
		return tools.Result{}, errors.New(rec.Error)
	}
	res := tools.Result{Summary: rec.Summary, Truncated: rec.Truncated}
	if len(rec.data) > 0 {
		res.Data = rec.data
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
