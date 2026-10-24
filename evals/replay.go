package evals

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/debeziumtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/mysqltools"
	"piperoom/internal/tools/runbooks"
	"piperoom/internal/tools/similarincidents"
)

// errNoRecording is what the model sees when it calls a tool with an input the
// scenario has no recording for. It reads like a target that returned nothing,
// so the run carries on; the miss is counted in the report so the scenario
// author can add a recording.
var errNoRecording = errors.New("no data available for this request")

// errNotReplayed is returned by replay client reads that action validation
// does not need.
var errNotReplayed = errors.New("evals: not available in replay")

// Miss is one tool call the scenario had no recording for.
type Miss struct {
	Tool  string          `json:"tool"`
	Input json.RawMessage `json:"input"`
}

// LiveTools returns the read-only tools the agent gets in production in a
// Postgres room (the default source kind), in production order. For tools
// that read live systems only the names, descriptions and schemas are used:
// the harness wraps each one and never calls its Run, which is why the
// clients can be nil here. Pure tools (see runsLive) are the exception and
// run for real. LiveToolsFor gives another source kind's list.
func LiveTools() ([]tools.Tool, error) {
	return LiveToolsFor(owners.SourceKindPostgres)
}

// LiveToolsFor returns the read-only tools of a room whose source is of
// sourceKind (owners.yaml source_kind; "" is postgres), the way wiring builds
// them for a client set of that kind (Q8g): a MySQL room gets dbtools' MySQL
// list (no pg_* tool; source_row and source_long_transactions),
// mysql_binlog_status and kafka_schema_history; a Postgres room gets none of
// them, so its list is the one it had before MySQL, byte for byte. Every tool
// then passes agent.ToolServesSource, as wiring filters it.
func LiveToolsFor(sourceKind string) ([]tools.Tool, error) {
	sk, err := agent.SourceKind(sourceKind)
	if err != nil {
		return nil, fmt.Errorf("evals: %w", err)
	}
	mysqlRoom := sk == owners.SourceKindMySQL
	reg := tools.NewRegistry()
	// kafkatools registers the F12 reads, and the Q1c, Q3a, Q4a and Q5a reads
	// (Q6d), only for clients that have them, so the definition-only clients
	// below say they do. kafka_schema_history (Q8e) is registered for a
	// SchemaHistoryReader, which only the MySQL room's client says it is.
	var kc clients.KafkaAdmin = definitionKafka{}
	if mysqlRoom {
		kc = definitionHistoryKafka{}
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{Kafka: kc, Connect: definitionConnect{}}); err != nil {
		return nil, fmt.Errorf("evals: kafka tool definitions: %w", err)
	}
	// dbtools refuses a nil Owners (a wiring guard); an empty one is enough
	// for definitions. Its SourceKind picks the list (Q8n); a Postgres room
	// keeps the zero value it always had.
	dd := dbtools.Deps{Owners: &owners.Owners{}}
	if mysqlRoom {
		dd.SourceKind = sk
	}
	if err := dbtools.Register(reg, dd); err != nil {
		return nil, fmt.Errorf("evals: db tool definitions: %w", err)
	}
	// mysql_binlog_status (Q8c), as wiring registers it: on a MySQL room only.
	if mysqlRoom {
		if err := mysqltools.Register(reg, mysqltools.Deps{Owners: &owners.Owners{}}); err != nil {
			return nil, fmt.Errorf("evals: mysql tool definitions: %w", err)
		}
	}
	// Definition only: no Connect client and no metrics URL; the harness never runs it.
	if err := debeziumtools.Register(reg, debeziumtools.Deps{Owners: &owners.Owners{}}); err != nil {
		return nil, fmt.Errorf("evals: debezium tool definitions: %w", err)
	}
	if err := registerWorkerToolDefs(reg); err != nil {
		return nil, err
	}
	if err := runbooks.Register(reg); err != nil {
		return nil, fmt.Errorf("evals: runbook tool definitions: %w", err)
	}
	// Definition only (no room store): scenarios record its answers, the
	// harness never runs it. See similar_fixture_test.go.
	if err := similarincidents.Register(reg, similarincidents.Deps{}); err != nil {
		return nil, fmt.Errorf("evals: similar_incidents tool definition: %w", err)
	}
	var out []tools.Tool
	for _, t := range reg.All() {
		if agent.ToolServesSource(t.Name(), sk) {
			out = append(out, t)
		}
	}
	return out, nil
}

// definitionHistoryKafka is definitionKafka plus the schema history reader,
// so kafkatools registers kafka_schema_history (Q8e) for a MySQL room. It is
// never called either.
type definitionHistoryKafka struct{ definitionKafka }

func (definitionHistoryKafka) SchemaHistoryTail(context.Context, string, int) ([]kafka.SchemaHistoryRecord, error) {
	return nil, errNotReplayed
}

// scenarioTools returns the tool definitions of sc's room: pg are the
// Postgres room's (the caller's list, LiveTools() in production), a MySQL
// scenario gets LiveToolsFor(mysql). my caches that list across calls; a nil
// my builds it every time. A pipeline that lists repos: also gets the
// repository tools after them, as Q7f's wiring registers them last
// (replay_forge.go); every other room's list is the one it had before.
func scenarioTools(sc Scenario, pg []tools.Tool, my *[]tools.Tool) ([]tools.Tool, error) {
	defs, err := sourceTools(sc, pg, my)
	if err != nil || len(sc.Pipeline.Repos) == 0 {
		return defs, err
	}
	fdefs, err := forgeToolDefs()
	if err != nil {
		return nil, err
	}
	return append(slices.Clone(defs), fdefs...), nil
}

// sourceTools is scenarioTools without the repository tools.
func sourceTools(sc Scenario, pg []tools.Tool, my *[]tools.Tool) ([]tools.Tool, error) {
	if sc.sourceKind() != owners.SourceKindMySQL {
		return pg, nil
	}
	if my != nil && *my != nil {
		return *my, nil
	}
	defs, err := LiveToolsFor(owners.SourceKindMySQL)
	if err != nil {
		return nil, err
	}
	if my != nil {
		*my = defs
	}
	return defs, nil
}

// definitionKafka is a Kafka client for tool definitions only. It implements the
// optional F12 readers so kafkatools registers their tools; no method is ever
// called, because the harness replays recordings.
type definitionKafka struct{ clients.KafkaAdmin }

func (definitionKafka) ClusterHealth(context.Context, ...string) (clients.ClusterHealth, error) {
	return clients.ClusterHealth{}, errNotReplayed
}

func (definitionKafka) PartitionRates(context.Context, time.Time, ...string) ([]clients.PartitionRate, error) {
	return nil, errNotReplayed
}

func (definitionKafka) ACLs(context.Context, []string, []string) ([]clients.ACLEntry, error) {
	return nil, errNotReplayed
}

// DupScan registers kafka_dup_keys (X1c), as the production Kafka client does.
func (definitionKafka) DupScan(context.Context, string, time.Time, int) (clients.DupReport, error) {
	return clients.DupReport{}, errNotReplayed
}

// LiveActions returns the production write actions, backed by replay clients
// that know only res and what recs recorded: actions.Register's seven, the
// P3 actions (replay_actions.go), the Q2 source admin actions
// (replay_source.go), the Q5 connector lifecycle actions
// (replay_connect.go) and, for a pipeline that lists repos:,
// repo_open_pull_request over the replay forge (replay_forge.go). The agent
// can propose them (their real
// Validate runs against that) but nothing is ever written: every write of a
// replay client is a no-op that only records the call.
func LiveActions(res Resources, recs []Recording) (*actions.Registry, error) {
	reg, _, err := liveActions(res, recs)
	return reg, err
}

// liveActions is LiveActions plus the log of the writes its clients took.
func liveActions(res Resources, recs []Recording) (*actions.Registry, *writeLog, error) {
	reg, wl, _, err := liveActionsForge(res, recs)
	return reg, wl, err
}

// liveActionsForge is liveActions plus the run's replay forge (nil when the
// pipeline lists no repos), which the run's repository tools read too.
func liveActionsForge(res Resources, recs []Recording) (*actions.Registry, *writeLog, *replayForge, error) {
	statuses, err := connectStatuses(recs)
	if err != nil {
		return nil, nil, nil, err
	}
	sk, err := agent.SourceKind(res.SourceKind)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("evals: resources: %w", err)
	}
	wl := &writeLog{}
	fg, err := newReplayForge(res, wl)
	if err != nil {
		return nil, nil, nil, err
	}
	reg := actions.NewRegistry()
	deps := actions.Deps{
		Connect:  replayConnect{connectors: res.Connectors, statuses: statuses, log: wl, res: res, recs: recs},
		Registry: replayRegistry{subjects: res.Subjects, log: wl},
	}
	// A MySQL room's actions get the source's binlog reader (Q8g wires the
	// MySQL client as actions.Deps.Binlog): connect_update_connector_config
	// then offers database.server.id and snapshot.mode, whose guards read
	// the connector's config, so its Connect also answers Config from the
	// recordings (replayConfig). A Postgres room keeps replayConnect, whose
	// Config is not replayed, so its Validate behaves as before.
	var binlog mysql.BinlogReader
	if sk == owners.SourceKindMySQL {
		binlog = replayBinlog{recs: recs}
		deps.Connect = replayMySQLConnect{
			replayConnect: replayConnect{connectors: res.Connectors, statuses: statuses, log: wl, res: res, recs: recs},
			cfg:           replayConfig{res: res, recs: recs},
		}
		deps.Binlog = binlog
	}
	if err := actions.Register(reg, deps); err != nil {
		return nil, nil, nil, fmt.Errorf("evals: action definitions: %w", err)
	}
	if err := registerP3(reg, res, recs, wl); err != nil {
		return nil, nil, nil, err
	}
	if err := registerSourceActions(reg, res, recs, wl); err != nil {
		return nil, nil, nil, err
	}
	if err := registerLifecycle(reg, res, recs, wl, binlog); err != nil {
		return nil, nil, nil, err
	}
	// Last, as wiring's registerFixPR, and only for a pipeline with repos:.
	if err := registerFixPR(reg, fg); err != nil {
		return nil, nil, nil, err
	}
	if sk == owners.SourceKindPostgres {
		return reg, wl, fg, nil
	}
	// Only the source's actions reach the room (agent.ActionServesSource), as
	// wiring registers them: a MySQL room has no pg_* action.
	out, err := agent.ActionsForSource(reg, sk)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("evals: %w", err)
	}
	return out, wl, fg, nil
}

// replayMySQLConnect is a MySQL room's Connect client for actions.Register's
// actions: replayConnect with Config answered from the recordings, which the
// MySQL key guards of connect_update_connector_config read (the connector
// class, the current snapshot.mode, the pipeline's other server ids).
type replayMySQLConnect struct {
	replayConnect
	cfg replayConfig
}

func (c replayMySQLConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	return c.cfg.Config(ctx, name)
}

// replayBinlog is a MySQL room's binlog reader (mysql.BinlogReader): the
// server state of the first mysql_binlog_status recording with data (its
// Data.server carries mysql.BinlogStatus's fields under the same names), so
// an action checks a position or a database.server.id against what the
// agent's read showed.
type replayBinlog struct{ recs []Recording }

var _ mysql.BinlogReader = replayBinlog{}

func (b replayBinlog) BinlogStatus(context.Context) (mysql.BinlogStatus, error) {
	var st mysql.BinlogStatus
	found := firstData(b.recs, mysqltools.NameBinlogStatus, func(_ Recording, data []byte) bool {
		var d struct {
			Server *mysql.BinlogStatus `json:"server"`
		}
		if json.Unmarshal(data, &d) != nil || d.Server == nil {
			return false
		}
		st = *d.Server
		return true
	})
	if !found {
		return mysql.BinlogStatus{}, fmt.Errorf("evals: binlog status: %w (record %s with data.server)", errNotReplayed, mysqltools.NameBinlogStatus)
	}
	return st, nil
}

// connectStatusData is the part of connect_status's Data that action
// validation reads (name, state, and task ids/states). The real tool's Data
// carries no type; Type is set only when a scenario adds one, otherwise it
// comes from the summary (statusType).
type connectStatusData struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	State string `json:"state"`
	Tasks []struct {
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
		st := clients.ConnectorStatus{Name: d.Name, Type: d.Type, State: d.State}
		if st.Name == "" {
			st.Name = name
		}
		if st.Type == "" {
			st.Type = statusType(st.Name, recordingResults(r)[0].Summary)
		}
		for _, t := range d.Tasks {
			st.Tasks = append(st.Tasks, clients.TaskStatus{ID: t.ID, State: t.State})
		}
		out[name] = st
	}
	return out, nil
}

// statusType reads a connector's type from connect_status's summary, whose
// first line the tool writes as "<name> (<type>): <state>". Its Data leaves
// the type out, and connect_reset_source_offsets refuses anything but a
// source connector (Q6d).
func statusType(name, summary string) string {
	for _, typ := range []string{"source", "sink"} {
		if strings.HasPrefix(summary, name+" ("+typ+"): ") {
			return typ
		}
	}
	return ""
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
	// isLookup tells the agent's own reads (the room-start past-incidents
	// lookup, the connect_plugins read of the running versions) from a call
	// the model made. It is a field so a test can mark a context; the
	// production value is agentOwnRead.
	isLookup func(context.Context) bool
}

// agentOwnRead reports a read the agent runner makes on its own: it is
// answered when recorded but is never a miss, never counts toward
// after_calls and never moves a sequence.
func agentOwnRead(ctx context.Context) bool {
	return agent.IsPastIncidentsLookup(ctx) || agent.IsRunningVersionsRead(ctx)
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
		isLookup:    agentOwnRead,
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
	return r.registryWith(defs, nil)
}

// registryWith is registry with live tools that take the place of their
// definitions by name: the repository tools over the run's replay forge.
func (r *replayer) registryWith(defs []tools.Tool, live map[string]tools.Tool) (*tools.Registry, error) {
	reg := tools.NewRegistry()
	for _, d := range defs {
		var t tools.Tool = &replayTool{def: d, r: r}
		if lt, ok := live[d.Name()]; ok {
			t = lt
		} else if runsLive(d.Name()) {
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

// peek answers the agent's own room-start lookup. The lookup is not a call the
// model made, so it neither advances a sequence, counts toward an after_calls
// transition, nor shows up as a miss: otherwise every room would start with a
// phantom call that moves phases and inflates the miss count.
func (r *replayer) peek(tool string, input json.RawMessage) (replayObs, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.match(tool, input)
	if !ok {
		return replayObs{}, false
	}
	return r.recs[i].obs[r.seqPos[i]], true
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
			return // the first that matches fires, as for executed
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

// setEvents sets the event counter the marks are stamped with.
func (r *replayer) setEvents(count func() int) {
	r.mu.Lock()
	r.events = count
	r.mu.Unlock()
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
	serve := t.r.serve
	if t.r.isLookup(ctx) {
		serve = t.r.peek
	}
	ob, ok := serve(t.def.Name(), input)
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
// connector names, where a recording gives it each connector's status, and
// the config of a connector the scenario declares in resources.connector_configs.
// Its writes are recorded no-ops.
type replayConnect struct {
	connectors []string
	statuses   map[string]clients.ConnectorStatus
	log        *writeLog
	// res and recs serve Config as replayConfig does for the P3 actions, but
	// only for a connector in res.ConnectorConfigs: the declaration is the
	// scenario's opt-in, so a scenario without one validates as before.
	res  Resources
	recs []Recording
}

var _ clients.ConnectFixWriter = replayConnect{}

func (c replayConnect) ListConnectors(context.Context) ([]string, error) {
	return append([]string(nil), c.connectors...), nil
}
func (c replayConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	if st, ok := c.statuses[name]; ok {
		return st, nil
	}
	return clients.ConnectorStatus{}, errNotReplayed
}
func (c replayConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	if _, declared := c.res.ConnectorConfigs[name]; !declared {
		return nil, errNotReplayed
	}
	return replayConfig{res: c.res, recs: c.recs}.Config(ctx, name)
}
func (c replayConnect) RestartTask(_ context.Context, name string, task int) error {
	c.log.record("RestartTask", map[string]any{"connector": name, "task": task})
	return nil
}

func (c replayConnect) RestartConnector(_ context.Context, name string) error {
	c.log.record("RestartConnector", map[string]any{"connector": name})
	return nil
}

// Resume matches the connectorResumer interface that connect_resume_connector
// Execute asserts for, so a replayed resume is recorded here and never
// refused for a client that does not know the method.
func (c replayConnect) Resume(_ context.Context, name string) error {
	c.log.record("Resume", map[string]any{"connector": name})
	return nil
}

func (c replayConnect) PauseConnector(_ context.Context, name string) error {
	c.log.record("PauseConnector", map[string]any{"connector": name})
	return nil
}

func (c replayConnect) UpdateConnectorConfigKeys(_ context.Context, name string, changes map[string]string) error {
	c.log.record("UpdateConnectorConfigKeys", map[string]any{"connector": name, "changes": len(changes)})
	return nil
}

// replayRegistry answers the one Schema Registry read that action validation
// needs. Its writes are recorded no-ops.
type replayRegistry struct {
	subjects []string
	log      *writeLog
}

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
func (r replayRegistry) SetCompatibility(_ context.Context, subject, level string) error {
	r.log.record("SetCompatibility", map[string]any{"subject": subject, "level": level})
	return nil
}
func (r replayRegistry) Register(_ context.Context, subject, _ string) (int, error) {
	r.log.record("Register", map[string]any{"subject": subject})
	return 0, nil
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
// On the room path the same valid step is validated again by the room
// (CreateProposal) and by its executor; a passing call identical to one
// already logged is not logged twice, so the log stays "what the agent
// tried". Failed calls are always logged.
type attemptLog struct {
	mu       sync.Mutex
	attempts []ActionAttempt
	// exec plays the writes of approved steps; nil keeps the actions'
	// own Execute, which fails on the replay clients.
	exec *execScript
}

// wrap returns a registry whose actions record each Validate call in l and
// otherwise behave exactly like reg's.
func (l *attemptLog) wrap(reg *actions.Registry) (*actions.Registry, error) {
	out := actions.NewRegistry()
	for _, a := range reg.All() {
		if err := out.Register(l.wrapAction(a)); err != nil {
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

func (a *recordingAction) Validate(ctx context.Context, params json.RawMessage) error {
	err := a.Action.Validate(ctx, params)
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
	defer a.log.mu.Unlock()
	if err == nil {
		for _, prev := range a.log.attempts {
			if prev.Error == "" && prev.Action == at.Action && bytes.Equal(prev.Params, at.Params) {
				return
			}
		}
	}
	a.log.attempts = append(a.log.attempts, at)
}

// Execute plays the write through the run's execScript (the room's executor
// calls it once every layer approved).
func (a *recordingAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	if a.log.exec == nil {
		return a.Action.Execute(ctx, params)
	}
	return a.log.exec.execute(a.Name(), params)
}

// Class passes the wrapped action's class through: without it every wrapped
// action reads as a fix, and the agent would never verify a recovery step by
// its counts (X2c).
func (a *recordingAction) Class() string { return actions.ClassOf(a.Action) }

// Verify runs the wrapped action's Verify on its replay clients (X2e), so a
// recovery step executed in a run is counted as in production. An action
// without one is not countable.
func (a *recordingAction) Verify(ctx context.Context, params json.RawMessage, executedAt time.Time) (actions.VerifyResult, error) {
	v, ok := a.Action.(actions.Verifier)
	if !ok {
		return actions.VerifyResult{}, fmt.Errorf("evals: action %q has no Verify: %w", a.Name(), clients.ErrNotSupported)
	}
	return v.Verify(ctx, params, executedAt)
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
