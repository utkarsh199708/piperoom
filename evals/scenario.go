// Package evals is a test harness for the investigation agent. It runs the
// agent through the room's own path (room.Service starting the Endpoint of
// wiring.NewAgentEndpoint, room_path.go) against replayed tool results
// instead of a live pipeline, scores each run against what the scenario
// expects, and compares two reports. That makes a prompt, tool-description or
// model change measurable: run the same scenarios before and after, compare.
//
// Tool names, descriptions and input schemas come from the current code (A2's
// kafkatools, A3's dbtools, A2's actions), so a changed tool description is
// part of what gets evaluated. Only the tool results are replayed.
//
// The scenario format is specified in evals/README.md.
package evals

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Scenario is one recorded incident: the alert, the tool results the agent
// will see, the human input it gets when it goes idle, and what a good
// investigation must conclude.
type Scenario struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	// Pipeline is the room's pipeline, as in owners.yaml. Proposal steps must
	// pass each action's CheckScope against it (security finding F10).
	Pipeline owners.Pipeline `yaml:"pipeline"`
	// Resources lists what exists on the recorded stack. Action validation
	// checks connector and subject names against it, as the real actions do
	// against Connect and the Schema Registry.
	Resources Resources `yaml:"resources"`
	// Start says what opens the room: an alert (default) or the built-in
	// watcher (watcher_start.go).
	Start Start `yaml:"start"`
	Alert Alert `yaml:"alert"`
	// Phases name the states the recorded stack goes through, start phase
	// first (e.g. broken, recovered). Empty means a static scenario. See
	// phases.go.
	Phases []string `yaml:"phases"`
	// Transitions move the run to another phase when a fix step is executed
	// or after a tool has been called n times.
	Transitions []Transition `yaml:"transitions"`
	// Recordings are the replayed tool results. See Recording for matching.
	Recordings []Recording `yaml:"recordings"`
	// Inputs are delivered one per idle, in order: when the agent finishes a
	// turn and waits, the scripted person acts through the room (steer,
	// comment, approve). When none are left the run ends there.
	Inputs []Input `yaml:"inputs"`
	Expect Expect  `yaml:"expect"`
	// Judge picks how the root cause and layer are judged: "terms"
	// (deterministic), "claude" (LLM judge, see judge.go) or "auto" (the
	// judge only for a hypothesis the keyword pass finds ambiguous,
	// judge_auto.go). Unset uses the run's default (evalrun: auto). The
	// evalrun -judge flag overrides it.
	Judge string `yaml:"judge"`

	// pipelineYAML is the pipeline as written, for the room path's owners
	// file; nil for a scenario built in code.
	pipelineYAML *yaml.Node
}

// Alert is what opens the room.
type Alert struct {
	Title string `yaml:"title"`
	// PipelineMap is the topology text the model gets up front.
	PipelineMap string `yaml:"pipeline_map"`
}

// Resources are the names that exist on the recorded stack.
//
// The P3 actions' Validate reads more than names (P3g-2). Most of it comes
// from the recordings, first recording first (replay_actions.go): a
// connector's config from connect_config, slots from pg_replication_slots, a
// group from kafka_consumer_lag, offsets from kafka_topic_offsets and
// dlq_peek. The fields below declare what no read tool shows; only a
// scenario whose fix is pg_add_publication_table or pg_snapshot_signal needs
// them.
type Resources struct {
	Connectors []string `yaml:"connectors"`
	Subjects   []string `yaml:"subjects"`
	// ConnectorConfigs are config keys merged over a connector's
	// connect_config recording: connect_config hides publication.* and
	// signal.* keys, which pg_add_publication_table and pg_snapshot_signal
	// read.
	ConnectorConfigs map[string]map[string]string `yaml:"connector_configs"`
	// Tables are the source database's tables as the pg actions see them,
	// by "schema.table". A table not listed does not exist.
	Tables map[string]TableState `yaml:"tables"`
	// Publications list each publication's tables ("schema.table"). A
	// publication not listed does not exist.
	Publications map[string][]string `yaml:"publications"`
}

// TableState is one source table as pg_add_publication_table and
// pg_snapshot_signal read it (postgres.PublicationAddState and
// postgres.SnapshotSignalState).
type TableState struct {
	// RelKind is pg_class.relkind; empty means "r" (an ordinary table).
	RelKind string `yaml:"relkind"`
	// ReplicaIdentity is pg_class.relreplident; empty means "d" (default:
	// the primary key).
	ReplicaIdentity string `yaml:"replica_identity"`
	PrimaryKey      bool   `yaml:"primary_key"`
	// EstimatedRows is pg_class.reltuples; unset means never analysed (-1).
	EstimatedRows *int64 `yaml:"estimated_rows"`
	// SignalColumns: the table has the id, type and data columns of a
	// Debezium signal table.
	SignalColumns bool `yaml:"signal_columns"`
}

// tableRe is a "schema.table" as owners.yaml lists source tables.
var tableRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`)

// validate checks the action-read state of resources.
func (r Resources) validate() []error {
	var errs []error
	known := set(r.Connectors)
	for _, c := range slices.Sorted(maps.Keys(r.ConnectorConfigs)) {
		if !known[c] {
			errs = append(errs, fmt.Errorf("resources.connector_configs: connector %q is not in resources.connectors", c))
		}
	}
	for _, t := range slices.Sorted(maps.Keys(r.Tables)) {
		st := r.Tables[t]
		if !tableRe.MatchString(t) {
			errs = append(errs, fmt.Errorf("resources.tables: %q is not \"schema.table\"", t))
		}
		switch st.RelKind {
		case "", "r", "p", "v", "m", "f":
		default:
			errs = append(errs, fmt.Errorf("resources.tables.%s: relkind %q is not one of r, p, v, m, f", t, st.RelKind))
		}
		switch st.ReplicaIdentity {
		case "", "d", "n", "f", "i":
		default:
			errs = append(errs, fmt.Errorf("resources.tables.%s: replica_identity %q is not one of d, n, f, i", t, st.ReplicaIdentity))
		}
	}
	for _, p := range slices.Sorted(maps.Keys(r.Publications)) {
		for _, t := range r.Publications[p] {
			if _, ok := r.Tables[t]; !ok {
				errs = append(errs, fmt.Errorf("resources.publications.%s: table %q is not in resources.tables", p, t))
			}
		}
	}
	return errs
}

// Recording is one replayed tool result.
//
// Matching: a recording matches a call to Tool when every field in Input is
// present in the call with an equal value. Fields the call has but Input does
// not mention are ignored, so leave out fields whose value the model picks
// freely (timestamps such as "since"). An empty Input matches every call to
// the tool. When several recordings match, the one with the most Input fields
// wins; on a tie a recording of the current phase beats one without a phase,
// and then the earliest in the file wins.
//
// Phase limits a recording to one phase of the scenario; empty means every
// phase. Sequence replaces Summary/Data/Truncated/Error with observations
// served in order, one per matching call; the last one repeats.
type Recording struct {
	Tool  string         `yaml:"tool"`
	Input map[string]any `yaml:"input"`
	Phase string         `yaml:"phase"`
	// Summary and Data are returned as the tool's Result.
	Summary   string `yaml:"summary"`
	Data      any    `yaml:"data"`
	Truncated bool   `yaml:"truncated"`
	// Error, when set, makes the call fail as if the target system did.
	Error    string        `yaml:"error"`
	Sequence []Observation `yaml:"sequence"`
}

// Input is one piece of human input.
type Input struct {
	Kind      string `yaml:"kind"` // steer | comment | approval_result | action_result
	From      string `yaml:"from"` // display name; defaults by kind
	StepID    string `yaml:"step_id"`
	CommentID string `yaml:"comment_id"`
	Text      string `yaml:"text"`
	// Action makes an action_result an executed fix step: the harness
	// approves the newest open proposal of this run that has a step with
	// this action, as each required layer's owner, and the room's executor
	// runs it (room_path.go). An input whose action was never proposed is
	// skipped.
	Action string `yaml:"action"`
	// OK is the outcome of that step: true or false (required with Action).
	OK *bool `yaml:"ok"`
	// Params, when set, are the step params the scripted outcome is for,
	// matched like a recording's input (every field present, equal value).
	// A bound step with other params fails (ok false, not found) and fires
	// no transition: the wrong fix does not recover the stack.
	Params map[string]any `yaml:"params"`
}

// Expect is what a good investigation of the scenario looks like.
//
// Term groups (RootCause, Fix) are AND of ORs: every group must match, and a
// group matches when any one of its terms does. Text terms match whole words,
// case-insensitively, over a normalised token stream: the text is lower-cased
// and split on punctuation and camelCase, and a term is split the same way, so
// "log start" and "log-start" are one term and "PreparingRebalance" holds the
// words "preparing" and "rebalance". A term ending in "*" matches a word prefix
// ("rebalanc*" matches rebalance, rebalancing, PreparingRebalance); without it
// "rebalanc" matches nothing. A multi-word term must occur as consecutive words.
// Write terms lower-case with spaces ("leave group", not "LeaveGroup"; "22p02"
// is one word, "22P02" is rejected by validation).
//
// Forbidden rows are matched clause by clause: all terms of a row must sit in
// one clause (split at ; ! ? newline and ". "), and a clause with a negator
// (not, never, no, avoid*, without, n't, instead of, rather than, against) is
// skipped, so "do not reset to latest" does not fire [reset*, latest].
type Expect struct {
	// RootCause term groups must all appear in the agent's final hypothesis.
	RootCause [][]string `yaml:"root_cause"`
	// RootCauseForbidden lists wrong stories. Each row is an AND of terms
	// (same matching as RootCause): when any row has all its terms in the
	// final hypothesis, the root cause is judged wrong even if RootCause
	// matches. Optional; catches "names the right word, tells the wrong story".
	RootCauseForbidden [][]string `yaml:"root_cause_forbidden"`
	// FirstFailingLayer is the most upstream layer that is actually broken
	// (one of events.IncidentLayers(); `environment` is refused). The final hypothesis must name it.
	FirstFailingLayer string `yaml:"first_failing_layer"`
	// EvidenceTools must each be called at least once (any input).
	EvidenceTools []string `yaml:"evidence_tools"`
	// Fix action groups must all appear among the proposals' steps. Empty
	// means the scenario expects no particular fix.
	Fix [][]string `yaml:"fix"`
	// FixTargets says, per action, the params a proposed step must contain
	// (matched like a recording's input). Set, it makes the fix check
	// strict: see fixscore.go.
	FixTargets map[string]map[string]any `yaml:"fix_targets"`
	// ForbiddenActions must never be proposed in this scenario.
	ForbiddenActions []string `yaml:"forbidden_actions"`
	// MaxVerifyCalls is the read-tool budget after the first executed fix
	// step (the re-check, including agent.Run's own verify reads); those
	// reads do not count against MaxToolCalls. Zero means the default,
	// defaultMaxVerifyCalls; it applies only to scenarios that execute a fix.
	MaxVerifyCalls int `yaml:"max_verify_calls"`
	// MaxToolCalls is the read-tool budget a focused investigation stays
	// within. Zero means no budget.
	MaxToolCalls int `yaml:"max_tool_calls"`

	// Outcome is how a good run ends (see the Outcome constants); empty means
	// diagnose. Only diagnose grades root_cause, root_cause_forbidden, the two
	// layers, fix and claims; validation rejects them on any other outcome.
	Outcome string `yaml:"outcome"`
	// RootCauseLayer is the layer the root cause sits in (one of
	// events.IncidentLayers()), graded apart from FirstFailingLayer: in a schema change
	// Connect fails first, but the cause is a Postgres DDL.
	RootCauseLayer string `yaml:"root_cause_layer"`
	// RootCauseLayerAlt are further layers the root cause may defensibly be
	// placed in (each one of events.IncidentLayers(), distinct from
	// RootCauseLayer and never FirstFailingLayer): a hypothesis naming
	// RootCauseLayer or any of them passes the root-cause-layer grade. Only
	// for a scenario with two readings of where the cause sits, such as a
	// consumer down for longer than the topic's retention; never to paper
	// over a wrong label. See evals/README.md.
	RootCauseLayerAlt []string `yaml:"root_cause_layer_alt"`
	// ExpectedFacts are what the diagnosis must rest on, each one seen in a
	// successful result of its tool.
	ExpectedFacts []Fact `yaml:"expected_facts"`
	// Claims tie a conclusion to the facts that may back it: the final
	// hypothesis must cite a step that established one of them.
	Claims []Claim `yaml:"claims"`
}

// Outcomes a scenario can expect (Expect.Outcome).
const (
	// OutcomeDiagnose: find the cause, back it with evidence, propose the fix
	// if there is one.
	OutcomeDiagnose = "diagnose"
	// OutcomeHealthy: nothing is broken. Propose nothing, claim no cause at
	// medium or high confidence, and say the pipeline is healthy or that there
	// is nothing to fix.
	OutcomeHealthy = "healthy"
	// OutcomeAbstain: the evidence names no cause. Propose nothing and claim
	// none at medium or high confidence.
	OutcomeAbstain = "abstain"
	// OutcomeAsk: the driver's instruction is unclear. Ask the driver a
	// question before proposing anything.
	OutcomeAsk = "ask"
)

// outcome returns the expected outcome, defaulting to diagnose.
func (e Expect) outcome() string {
	if e.Outcome == "" {
		return OutcomeDiagnose
	}
	return e.Outcome
}

// Fact is one observation a good investigation makes. It is established at a
// step when the step ran Tool, the result was not an error, and every Match
// term occurs in the result's summary (whole words, as in Expect; no
// negation: tool output is not prose).
type Fact struct {
	ID       string   `yaml:"id"`
	Tool     string   `yaml:"tool"`
	Match    []string `yaml:"match"`
	Required bool     `yaml:"required"`
}

// Claim kinds (Claim.About).
const ClaimRootCause = "root_cause"

// Claim says which facts may back a conclusion. For about: root_cause the
// final hypothesis's based_on_steps must include a step that established at
// least one of Facts.
type Claim struct {
	About string   `yaml:"about"`
	Facts []string `yaml:"facts"`
}

// ParseScenario decodes one YAML scenario. Unknown fields are an error: a
// typo would otherwise silently drop an expectation.
func ParseScenario(b []byte) (Scenario, error) {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var s Scenario
	if err := dec.Decode(&s); err != nil {
		return Scenario{}, fmt.Errorf("evals: parse scenario: %w", err)
	}
	var raw struct {
		Pipeline yaml.Node `yaml:"pipeline"`
	}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return Scenario{}, fmt.Errorf("evals: parse scenario: %w", err)
	}
	if raw.Pipeline.Kind != 0 {
		s.pipelineYAML = &raw.Pipeline
	}
	return s, nil
}

// LoadScenario reads one scenario file. The name defaults to the file name.
func LoadScenario(path string) (Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("evals: read scenario: %w", err)
	}
	s, err := ParseScenario(b)
	if err != nil {
		return Scenario{}, fmt.Errorf("%s: %w", path, err)
	}
	if s.Name == "" {
		s.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	// The pipeline as the server loads it: its watch block resolved, which
	// dlq_replay's and pg_drop_replication_slot's CheckScope read
	// (watch.dlq_topics, watch.slot). A pipeline owners.Load refuses is an
	// error of the scenario, not a silent plain decode.
	if s.Pipeline.WatchSpec != nil {
		own, err := loadOwnersFor(s)
		if err != nil {
			return Scenario{}, fmt.Errorf("%s: owners: %w", path, err)
		}
		if p, ok := own.Pipeline(s.Pipeline.ID); ok {
			s.Pipeline = p
		}
	}
	return s, nil
}

// LoadScenarios reads every *.yaml scenario in dir, sorted by file name.
func LoadScenarios(dir string) ([]Scenario, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, fmt.Errorf("evals: list scenarios: %w", err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("evals: no *.yaml scenarios in %s", dir)
	}
	sort.Strings(paths)
	out := make([]Scenario, 0, len(paths))
	seen := map[string]bool{}
	for _, p := range paths {
		s, err := LoadScenario(p)
		if err != nil {
			return nil, err
		}
		if seen[s.Name] {
			return nil, fmt.Errorf("evals: duplicate scenario name %q", s.Name)
		}
		seen[s.Name] = true
		out = append(out, s)
	}
	return out, nil
}

// agentAlert builds the alert agent.Run gets.
func (s Scenario) agentAlert(roomID string) agent.Alert {
	return agent.Alert{
		RoomID:      roomID,
		Title:       s.Alert.Title,
		PipelineID:  s.Pipeline.ID,
		PipelineMap: s.Alert.PipelineMap,
	}
}

// humanInputs converts the scripted inputs to what the agent reads.
func (s Scenario) humanInputs() []agent.HumanInput {
	out := make([]agent.HumanInput, len(s.Inputs))
	for i, in := range s.Inputs {
		actor := defaultActor(in.Kind)
		if in.From != "" {
			actor.ID, actor.Name = in.From, in.From
		}
		out[i] = agent.HumanInput{
			Seq:       int64(i + 1),
			Actor:     actor,
			Kind:      in.Kind,
			StepID:    in.StepID,
			CommentID: in.CommentID,
			Text:      in.Text,
		}
	}
	return out
}

func defaultActor(kind string) events.Actor {
	switch kind {
	case "steer":
		return events.Actor{Kind: events.ActorHuman, ID: "driver", Name: "driver"}
	case "comment":
		return events.Actor{Kind: events.ActorHuman, ID: "watcher", Name: "watcher"}
	default:
		return events.Actor{Kind: events.ActorSystem, ID: "system", Name: "system"}
	}
}

// Validate checks the scenario against the tools and actions the agent will
// actually have, so a stale scenario fails loudly before any model call
// instead of quietly producing replay misses. toolInputs maps each tool name
// to the property names of its input schema (see ToolInputs).
func (s Scenario) Validate(toolInputs map[string][]string, actionNames []string) error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("name is empty"))
	}
	if s.Pipeline.ID == "" {
		errs = append(errs, errors.New("pipeline.id is empty"))
	}
	// owners.Load requires it too; without it every subject fails CheckScope
	// and a correct schema fix would score as out of scope.
	if s.Pipeline.TopicPrefix == "" {
		errs = append(errs, errors.New("pipeline.topic_prefix is empty"))
	}
	if strings.TrimSpace(s.Alert.Title) == "" && s.Start.By != StartWatcher {
		errs = append(errs, errors.New("alert.title is empty"))
	}
	errs = append(errs, s.validateStart()...)
	// The room path loads the pipeline as an owners.yaml entry; a pipeline
	// the server would refuse cannot be evaluated.
	if s.Pipeline.ID != "" && s.Pipeline.TopicPrefix != "" {
		if _, err := newRoomFixture(s); err != nil {
			errs = append(errs, err)
		} else if s.Pipeline.Shadow() {
			errs = append(errs, errors.New("pipeline.shadow: evals measure live rooms; a shadow pipeline's room never shows the agent's work to anyone"))
		}
	}
	errs = append(errs, s.checkOutcome()...)
	errs = append(errs, s.checkFacts(toolInputs)...)
	errs = append(errs, checkGroups("expect.root_cause", s.Expect.RootCause, nil)...)
	errs = append(errs, checkGroups("expect.root_cause_forbidden", s.Expect.RootCauseForbidden, nil)...)
	switch s.Judge {
	case "", JudgeTerms, JudgeClaude, JudgeAuto:
	default:
		errs = append(errs, fmt.Errorf("judge: unknown judge %q (want %q, %q or %q)", s.Judge, JudgeTerms, JudgeClaude, JudgeAuto))
	}
	errs = append(errs, checkGroups("expect.fix", s.Expect.Fix, actionNames)...)
	if l := s.Expect.FirstFailingLayer; l != "" && !contains(incidentLayers(), l) {
		errs = append(errs, fmt.Errorf("expect.first_failing_layer: unknown layer %q (want one of %s)", l, strings.Join(incidentLayers(), ", ")))
	}
	if l := s.Expect.RootCauseLayer; l != "" && !contains(incidentLayers(), l) {
		errs = append(errs, fmt.Errorf("expect.root_cause_layer: unknown layer %q (want one of %s)", l, strings.Join(incidentLayers(), ", ")))
	}
	errs = append(errs, s.Expect.checkRootCauseLayerAlt()...)

	recKeys := map[string]int{}
	for i, r := range s.Recordings {
		// Replay breaks a tie for the earlier recording, so a later one with
		// the same tool, input and phase never answers (shadow.go drops such
		// duplicates for the same reason).
		in := r.Input
		if len(in) == 0 {
			in = nil
		}
		key, err := json.Marshal(struct {
			T, P string
			I    map[string]any
		}{r.Tool, r.Phase, in})
		if err != nil {
			errs = append(errs, fmt.Errorf("recordings[%d] (%s): input does not convert to JSON: %v", i, r.Tool, err))
		} else {
			if j, dup := recKeys[string(key)]; dup {
				errs = append(errs, fmt.Errorf("recordings[%d] (%s): never matches: recordings[%d] has the same tool, input and phase and wins every tie", i, r.Tool, j))
			} else {
				recKeys[string(key)] = i
			}
		}
		fields, isTool := toolInputs[r.Tool]
		if !isTool {
			errs = append(errs, fmt.Errorf("recordings[%d]: unknown tool %q", i, r.Tool))
		} else {
			// A key the schema does not have can never appear in a real call
			// (schemas set additionalProperties: false), so the recording
			// would never match and every run would count a miss.
			known := set(fields)
			for _, k := range sortedKeys(r.Input) {
				if !known[k] {
					errs = append(errs, fmt.Errorf("recordings[%d] (%s): unknown input field %q", i, r.Tool, k))
				}
			}
		}
		if r.Sequence != nil {
			// The observations are the result; a top-level one would be
			// silently ignored.
			errs = append(errs, checkSequence(i, r)...)
			continue
		}
		if r.Error == "" && r.Summary == "" {
			errs = append(errs, fmt.Errorf("recordings[%d] (%s): needs summary or error", i, r.Tool))
		}
		if r.Data != nil {
			if _, err := json.Marshal(r.Data); err != nil {
				errs = append(errs, fmt.Errorf("recordings[%d] (%s): data does not convert to JSON: %v", i, r.Tool, err))
			}
		}
	}
	errs = append(errs, s.validatePhases(toolInputs, actionNames)...)
	errs = append(errs, s.Resources.validate()...)
	for _, t := range s.Expect.EvidenceTools {
		if _, ok := toolInputs[t]; !ok {
			errs = append(errs, fmt.Errorf("expect.evidence_tools: unknown tool %q", t))
		}
	}
	isAction := set(actionNames)
	for _, a := range s.Expect.ForbiddenActions {
		if !isAction[a] {
			errs = append(errs, fmt.Errorf("expect.forbidden_actions: unknown action %q", a))
		}
	}
	// A fix action that is also forbidden fails every run that proposes it.
	forbidden := set(s.Expect.ForbiddenActions)
	for i, g := range s.Expect.Fix {
		for _, a := range g {
			if forbidden[a] {
				errs = append(errs, fmt.Errorf("expect.fix[%d]: action %q is also in expect.forbidden_actions", i, a))
			}
		}
	}
	for i, in := range s.Inputs {
		switch in.Kind {
		case "steer", "comment", "approval_result", "action_result":
		default:
			errs = append(errs, fmt.Errorf("inputs[%d]: unknown kind %q", i, in.Kind))
		}
		if strings.TrimSpace(in.Text) == "" {
			errs = append(errs, fmt.Errorf("inputs[%d]: text is empty", i))
		}
		errs = append(errs, checkExecutedInput(i, in, isAction)...)
		// A room produces approval and action results only by executing a
		// proposal; a bare result text would be a message no room can send.
		if in.Kind == "approval_result" || (in.Kind == "action_result" && in.Action == "") {
			errs = append(errs, fmt.Errorf("inputs[%d]: a %s comes from executing a proposal in a room; use kind action_result with action", i, in.Kind))
		}
	}
	if s.Expect.MaxToolCalls < 0 {
		errs = append(errs, errors.New("expect.max_tool_calls is negative"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("evals: scenario %q: %w", s.Name, err)
	}
	return nil
}

// checkOutcome checks expect.outcome and that only the fields that outcome
// grades are set: a root_cause on a healthy scenario would be silently
// ignored, so it is an error instead.
func (s Scenario) checkOutcome() []error {
	e := s.Expect
	switch e.outcome() {
	case OutcomeDiagnose:
		if len(e.RootCause) == 0 {
			return []error{errors.New("expect.root_cause is empty: a run could never be judged")}
		}
		return nil
	case OutcomeHealthy, OutcomeAbstain, OutcomeAsk:
	default:
		return []error{fmt.Errorf("expect.outcome: unknown outcome %q (want %s, %s, %s or %s)",
			e.Outcome, OutcomeDiagnose, OutcomeHealthy, OutcomeAbstain, OutcomeAsk)}
	}
	var errs []error
	for field, set := range map[string]bool{
		"root_cause":           len(e.RootCause) > 0,
		"root_cause_forbidden": len(e.RootCauseForbidden) > 0,
		"first_failing_layer":  e.FirstFailingLayer != "",
		"root_cause_layer":     e.RootCauseLayer != "",
		"root_cause_layer_alt": len(e.RootCauseLayerAlt) > 0,
		"fix":                  len(e.Fix) > 0,
		"fix_targets":          len(e.FixTargets) > 0,
		"claims":               len(e.Claims) > 0,
	} {
		if set {
			errs = append(errs, fmt.Errorf("expect.%s: only graded when outcome is %s, not %s", field, OutcomeDiagnose, e.Outcome))
		}
	}
	if s.Judge == JudgeClaude || s.Judge == JudgeAuto {
		errs = append(errs, fmt.Errorf("judge: %q grades a root cause; outcome %s has none", s.Judge, e.Outcome))
	}
	if e.Outcome == OutcomeAsk && !s.hasDriverInput() {
		errs = append(errs, errors.New("inputs: outcome ask needs a steer or comment to ask about; only questions after the first one count"))
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return errs
}

// checkFacts checks expected_facts and claims. Every fact must be possible:
// some successful recording of its tool must contain all its terms, or no run
// could ever establish it (an author typo would fail every run).
func (s Scenario) checkFacts(toolInputs map[string][]string) []error {
	var errs []error
	ids := map[string]bool{}
	for i, f := range s.Expect.ExpectedFacts {
		where := fmt.Sprintf("expect.expected_facts[%d]", i)
		switch {
		case strings.TrimSpace(f.ID) == "":
			errs = append(errs, fmt.Errorf("%s: id is empty", where))
		case ids[f.ID]:
			errs = append(errs, fmt.Errorf("%s: duplicate id %q", where, f.ID))
		}
		ids[f.ID] = true
		where = fmt.Sprintf("%s (%s)", where, f.ID)
		if _, ok := toolInputs[f.Tool]; !ok {
			errs = append(errs, fmt.Errorf("%s: unknown tool %q", where, f.Tool))
		}
		if len(f.Match) == 0 {
			errs = append(errs, fmt.Errorf("%s: match is empty", where))
			continue
		}
		if termErrs := checkGroups(where+".match", [][]string{f.Match}, nil); len(termErrs) > 0 {
			errs = append(errs, termErrs...)
			continue
		}
		if !s.factRecorded(f) {
			errs = append(errs, fmt.Errorf("%s: no successful %s recording contains all of %q; a run could never establish it", where, f.Tool, f.Match))
		}
	}
	for i, c := range s.Expect.Claims {
		where := fmt.Sprintf("expect.claims[%d]", i)
		if c.About != ClaimRootCause {
			errs = append(errs, fmt.Errorf("%s: unknown about %q (want %q)", where, c.About, ClaimRootCause))
		}
		if len(c.Facts) == 0 {
			errs = append(errs, fmt.Errorf("%s: facts is empty", where))
		}
		for _, id := range c.Facts {
			if !ids[id] {
				errs = append(errs, fmt.Errorf("%s: unknown fact %q (not in expected_facts)", where, id))
			}
		}
	}
	return errs
}

// hasDriverInput reports whether an input is a steer or a comment: the inputs
// the harness logs, which mark when the agent can first ask about them.
func (s Scenario) hasDriverInput() bool {
	for _, in := range s.Inputs {
		if in.Kind == "steer" || in.Kind == "comment" {
			return true
		}
	}
	return false
}

// factRecorded reports whether a successful result of f.Tool that the replay
// can return contains every term of f, so a run that calls the tool can
// establish the fact.
func (s Scenario) factRecorded(f Fact) bool {
	for _, r := range s.Recordings {
		if r.Tool != f.Tool {
			continue
		}
		for _, summary := range successfulSummaries(r) {
			if factIn(f, summary) {
				return true
			}
		}
	}
	return false
}

// successfulSummaries are the summaries a recording can return as a
// successful result, in any phase and at any position of a sequence. It reads
// the recording through recordingResults, the one place that knows a
// recording's shape.
func successfulSummaries(r Recording) []string {
	var out []string
	for _, o := range recordingResults(r) {
		if o.Error == "" {
			out = append(out, o.Summary)
		}
	}
	return out
}

// checkGroups rejects empty groups and blank terms; with a known list, it
// also rejects terms that are not in it.
func checkGroups(field string, groups [][]string, known []string) []error {
	var errs []error
	isKnown := set(known)
	for i, g := range groups {
		if len(g) == 0 {
			errs = append(errs, fmt.Errorf("%s[%d]: empty group", field, i))
		}
		for _, term := range g {
			if strings.TrimSpace(term) == "" {
				errs = append(errs, fmt.Errorf("%s[%d]: blank term", field, i))
			} else if known != nil && !isKnown[term] {
				errs = append(errs, fmt.Errorf("%s[%d]: unknown action %q", field, i, term))
			} else if known == nil {
				// Text terms: "*" only as the last character, and at least one word.
				toks, _, ok := termTokens(term)
				if !ok {
					errs = append(errs, fmt.Errorf("%s[%d]: term %q has no words or a \"*\" that is not the last character", field, i, term))
				} else if !reflect.DeepEqual(toks, tokenize(strings.ToLower(strings.TrimSuffix(strings.TrimSpace(term), "*")))) {
					// Text is matched lower-cased then split, so a camelCase or
					// digit-joined term (22P02, HTTP409, leaveGroup) would
					// never be seen as the words its tokens claim.
					errs = append(errs, fmt.Errorf("%s[%d]: term %q splits differently once lower-cased; write it lower-case with spaces", field, i, term))
				}
			}
		}
	}
	return errs
}

func set(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// sortedKeys returns m's keys in order, so validation errors are stable.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
