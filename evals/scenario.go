// Package evals is a test harness for the investigation agent. It runs the
// real agent loop (agent.Run, real model) against replayed tool results
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
	"os"
	"path/filepath"
	"reflect"
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
	Alert     Alert     `yaml:"alert"`
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
	// turn and waits, it gets the next one. When none are left the inbox
	// closes and the run ends.
	Inputs []Input `yaml:"inputs"`
	Expect Expect  `yaml:"expect"`
	// Judge picks how the root cause and layer are judged: "terms" (default,
	// deterministic) or "claude" (LLM judge, see judge.go). The evalrun -judge
	// flag overrides it.
	Judge string `yaml:"judge"`
}

// Alert is what opens the room.
type Alert struct {
	Title string `yaml:"title"`
	// PipelineMap is the topology text the model gets up front.
	PipelineMap string `yaml:"pipeline_map"`
}

// Resources are the names that exist on the recorded stack.
type Resources struct {
	Connectors []string `yaml:"connectors"`
	Subjects   []string `yaml:"subjects"`
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
	// Action makes an action_result an executed fix step: the harness acts as
	// the room and the executor, binds it to the newest proposal of this run
	// that has a not yet executed step with this action, and delivers the
	// approvals and step results the room would (see executor in harness.go).
	// An input whose action was never proposed is skipped.
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
	// (one of events.Layers). The final hypothesis must name it.
	FirstFailingLayer string `yaml:"first_failing_layer"`
	// EvidenceTools must each be called at least once (any input).
	EvidenceTools []string `yaml:"evidence_tools"`
	// Fix action groups must all appear among the proposals' steps. Empty
	// means the scenario expects no particular fix.
	Fix [][]string `yaml:"fix"`
	// ForbiddenActions must never be proposed in this scenario.
	ForbiddenActions []string `yaml:"forbidden_actions"`
	// Pending holds expect keys this build does not score yet. Only the
	// shared-schema keys E2 is adding (pendingExpectKeys) are accepted, with a
	// warning; any other key is still a validation error, so a typo cannot
	// silently drop an expectation. Once E2's typed fields exist they take
	// those keys and Pending stays empty.
	Pending map[string]any `yaml:",inline"`
	// MaxVerifyCalls is the read-tool budget after the first executed fix
	// step (the re-check, including agent.Run's own verify reads); those
	// reads do not count against MaxToolCalls. Zero means the default,
	// defaultMaxVerifyCalls; it applies only to scenarios that execute a fix.
	MaxVerifyCalls int `yaml:"max_verify_calls"`
	// MaxToolCalls is the read-tool budget a focused investigation stays
	// within. Zero means no budget.
	MaxToolCalls int `yaml:"max_tool_calls"`
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
	// expect collects unknown keys instead of failing (Expect.Pending), so
	// the strict check for everything but the pending keys happens here.
	for _, k := range sortedKeys(s.Expect.Pending) {
		if !pendingExpectKeys[k] {
			return Scenario{}, fmt.Errorf("evals: parse scenario: expect: field %s not found", k)
		}
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
	if strings.TrimSpace(s.Alert.Title) == "" {
		errs = append(errs, errors.New("alert.title is empty"))
	}
	if len(s.Expect.RootCause) == 0 && !s.Expect.pendingNoCause() {
		errs = append(errs, errors.New("expect.root_cause is empty: a run could never be judged"))
	}
	errs = append(errs, checkGroups("expect.root_cause", s.Expect.RootCause, nil)...)
	errs = append(errs, checkGroups("expect.root_cause_forbidden", s.Expect.RootCauseForbidden, nil)...)
	switch s.Judge {
	case "", JudgeTerms, JudgeClaude:
	default:
		errs = append(errs, fmt.Errorf("judge: unknown judge %q (want %q or %q)", s.Judge, JudgeTerms, JudgeClaude))
	}
	errs = append(errs, checkGroups("expect.fix", s.Expect.Fix, actionNames)...)
	if l := s.Expect.FirstFailingLayer; l != "" && layerTerms[l] == nil {
		errs = append(errs, fmt.Errorf("expect.first_failing_layer: unknown layer %q (want one of %s)", l, strings.Join(events.Layers, ", ")))
	}

	for i, r := range s.Recordings {
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
	}
	if s.Expect.MaxToolCalls < 0 {
		errs = append(errs, errors.New("expect.max_tool_calls is negative"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("evals: scenario %q: %w", s.Name, err)
	}
	return nil
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
