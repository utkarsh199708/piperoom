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
	// Recordings are the replayed tool results. See Recording for matching.
	Recordings []Recording `yaml:"recordings"`
	// Inputs are delivered one per idle, in order: when the agent finishes a
	// turn and waits, it gets the next one. When none are left the inbox
	// closes and the run ends.
	Inputs []Input `yaml:"inputs"`
	Expect Expect  `yaml:"expect"`
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
// wins; ties go to the earliest in the file.
type Recording struct {
	Tool  string         `yaml:"tool"`
	Input map[string]any `yaml:"input"`
	// Summary and Data are returned as the tool's Result.
	Summary   string `yaml:"summary"`
	Data      any    `yaml:"data"`
	Truncated bool   `yaml:"truncated"`
	// Error, when set, makes the call fail as if the target system did.
	Error string `yaml:"error"`
}

// Input is one piece of human input.
type Input struct {
	Kind      string `yaml:"kind"` // steer | comment | approval_result | action_result
	From      string `yaml:"from"` // display name; defaults by kind
	StepID    string `yaml:"step_id"`
	CommentID string `yaml:"comment_id"`
	Text      string `yaml:"text"`
}

// Expect is what a good investigation of the scenario looks like.
//
// Term groups (RootCause, Fix) are AND of ORs: every group must match, and a
// group matches when any one of its terms does. Text terms match
// case-insensitively as substrings.
type Expect struct {
	// RootCause term groups must all appear in the agent's final hypothesis.
	RootCause [][]string `yaml:"root_cause"`
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

// Validate checks the scenario against the tool and action names the agent
// will actually have, so a stale scenario fails loudly before any model call
// instead of quietly producing replay misses.
func (s Scenario) Validate(toolNames, actionNames []string) error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("name is empty"))
	}
	if s.Pipeline.ID == "" {
		errs = append(errs, errors.New("pipeline.id is empty"))
	}
	if strings.TrimSpace(s.Alert.Title) == "" {
		errs = append(errs, errors.New("alert.title is empty"))
	}
	if len(s.Expect.RootCause) == 0 {
		errs = append(errs, errors.New("expect.root_cause is empty: a run could never be judged"))
	}
	errs = append(errs, checkGroups("expect.root_cause", s.Expect.RootCause, nil)...)
	errs = append(errs, checkGroups("expect.fix", s.Expect.Fix, actionNames)...)
	if l := s.Expect.FirstFailingLayer; l != "" && layerTerms[l] == nil {
		errs = append(errs, fmt.Errorf("expect.first_failing_layer: unknown layer %q (want one of %s)", l, strings.Join(events.Layers, ", ")))
	}

	isTool := set(toolNames)
	for i, r := range s.Recordings {
		if !isTool[r.Tool] {
			errs = append(errs, fmt.Errorf("recordings[%d]: unknown tool %q", i, r.Tool))
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
	for _, t := range s.Expect.EvidenceTools {
		if !isTool[t] {
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
