package evals

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Errors ScenarioFromShadow returns, wrapped, so callers and tests can tell
// the cases apart with errors.Is.
var (
	// ErrShadowMalformed is returned for JSON that is not a GET /shadow response
	// or that is missing a required field.
	ErrShadowMalformed = errors.New("evals: malformed shadow response")
	// ErrShadowNoRoom is returned when the response lacks the "room" member
	// (it was saved without ?include=room).
	ErrShadowNoRoom = errors.New("evals: shadow response has no room (save it with ?include=room)")
	// ErrShadowUnlabelled is returned when there is no usable human label. The
	// label is the only ground truth a scenario can have.
	ErrShadowUnlabelled = errors.New("evals: shadow run has no human label")
	// ErrShadowUnknownLayer is returned when the label names a layer that is
	// not one of events.IncidentLayers().
	ErrShadowUnknownLayer = errors.New("evals: shadow label names an unknown layer")
	// ErrShadowNotFinished is returned for a run that is still queued or running.
	ErrShadowNotFinished = errors.New("evals: shadow run has not finished")
	// ErrShadowNothingToReplay is returned when the run made no completed tool call.
	ErrShadowNothingToReplay = errors.New("evals: shadow run has no completed tool call to replay")
	// ErrShadowUnknownPipeline is returned when the room's pipeline is not in
	// the owners file the caller gave.
	ErrShadowUnknownPipeline = errors.New("evals: shadow room's pipeline is not in owners.yaml")
	// ErrShadowInvalid is returned when the scenario built from the run does
	// not pass the same validation as `evalrun -check`.
	ErrShadowInvalid = errors.New("evals: scenario built from shadow run does not validate")
)

// Size caps. A shadow response is agent and tool output, so nothing in it
// decides how big the scenario gets.
const (
	// MaxShadowInputBytes is the largest GET /shadow response read.
	MaxShadowInputBytes = 16 << 20
	maxShadowEvents     = 20000
	maxRecordings       = 200
	maxRecordingData    = 64 << 10
	maxPipelineMapBytes = 16 << 10
	maxToolErrorBytes   = tools.MaxSummaryBytes
	maxTerms            = 20
	maxTermLen          = 200
	maxScenarioBytes    = 4 << 20
	// toolCallSlack is added to the observed call count for expect.max_tool_calls.
	toolCallSlack = 5
	maxNameLen    = 60
)

var (
	nameChars = regexp.MustCompile(`[^A-Za-z0-9_-]`)
	// ShadowNameRE is what a scenario name built from a shadow run must match.
	shadowNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,60}$`)
)

// ShadowResult is a scenario built from a saved shadow run.
type ShadowResult struct {
	// YAML is the scenario file, validated.
	YAML []byte
	// Scenario is YAML parsed back, the exact value `evalrun -check` would see.
	Scenario Scenario
	// Warnings say what was dropped or why the scenario is weaker than a
	// hand-written one (partial run, skipped calls).
	Warnings []string
}

// ---- the GET /shadow shape (docs/api.md, docs/design/shadow.md 5.4) ----

type shadowDoc struct {
	Run    *shadowRun    `json:"run"`
	Events []shadowEvent `json:"events"`
	Room   *shadowRoom   `json:"room"`
}

type shadowRun struct {
	RoomID             string          `json:"room_id"`
	Status             string          `json:"status"`
	Model              string          `json:"model"`
	PipelineMap        string          `json:"pipeline_map"`
	StartedAt          *time.Time      `json:"started_at"`
	FirstHypothesisAt  *time.Time      `json:"first_hypothesis_at"`
	FinalHypothesisAt  *time.Time      `json:"final_hypothesis_at"`
	FinishedAt         *time.Time      `json:"finished_at"`
	FinishedAfterClose bool            `json:"finished_after_close"`
	FinalHypothesis    json.RawMessage `json:"final_hypothesis"` // events.AgentHypothesisPayload, or null
	ToolCalls          int             `json:"tool_calls"`
	Tokens             int64           `json:"tokens"`
	RevealedAt         *time.Time      `json:"revealed_at"`
}

// shadowEvent is events.Event plus the "shadow": true marker.
type shadowEvent struct {
	events.Event
	Shadow bool `json:"shadow"`
}

// shadowRoom is the include=room member (room.ShadowRoomEvents): each part is
// a whole events.Event (room.created, shadow.labelled, the last shadow.graded),
// null when missing. A bare payload is accepted too, so a hand-edited file
// still loads.
type shadowRoom struct {
	Created  json.RawMessage `json:"created"`
	Labelled json.RawMessage `json:"labelled"`
	Graded   json.RawMessage `json:"graded"`
}

// ---- the scenario as written (a subset of Scenario with omitempty) ----

type outScenario struct {
	Name        string      `yaml:"name"`
	Description string      `yaml:"description"`
	Pipeline    outPipeline `yaml:"pipeline"`
	Resources   Resources   `yaml:"resources"`
	Alert       Alert       `yaml:"alert"`
	Recordings  []outRecord `yaml:"recordings,omitempty"`
	Expect      outExpect   `yaml:"expect"`
}

// outPipeline is the plain fields of owners.Pipeline; its watch and owners
// blocks have no place in a scenario.
type outPipeline struct {
	ID             string   `yaml:"id"`
	Title          string   `yaml:"title,omitempty"`
	TopicPrefix    string   `yaml:"topic_prefix"`
	SourceTables   []string `yaml:"source_tables,omitempty"`
	Connectors     []string `yaml:"connectors,omitempty"`
	ConsumerGroups []string `yaml:"consumer_groups,omitempty"`
	SinkTables     []string `yaml:"sink_tables,omitempty"`
	SinkTSColumn   string   `yaml:"sink_ts_column,omitempty"`
}

type outRecord struct {
	Tool      string         `yaml:"tool"`
	Input     map[string]any `yaml:"input,omitempty"`
	Summary   string         `yaml:"summary,omitempty"`
	Data      any            `yaml:"data,omitempty"`
	Truncated bool           `yaml:"truncated,omitempty"`
	Error     string         `yaml:"error,omitempty"`
}

type outExpect struct {
	RootCause         [][]string `yaml:"root_cause"`
	FirstFailingLayer string     `yaml:"first_failing_layer"`
	MaxToolCalls      int        `yaml:"max_tool_calls"`
}

const shadowYAMLHeader = "# Built by `evalrun -from-shadow` from a graded shadow run. Review before committing:\n" +
	"# recordings cover only the path that run took (see evals/README.md, \"Scenarios from shadow runs\").\n"

// ScenarioFromShadow builds a scenario from one saved GET /api/rooms/{id}/shadow?include=room
// response. lookup resolves the room's pipeline id (owners.yaml). name, when
// set, replaces the default "shadow-<room id>".
//
// Everything in raw is untrusted agent and tool output. It is only ever
// copied into YAML string and number values (marshalled, never concatenated),
// is size-capped, and is never executed or interpreted. The result has been
// parsed back and validated the way `evalrun -check` does, so a scenario that
// is returned loads.
func ScenarioFromShadow(raw []byte, lookup func(id string) (owners.Pipeline, bool), name string) (ShadowResult, error) {
	if len(raw) > MaxShadowInputBytes {
		return ShadowResult{}, fmt.Errorf("%w: %d bytes exceeds the %d byte cap", ErrShadowMalformed, len(raw), MaxShadowInputBytes)
	}
	if name != "" && !shadowNameRE.MatchString(name) {
		return ShadowResult{}, fmt.Errorf("evals: scenario name %q must match %s", clip(name, 40), shadowNameRE)
	}
	var doc shadowDoc
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&doc); err != nil {
		return ShadowResult{}, fmt.Errorf("%w: %v", ErrShadowMalformed, err)
	}
	if dec.More() {
		return ShadowResult{}, fmt.Errorf("%w: trailing data after the JSON document", ErrShadowMalformed)
	}
	if doc.Run == nil || doc.Run.RoomID == "" {
		return ShadowResult{}, fmt.Errorf("%w: run.room_id is missing", ErrShadowMalformed)
	}
	if doc.Events == nil {
		return ShadowResult{}, fmt.Errorf("%w: events is missing", ErrShadowMalformed)
	}
	if len(doc.Events) > maxShadowEvents {
		return ShadowResult{}, fmt.Errorf("%w: %d events exceeds the %d event cap", ErrShadowMalformed, len(doc.Events), maxShadowEvents)
	}
	if doc.Room == nil {
		return ShadowResult{}, ErrShadowNoRoom
	}
	run := doc.Run

	switch run.Status {
	case "queued", "running":
		return ShadowResult{}, fmt.Errorf("%w (status %q)", ErrShadowNotFinished, run.Status)
	case "hypothesised", "no_hypothesis", "error", "budget", "interrupted":
	default:
		return ShadowResult{}, fmt.Errorf("%w: run.status %q is not a shadow status", ErrShadowMalformed, run.Status)
	}

	// store.ShadowRun.FinalHypothesis is the hypothesis event payload, an object.
	if !isNullJSON(run.FinalHypothesis) {
		var h events.AgentHypothesisPayload
		if err := json.Unmarshal(run.FinalHypothesis, &h); err != nil {
			return ShadowResult{}, fmt.Errorf("%w: run.final_hypothesis: %v", ErrShadowMalformed, err)
		}
	}

	created, err := decodePart[events.RoomCreatedPayload](doc.Room.Created, events.RoomCreated)
	if err != nil {
		return ShadowResult{}, fmt.Errorf("%w: room.created: %v", ErrShadowMalformed, err)
	}
	if created.PipelineID == "" {
		return ShadowResult{}, fmt.Errorf("%w: room.created.pipeline_id is empty", ErrShadowMalformed)
	}

	// The label is the ground truth. Check it before anything expensive.
	if isNullJSON(doc.Room.Labelled) {
		return ShadowResult{}, fmt.Errorf("%w: room.labelled is empty; a run nobody labelled has no ground truth, so no scenario can be judged against it", ErrShadowUnlabelled)
	}
	label, err := decodePart[events.ShadowLabelledPayload](doc.Room.Labelled, events.ShadowLabelled)
	if err != nil {
		return ShadowResult{}, fmt.Errorf("%w: room.labelled: %v", ErrShadowMalformed, err)
	}
	if label.ActualLayer == "" {
		return ShadowResult{}, fmt.Errorf("%w: room.labelled.actual_layer is empty", ErrShadowUnlabelled)
	}
	if !contains(incidentLayers(), label.ActualLayer) {
		return ShadowResult{}, fmt.Errorf("%w: %q (want one of %s)", ErrShadowUnknownLayer, clip(label.ActualLayer, 40), strings.Join(incidentLayers(), ", "))
	}
	terms, err := cleanTerms(label.RootCauseTerms)
	if err != nil {
		return ShadowResult{}, err
	}

	pipeline, ok := lookup(created.PipelineID)
	if !ok {
		return ShadowResult{}, fmt.Errorf("%w: %q", ErrShadowUnknownPipeline, clip(created.PipelineID, 80))
	}

	defs, err := LiveTools()
	if err != nil {
		return ShadowResult{}, err
	}
	var warnings []string
	recs, observed, skipped, err := recordingsFromEvents(doc.Events, ToolInputs(defs), &warnings)
	if err != nil {
		return ShadowResult{}, err
	}
	if len(recs) == 0 {
		return ShadowResult{}, fmt.Errorf("%w (%d call(s) seen, status %s)", ErrShadowNothingToReplay, observed, run.Status)
	}
	sc := outScenario{
		Name:        scenarioName(name, run.RoomID),
		Description: describe(run, label, doc.Room.Graded, observed, skipped, &warnings),
		Pipeline: outPipeline{
			ID: pipeline.ID, Title: pipeline.Title, TopicPrefix: pipeline.TopicPrefix,
			SourceTables: pipeline.SourceTables, Connectors: pipeline.Connectors,
			ConsumerGroups: pipeline.ConsumerGroups, SinkTables: pipeline.SinkTables,
			SinkTSColumn: pipeline.SinkTSColumn,
		},
		Resources:  Resources{Connectors: pipeline.Connectors, Subjects: subjectsFor(pipeline)},
		Alert:      Alert{Title: created.Title, PipelineMap: capString(run.PipelineMap, maxPipelineMapBytes)},
		Recordings: recs,
		Expect: outExpect{
			RootCause:         [][]string{terms},
			FirstFailingLayer: label.ActualLayer,
			MaxToolCalls:      observed + toolCallSlack,
		},
	}
	warnings = append(warnings, freeChoiceWarnings(recs)...)
	warnings = append(warnings, "review alert, pipeline map, tool inputs, and summaries before committing: they may contain production values such as hosts, keys, or DDL; raw tool data is omitted except for the allow-listed connect_status name, state, and task id/state fields")
	if run.Status != "hypothesised" {
		warnings = append(warnings, fmt.Sprintf("partial run (status %s): the recordings stop where the agent stopped; see evals/README.md", run.Status))
	}

	out, parsed, err := encodeChecked(&sc, &warnings)
	if err != nil {
		return ShadowResult{}, err
	}
	// Same path as `evalrun -check`: validate what a reader of the file gets
	// against the live tool and action definitions.
	if err := ValidateAll([]Scenario{parsed}, defs); err != nil {
		return ShadowResult{}, fmt.Errorf("%w: %v", ErrShadowInvalid, err)
	}
	return ShadowResult{YAML: out, Scenario: parsed, Warnings: warnings}, nil
}

// encodeChecked marshals sc, parses the text back the way the loader will, and
// compares what came back with what was meant. YAML has constructs that change
// meaning on the way back (a "<<" key is a merge key, for one), and the text
// is untrusted, so the comparison covers every recording and every string the
// run supplied. A recording whose data does not survive loses its data, with a
// warning; anything else that differs is an error.
func encodeChecked(sc *outScenario, warnings *[]string) ([]byte, Scenario, error) {
	for pass := 0; pass < 2; pass++ {
		body, err := yaml.Marshal(sc)
		if err != nil {
			return nil, Scenario{}, fmt.Errorf("evals: encode scenario: %w", err)
		}
		out := append([]byte(shadowYAMLHeader), body...)
		if len(out) > maxScenarioBytes {
			return nil, Scenario{}, fmt.Errorf("%w: scenario would be %d bytes, cap %d", ErrShadowMalformed, len(out), maxScenarioBytes)
		}
		parsed, err := ParseScenario(out)
		if err != nil {
			return nil, Scenario{}, fmt.Errorf("%w: %v", ErrShadowInvalid, err)
		}
		bad, err := roundTripMismatches(sc, parsed)
		if err != nil {
			return nil, Scenario{}, err
		}
		if len(bad) == 0 {
			return out, parsed, nil
		}
		if pass == 1 {
			return nil, Scenario{}, fmt.Errorf("%w: text changed meaning in YAML (recordings %v)", ErrShadowInvalid, bad)
		}
		for _, i := range bad {
			sc.Recordings[i].Data = nil
			*warnings = append(*warnings, fmt.Sprintf("recordings[%d] (%s): data dropped because it does not survive YAML (a \"<<\" key reads back as a merge)", i, sc.Recordings[i].Tool))
		}
	}
	return nil, Scenario{}, errors.New("evals: unreachable")
}

// roundTripMismatches returns the recordings whose data differs after the
// round trip. Any other difference is an error: those fields cannot be dropped.
func roundTripMismatches(sc *outScenario, parsed Scenario) ([]int, error) {
	fail := func(what string) error {
		return fmt.Errorf("%w: %s changed in the YAML round trip", ErrShadowInvalid, what)
	}
	if parsed.Name != sc.Name || parsed.Description != sc.Description ||
		parsed.Alert != sc.Alert || parsed.Expect.FirstFailingLayer != sc.Expect.FirstFailingLayer ||
		parsed.Expect.MaxToolCalls != sc.Expect.MaxToolCalls ||
		!reflect.DeepEqual(parsed.Expect.RootCause, sc.Expect.RootCause) {
		return nil, fail("name, description, alert or expectation")
	}
	if len(parsed.Recordings) != len(sc.Recordings) {
		return nil, fail("the recording list")
	}
	var bad []int
	for i, r := range sc.Recordings {
		p := parsed.Recordings[i]
		if p.Tool != r.Tool || p.Summary != r.Summary || p.Error != r.Error || p.Truncated != r.Truncated {
			return nil, fail(fmt.Sprintf("recordings[%d]", i))
		}
		if !sameJSON(p.Input, r.Input) {
			return nil, fail(fmt.Sprintf("recordings[%d].input", i))
		}
		if !sameJSON(p.Data, r.Data) {
			bad = append(bad, i)
		}
	}
	return bad, nil
}

// sameJSON compares two values by their JSON form (sorted keys), which treats
// int and int64 alike and nil map and absent alike.
func sameJSON(a, b any) bool {
	ja, ea := json.Marshal(a)
	jb, eb := json.Marshal(b)
	if ea != nil || eb != nil {
		return false
	}
	na, nb := string(ja), string(jb)
	if na == "{}" {
		na = "null"
	}
	if nb == "{}" {
		nb = "null"
	}
	return na == nb
}

func isNullJSON(b json.RawMessage) bool {
	s := bytes.TrimSpace(b)
	return len(s) == 0 || bytes.Equal(s, []byte("null"))
}

// decodePart decodes a room.* part. It accepts the bare payload or the whole
// Event (an object with "type" and "payload"); an Event of another type is
// refused.
func decodePart[T any](raw json.RawMessage, want events.Type) (T, error) {
	var zero T
	if isNullJSON(raw) {
		return zero, errors.New("missing")
	}
	var probe struct {
		Type    events.Type     `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return zero, err
	}
	if probe.Type != "" && !isNullJSON(probe.Payload) {
		if probe.Type != want {
			return zero, fmt.Errorf("event type %q, want %q", probe.Type, want)
		}
		raw = probe.Payload
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, err
	}
	return v, nil
}

// recordingsFromEvents pairs agent.tool_call with agent.tool_result by
// tool_use_id, in call order. observed counts every tool call in the run;
// skipped counts calls with no result.
func recordingsFromEvents(evs []shadowEvent, toolInputs map[string][]string, warnings *[]string) (recs []outRecord, observed, skipped int, err error) {
	type call struct {
		tool  string
		input json.RawMessage
	}
	calls := map[string]call{}
	var order []string
	results := map[string]events.AgentToolResultPayload{}
	for _, e := range evs {
		switch e.Type {
		case events.AgentToolCall:
			p, err := events.Decode[events.AgentToolCallPayload](e.Event)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("%w: %v", ErrShadowMalformed, err)
			}
			if p.ToolUseID == "" || p.Tool == "" {
				return nil, 0, 0, fmt.Errorf("%w: agent.tool_call seq %d has no tool_use_id or tool", ErrShadowMalformed, e.Seq)
			}
			if _, dup := calls[p.ToolUseID]; dup {
				continue
			}
			calls[p.ToolUseID] = call{p.Tool, p.Input}
			order = append(order, p.ToolUseID)
		case events.AgentToolResult:
			p, err := events.Decode[events.AgentToolResultPayload](e.Event)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("%w: %v", ErrShadowMalformed, err)
			}
			if p.ToolUseID == "" {
				return nil, 0, 0, fmt.Errorf("%w: agent.tool_result seq %d has no tool_use_id", ErrShadowMalformed, e.Seq)
			}
			results[p.ToolUseID] = p
		}
	}
	observed = len(order)

	seen := map[string]bool{}
	var live, dups, bigData, omittedData, invalidStatusData, orphan, rejected int
	for _, id := range order {
		c := calls[id]
		res, ok := results[id]
		if !ok {
			skipped++ // the run stopped between the call and its result
			continue
		}
		if res.Tool != "" && res.Tool != c.tool {
			return nil, 0, 0, fmt.Errorf("%w: tool_use_id %q is call to %q but result for %q", ErrShadowMalformed, clip(id, 40), clip(c.tool, 40), clip(res.Tool, 40))
		}
		if runsLive(c.tool) {
			live++ // answered by the real tool in an eval
			continue
		}
		input, err := cleanInput(c.input)
		if err != nil {
			if res.IsError {
				rejected++ // the agent loop refused input that was not an object
				continue
			}
			return nil, 0, 0, fmt.Errorf("%w: tool_call %q input: %v", ErrShadowMalformed, clip(id, 40), err)
		}
		// The agent loop rejects a call to an unknown tool or with input the
		// schema does not allow before running anything, and records it as an
		// error result. Such a call can never match in replay (and would fail
		// validation), so it is not recorded. A successful result for the same
		// shape is a real mismatch and stays a hard error (ValidateAll).
		if res.IsError && !knownCall(toolInputs, c.tool, input) {
			rejected++
			continue
		}
		// A "<<" key anywhere in the input would be read back as a YAML merge.
		// Input that cannot be written faithfully cannot match a replayed call.
		if hasMergeKey(input) {
			if res.IsError {
				rejected++
				continue
			}
			return nil, 0, 0, fmt.Errorf("%w: tool_call %q input has a \"<<\" key, which YAML would read as a merge", ErrShadowInvalid, clip(id, 40))
		}
		key, _ := json.Marshal(struct {
			T string
			I map[string]any
		}{c.tool, input})
		if seen[string(key)] {
			dups++ // replay would never pick the later duplicate: ties go to the earlier recording
			continue
		}
		seen[string(key)] = true

		r := outRecord{Tool: c.tool, Input: input, Truncated: res.Truncated}
		if res.IsError {
			msg := res.Summary
			if strings.TrimSpace(msg) == "" {
				msg = "tool failed (no message recorded)"
			}
			// The agent loop prefixes tool errors with "error: " and the replay
			// tool adds its own, so keep one.
			msg = strings.TrimPrefix(msg, "error: ")
			r.Error, _ = truncateTo(msg, maxToolErrorBytes)
		} else {
			if strings.TrimSpace(res.Summary) == "" {
				// A scenario recording needs summary or error; "" would not load.
				orphan++
				continue
			}
			var cut bool
			r.Summary, cut = tools.Truncate(res.Summary)
			r.Truncated = r.Truncated || cut
			if !isNullJSON(res.Data) {
				if c.tool != "connect_status" {
					omittedData++
				} else if len(res.Data) > maxRecordingData {
					bigData++
				} else {
					// Connector status is the only tool data used by replay validation.
					// Decode into an allow-list so worker hosts, task errors, and any
					// future fields from Connect never enter the committed scenario.
					var status struct {
						Name  string `json:"name"`
						State string `json:"state"`
						Tasks []struct {
							ID    int    `json:"id"`
							State string `json:"state"`
						} `json:"tasks"`
					}
					if err := json.Unmarshal(res.Data, &status); err != nil {
						invalidStatusData++
					} else if status.Name != "" || status.State != "" || status.Tasks != nil {
						projected, err := json.Marshal(status)
						if err != nil {
							return nil, 0, 0, fmt.Errorf("%w: tool_result %q connect_status projection: %v", ErrShadowMalformed, clip(id, 40), err)
						}
						v, err := jsonToNative(projected)
						if err != nil {
							return nil, 0, 0, fmt.Errorf("%w: tool_result %q connect_status projection: %v", ErrShadowMalformed, clip(id, 40), err)
						}
						r.Data = v
					} else {
						omittedData++
					}
				}
			}
		}
		recs = append(recs, r)
		if len(recs) > maxRecordings {
			return nil, 0, 0, fmt.Errorf("%w: more than %d recordings", ErrShadowMalformed, maxRecordings)
		}
	}
	if skipped > 0 {
		*warnings = append(*warnings, fmt.Sprintf("%d tool call(s) had no result (run cut off) and were not recorded", skipped))
	}
	if live > 0 {
		*warnings = append(*warnings, fmt.Sprintf("%d runbook_lookup call(s) not recorded: that tool runs for real in an eval", live))
	}
	if dups > 0 {
		*warnings = append(*warnings, fmt.Sprintf("%d repeated call(s) with identical input folded into the first result", dups))
	}
	if rejected > 0 {
		*warnings = append(*warnings, fmt.Sprintf("%d call(s) the agent loop rejected before running (unknown tool or input the schema does not allow) were not recorded; they still count toward max_tool_calls", rejected))
	}
	if omittedData > 0 {
		*warnings = append(*warnings, fmt.Sprintf("raw data omitted from %d recording(s); only allow-listed connect_status fields are retained", omittedData))
	}
	if invalidStatusData > 0 {
		*warnings = append(*warnings, fmt.Sprintf("connect_status data omitted from %d recording(s) because it did not match the allow-listed shape", invalidStatusData))
	}
	if bigData > 0 {
		*warnings = append(*warnings, fmt.Sprintf("data dropped from %d recording(s) larger than %d bytes", bigData, maxRecordingData))
	}
	if orphan > 0 {
		*warnings = append(*warnings, fmt.Sprintf("%d result(s) with an empty summary were not recorded", orphan))
	}
	return recs, observed, skipped, nil
}

// hasMergeKey reports whether v holds a map key "<<" at any depth.
func hasMergeKey(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			if k == "<<" || hasMergeKey(e) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if hasMergeKey(e) {
				return true
			}
		}
	}
	return false
}

// knownCall reports whether tool exists and every top-level input key is one
// of its schema properties.
func knownCall(toolInputs map[string][]string, tool string, input map[string]any) bool {
	fields, ok := toolInputs[tool]
	if !ok {
		return false
	}
	known := set(fields)
	for k := range input {
		if !known[k] {
			return false
		}
	}
	return true
}

// freeChoice matches input field names whose value the model picks freely
// (see Recording in scenario.go): times, windows, limits.
var freeChoice = regexp.MustCompile(`(?i)^(since|until|from|to|start|end|limit|offset|lookback|window|timeout|timestamp|time|ts|max_?\w*|\w*_(at|ts|time|since|until|ms|seconds|minutes|hours))$`)

// freeChoiceWarnings lists, per recording, input fields that look like free
// model choices. They make the recording match only that exact value.
func freeChoiceWarnings(recs []outRecord) []string {
	var out []string
	for i, r := range recs {
		var names []string
		for _, k := range sortedKeys(r.Input) {
			if freeChoice.MatchString(k) {
				names = append(names, k)
			}
		}
		if len(names) > 0 {
			out = append(out, fmt.Sprintf("recordings[%d] (%s): input field(s) %s look like free choices (times, limits); a call with another value will miss. Delete them from the input unless the value matters", i, r.Tool, strings.Join(names, ", ")))
		}
	}
	return out
}

// cleanInput removes the injected "why" property (the model's reason, not a
// tool argument) and returns the rest with numbers kept exact.
func cleanInput(raw json.RawMessage) (map[string]any, error) {
	if isNullJSON(raw) {
		return nil, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	delete(m, "why")
	if len(m) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		n, err := jsonToNative(v)
		if err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, nil
}

// jsonToNative decodes JSON into plain Go values for YAML. Integers stay
// integers (an offset or LSN must not become 1.2e+09), which json.Number would
// not do: yaml.v3 writes it as a string.
func jsonToNative(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return nativeNumbers(v)
}

func nativeNumbers(v any) (any, error) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, err := t.Float64()
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, fmt.Errorf("number %q does not fit", t.String())
		}
		return f, nil
	case map[string]any:
		for k, e := range t {
			n, err := nativeNumbers(e)
			if err != nil {
				return nil, err
			}
			t[k] = n
		}
	case []any:
		for i, e := range t {
			n, err := nativeNumbers(e)
			if err != nil {
				return nil, err
			}
			t[i] = n
		}
	}
	return v, nil
}

// cleanTerms turns human label terms into scenario terms. Each is stored in
// the matcher's word form (lower-case words joined by spaces, see tokenize):
// a label is free text, so "*" is not meant as the prefix operator, and a
// term like "0x1F" or "LeaveGroup" would otherwise fail Validate. A term with
// no words is dropped like a blank one.
func cleanTerms(in []string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, t := range in {
		if utf8.RuneCountInString(strings.TrimSpace(t)) > maxTermLen {
			return nil, fmt.Errorf("%w: a root_cause_terms entry is longer than %d characters", ErrShadowMalformed, maxTermLen)
		}
		t = strings.Join(tokenize(t), " ")
		if t == "" {
			continue
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: room.labelled.root_cause_terms is empty", ErrShadowUnlabelled)
	}
	if len(out) > maxTerms {
		return nil, fmt.Errorf("%w: more than %d root_cause_terms", ErrShadowMalformed, maxTerms)
	}
	return out, nil
}

// subjectsFor derives the Schema Registry subjects the pipeline's tables
// have, the way Debezium names them (<topic_prefix>.<schema>.<table>-key/-value).
func subjectsFor(p owners.Pipeline) []string {
	var out []string
	for _, t := range p.SourceTables {
		topic := p.TopicPrefix + "." + t
		out = append(out, topic+"-key", topic+"-value")
	}
	return out
}

func scenarioName(name, roomID string) string {
	if name != "" {
		return name
	}
	n := "shadow-" + nameChars.ReplaceAllString(roomID, "_")
	if len(n) > maxNameLen {
		n = n[:maxNameLen]
	}
	return n
}

// describe writes the scenario description. Only enumerated and numeric
// fields of the run go in; the agent's own text never does.
func describe(run *shadowRun, label events.ShadowLabelledPayload, graded json.RawMessage, observed, skipped int, warnings *[]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Built from shadow run %s (status %s, %d tool call(s) seen).", nameChars.ReplaceAllString(clip(run.RoomID, 60), "_"), run.Status, observed)
	if run.Status != "hypothesised" {
		fmt.Fprintf(&b, " PARTIAL RUN: the recordings stop where the agent stopped.")
	}
	fmt.Fprintf(&b, " Recordings cover only the path this run took. Human label: layer %s.", label.ActualLayer)
	if isNullJSON(graded) {
		b.WriteString(" Not graded.")
		return b.String()
	}
	g, err := decodePart[events.ShadowGradedPayload](graded, events.ShadowGraded)
	if err != nil {
		*warnings = append(*warnings, "room.graded could not be read and was left out of the description")
		return b.String()
	}
	rc := g.RootCause
	if rc != "yes" && rc != "partial" && rc != "no" {
		rc = "unknown"
	}
	fmt.Fprintf(&b, " Grade of that run: layer_correct=%t, root_cause=%s.", g.LayerCorrect, rc)
	return b.String()
}

func contains(xs []string, x string) bool {
	for _, e := range xs {
		if e == x {
			return true
		}
	}
	return false
}

// clip shortens untrusted text for an error message.
func clip(s string, n int) string {
	s, _ = truncateTo(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s), n)
	return s
}

// capString cuts s to n bytes on a rune boundary, silently.
func capString(s string, n int) string {
	s, _ = truncateTo(s, n)
	return s
}

func truncateTo(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n], true
}
