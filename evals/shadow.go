package evals

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	ErrShadowNoRoom = errors.New("evals: shadow response has no room (save it with ?include=data,room)")
	// ErrShadowNoData is returned when no tool_result carries data (the
	// response was saved without ?include=data).
	ErrShadowNoData = errors.New("evals: shadow response has no tool_result data (save it with ?include=data,room)")
	// ErrShadowUnlabelled is returned when there is no usable human label. The
	// label is the only ground truth a scenario can have.
	ErrShadowUnlabelled = errors.New("evals: shadow run has no human label")
	// ErrShadowUnknownLayer is returned when the label names a layer that is
	// not one of events.Layers.
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
	RoomID             string     `json:"room_id"`
	Status             string     `json:"status"`
	Model              string     `json:"model"`
	PipelineMap        string     `json:"pipeline_map"`
	StartedAt          *time.Time `json:"started_at"`
	FirstHypothesisAt  *time.Time `json:"first_hypothesis_at"`
	FinalHypothesisAt  *time.Time `json:"final_hypothesis_at"`
	FinishedAt         *time.Time `json:"finished_at"`
	FinishedAfterClose bool       `json:"finished_after_close"`
	FinalHypothesis    string     `json:"final_hypothesis"`
	ToolCalls          int        `json:"tool_calls"`
	Tokens             int64      `json:"tokens"`
	RevealedAt         *time.Time `json:"revealed_at"`
}

// shadowEvent is events.Event plus the "shadow": true marker.
type shadowEvent struct {
	events.Event
	Shadow bool `json:"shadow"`
}

// shadowRoom is the include=room member. Each part is the event payload
// (room.created, shadow.labelled, the latest shadow.graded); a part given as
// the whole Event is unwrapped, because the design names the keys but not
// their depth.
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

// ScenarioFromShadow builds a scenario from one saved GET /api/rooms/{id}/shadow?include=data,room
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
	if !contains(events.Layers, label.ActualLayer) {
		return ShadowResult{}, fmt.Errorf("%w: %q (want one of %s)", ErrShadowUnknownLayer, clip(label.ActualLayer, 40), strings.Join(events.Layers, ", "))
	}
	terms, err := cleanTerms(label.RootCauseTerms)
	if err != nil {
		return ShadowResult{}, err
	}

	pipeline, ok := lookup(created.PipelineID)
	if !ok {
		return ShadowResult{}, fmt.Errorf("%w: %q", ErrShadowUnknownPipeline, clip(created.PipelineID, 80))
	}

	var warnings []string
	recs, observed, skipped, err := recordingsFromEvents(doc.Events, &warnings)
	if err != nil {
		return ShadowResult{}, err
	}
	if len(recs) == 0 {
		return ShadowResult{}, fmt.Errorf("%w (%d call(s) seen, status %s)", ErrShadowNothingToReplay, observed, run.Status)
	}
	if !anyData(recs) && hasNonErrorResult(recs) {
		return ShadowResult{}, ErrShadowNoData
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
	if run.Status != "hypothesised" {
		warnings = append(warnings, fmt.Sprintf("partial run (status %s): the recordings stop where the agent stopped; see evals/README.md", run.Status))
	}

	body, err := yaml.Marshal(sc)
	if err != nil {
		return ShadowResult{}, fmt.Errorf("evals: encode scenario: %w", err)
	}
	out := append([]byte(shadowYAMLHeader), body...)
	if len(out) > maxScenarioBytes {
		return ShadowResult{}, fmt.Errorf("%w: scenario would be %d bytes, cap %d", ErrShadowMalformed, len(out), maxScenarioBytes)
	}

	// Same path as `evalrun -check`: parse what a reader of the file gets, and
	// validate it against the live tool and action definitions.
	parsed, err := ParseScenario(out)
	if err != nil {
		return ShadowResult{}, fmt.Errorf("%w: %v", ErrShadowInvalid, err)
	}
	defs, err := LiveTools()
	if err != nil {
		return ShadowResult{}, err
	}
	if err := ValidateAll([]Scenario{parsed}, defs); err != nil {
		return ShadowResult{}, fmt.Errorf("%w: %v", ErrShadowInvalid, err)
	}
	return ShadowResult{YAML: out, Scenario: parsed, Warnings: warnings}, nil
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
func recordingsFromEvents(evs []shadowEvent, warnings *[]string) (recs []outRecord, observed, skipped int, err error) {
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
	var live, dups, bigData, orphan int
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
			return nil, 0, 0, fmt.Errorf("%w: tool_call %q input: %v", ErrShadowMalformed, clip(id, 40), err)
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
				if len(res.Data) > maxRecordingData {
					bigData++
				} else if v, err := jsonToNative(res.Data); err != nil {
					return nil, 0, 0, fmt.Errorf("%w: tool_result %q data: %v", ErrShadowMalformed, clip(id, 40), err)
				} else {
					r.Data = v
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
	if bigData > 0 {
		*warnings = append(*warnings, fmt.Sprintf("data dropped from %d recording(s) larger than %d bytes", bigData, maxRecordingData))
	}
	if orphan > 0 {
		*warnings = append(*warnings, fmt.Sprintf("%d result(s) with an empty summary were not recorded", orphan))
	}
	return recs, observed, skipped, nil
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

func cleanTerms(in []string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, t := range in {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if utf8.RuneCountInString(t) > maxTermLen {
			return nil, fmt.Errorf("%w: a root_cause_terms entry is longer than %d characters", ErrShadowMalformed, maxTermLen)
		}
		if k := strings.ToLower(t); !seen[k] {
			seen[k] = true
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

func anyData(recs []outRecord) bool {
	for _, r := range recs {
		if r.Data != nil {
			return true
		}
	}
	return false
}

func hasNonErrorResult(recs []outRecord) bool {
	for _, r := range recs {
		if r.Error == "" {
			return true
		}
	}
	return false
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
