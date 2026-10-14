package evals

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
)

// Learned scenarios (L1a). A closed room that people labelled and confirmed
// holds everything a scenario needs; BuildLearned maps one room export
// (GET /api/rooms/{id}/export) to scenario YAML. The export is the only input:
// no database, no network. The rules that decide the design (tasks/L1):
//
//  1. only human-confirmed rooms teach (eligibility below);
//  2. every written string goes through the redactor, a second pass over the
//     written file must change nothing, and the file lands in learned/pending/,
//     which no eval run reads: a person moves it into learned/ in a PR;
//  5. a fingerprint already in learned/ or learned/pending/ is refused, and a
//     pipeline holds at most MaxLearnedPerPipeline scenarios.

// Directories under evals/scenarios.
const (
	// LearnedDir holds reviewed learned scenarios; the default set replays it.
	LearnedDir = "learned"
	// PendingDir (under LearnedDir) holds fresh exports. Nothing replays it;
	// -check only validates it.
	PendingDir = "pending"
)

// MaxLearnedPerPipeline caps learned plus pending scenarios per pipeline, so
// one noisy pipeline cannot take over the set (and the eval bill).
const MaxLearnedPerPipeline = 10

const (
	// maxLearnedData is the largest data object kept per result: the model reads
	// summary, data is detail for the UI and a place for values to hide.
	maxLearnedData = 4 << 10
	// MaxExportBytes bounds what the exporter reads.
	MaxExportBytes = 64 << 20
	// learnedExportSchema is the export format this file understands.
	learnedExportSchema = "piperoom.room-export/v1"
	// maxIdentTerms caps identifier and number terms in expect.root_cause, so a
	// long cause cannot demand an impossible hypothesis.
	maxIdentTerms = 5
	maxWordTerms  = 3
	minWordLen    = 5
)

// Errors BuildLearned returns, wrapped, so callers and tests can tell the
// cases apart with errors.Is.
var (
	// ErrExportMalformed: the input is not a room export of the known schema.
	ErrExportMalformed = errors.New("evals: not a room export")
	// ErrExportHash: integrity.sha256 does not match the document.
	ErrExportHash = errors.New("evals: room export hash mismatch")
	// ErrLearnedShadow: shadow rooms never teach; their agent output stays out of git.
	ErrLearnedShadow = errors.New("evals: shadow rooms are not exported as learned scenarios")
	// ErrLearnedOpen: the room has not been closed.
	ErrLearnedOpen = errors.New("evals: room is not closed")
	// ErrLearnedUnlabelled: no room.labelled with both cause and actual_layer by a person.
	ErrLearnedUnlabelled = errors.New("evals: room has no human label with cause and actual_layer")
	// ErrLearnedNoVerdict: no person confirmed or disputed a hypothesis.
	ErrLearnedNoVerdict = errors.New("evals: no hypothesis.verdict by a person")
	// ErrLearnedNotRecovered: a fix was executed but its verification does not say recovered.
	ErrLearnedNotRecovered = errors.New("evals: executed fix has no recovered verification")
	// ErrLearnedNoTerms: the cause has no term that is not already in the alert title.
	ErrLearnedNoTerms = errors.New("evals: cause has no root-cause term beyond the alert title")
	// ErrLearnedUnknownPipeline: the room's pipeline is not in the owners file.
	ErrLearnedUnknownPipeline = errors.New("evals: room's pipeline is not in owners.yaml")
	// ErrLearnedDuplicate: the fingerprint already exists in learned/ or learned/pending/.
	ErrLearnedDuplicate = errors.New("evals: a learned scenario with this fingerprint exists")
	// ErrLearnedCap: the pipeline already has MaxLearnedPerPipeline scenarios.
	ErrLearnedCap = errors.New("evals: pipeline has reached the learned scenario cap")
	// ErrLearnedRedaction: a second redaction pass would change the file.
	ErrLearnedRedaction = errors.New("evals: a second redaction pass changes the scenario")
	// ErrLearnedExists: the output file exists (use force to overwrite).
	ErrLearnedExists = errors.New("evals: output file exists")
)

// Redactor is the part of *redact.Redactor the exporter uses; tests pass a
// wrapper to prove a non-idempotent redactor is refused.
type Redactor interface {
	Text(string) (string, redact.Stats)
	JSON(any) any
}

// LearnedOptions configure an export.
type LearnedOptions struct {
	// Owners resolves room.created's pipeline id to the pipeline written into the file.
	Owners *owners.Owners
	// Dir is evals/scenarios/learned: dedup and the cap read it and Dir/pending.
	Dir string
	// Force allows a fingerprint that already exists, and overwriting the output
	// file. It never lifts the per-pipeline cap.
	Force bool
	// Redactor defaults to the product's redactor.
	Redactor Redactor
	// Validate runs the scenario through the same checks as evalrun -check
	// before it is returned, so a file the exporter writes is one -check accepts.
	Validate bool
}

// LearnedResult is one exported scenario.
type LearnedResult struct {
	// Name is the scenario name and the file name without .yaml.
	Name string
	// YAML is the file, validated by parsing it back with ParseScenario.
	YAML []byte
	// Scenario is YAML parsed back: the value evalrun -check sees.
	Scenario Scenario
	// Fingerprint is the dedup key written to the file's first line.
	Fingerprint string
	// Hard is true for a room whose every verdict was disputed.
	Hard bool
}

// ---- reading the export ----

type roomExport struct {
	Schema    string `json:"schema"`
	Integrity struct {
		SHA256 string `json:"sha256"`
	} `json:"integrity"`
	Room struct {
		ID         string     `json:"id"`
		Title      string     `json:"title"`
		PipelineID string     `json:"pipeline_id"`
		Status     string     `json:"status"`
		ClosedAt   *time.Time `json:"closed_at"`
	} `json:"room"`
	Events    []events.Event `json:"events"`
	Proposals []struct {
		ProposalID string                `json:"proposal_id"`
		Steps      []events.ProposalStep `json:"steps"`
	} `json:"proposals"`
}

// VerifyExportHash recomputes the document hash the way an auditor does
// (docs/api.md): drop the top-level "integrity" member, canonicalise with
// RFC 8785, SHA-256, and compare with integrity.sha256. A document JCS cannot
// write (a number outside double range, a repeated key) is refused, never
// skipped.
func VerifyExportHash(raw []byte) error {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		return fmt.Errorf("%w: %v", ErrExportMalformed, err)
	}
	var integ struct {
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(members["integrity"], &integ); err != nil || integ.SHA256 == "" {
		return fmt.Errorf("%w: no integrity.sha256", ErrExportMalformed)
	}
	delete(members, "integrity")
	rest, err := json.Marshal(members)
	if err != nil {
		return fmt.Errorf("evals: re-encode export: %w", err)
	}
	canon, err := jcs.Transform(rest)
	if err != nil {
		return fmt.Errorf("%w: cannot canonicalise the document: %v", ErrExportHash, err)
	}
	sum := sha256.Sum256(canon)
	if got := hex.EncodeToString(sum[:]); got != integ.SHA256 {
		return fmt.Errorf("%w: document hashes to %s, integrity.sha256 says %s", ErrExportHash, got, integ.SHA256)
	}
	return nil
}

func parseRoomExport(raw []byte) (roomExport, error) {
	if len(raw) > MaxExportBytes {
		return roomExport{}, fmt.Errorf("%w: %d bytes, at most %d are read", ErrExportMalformed, len(raw), MaxExportBytes)
	}
	if err := VerifyExportHash(raw); err != nil {
		return roomExport{}, err
	}
	var x roomExport
	if err := json.Unmarshal(raw, &x); err != nil {
		return roomExport{}, fmt.Errorf("%w: %v", ErrExportMalformed, err)
	}
	if x.Schema != learnedExportSchema {
		return roomExport{}, fmt.Errorf("%w: schema %q, want %q", ErrExportMalformed, x.Schema, learnedExportSchema)
	}
	sort.SliceStable(x.Events, func(i, j int) bool { return x.Events[i].Seq < x.Events[j].Seq })
	return x, nil
}

// ---- what the log says ----

type learnedCall struct {
	tool  string
	input json.RawMessage
}

type learnedResultEv struct {
	seq int64
	events.AgentToolResultPayload
}

type verdictEv struct {
	hypothesisSeq int64
	confirmed     bool
}

type roomFacts struct {
	created    events.RoomCreatedPayload
	closed     bool
	closedAt   time.Time
	label      events.RoomLabelledPayload
	hasLabel   bool
	verdicts   []verdictEv // last per hypothesis
	hypotheses map[int64]string
	calls      map[string]learnedCall
	results    []learnedResultEv
	inputs     []learnedInput // steers, comments, executed steps, in log order
	// executed lists proposals with a successful step, in order of first success.
	executed []string
	// verified is the last agent.verification per proposal.
	verified map[string]events.AgentVerificationPayload
	// firstOK is the seq of the first successful step, 0 when none ran.
	firstOK    int64
	firstOKAct string
}

// learnedInput is an input before redaction; seq keeps log order.
type learnedInput struct {
	kind, from, text string
	action           string
	params           json.RawMessage
}

func readFacts(x roomExport) (roomFacts, error) {
	f := roomFacts{
		hypotheses: map[int64]string{},
		calls:      map[string]learnedCall{},
		verified:   map[string]events.AgentVerificationPayload{},
	}
	steps := map[string][]events.ProposalStep{}
	for _, p := range x.Proposals {
		steps[p.ProposalID] = p.Steps
	}
	byHyp := map[int64]int{}
	seenExec := map[string]bool{}
	for _, e := range x.Events {
		human := e.Actor.Kind == events.ActorHuman
		switch e.Type {
		case events.RoomCreated:
			p, err := events.Decode[events.RoomCreatedPayload](e)
			if err != nil {
				return f, fmt.Errorf("%w: %v", ErrExportMalformed, err)
			}
			f.created = p
		case events.RoomClosed:
			f.closed, f.closedAt = true, e.At
		case events.RoomLabelled:
			if !human {
				continue
			}
			if p, err := events.Decode[events.RoomLabelledPayload](e); err == nil {
				f.label, f.hasLabel = p, true // the last label wins
			}
		case events.AgentHypothesis:
			if p, err := events.Decode[events.AgentHypothesisPayload](e); err == nil {
				f.hypotheses[e.Seq] = p.Text
			}
		case events.HypothesisVerdict:
			if !human {
				continue
			}
			p, err := events.Decode[events.HypothesisVerdictPayload](e)
			if err != nil || (p.Verdict != "confirmed" && p.Verdict != "disputed") {
				continue
			}
			v := verdictEv{hypothesisSeq: p.HypothesisSeq, confirmed: p.Verdict == "confirmed"}
			if i, ok := byHyp[p.HypothesisSeq]; ok {
				f.verdicts[i] = v // a person changed their mind: the last one stands
			} else {
				byHyp[p.HypothesisSeq] = len(f.verdicts)
				f.verdicts = append(f.verdicts, v)
			}
		case events.AgentToolCall:
			if p, err := events.Decode[events.AgentToolCallPayload](e); err == nil {
				f.calls[p.ToolUseID] = learnedCall{tool: p.Tool, input: p.Input}
			}
		case events.AgentToolResult:
			if p, err := events.Decode[events.AgentToolResultPayload](e); err == nil {
				f.results = append(f.results, learnedResultEv{seq: e.Seq, AgentToolResultPayload: p})
			}
		case events.AgentVerification:
			if p, err := events.Decode[events.AgentVerificationPayload](e); err == nil {
				if old, ok := f.verified[p.ProposalID]; !ok || !p.Repeat || !old.Recovered {
					f.verified[p.ProposalID] = p
				}
			}
		case events.SteerSent:
			if p, err := events.Decode[events.SteerSentPayload](e); err == nil {
				f.inputs = append(f.inputs, learnedInput{kind: "steer", from: e.Actor.Name, text: p.Text})
			}
		case events.CommentAdded:
			if human {
				if p, err := events.Decode[events.CommentAddedPayload](e); err == nil {
					f.inputs = append(f.inputs, learnedInput{kind: "comment", from: e.Actor.Name, text: p.Text})
				}
			}
		case events.ActionExecuted:
			p, err := events.Decode[events.ActionExecutedPayload](e)
			if err != nil || !p.OK {
				continue // a failed step is not a fix; the agent saw it, the replay need not
			}
			ps := steps[p.ProposalID]
			if p.StepIndex < 0 || p.StepIndex >= len(ps) {
				return f, fmt.Errorf("%w: action.executed names step %d of proposal %q, which the export does not hold", ErrExportMalformed, p.StepIndex, p.ProposalID)
			}
			st := ps[p.StepIndex]
			if f.firstOK == 0 {
				f.firstOK, f.firstOKAct = e.Seq, st.Action
			}
			if !seenExec[p.ProposalID] {
				seenExec[p.ProposalID] = true
				f.executed = append(f.executed, p.ProposalID)
			}
			f.inputs = append(f.inputs, learnedInput{kind: "action_result", action: st.Action, params: st.Params, text: p.Output})
		}
	}
	return f, nil
}

// ---- eligibility (guardrail 1) ----

type eligibility struct {
	hard     bool
	disputed []string // hypothesis texts a person disputed (hard cases)
}

func (f roomFacts) eligible() (eligibility, error) {
	if f.created.Shadow {
		return eligibility{}, ErrLearnedShadow
	}
	if !f.closed {
		return eligibility{}, ErrLearnedOpen
	}
	if !f.hasLabel || strings.TrimSpace(f.label.Cause) == "" || strings.TrimSpace(f.label.ActualLayer) == "" {
		return eligibility{}, ErrLearnedUnlabelled
	}
	known := false
	for _, l := range events.Layers {
		known = known || l == f.label.ActualLayer
	}
	if !known {
		return eligibility{}, fmt.Errorf("%w: actual_layer %q is not one of %s", ErrLearnedUnlabelled, f.label.ActualLayer, strings.Join(events.Layers, ", "))
	}
	var el eligibility
	confirmed := false
	for _, v := range f.verdicts {
		if v.confirmed {
			confirmed = true
		} else if t := f.hypotheses[v.hypothesisSeq]; t != "" {
			el.disputed = append(el.disputed, t)
		}
	}
	switch {
	case confirmed:
	case len(f.verdicts) > 0:
		el.hard = true
	default:
		return eligibility{}, ErrLearnedNoVerdict
	}
	for _, id := range f.executed {
		if v, ok := f.verified[id]; !ok || !v.Recovered {
			return eligibility{}, fmt.Errorf("%w: proposal %s", ErrLearnedNotRecovered, id)
		}
	}
	return el, nil
}

// ---- output shapes (field order of the scenario format, empty fields omitted) ----

type lrnPipeline struct {
	ID             string   `yaml:"id"`
	Title          string   `yaml:"title,omitempty"`
	TopicPrefix    string   `yaml:"topic_prefix"`
	SourceTables   []string `yaml:"source_tables,omitempty,flow"`
	Connectors     []string `yaml:"connectors,omitempty,flow"`
	ConsumerGroups []string `yaml:"consumer_groups,omitempty,flow"`
	SinkTables     []string `yaml:"sink_tables,omitempty,flow"`
	SinkTSColumn   string   `yaml:"sink_ts_column,omitempty"`
}

type lrnResources struct {
	Connectors []string `yaml:"connectors,omitempty,flow"`
	Subjects   []string `yaml:"subjects,omitempty,flow"`
}

type lrnAlert struct {
	Title       string `yaml:"title"`
	PipelineMap string `yaml:"pipeline_map,omitempty"`
}

type lrnObs struct {
	Summary   string `yaml:"summary,omitempty"`
	Data      any    `yaml:"data,omitempty"`
	Truncated bool   `yaml:"truncated,omitempty"`
	Error     string `yaml:"error,omitempty"`
}

type lrnRecording struct {
	Tool      string         `yaml:"tool"`
	Input     map[string]any `yaml:"input,omitempty"`
	Phase     string         `yaml:"phase,omitempty"`
	Summary   string         `yaml:"summary,omitempty"`
	Data      any            `yaml:"data,omitempty"`
	Truncated bool           `yaml:"truncated,omitempty"`
	Error     string         `yaml:"error,omitempty"`
	Sequence  []lrnObs       `yaml:"sequence,omitempty"`
}

type lrnTransition struct {
	On   lrnOn  `yaml:"on"`
	To   string `yaml:"to"`
	From string `yaml:"from,omitempty"`
}

type lrnOn struct {
	Action string `yaml:"action"`
	OK     *bool  `yaml:"ok,omitempty"`
}

type lrnInput struct {
	Kind   string         `yaml:"kind"`
	From   string         `yaml:"from,omitempty"`
	Text   string         `yaml:"text"`
	Action string         `yaml:"action,omitempty"`
	OK     *bool          `yaml:"ok,omitempty"`
	Params map[string]any `yaml:"params,omitempty"`
}

type lrnExpect struct {
	Outcome        string     `yaml:"outcome"`
	RootCause      [][]string `yaml:"root_cause,omitempty,flow"`
	RootCauseLayer string     `yaml:"root_cause_layer,omitempty"`
	Fix            [][]string `yaml:"fix,omitempty,flow"`
}

type lrnScenario struct {
	Name        string          `yaml:"name"`
	Description string          `yaml:"description"`
	Pipeline    lrnPipeline     `yaml:"pipeline"`
	Resources   *lrnResources   `yaml:"resources,omitempty"`
	Alert       lrnAlert        `yaml:"alert"`
	Phases      []string        `yaml:"phases,omitempty,flow"`
	Transitions []lrnTransition `yaml:"transitions,omitempty"`
	Recordings  []lrnRecording  `yaml:"recordings,omitempty"`
	Inputs      []lrnInput      `yaml:"inputs,omitempty"`
	Expect      lrnExpect       `yaml:"expect"`
}

// droppedInputKeys are call inputs the model picks freely (a time window, a
// count, a free-text query). A recording that kept them would match only a
// call that repeats the same pick. The one table; tests pin it.
var droppedInputKeys = map[string]bool{
	"since": true, "until": true, "limit": true, "max_scan": true, "window": true,
	"older_than_minutes": true, "max_results": true, "query": true,
	"from": true, "to": true, "at": true, "timestamp": true,
}

// skippedTools are never recorded: similar_incidents quotes other rooms (its
// text is not this incident's evidence, and guardrail 4 keeps retrieval out of
// evals), and an eval does not register it.
var skippedTools = map[string]bool{"similar_incidents": true}

var (
	learnedNameBad = regexp.MustCompile(`[^A-Za-z0-9_-]`)
	identRe        = regexp.MustCompile(`[A-Za-z0-9]+(?:[-_.][A-Za-z0-9]+)+`)
	digitsRe       = regexp.MustCompile(`^[0-9]{2,}$`)
	lettersRe      = regexp.MustCompile(`^[a-z]+$`)
)

// causeStop are words too generic to tell one incident from another.
var causeStop = set(strings.Fields(`about after again because before being between cannot caused causes could
	during error errors every failed failing failure fails first found gives given having issue later never other
	rather really should since still their there these things those through under until using value values
	which while would without wrong update updated broke broken stopped working`))

// rootCauseTerms picks the terms of a typed cause in this order: identifiers
// (pg-orders-connector, public.orders), numbers (409), then the longest words,
// three at most. A term whose words all sit in skip (the alert title) is passed
// over, so it neither counts nor takes a slot.
func rootCauseTerms(cause string, skip map[string]bool) []string {
	var out []string
	seen := map[string]bool{}
	add := func(t string) bool {
		if t == "" || seen[t] {
			return false
		}
		if len(skip) > 0 {
			all := true
			for _, w := range tokenize(t) {
				all = all && skip[w]
			}
			if all {
				return false // already in the alert title: a lazy match
			}
		}
		seen[t] = true
		out = append(out, t)
		return true
	}
	inIdent := map[string]bool{}
	nIdent := 0
	for _, id := range identRe.FindAllString(cause, -1) {
		toks := tokenize(id)
		for _, t := range toks {
			inIdent[t] = true
		}
		if nIdent < maxIdentTerms && add(strings.Join(toks, " ")) {
			nIdent++
		}
	}
	toks := tokenize(cause)
	for _, t := range toks {
		if nIdent >= maxIdentTerms {
			break
		}
		if digitsRe.MatchString(t) && !inIdent[t] && add(t) {
			nIdent++
		}
	}
	type word struct {
		w   string
		pos int
	}
	var words []word
	have := map[string]bool{}
	for i, t := range toks {
		if len(t) < minWordLen || !lettersRe.MatchString(t) || causeStop[t] || inIdent[t] || have[t] || skip[t] {
			continue
		}
		have[t] = true
		words = append(words, word{t, i})
	}
	sort.SliceStable(words, func(i, j int) bool { return len(words[i].w) > len(words[j].w) })
	if len(words) > maxWordTerms {
		words = words[:maxWordTerms]
	}
	sort.SliceStable(words, func(i, j int) bool { return words[i].pos < words[j].pos })
	for _, w := range words {
		add(w.w)
	}
	return out
}

func fingerprintOf(pipeline, layer string, terms []string) string {
	ts := append([]string(nil), terms...)
	sort.Strings(ts)
	return pipeline + "|" + layer + "|" + strings.Join(ts, ",")
}

const fingerprintPrefix = "# fingerprint: "

// ---- dedup and cap (guardrail 5) ----

type existingLearned struct {
	file        string
	pipeline    string
	fingerprint string
}

// scanLearned reads learned/*.yaml and learned/pending/*.yaml. A file it
// cannot read is an error, not a gap in the cap.
func scanLearned(dir string) ([]existingLearned, error) {
	var out []existingLearned
	for _, d := range []string{dir, filepath.Join(dir, PendingDir)} {
		paths, err := filepath.Glob(filepath.Join(d, "*.yaml"))
		if err != nil {
			return nil, fmt.Errorf("evals: list %s: %w", d, err)
		}
		sort.Strings(paths)
		for _, p := range paths {
			b, err := os.ReadFile(p)
			if err != nil {
				return nil, fmt.Errorf("evals: read %s: %w", p, err)
			}
			var doc struct {
				Pipeline struct {
					ID string `yaml:"id"`
				} `yaml:"pipeline"`
				Expect struct {
					RootCause      [][]string `yaml:"root_cause"`
					RootCauseLayer string     `yaml:"root_cause_layer"`
				} `yaml:"expect"`
			}
			if err := yaml.Unmarshal(b, &doc); err != nil {
				return nil, fmt.Errorf("evals: parse %s: %w", p, err)
			}
			fp := headerFingerprint(b)
			if fp == "" {
				// A hand-moved or older file: rebuild from what it expects.
				var terms []string
				for _, g := range doc.Expect.RootCause {
					terms = append(terms, g...)
				}
				fp = fingerprintOf(doc.Pipeline.ID, doc.Expect.RootCauseLayer, terms)
			}
			out = append(out, existingLearned{file: p, pipeline: doc.Pipeline.ID, fingerprint: fp})
		}
	}
	return out, nil
}

func headerFingerprint(b []byte) string {
	for _, line := range strings.SplitN(string(b), "\n", 4) {
		if rest, ok := strings.CutPrefix(line, fingerprintPrefix); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

// ---- BuildLearned ----

// BuildLearned maps one room export to a learned scenario. It writes nothing.
func BuildLearned(raw []byte, o LearnedOptions) (LearnedResult, error) {
	rd := o.Redactor
	if rd == nil {
		rd = redact.New(redact.DefaultOptions())
	}
	x, err := parseRoomExport(raw)
	if err != nil {
		return LearnedResult{}, err
	}
	f, err := readFacts(x)
	if err != nil {
		return LearnedResult{}, err
	}
	el, err := f.eligible()
	if err != nil {
		return LearnedResult{}, err
	}
	if o.Owners == nil {
		return LearnedResult{}, errors.New("evals: LearnedOptions.Owners is nil")
	}
	pipe, ok := o.Owners.Pipeline(x.Room.PipelineID)
	if !ok {
		return LearnedResult{}, fmt.Errorf("%w: %q", ErrLearnedUnknownPipeline, x.Room.PipelineID)
	}

	text := func(s string) string { t, _ := rd.Text(s); return t }
	cause := text(f.label.Cause)
	title := text(f.created.Title)
	if title == "" {
		title = text(x.Room.Title)
	}
	// The fingerprint is the incident itself, so it ignores the title; the
	// expectation skips what the alert title already says.
	fp := fingerprintOf(pipe.ID, f.label.ActualLayer, rootCauseTerms(cause, nil))

	exp := lrnExpect{Outcome: OutcomeDiagnose, RootCauseLayer: f.label.ActualLayer}
	if el.hard {
		// Validation grades only an abstain outcome on its own: no root_cause,
		// layer or forbidden fields. The disputed story is named in the description.
		exp = lrnExpect{Outcome: OutcomeAbstain}
	} else {
		for _, t := range rootCauseTerms(cause, set(tokenize(title))) {
			exp.RootCause = append(exp.RootCause, []string{t})
		}
		if len(exp.RootCause) == 0 {
			return LearnedResult{}, fmt.Errorf("%w: cause %q, title %q", ErrLearnedNoTerms, cause, title)
		}
		if id := f.label.FixProposalID; id != "" {
			acts, err := proposalActions(x, id)
			if err != nil {
				return LearnedResult{}, err
			}
			for _, a := range acts {
				exp.Fix = append(exp.Fix, []string{a})
			}
		}
	}

	closed := f.closedAt
	if x.Room.ClosedAt != nil {
		closed = *x.Room.ClosedAt
	}
	name := learnedName(pipe.ID, closed, x.Room.ID)

	if o.Dir != "" {
		existing, err := scanLearned(o.Dir)
		if err != nil {
			return LearnedResult{}, err
		}
		n := 0
		for _, e := range existing {
			if e.pipeline == pipe.ID {
				n++
			}
			if e.fingerprint == fp && !o.Force {
				return LearnedResult{}, fmt.Errorf("%w: %s (%s)", ErrLearnedDuplicate, e.file, fp)
			}
		}
		if n >= MaxLearnedPerPipeline {
			return LearnedResult{}, fmt.Errorf("%w: %d for %q", ErrLearnedCap, n, pipe.ID)
		}
	}

	desc := title + " — " + cause
	if el.hard {
		var dis []string
		for _, t := range el.disputed {
			dis = append(dis, strings.Join(rootCauseTerms(text(t), nil), ", "))
		}
		desc = "hard: true; people disputed every hypothesis; the agent must not assert: " +
			strings.Join(dis, " | ") + "; " + desc
	}

	sc := lrnScenario{
		Name:        name,
		Description: desc,
		Pipeline: lrnPipeline{
			ID: pipe.ID, Title: pipe.Title, TopicPrefix: pipe.TopicPrefix, SourceTables: pipe.SourceTables,
			Connectors: pipe.Connectors, ConsumerGroups: pipe.ConsumerGroups, SinkTables: pipe.SinkTables,
			SinkTSColumn: pipe.SinkTSColumn,
		},
		Alert:  lrnAlert{Title: title},
		Expect: exp,
	}
	buildRecordings(&sc, f, rd, text)
	buildInputs(&sc, f, x, rd, text)

	b, err := marshalLearned(sc, fp)
	if err != nil {
		return LearnedResult{}, err
	}
	if err := checkSecondPass(b, text); err != nil {
		return LearnedResult{}, err
	}
	parsed, err := ParseScenario(b)
	if err != nil {
		return LearnedResult{}, fmt.Errorf("evals: exported scenario does not parse: %w", err)
	}
	if o.Validate {
		defs, err := LiveTools()
		if err != nil {
			return LearnedResult{}, err
		}
		if err := ValidateAll([]Scenario{parsed}, defs); err != nil {
			return LearnedResult{}, fmt.Errorf("evals: exported scenario does not validate (it is not written): %w", err)
		}
	}
	return LearnedResult{Name: name, YAML: b, Scenario: parsed, Fingerprint: fp, Hard: el.hard}, nil
}

func proposalActions(x roomExport, id string) ([]string, error) {
	for _, p := range x.Proposals {
		if p.ProposalID != id {
			continue
		}
		var out []string
		seen := map[string]bool{}
		for _, s := range p.Steps {
			if !seen[s.Action] {
				seen[s.Action] = true
				out = append(out, s.Action)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: label names fix_proposal_id %q, which the export does not hold", ErrExportMalformed, id)
}

func learnedName(pipeline string, closed time.Time, roomID string) string {
	p := learnedNameBad.ReplaceAllString(pipeline, "-")
	if len(p) > 30 {
		p = p[:30]
	}
	short := learnedNameBad.ReplaceAllString(roomID, "")
	if len(short) > 8 {
		short = short[len(short)-8:]
	}
	if short == "" {
		short = "room"
	}
	date := "0000-00-00"
	if !closed.IsZero() {
		date = closed.UTC().Format("2006-01-02")
	}
	return p + "-" + date + "-" + short
}

// ---- recordings ----

func buildRecordings(sc *lrnScenario, f roomFacts, rd Redactor, text func(string) string) {
	type group struct {
		tool  string
		input map[string]any
		phase string
		obs   []lrnObs
	}
	var order []string
	groups := map[string]*group{}
	for _, r := range f.results {
		c, ok := f.calls[r.ToolUseID]
		if !ok || skippedTools[c.tool] {
			continue
		}
		phase := ""
		if f.firstOK != 0 && r.seq > f.firstOK {
			phase = "recovered"
		}
		in := keptInput(c.input, rd)
		key, _ := json.Marshal(in)
		k := c.tool + "\x00" + string(key) + "\x00" + phase
		g := groups[k]
		if g == nil {
			g = &group{tool: c.tool, input: in, phase: phase}
			groups[k] = g
			order = append(order, k)
		}
		o := lrnObs{Summary: text(r.Summary), Truncated: r.Truncated}
		if r.IsError {
			// The agent loop prefixes tool errors with "error: " and the replay adds it again.
			o = lrnObs{Error: text(strings.TrimPrefix(r.Summary, "error: "))}
		} else if d := keptData(r.Data, rd); d != nil {
			o.Data = d
		}
		if o.Summary == "" && o.Error == "" {
			continue // a recording needs one of them
		}
		g.obs = append(g.obs, o)
		if g.tool == "pipeline_map" && sc.Alert.PipelineMap == "" && !r.IsError && phase == "" {
			sc.Alert.PipelineMap = o.Summary
		}
	}
	for _, k := range order {
		g := groups[k]
		if len(g.obs) == 0 {
			continue
		}
		rec := lrnRecording{Tool: g.tool, Input: g.input, Phase: g.phase}
		if len(g.obs) == 1 {
			o := g.obs[0]
			rec.Summary, rec.Data, rec.Truncated, rec.Error = o.Summary, o.Data, o.Truncated, o.Error
		} else {
			rec.Sequence = g.obs
		}
		sc.Recordings = append(sc.Recordings, rec)
	}
	if f.firstOK != 0 {
		ok := true
		sc.Phases = []string{"broken", "recovered"}
		sc.Transitions = []lrnTransition{{On: lrnOn{Action: f.firstOKAct, OK: &ok}, To: "recovered"}}
	}
}

// keptInput is the call's input without the keys the model picks freely.
func keptInput(raw json.RawMessage, rd Redactor) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	v, ok := decodePlain(raw).(map[string]any)
	if !ok {
		return nil
	}
	for k := range v {
		if droppedInputKeys[k] {
			delete(v, k)
		}
	}
	if len(v) == 0 {
		return nil
	}
	return rd.JSON(v).(map[string]any)
}

// keptData is the result's data when it is small and whole.
func keptData(raw json.RawMessage, rd Redactor) any {
	if len(raw) == 0 || len(raw) > 4*maxLearnedData {
		return nil
	}
	v := decodePlain(raw)
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		if t, _ := m["truncated"].(bool); t {
			return nil // events.TruncatedData: a cut preview teaches the wrong shape
		}
	}
	v = rd.JSON(v)
	b, err := json.Marshal(v)
	if err != nil || len(b) > maxLearnedData {
		return nil
	}
	return v
}

// decodePlain turns JSON into YAML-friendly values (whole numbers stay integers).
func decodePlain(raw []byte) any {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil
	}
	return plainNumbers(v)
}

func plainNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = plainNumbers(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = plainNumbers(e)
		}
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		fl, _ := x.Float64()
		return fl
	}
	return v
}

// ---- inputs ----

func buildInputs(sc *lrnScenario, f roomFacts, x roomExport, rd Redactor, text func(string) string) {
	for _, in := range f.inputs {
		switch in.kind {
		case "action_result":
			ok := true
			t := text(in.text)
			if strings.TrimSpace(t) == "" {
				t = "executed"
			}
			o := lrnInput{Kind: "action_result", Text: t, Action: in.action, OK: &ok}
			if p, isMap := decodePlain(in.params).(map[string]any); isMap && len(p) > 0 {
				o.Params = rd.JSON(p).(map[string]any)
			}
			sc.Inputs = append(sc.Inputs, o)
			addResources(sc, o.Params, sc.Pipeline.Connectors)
		default:
			t := text(in.text)
			if strings.TrimSpace(t) == "" {
				continue
			}
			sc.Inputs = append(sc.Inputs, lrnInput{Kind: in.kind, From: text(in.from), Text: t})
		}
	}
	if sc.Resources == nil && len(sc.Pipeline.Connectors) > 0 {
		sc.Resources = &lrnResources{Connectors: sc.Pipeline.Connectors}
	}
}

// addResources lists what an executed step names, so the action's Validate
// finds it on the recorded stack.
func addResources(sc *lrnScenario, params map[string]any, base []string) {
	if sc.Resources == nil {
		sc.Resources = &lrnResources{Connectors: append([]string(nil), base...)}
	}
	if c, _ := params["connector"].(string); c != "" && !containsStr(sc.Resources.Connectors, c) {
		sc.Resources.Connectors = append(sc.Resources.Connectors, c)
	}
	if s, _ := params["subject"].(string); s != "" && !containsStr(sc.Resources.Subjects, s) {
		sc.Resources.Subjects = append(sc.Resources.Subjects, s)
	}
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// ---- output and the second pass (guardrail 2) ----

func marshalLearned(sc lrnScenario, fingerprint string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("# Learned from a closed room (L1a): human-confirmed, redacted, one tool result per call.\n")
	buf.WriteString("# Review before moving out of pending/: read every summary and input; see evals/README.md.\n")
	buf.WriteString(fingerprintPrefix + strings.NewReplacer("\n", " ", "\r", " ").Replace(fingerprint) + "\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(sc); err != nil {
		return nil, fmt.Errorf("evals: encode learned scenario: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("evals: encode learned scenario: %w", err)
	}
	return buf.Bytes(), nil
}

// checkSecondPass runs the redactor over every string the file holds. The file
// is read back as YAML, so what is checked is what is written, not what was
// meant to be written.
func checkSecondPass(b []byte, text func(string) string) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return fmt.Errorf("evals: re-read learned scenario: %w", err)
	}
	var walk func(n *yaml.Node) error
	walk = func(n *yaml.Node) error {
		if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
			if got := text(n.Value); got != n.Value {
				return fmt.Errorf("%w: line %d", ErrLearnedRedaction, n.Line)
			}
		}
		for _, c := range n.Content {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(&doc)
}

// ExportLearned builds the scenario and writes it to Dir/pending/<name>.yaml.
func ExportLearned(raw []byte, o LearnedOptions) (LearnedResult, string, error) {
	if o.Dir == "" {
		return LearnedResult{}, "", errors.New("evals: LearnedOptions.Dir is empty")
	}
	res, err := BuildLearned(raw, o)
	if err != nil {
		return LearnedResult{}, "", err
	}
	dir := filepath.Join(o.Dir, PendingDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return LearnedResult{}, "", fmt.Errorf("evals: create %s: %w", dir, err)
	}
	path := filepath.Join(dir, res.Name+".yaml")
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if o.Force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	fh, err := os.OpenFile(path, flags, 0o644)
	if errors.Is(err, os.ErrExist) {
		return LearnedResult{}, "", fmt.Errorf("%w: %s", ErrLearnedExists, path)
	}
	if err != nil {
		return LearnedResult{}, "", fmt.Errorf("evals: write %s: %w", path, err)
	}
	_, werr := fh.Write(res.YAML)
	if cerr := fh.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return LearnedResult{}, "", fmt.Errorf("evals: write %s: %w", path, werr)
	}
	return res, path, nil
}

// LoadLearned reads the reviewed learned scenarios (learned/*.yaml under the
// scenarios directory) for the default set. A missing or empty directory is no
// scenarios, not an error. It never reads pending/.
func LoadLearned(scenariosDir string) ([]Scenario, error) {
	return loadOptionalDir(filepath.Join(scenariosDir, LearnedDir))
}

// LoadPending reads learned/pending/*.yaml. Only validation uses it; nothing
// replays a pending file.
func LoadPending(scenariosDir string) ([]Scenario, error) {
	return loadOptionalDir(filepath.Join(scenariosDir, LearnedDir, PendingDir))
}

func loadOptionalDir(dir string) ([]Scenario, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, fmt.Errorf("evals: list %s: %w", dir, err)
	}
	if len(paths) == 0 {
		return nil, nil
	}
	return LoadScenarios(dir)
}
