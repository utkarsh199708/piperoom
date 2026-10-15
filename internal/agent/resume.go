package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/injectscan"
	"piperoom/internal/tools/runbooks"
)

// Resumed runs (QA T1). A server restart or a hand-off starts a new run in a
// room that already has an investigation: hypothesis, steps, proposals, maybe a
// hand-off. The room gives that run a recap as one HumanInput of Kind
// RecapKind in its first batch, and the run must (1) tell the model it is
// resuming and must not redo finished steps, and (2) verify the proposals of
// earlier runs when their last step's result arrives.
//
// Input handling, kept in one place so it is easy to adapt:
//
//   - HumanInput.Kind == RecapKind. Text is the room's JSON recap (RoomRecap,
//     kind "piperoom.room_recap", version 1), or plain text. It comes first in
//     the run's first batch, before any action_result.
//   - absorbRecaps (called by handleBatch before anything else) records every
//     recap proposal that has steps in the proposal tracker, with the verdict
//     the room already has for it, so a later action_result finds it.
//   - formatRecap renders the recap for the model: the preface and the field
//     labels are trusted structure written here; ids and fixed vocabularies are
//     checked against patterns; every free text (note, titles, hypothesis,
//     summaries, hand-off, plain text) is untrusted and wrapped.
//   - A proposal that is neither in this run's tracker nor in a recap is loaded
//     through ProposalLookup when Deps.Proposals implements it. When it cannot
//     be loaded the room records "not verified" with the reason, once.

// RecapKind is the HumanInput.Kind of a room recap (see RoomRecap).
const RecapKind = "room_recap"

// recapJSONKind is RoomRecap.Kind of the room service's recap (room.Recap).
const recapJSONKind = "piperoom.room_recap"

// recapPreface opens the recap. The room wrote it, not a person: it stands
// outside any wrapper. The system prompt explains the tag.
const recapPreface = "[room recap] This run resumes an earlier investigation of this room. Below is what earlier runs found. Do not redo finished steps: cite their step ids and re-read only what may have changed since."

// RoomRecap is the room service's recap (internal/room Recap, version 1), the
// JSON Text of a RecapKind input. It is mirrored here rather than imported so
// the agent does not depend on the room package. Every field is optional.
type RoomRecap struct {
	Kind         string           `json:"kind"`
	Version      int              `json:"version"`
	RoomID       string           `json:"room_id,omitempty"`
	AsOfSeq      int64            `json:"as_of_seq,omitempty"`
	Note         string           `json:"note,omitempty"`
	Hypothesis   *RecapHypothesis `json:"hypothesis"`
	Steps        []RecapStep      `json:"steps,omitempty"`
	StepsOmitted int              `json:"steps_omitted,omitempty"`
	Proposals    []RecapProposal  `json:"proposals,omitempty"`
	Handoff      *RecapHandoff    `json:"handoff"`
}

// RecapHypothesis is the room's latest hypothesis.
type RecapHypothesis struct {
	Seq          int64    `json:"seq,omitempty"`
	Text         string   `json:"text"`
	Confidence   string   `json:"confidence,omitempty"`
	BasedOnSteps []string `json:"based_on_steps,omitempty"`
}

// RecapStep is one finished read of an earlier run.
type RecapStep struct {
	Seq     int64  `json:"seq,omitempty"`
	StepID  string `json:"step_id"`
	Tool    string `json:"tool"`
	Summary string `json:"summary"`
	IsError bool   `json:"is_error,omitempty"`
}

// RecapProposal is one proposal of the room. Steps let this run verify it;
// Verified and Verdict are the verification the room already recorded.
type RecapProposal struct {
	Seq            int64                 `json:"seq,omitempty"`
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title,omitempty"`
	Status         string                `json:"status,omitempty"`
	ByAgent        bool                  `json:"by_agent,omitempty"`
	Steps          []events.ProposalStep `json:"steps,omitempty"`
	RequiredLayers []string              `json:"required_layers,omitempty"`
	// StepsOK is how far execution got. It is kept raw and only shown when it
	// is a plain count or boolean, so its exact type does not break decoding.
	StepsOK     json.RawMessage `json:"steps_ok,omitempty"`
	LastStepSeq int64           `json:"last_step_seq,omitempty"`
	Verified    bool            `json:"verified,omitempty"`
	Verdict     string          `json:"verdict,omitempty"`
}

// RecapHandoff is the latest hand-off. Summary is a string or an
// events.HandoffSummary object.
type RecapHandoff struct {
	Seq       int64           `json:"seq,omitempty"`
	HandoffID string          `json:"handoff_id,omitempty"`
	Summary   json.RawMessage `json:"summary,omitempty"`
}

// ProposalLookup is implemented by a Proposals that can read back a proposal of
// the room, including one an earlier run made. The runner uses it when a step
// result arrives for a proposal it does not know. Errors are logged, not shown.
type ProposalLookup interface {
	Lookup(ctx context.Context, proposalID string) (events.ProposalCreatedPayload, error)
}

const (
	maxRecapSteps     = 20
	maxRecapProposals = 10
	recapFieldBytes   = 1024
	recapTextBytes    = 4096
)

var (
	recapProposalID = regexp.MustCompile(`^p_[A-Za-z0-9_-]{1,64}$`)
	recapStatuses   = []string{"open", "approved", "executing", "executed", "failed", "outcome_unknown", "rejected", "withdrawn"}
	recapConfidence = []string{"low", "medium", "high"}
	recapStepsOK    = regexp.MustCompile(`^(true|false|[0-9]{1,4})$`)
)

// parseRecap reads a recap input: the room's JSON recap, or nil with the plain
// text. JSON of any other kind is treated as plain text.
func parseRecap(text string) (*RoomRecap, string) {
	t := strings.TrimSpace(text)
	if strings.HasPrefix(t, "{") {
		var rc RoomRecap
		if err := json.Unmarshal([]byte(t), &rc); err == nil && rc.Kind == recapJSONKind {
			return &rc, ""
		}
	}
	return nil, text
}

// recapVerdictWord maps a verification the room recorded to the tracker's word.
// A verdict that is not one of the three ("not verified ...", or text this code
// does not know) gives "", and the proposal stays open for verification.
func recapVerdictWord(p RecapProposal) string {
	if !p.Verified {
		return ""
	}
	v := strings.TrimPrefix(strings.TrimSpace(p.Verdict), "already verified: ")
	switch {
	case strings.HasPrefix(v, "NOT recovered"):
		return "NOT recovered"
	case strings.HasPrefix(v, "recovered"):
		return "recovered"
	case strings.HasPrefix(v, "inconclusive"):
		return "inconclusive"
	}
	return ""
}

// absorbRecaps records the proposals of every recap in the batch, so an
// action_result later in the same batch (the room sends the recap first) finds
// its proposal. Rejected and withdrawn proposals never run, so they are left
// out; a proposal already known keeps this run's record.
func (r *runner) absorbRecaps(inputs []HumanInput) {
	for _, in := range inputs {
		if in.Kind != RecapKind {
			continue
		}
		rc, _ := parseRecap(in.Text)
		if rc == nil {
			continue
		}
		for _, p := range rc.Proposals {
			if !recapProposalID.MatchString(p.ProposalID) || len(p.Steps) == 0 || p.Status == "rejected" || p.Status == "withdrawn" {
				continue
			}
			if _, known := r.proposals.get(p.ProposalID); known {
				continue
			}
			r.proposals.record(p.ProposalID, p.Steps, p.RequiredLayers)
			if word := recapVerdictWord(p); word != "" {
				r.proposals.markVerified(p.ProposalID, word == "recovered", word)
			}
		}
	}
}

// formatRecap renders a recap input for the model (see the top of this file).
func formatRecap(in HumanInput) string {
	rc, plain := parseRecap(in.Text)
	if rc == nil {
		return recapPreface + "\n" + wrapUntrusted(truncatePromptField(plain, recapTextBytes))
	}
	var b strings.Builder
	b.WriteString(recapPreface)
	b.WriteString("\n")
	if rc.Note != "" {
		b.WriteString("Room note: " + wrapUntrusted(truncatePromptField(rc.Note, recapFieldBytes)) + "\n")
	}
	if h := rc.Hypothesis; h != nil && h.Text != "" {
		conf := h.Confidence
		if !slices.Contains(recapConfidence, conf) {
			conf = "unknown"
		}
		var ids []string
		for _, id := range h.BasedOnSteps {
			if triageStepID.MatchString(id) {
				ids = append(ids, id)
			}
		}
		stepText := "none"
		if len(ids) > 0 {
			stepText = strings.Join(ids, ", ")
		}
		fmt.Fprintf(&b, "Hypothesis (confidence %s; steps %s): %s\n", conf, stepText, wrapUntrusted(truncatePromptField(h.Text, recapFieldBytes)))
	}
	if len(rc.Steps) > 0 || rc.StepsOmitted > 0 {
		b.WriteString("Finished steps:\n")
		n, skipped := 0, max(rc.StepsOmitted, 0)
		for _, s := range rc.Steps {
			if !triageStepID.MatchString(s.StepID) || n == maxRecapSteps {
				skipped++
				continue
			}
			n++
			text := truncatePromptField(s.Summary, recapFieldBytes)
			if s.IsError && !strings.HasPrefix(text, "error: ") {
				text = "error: " + text
			}
			w := wrapToolOutput(sanitizeLabel(s.Tool), s.StepID, text)
			if s.Tool != runbooks.ToolName && len(injectscan.Scan(s.Summary)) > 0 {
				w = markSuspected(w)
			}
			b.WriteString(w)
			b.WriteString("\n")
		}
		if skipped > 0 {
			fmt.Fprintf(&b, "(%d earlier steps not shown)\n", skipped)
		}
	}
	if len(rc.Proposals) > 0 {
		b.WriteString("Proposals:\n")
		n := 0
		for _, p := range rc.Proposals {
			if !recapProposalID.MatchString(p.ProposalID) {
				continue
			}
			if n == maxRecapProposals {
				b.WriteString("(more proposals not shown)\n")
				break
			}
			n++
			b.WriteString(recapProposalLine(p))
			b.WriteString("\n")
		}
	}
	if h := rc.Handoff; h != nil {
		if text := handoffRecapText(h.Summary); text != "" {
			b.WriteString("Last hand-off: " + wrapUntrusted(truncatePromptField(text, recapTextBytes)) + "\n")
		}
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// recapProposalLine renders one proposal: id, status and author are checked
// structure, the title is wrapped.
func recapProposalLine(p RecapProposal) string {
	status := p.Status
	if !slices.Contains(recapStatuses, status) {
		status = "status unknown"
	}
	by := "a person"
	if p.ByAgent {
		by = "the agent"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "proposal %s (%s, by %s): %s", p.ProposalID, status, by, wrapUntrusted(truncatePromptField(p.Title, recapFieldBytes)))
	if len(p.Steps) > 0 {
		fmt.Fprintf(&b, "; %d steps", len(p.Steps))
	}
	if ok := strings.TrimSpace(string(p.StepsOK)); recapStepsOK.MatchString(ok) {
		b.WriteString("; steps ok: " + ok)
	}
	var layers []string
	for _, l := range p.RequiredLayers {
		if slices.Contains(events.Layers, l) {
			layers = append(layers, l)
		}
	}
	if len(layers) > 0 {
		b.WriteString("; approvers: " + strings.Join(layers, ", "))
	}
	if word := recapVerdictWord(p); word != "" {
		b.WriteString("; verified: " + word)
	} else if p.Status == "executed" {
		b.WriteString("; not verified yet")
	}
	return b.String()
}

// handoffRecapText renders a recap's hand-off summary, a string or an
// events.HandoffSummary object, as plain text for the untrusted wrapper.
func handoffRecapText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var h events.HandoffSummary
	if err := json.Unmarshal(raw, &h); err != nil {
		return ""
	}
	var parts []string
	add := func(label string, items ...string) {
		var keep []string
		for _, it := range items {
			if strings.TrimSpace(it) != "" {
				keep = append(keep, it)
			}
		}
		if len(keep) > 0 {
			parts = append(parts, label+": "+strings.Join(keep, "; "))
		}
	}
	add("what broke", h.WhatBroke)
	add("why", h.Why)
	add("done", h.Done...)
	add("ruled out", h.RuledOut...)
	add("open", h.Open...)
	add("risks", h.Risks...)
	return strings.Join(parts, "\n")
}

// earlierProposalUnloadable is the reason the room hears when a proposal of an
// earlier run cannot be loaded.
const earlierProposalUnloadable = "the proposal was made by an earlier run of this room, and this run could not load its steps to check it"

// loadEarlierProposal loads a proposal this run does not know through
// ProposalLookup and records it. why is non-empty when it cannot.
func (r *runner) loadEarlierProposal(ctx context.Context, id string) (proposalRecord, string) {
	lookup, ok := r.deps.Proposals.(ProposalLookup)
	if !ok {
		r.log.Warn("agent: action_result for a proposal this run does not know, and the room cannot look it up", "proposal", id)
		return proposalRecord{}, earlierProposalUnloadable
	}
	p, err := lookup.Lookup(ctx, id)
	if err != nil {
		r.log.Warn("agent: loading an earlier run's proposal failed", "proposal", id, "err", err)
		return proposalRecord{}, earlierProposalUnloadable
	}
	if (p.ProposalID != "" && p.ProposalID != id) || len(p.Steps) == 0 {
		r.log.Warn("agent: an earlier run's proposal came back without its steps", "proposal", id)
		return proposalRecord{}, earlierProposalUnloadable
	}
	r.proposals.record(id, p.Steps, p.RequiredLayers)
	rec, _ := r.proposals.get(id)
	return rec, ""
}

// notVerifiedEarlier records, once per proposal, that a proposal of an earlier
// run is not verified and why, and tells the model to re-check. It never claims
// recovery.
func (r *runner) notVerifiedEarlier(ctx context.Context, id, why string) actionOutcome {
	if !r.proposals.markUnverifiable(id) {
		return actionOutcome{broken: true}
	}
	verdict := "not verified: " + why
	err := r.tryEmit(ctx, events.AgentVerification, events.AgentVerificationPayload{
		ProposalID: id,
		Checks:     []events.VerificationCheck{},
		DataFlow:   &events.DataFlowCheck{Checked: false, Detail: why},
		Recovered:  false,
		Verdict:    verdict,
	})
	if err != nil {
		r.log.Warn("agent: verification event not accepted; announcing it as a message", "proposal", id, "err", err)
		r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: verdict + "."})
	}
	return actionOutcome{broken: true, needsModel: true, notes: []string{
		verifyTag + fmt.Sprintf("not verified: proposal %q was made by an earlier run of this room and its steps could not be loaded, so no checks ran; recovery is not claimed. Re-check the layers it touched with the read tools.", id),
	}}
}
