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
// Input handling, kept in one place so it is easy to adapt to the room's final
// shape:
//
//   - HumanInput.Kind == RecapKind. Text is either a JSON RoomRecap (it starts
//     with "{" and decodes) or plain text the server rendered.
//   - absorbRecaps (called by handleBatch before anything else) records every
//     recap proposal that has steps in the proposal tracker, with the verdict
//     the room already has for it, so a later action_result finds it.
//   - formatRecap renders the recap for the model: the preface and the field
//     labels are trusted structure written here; ids and fixed vocabularies are
//     checked against patterns; every free text (titles, hypothesis, summaries,
//     hand-off, plain text) is untrusted and wrapped.
//   - A proposal that is neither in this run's tracker nor in a recap is loaded
//     through ProposalLookup when Deps.Proposals implements it. When it cannot
//     be loaded the room records "not verified" with the reason, once.

// RecapKind is the HumanInput.Kind of a room recap (see RoomRecap).
const RecapKind = "room_recap"

// recapPreface opens the recap. The room wrote it, not a person: it stands
// outside any wrapper. The system prompt explains the tag.
const recapPreface = "[room recap] This run resumes an earlier investigation of this room. Below is what earlier runs found. Do not redo finished steps: cite their step ids and re-read only what may have changed since."

// RoomRecap is the structured form of a recap: what earlier runs of the room
// found. Every field is optional.
type RoomRecap struct {
	Hypothesis *RecapHypothesis `json:"hypothesis,omitempty"`
	Steps      []RecapStep      `json:"steps,omitempty"`
	Proposals  []RecapProposal  `json:"proposals,omitempty"`
	// Handoff is the latest hand-off summary, rendered as text.
	Handoff string `json:"handoff,omitempty"`
	// Text is any other server-rendered recap text.
	Text string `json:"text,omitempty"`
}

// RecapHypothesis is the room's latest hypothesis.
type RecapHypothesis struct {
	Text       string   `json:"text"`
	Confidence string   `json:"confidence,omitempty"`
	StepIDs    []string `json:"step_ids,omitempty"`
}

// RecapStep is one finished read of an earlier run.
type RecapStep struct {
	StepID  string `json:"step_id"`
	Tool    string `json:"tool"`
	Summary string `json:"summary"`
	IsError bool   `json:"is_error,omitempty"`
}

// RecapProposal is one proposal of an earlier run. Steps let this run verify
// it; Verdict is the verification the room already recorded for it
// ("recovered", "NOT recovered", "inconclusive"), empty when none.
type RecapProposal struct {
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title,omitempty"`
	State          string                `json:"state,omitempty"`
	RequiredLayers []string              `json:"required_layers,omitempty"`
	Steps          []events.ProposalStep `json:"steps,omitempty"`
	Verdict        string                `json:"verdict,omitempty"`
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
	recapStates     = []string{"open", "approved", "rejected", "withdrawn", "executing", "executed", "failed"}
	recapVerdicts   = []string{"recovered", "NOT recovered", "inconclusive"}
	recapConfidence = []string{"low", "medium", "high"}
)

// parseRecap reads a recap input: a JSON RoomRecap, or nil with the plain text.
func parseRecap(text string) (*RoomRecap, string) {
	t := strings.TrimSpace(text)
	if strings.HasPrefix(t, "{") {
		var rc RoomRecap
		if err := json.Unmarshal([]byte(t), &rc); err == nil {
			return &rc, ""
		}
	}
	return nil, text
}

// absorbRecaps records the proposals of every recap in the batch.
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
			if !recapProposalID.MatchString(p.ProposalID) || len(p.Steps) == 0 {
				continue
			}
			if _, known := r.proposals.get(p.ProposalID); known {
				continue
			}
			r.proposals.record(p.ProposalID, p.Steps, p.RequiredLayers)
			if slices.Contains(recapVerdicts, p.Verdict) {
				r.proposals.markVerified(p.ProposalID, p.Verdict == "recovered", p.Verdict)
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
	if h := rc.Hypothesis; h != nil && h.Text != "" {
		conf := h.Confidence
		if !slices.Contains(recapConfidence, conf) {
			conf = "unknown"
		}
		var ids []string
		for _, id := range h.StepIDs {
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
	if len(rc.Steps) > 0 {
		b.WriteString("Finished steps:\n")
		n := 0
		for _, s := range rc.Steps {
			if !triageStepID.MatchString(s.StepID) {
				continue
			}
			if n == maxRecapSteps {
				fmt.Fprintf(&b, "(%d more steps not shown)\n", len(rc.Steps)-n)
				break
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
	}
	if len(rc.Proposals) > 0 {
		b.WriteString("Proposals of earlier runs:\n")
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
			state := p.State
			if !slices.Contains(recapStates, state) {
				state = "state unknown"
			}
			fmt.Fprintf(&b, "proposal %s (%s): %s", p.ProposalID, state, wrapUntrusted(truncatePromptField(p.Title, recapFieldBytes)))
			if len(p.Steps) > 0 {
				fmt.Fprintf(&b, "; %d steps", len(p.Steps))
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
			if slices.Contains(recapVerdicts, p.Verdict) {
				b.WriteString("; verified: " + p.Verdict)
			}
			b.WriteString("\n")
		}
	}
	if rc.Handoff != "" {
		b.WriteString("Last hand-off: " + wrapUntrusted(truncatePromptField(rc.Handoff, recapTextBytes)) + "\n")
	}
	if rc.Text != "" {
		b.WriteString("Room notes: " + wrapUntrusted(truncatePromptField(rc.Text, recapTextBytes)) + "\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
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
