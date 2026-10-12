package agent

import (
	"context"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// Triage model rules. When PIPEROOM_TRIAGE_MODEL is set, the first turns of an
// investigation (reading tools, forming a hypothesis) run on it. The main
// model (Options.Model) takes over:
//
//   - for a turn that calls propose_fix: the triage answer is dropped and the
//     same request runs again on the main model, so a fix is never written by
//     the cheaper model;
//   - for every turn after a [verification] verdict has gone to the model;
//   - for every turn after triageTurns turns have run on the triage model.
//
// The switch is one way for the last two rules and never depends on model
// output. Prompt caches are per model: the first main-model request after
// triage writes the whole prefix (system, tools, history) to the main model's
// cache again, so a short triage phase saves less than the token prices
// suggest. Thinking blocks of triage turns stay in the history; another model
// ignores them without charge.

// writesProposal reports whether a tool is one that creates a write proposal.
// propose_fix is the only one today; a new write-proposal room tool must be
// added here.
func writesProposal(tool string) bool {
	return tool == "propose_fix"
}

// pickModel chooses the model for the next turn and says why, for the debug
// log. It counts a triage turn when it picks the triage model.
func (r *runner) pickModel() (name, reason string, triage bool) {
	switch {
	case r.triageModel == "":
		return r.opts.Model, "no triage model", false
	case r.verified:
		return r.opts.Model, "after verification", false
	case r.triageRan >= r.triageTurns:
		return r.opts.Model, "triage turn limit reached", false
	}
	r.triageRan++
	return r.triageModel, "triage turn", true
}

// modelTurn runs one model request with the model the triage rules pick.
func (r *runner) modelTurn(ctx context.Context, messages []anthropic.BetaMessageParam) (anthropic.BetaMessage, error) {
	req := r.request(messages)
	name, reason, triage := r.pickModel()
	req.Model = name
	r.log.Debug("agent: model for turn", "model", name, "reason", reason, "triage_turns_run", r.triageRan)

	resp, err := r.model.Turn(ctx, req)
	if err != nil || !triage || !callsWriteProposal(resp.Content) {
		return resp, err
	}
	// Nothing of the triage answer was kept, so the history stays append-only.
	r.log.Debug("agent: triage turn proposes a fix; running the turn again on the main model",
		"triage_model", name, "model", r.opts.Model)
	req.Model = r.opts.Model
	return r.model.Turn(ctx, req)
}

// callsWriteProposal reports whether a turn asks for a write-proposal tool.
func callsWriteProposal(content []anthropic.BetaContentBlockUnion) bool {
	for _, b := range content {
		if b.Type == "tool_use" && writesProposal(b.Name) {
			return true
		}
	}
	return false
}

// stopForBudget ends the run because the room's token budget is spent. The
// agent says so in the room and goes idle with the same text as the detail.
func (r *runner) stopForBudget(ctx context.Context, be *BudgetError) {
	text := be.Error()
	r.log.Warn("agent: room token budget reached", "used", be.Used, "limit", be.Limit, "room", r.alert.RoomID)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: text})
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: text})
}
