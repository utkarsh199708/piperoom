package agent

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// Triage model rules. When PIPEROOM_TRIAGE_MODEL is set, the first turns of an
// investigation (reading tools, forming a hypothesis) run on it. The main
// model (Options.Model) takes over:
//
//   - for a turn that calls propose_fix: the triage answer is dropped and the
//     same request runs again on the main model, so a fix is never written by
//     the cheaper model; the run then stays on the main model;
//   - for every turn after a [verification] verdict has gone to the model;
//   - for every turn after triageTurns turns have run on the triage model.
//
// The switch is one way and never depends on model output beyond the tool
// name. Prompt caches are per model: the first main-model request after triage
// writes the whole prefix (system, tools, history) to the main model's cache
// again, so a short triage phase saves less than the token prices suggest.
// Thinking blocks of triage turns stay in the history; another model ignores
// them without charge.
//
// The triage model gets the same request as the main model (adaptive thinking,
// effort, strict tools, compaction, top-level cache_control, eager input
// streaming). A model that rejects that shape answers 4xx on every turn. The
// first such answer turns triage off for the whole endpoint and the turn is
// redone on the main model (see modelTurn), so a wrong PIPEROOM_TRIAGE_MODEL
// costs one failed request, not every room's first alert.

// triageSwitch is the endpoint-wide "triage is off" flag, shared by every run.
type triageSwitch struct{ off atomic.Bool }

// isOff reports whether triage was turned off. A nil switch is never off.
func (s *triageSwitch) isOff() bool { return s != nil && s.off.Load() }

// turnOff switches triage off and reports whether this call did it, so only the
// first caller logs. A nil switch reports true and stores nothing.
func (s *triageSwitch) turnOff() bool { return s == nil || !s.off.Swap(true) }

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
	case r.triageSw.isOff():
		return r.opts.Model, "triage model disabled after a rejected request", false
	case r.escalated:
		return r.opts.Model, "after a proposal turn", false
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
	if !triage {
		return resp, err
	}
	if err != nil {
		if ctx.Err() == nil && rejectsRequestShape(err) {
			// Only the first run to see this logs it.
			if r.triageSw.turnOff() {
				r.log.Warn("agent: the triage model rejected the request; triage is off until restart, turns run on the main model",
					"triage_model", name, "model", r.opts.Model, "err", err)
			}
			req.Model = r.opts.Model
			return r.model.Turn(ctx, req)
		}
		return resp, err
	}
	if !callsWriteProposal(resp.Content) {
		return resp, nil
	}
	// Nothing of the triage answer was kept, so the history stays append-only.
	r.log.Debug("agent: triage turn proposes a fix; running the turn again on the main model",
		"triage_model", name, "model", r.opts.Model)
	r.escalated = true
	req.Model = r.opts.Model
	return r.model.Turn(ctx, req)
}

// rejectsRequestShape reports whether err is the API refusing the request
// itself (bad shape, unknown model) rather than a condition the main model
// would hit too: 401/403 (credentials), 408, 409 and 429 (retry later) are
// not.
func rejectsRequestShape(err error) bool {
	var apierr *anthropic.Error
	if !errors.As(err, &apierr) {
		return false
	}
	switch c := apierr.StatusCode; {
	case c < 400 || c >= 500:
		return false
	case c == http.StatusUnauthorized, c == http.StatusForbidden, c == http.StatusRequestTimeout,
		c == http.StatusConflict, c == http.StatusTooManyRequests:
		return false
	}
	return true
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

// incompatibleTriageMarkers are substrings of model ids (any provider's
// spelling) of models that cannot take the loop's request: they need
// budget_tokens instead of adaptive thinking and reject effort. Opus 4.5 and
// Sonnet 4.5 accept no adaptive thinking either; "4-2" matches the dated
// Claude 4.0 snapshots.
var incompatibleTriageMarkers = []string{
	"haiku-4-5", "haiku-3", "claude-3",
	"sonnet-4-5", "sonnet-4-2", "sonnet-4-0",
	"opus-4-5", "opus-4-1", "opus-4-2", "opus-4-0",
}

// TriageModelWarning returns why id is known not to work as the triage model,
// or "" when nothing is known against it. It is advice for a startup log, not
// a check: an unknown id may still work, and the runtime fallback in modelTurn
// covers one that does not.
func TriageModelWarning(id string) string {
	l := strings.ToLower(id)
	for _, m := range incompatibleTriageMarkers {
		if strings.Contains(l, m) {
			return "this model does not support adaptive thinking and effort, so every triage request would be rejected; triage will switch itself off at the first rejection. Use a current model such as claude-haiku-5-5 or claude-sonnet-5-5"
		}
	}
	return ""
}

// stopForBudget ends the run because the room's token budget is spent. The
// agent says so in the room and goes idle with the same text as the detail.
func (r *runner) stopForBudget(ctx context.Context, be *BudgetError) {
	text := be.Error()
	r.log.Warn("agent: room token budget reached", "used", be.Used, "limit", be.Limit, "room", r.alert.RoomID)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: text})
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: text})
}
