package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// The hypothesis guard (H1, docs/design/hypothesis-guard.md) checks what a
// verdict claims against what the run really read, in update_hypothesis and
// post_message. Each rule is a check that returns a guardFinding or nil;
// hypothesisGuard holds the mode and the refusal cap all rules share:
//
//   - GuardOff: no rule runs.
//   - GuardLog: every rule runs and a finding is logged (names and counts,
//     never the model's text); nothing is refused.
//   - GuardEnforce: a finding refuses the call with its tools.ErrBadInput
//     error, which the model reads and can recover from. A rule refuses at
//     most guardMaxRefusals times in a row per episode; the next finding of
//     that rule is accepted with the confidence forced to low, its leftover
//     items in UnbackedTerms, so a model that cannot satisfy a rule never
//     loops and the room still sees what was not backed.
//
// The streak resets on any accepted update_hypothesis or post_message and on
// any input that resets the turn guard (resetsTurns: a person spoke, an
// executor result arrived), which is what an episode is in run.go.

// Guard rule names: stable, they go into log lines and the refusal text the
// evals count (prefix "hypothesis guard (").
const (
	guardRuleRuledOut = "unbacked_rule_out" // rule 2, ruledout.go
	guardRuleSymptom  = "symptom_persists"  // rule 1, symptomguard.go
)

// guardMaxRefusals is how many refusals in a row one rule gives per episode
// before it accepts the call at low confidence.
const guardMaxRefusals = 2

// resolveGuardMode maps Options.HypothesisGuard to a mode: "" is GuardLog. An
// unknown value is an error, so a typo in code stops the run like a typo in
// PIPEROOM_HYPOTHESIS_GUARD stops the server (config.ParseHypothesisGuard).
func resolveGuardMode(mode string) (string, error) {
	switch mode {
	case "":
		return GuardLog, nil
	case GuardOff, GuardLog, GuardEnforce:
		return mode, nil
	}
	return "", fmt.Errorf("agent: Options.HypothesisGuard %q: want %s, %s or %s", mode, GuardOff, GuardLog, GuardEnforce)
}

// guardFinding is what one rule found wrong with a claim.
type guardFinding struct {
	rule string
	// refusal is the error the model sees in enforce mode; it wraps
	// tools.ErrBadInput and starts with "hypothesis guard (<rule>): ".
	refusal error
	// attrs are extra log attributes: names and counts only, never the
	// model's text or tool output.
	attrs []any
	// unbacked are items added to UnbackedTerms when the cap accepts the call.
	unbacked []string
	// note, when set, is posted as one agent.message when the cap accepts
	// the call (rule 1's "[guard] ... still shows ... firing" line).
	note string
	// onRefuse, when set, runs when the finding really refuses the call
	// (enforce mode, under the cap): rule 1 arms its recheck there, so log
	// mode and a capped call never start one.
	onRefuse func()
}

// guardResult is what the guard decided for an accepted call.
type guardResult struct {
	// capped is true when a rule's cap accepted a finding: the caller forces
	// the confidence to low.
	capped   bool
	unbacked []string
	notes    []string
}

// hypothesisGuard is one run's guard state. A nil guard is off, for the
// harnesses that build room tools without a runner.
type hypothesisGuard struct {
	mode           string
	log            *slog.Logger
	room, pipeline string
	// offered reports whether the room offers a read tool, so a refusal only
	// names tools the model can call; nil offers every tool.
	offered func(name string) bool

	// symptom is rule 1's watch reader and recheck (symptomguard.go); nil
	// turns rule 1 off.
	symptom *symptomGuard

	mu       sync.Mutex
	refusals map[string]int // per rule, refusals in a row this episode
}

// newHypothesisGuard returns the guard of a run in mode (already resolved).
func newHypothesisGuard(mode string, log *slog.Logger, a Alert, offered func(string) bool) *hypothesisGuard {
	if log == nil {
		log = slog.Default()
	}
	return &hypothesisGuard{mode: mode, log: log, room: a.RoomID, pipeline: a.PipelineID, offered: offered, refusals: map[string]int{}}
}

// active reports whether rules run at all.
func (g *hypothesisGuard) active() bool { return g != nil && g.mode != GuardOff }

// check runs the checks in order and applies the mode and the cap to each
// finding. The checks are functions so a costly rule (rule 1 reads the
// store) does not run when a cheaper one already refused. A non-nil error
// is the refusal the tool returns.
func (g *hypothesisGuard) check(tool string, checks ...func() *guardFinding) (guardResult, error) {
	var out guardResult
	if !g.active() {
		return out, nil
	}
	for _, c := range checks {
		f := c()
		if f == nil {
			continue
		}
		attrs := append([]any{"rule", f.rule, "tool", tool, "room", g.room, "pipeline", g.pipeline}, f.attrs...)
		if g.mode == GuardLog {
			g.log.Warn("agent: hypothesis guard would refuse", attrs...)
			continue
		}
		g.mu.Lock()
		n := g.refusals[f.rule]
		if n < guardMaxRefusals {
			g.refusals[f.rule] = n + 1
		}
		g.mu.Unlock()
		if n < guardMaxRefusals {
			g.log.Warn("agent: hypothesis guard refused", append(attrs, "refusal", n+1)...)
			if f.onRefuse != nil {
				f.onRefuse()
			}
			return guardResult{}, f.refusal
		}
		g.log.Warn("agent: hypothesis guard accepted at low confidence after refusals", append(attrs, "refusals", n)...)
		out.capped = true
		out.unbacked = append(out.unbacked, f.unbacked...)
		if f.note != "" {
			out.notes = append(out.notes, f.note)
		}
	}
	return out, nil
}

// offeredTool reports whether the room offers a read tool (true for all when
// the guard knows no registry).
func (g *hypothesisGuard) offeredTool(name string) bool {
	return g == nil || g.offered == nil || g.offered(name)
}

// post emits the [guard] lines of a capped call. A failed emit does not fail
// the call (its verdict is already stored); it is logged.
func (g *hypothesisGuard) post(ctx context.Context, sink Sink, notes []string) {
	for _, n := range notes {
		if err := sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: n}); err != nil {
			g.log.Warn("agent: hypothesis guard note not posted", "room", g.room, "err", err)
		}
	}
}

// reset ends the refusal streaks: an accepted call or a new episode.
func (g *hypothesisGuard) reset() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	clear(g.refusals)
}

// observeInputs starts a new episode when inputs would reset the turn guard.
func (g *hypothesisGuard) observeInputs(inputs []HumanInput) {
	if resetsTurns(inputs) {
		g.reset()
	}
}

// setRoomGuard gives update_hypothesis and post_message the run's guard.
func setRoomGuard(room []tools.Tool, g *hypothesisGuard) {
	for _, t := range room {
		switch x := t.(type) {
		case *updateHypothesisTool:
			x.guard = g
		case *postMessageTool:
			x.guard = g
		}
	}
}

// mergeUnbacked appends extra to terms, without duplicates, within the
// UnbackedTerms caps.
func mergeUnbacked(terms, extra []string) []string {
	seen := map[string]bool{}
	for _, t := range terms {
		seen[t] = true
	}
	for _, e := range extra {
		if len(terms) >= maxUnbackedTerms {
			break
		}
		e = cutBytes(e, maxUnbackedTermBytes)
		if seen[e] {
			continue
		}
		seen[e] = true
		terms = append(terms, e)
	}
	return terms
}
