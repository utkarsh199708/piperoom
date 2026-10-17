package room

import (
	"encoding/json"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/events"
)

// Bounds of ProposalCreatedPayload.ImpactReasons (events.go, Q0).
const (
	maxImpactReasons     = 8
	maxImpactReasonChars = 300
)

// riskRank orders the risk levels; an unknown level has rank 0.
var riskRank = map[string]int{events.RiskLow: 1, events.RiskMedium: 2, events.RiskHigh: 3}

// stepImpact is the actions.Impact of one validated step, or false when its
// action does not implement actions.Impacting. Impact decides from params
// alone (no I/O), so it is called inline (QA G5).
func stepImpact(a actions.Action, params json.RawMessage) (actions.Impact, bool) {
	im, ok := a.(actions.Impacting)
	if !ok {
		return actions.Impact{}, false
	}
	return im.Impact(params), true
}

// applyImpact fills the server-derived approval fields of a proposal (QA G5):
//
//   - ImpactedLayers: the proposer's impacted layers plus every step action's
//     Impact layers, known layers only, minus the step layers, deduplicated
//     and sorted; nil when none (the event omits it).
//   - RequiredLayers: the step layers in step order, then the impacted ones, so
//     the owners of an impacted layer must approve too.
//   - RiskLevel: the proposer's level (unknown or empty counts as medium),
//     raised to the highest Impact level, never lowered.
//   - ImpactReasons: the non-empty Impact reasons in step order, sanitized,
//     each at most maxImpactReasonChars, at most maxImpactReasons, no repeats.
//
// stepLayers are the step layers in step order without repeats; impacts are
// the steps' impacts in step order.
func applyImpact(p *events.ProposalCreatedPayload, stepLayers []string, impacts []actions.Impact) {
	var impacted []string
	add := func(layer string) {
		layer = strings.TrimSpace(layer)
		if !isKnownLayer(layer) || slices.Contains(stepLayers, layer) || slices.Contains(impacted, layer) {
			return
		}
		impacted = append(impacted, layer)
	}
	for _, l := range p.ImpactedLayers {
		add(l)
	}
	for _, im := range impacts {
		for _, l := range im.Layers {
			add(l)
		}
	}
	slices.Sort(impacted)
	p.ImpactedLayers = impacted
	p.RequiredLayers = append(append([]string{}, stepLayers...), impacted...)

	risk := normalizeRisk(p.RiskLevel)
	for _, im := range impacts {
		if riskRank[im.RiskLevel] > riskRank[risk] {
			risk = im.RiskLevel
		}
	}
	p.RiskLevel = risk

	var reasons []string
	for _, im := range impacts {
		r := clipChars(strings.TrimSpace(sanitizeAgentText(im.Reason)), maxImpactReasonChars)
		if r == "" || slices.Contains(reasons, r) {
			continue
		}
		if len(reasons) == maxImpactReasons {
			break
		}
		reasons = append(reasons, r)
	}
	p.ImpactReasons = reasons
}

// normalizeRisk returns a known risk level for the proposer's value: case and
// spaces do not matter, and anything else counts as medium.
func normalizeRisk(level string) string {
	level = strings.ToLower(strings.TrimSpace(level))
	if riskRank[level] > 0 {
		return level
	}
	return events.RiskMedium
}

// clipChars shortens s to at most max characters (runes), ending a cut text
// with "…".
func clipChars(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max-1 {
			return s[:i] + "…"
		}
		n++
	}
	return s
}
