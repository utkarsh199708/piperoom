package owners

import (
	"slices"
	"sort"
)

// Kinds of AutonomyChange.Change. A reload that tightens or removes a rule is
// what AU1b revokes the action's grant on (autonomy.revoked, reason
// config_changed); a loosened rule never grants by itself, the streak has to
// reach the new threshold on its own (KAN-463 TC4, KAN-464).
const (
	// AutonomyAdded: the action has a rule now and had none.
	AutonomyAdded = "added"
	// AutonomyRemoved: the rule is gone, or its whole pipeline is.
	AutonomyRemoved = "removed"
	// AutonomyTightened: harder to earn or keep: more recoveries, a shorter
	// window or a lower max_tier.
	AutonomyTightened = "tightened"
	// AutonomyLoosened: easier: fewer recoveries, a longer window or a higher
	// max_tier.
	AutonomyLoosened = "loosened"
)

// AutonomyChange is one difference between the autonomy: rules of two owners
// snapshots, as AutonomyDiff reports it.
type AutonomyChange struct {
	Pipeline string
	Action   string
	// Change is AutonomyAdded, AutonomyRemoved, AutonomyTightened or
	// AutonomyLoosened.
	Change string
}

// AutonomyDiff compares the autonomy: rules of prev and cur, two loaded
// snapshots of the same owners config (Live.Load before and after a Reload),
// and returns every rule that was added, removed, tightened or loosened,
// sorted by pipeline then action; an unchanged rule is not listed. A rule
// changed in both directions at once (more recoveries but a longer window) is
// reported tightened: the caller revokes on tightened, so the conservative
// reading wins. Either snapshot may be nil (no rules). The result is never
// nil.
func AutonomyDiff(prev, cur *Owners) []AutonomyChange {
	changes := []AutonomyChange{}
	before := allAutonomyRules(prev)
	after := allAutonomyRules(cur)
	for key, old := range before {
		now, ok := after[key]
		if !ok {
			changes = append(changes, AutonomyChange{key.pipeline, key.action, AutonomyRemoved})
			continue
		}
		if kind := compareAutonomyRules(old, now); kind != "" {
			changes = append(changes, AutonomyChange{key.pipeline, key.action, kind})
		}
	}
	for key := range after {
		if _, ok := before[key]; !ok {
			changes = append(changes, AutonomyChange{key.pipeline, key.action, AutonomyAdded})
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Pipeline != changes[j].Pipeline {
			return changes[i].Pipeline < changes[j].Pipeline
		}
		return changes[i].Action < changes[j].Action
	})
	return changes
}

// autonomyKey names one rule: pipeline ids are unique within an Owners
// (validate refuses a repeat), so the pair is unambiguous.
type autonomyKey struct{ pipeline, action string }

// allAutonomyRules flattens the validated rules of every pipeline of o.
func allAutonomyRules(o *Owners) map[autonomyKey]AutonomyRule {
	rules := map[autonomyKey]AutonomyRule{}
	if o == nil {
		return rules
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		for action, r := range p.autonomy {
			rules[autonomyKey{p.ID, action}] = r
		}
	}
	return rules
}

// compareAutonomyRules classifies the change from old to now, or returns ""
// when the two rules are the same. Any tightening wins over any loosening.
func compareAutonomyRules(old, now AutonomyRule) string {
	oldTier, nowTier := slices.Index(autonomyTiers, old.MaxTier), slices.Index(autonomyTiers, now.MaxTier)
	tighter := now.AfterVerified > old.AfterVerified || now.Window < old.Window || nowTier < oldTier
	looser := now.AfterVerified < old.AfterVerified || now.Window > old.Window || nowTier > oldTier
	switch {
	case tighter:
		return AutonomyTightened
	case looser:
		return AutonomyLoosened
	}
	return ""
}
