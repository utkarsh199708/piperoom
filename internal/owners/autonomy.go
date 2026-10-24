package owners

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// This file holds the AU1a part of earned autonomy: the optional per-pipeline
// `autonomy:` block of owners.yaml.
//
//	autonomy:
//	  connect_resume_connector: {after_verified: 10, window: 30d, max_tier: T1}
//
// A rule says how many verified recoveries in a row grant one action the
// auto-approve policy on the pipeline, inside what window, and the widest
// setup tier an autonomous run may use. Load validates the block; the trust
// ledger and the scheduling it feeds are AU1b's (A5). Read the validated
// entries with Pipeline.AutonomyActions and Owners.AutonomyRule.

const (
	// MinAutonomyAfterVerified is the smallest streak an autonomy rule may
	// ask for: fewer than three recoveries is not evidence.
	MinAutonomyAfterVerified = 3
	// MaxAutonomyAfterVerified is the largest streak an autonomy rule may ask
	// for; 100 keeps the rule reachable in one window.
	MaxAutonomyAfterVerified = 100
	// MinAutonomyWindow is the shortest autonomy window (1 day).
	MinAutonomyWindow = 24 * time.Hour
	// MaxAutonomyWindow is the longest autonomy window (90 days).
	MaxAutonomyWindow = 90 * 24 * time.Hour
	// DefaultAutonomyTier is the max_tier an entry gets when it names none.
	DefaultAutonomyTier = "T1"
)

// autonomyTiers are the max_tier values a rule may name. T3 exists on the
// setup scale but is never earned autonomously, so Load refuses it.
var autonomyTiers = []string{"T0", "T1", "T2"}

// AutonomySpec is the optional per-pipeline `autonomy:` block: action name ->
// rule as written. A nil or empty map means the pipeline declares no earned
// autonomy. Read the parsed rules with Pipeline.AutonomyActions and
// Owners.AutonomyRule; the raw values are validated by Load.
type AutonomySpec map[string]AutonomyEntry

// AutonomyEntry is one action's rule as written. Load parses and range-checks
// it into an AutonomyRule.
type AutonomyEntry struct {
	// AfterVerified is how many verified recoveries in a row grant the action.
	AfterVerified AutonomyCount `yaml:"after_verified"`
	// WindowText is the window as written: a Go-style duration whose d unit
	// means 24h, e.g. "30d" or "720h".
	WindowText string `yaml:"window"`
	// MaxTier is the widest setup tier the action may run at: T0, T1 or T2.
	// Empty means DefaultAutonomyTier (T1).
	MaxTier string `yaml:"max_tier"`
}

// AutonomyCount is a number of verified recoveries written as a YAML integer.
// yaml.v3 would otherwise turn 3.5 into 3 without a word; a streak must never
// be rounded.
type AutonomyCount int

// UnmarshalYAML accepts only a plain integer scalar.
func (n *AutonomyCount) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
		return fmt.Errorf("line %d: after_verified: want a whole number of verified recoveries, got %q", node.Line, node.Value)
	}
	var v int
	if err := node.Decode(&v); err != nil {
		return fmt.Errorf("line %d: after_verified: want a whole number: %w", node.Line, err)
	}
	*n = AutonomyCount(v)
	return nil
}

// AutonomyRule is one validated `autonomy:` entry, the form later rows read.
// The zero value is not a rule.
type AutonomyRule struct {
	// Action is the action name (the key of the block).
	Action string
	// AfterVerified is the streak of verified recoveries required, 3 to 100.
	AfterVerified int
	// Window is the parsed window, 1d to 90d.
	Window time.Duration
	// WindowText is the window as written, e.g. "30d" or "720h".
	WindowText string
	// MaxTier is the widest setup tier the action may run at: T0, T1 or T2.
	MaxTier string
}

// UnmarshalYAML rejects unknown keys in every rule, a key that is not an action
// name, a duplicate action and a rule that is not a mapping. The node decoder
// ignores unknown keys, so checkKeys cannot be left to the pipeline decode.
func (s *AutonomySpec) UnmarshalYAML(node *yaml.Node) error {
	if isNull(node) {
		// Like auto_approve: null means "no rules", the safe reading.
		*s = nil
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: autonomy: want a mapping of action name to rule, got %s", node.Line, node.ShortTag())
	}
	out := make(AutonomySpec, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		action := key.Value
		where := "autonomy." + action
		if key.Kind != yaml.ScalarNode || isNull(key) || action == "" {
			return fmt.Errorf("line %d: autonomy: want an action name as the key", key.Line)
		}
		if key.ShortTag() == "!!merge" {
			return fmt.Errorf("line %d: autonomy: merge key << is not supported; write the action out", key.Line)
		}
		if _, dup := out[action]; dup {
			return fmt.Errorf("line %d: %s: listed twice", key.Line, where)
		}
		if isNull(value) {
			return fmt.Errorf("line %d: %s: rule is empty (null); set after_verified and window", value.Line, where)
		}
		if value.Kind != yaml.MappingNode {
			return fmt.Errorf("line %d: %s: want a mapping with after_verified, window and max_tier", value.Line, where)
		}
		if err := checkKeys(value, where, "after_verified", "window", "max_tier"); err != nil {
			return err
		}
		type plain AutonomyEntry // no methods, so Decode does not recurse into this one
		var e plain
		if err := value.Decode(&e); err != nil {
			return fmt.Errorf("line %d: %s: %w", value.Line, where, err)
		}
		out[action] = AutonomyEntry(e)
	}
	*s = out
	return nil
}

// AutonomyActions returns the actions of the pipeline's autonomy: block sorted
// by name. The result is never nil and the caller may modify it.
func (p *Pipeline) AutonomyActions() []string {
	if p == nil {
		return []string{}
	}
	names := make([]string, 0, len(p.autonomy))
	for a := range p.autonomy {
		names = append(names, a)
	}
	sort.Strings(names)
	return names
}

// AutonomyRule returns the validated autonomy rule for action on the pipeline
// and whether the block declares one. It is false for an unknown pipeline, a
// pipeline with no autonomy: block and an action outside it.
func (o *Owners) AutonomyRule(pipelineID, action string) (AutonomyRule, bool) {
	if o == nil {
		return AutonomyRule{}, false
	}
	p, ok := o.Pipeline(pipelineID)
	if !ok {
		return AutonomyRule{}, false
	}
	return p.AutonomyRule(action)
}

// AutonomyRule returns the validated autonomy rule for action, if the
// pipeline declares one. Nil-safe.
func (p *Pipeline) AutonomyRule(action string) (AutonomyRule, bool) {
	if p == nil {
		return AutonomyRule{}, false
	}
	r, ok := p.autonomy[action]
	return r, ok
}

// ValidateAutonomyActions refuses an `autonomy:` action that is not in
// actionNames, the action registry's names. Load checks the block's syntax and
// values before a registry exists; a server calls this (or LoadWithActions),
// like ValidateAutoApprove. A nil or empty set is an empty registry, not "skip
// the check": every action is then refused. Pass the FULL proposal registry,
// as for auto_approve.
func (o *Owners) ValidateAutonomyActions(actionNames []string) error {
	return o.ValidateAutonomyActionsDeferred(actionNames, nil)
}

// ValidateAutonomyActionsDeferred is ValidateAutonomyActions that also refuses
// an autonomy: action in deferredNames, the actions.Deferred names: a
// pull-request fix is never approved by a policy, earned autonomy included
// (docs/design/fix-pr.md §5.8). It is the autonomy twin of
// ValidateAutoApproveDeferred.
func (o *Owners) ValidateAutonomyActionsDeferred(actionNames, deferredNames []string) error {
	if o == nil {
		return nil
	}
	if err := o.checkAutonomyNames(actionNames, deferredNames); err != nil {
		return fmt.Errorf("owners: %w", err)
	}
	return nil
}

// checkAutonomyNames returns the first block action (pipelines in file order,
// actions sorted) that is not in actionNames or that is in deferredNames.
func (o *Owners) checkAutonomyNames(actionNames, deferredNames []string) error {
	known := make(map[string]bool, len(actionNames))
	for _, n := range actionNames {
		known[n] = true
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		for _, a := range p.AutonomyActions() {
			if slices.Contains(deferredNames, a) {
				return fmt.Errorf("pipeline %q: autonomy: %q is never approved by a policy (it opens a pull request that a reviewer merges)", p.ID, a)
			}
			if !known[a] {
				return fmt.Errorf("pipeline %q: autonomy: unknown action %q (known: %s)", p.ID, a, knownList(known))
			}
		}
	}
	return nil
}

// validateAutonomy checks one pipeline's block and stores the parsed rules.
// The errors do not name the pipeline; the caller adds it. An absent or empty
// block is fine and leaves no rules.
func (p *Pipeline) validateAutonomy() error {
	if len(p.Autonomy) == 0 {
		p.autonomy = nil
		return nil
	}
	// Sorted actions make the first reported error deterministic, whatever
	// order the YAML map happened to decode in.
	names := make([]string, 0, len(p.Autonomy))
	for a := range p.Autonomy {
		names = append(names, a)
	}
	sort.Strings(names)
	approved := make(map[string]bool, len(p.AutoApprove))
	for _, a := range p.AutoApprove {
		approved[a] = true
	}
	rules := make(map[string]AutonomyRule, len(p.Autonomy))
	for _, action := range names {
		e := p.Autonomy[action]
		if !validActionName(action) {
			return fmt.Errorf("autonomy: %q is not an action name (want lowercase letters, digits and underscores, starting with a letter)", action)
		}
		if approved[action] {
			return fmt.Errorf("autonomy: action %q is also in auto_approve: an action is approved by one policy or the other, not both", action)
		}
		after := int(e.AfterVerified)
		if after < MinAutonomyAfterVerified || after > MaxAutonomyAfterVerified {
			return fmt.Errorf("autonomy.%s: after_verified %d: want %d to %d", action, after, MinAutonomyAfterVerified, MaxAutonomyAfterVerified)
		}
		window, err := parseAutonomyWindow(e.WindowText)
		if err != nil {
			return fmt.Errorf("autonomy.%s: window %q: %w", action, e.WindowText, err)
		}
		if window < MinAutonomyWindow || window > MaxAutonomyWindow {
			return fmt.Errorf("autonomy.%s: window %q: want 1d to 90d (24h to 2160h)", action, e.WindowText)
		}
		tier := e.MaxTier
		if tier == "" {
			tier = DefaultAutonomyTier
		}
		if !slices.Contains(autonomyTiers, tier) {
			return fmt.Errorf("autonomy.%s: max_tier %q: want T0, T1 or T2 (T3 is never earned)", action, e.MaxTier)
		}
		rules[action] = AutonomyRule{
			Action:        action,
			AfterVerified: after,
			Window:        window,
			WindowText:    e.WindowText,
			MaxTier:       tier,
		}
	}
	p.autonomy = rules
	return nil
}

// parseAutonomyWindow parses a rule's window: a Go-style duration that also
// accepts a d unit meaning 24h, so "30d" and "720h" name the same window. The
// range (1d to 90d) is checked by validateAutonomy so its error can name the
// action and the value as written.
func parseAutonomyWindow(text string) (time.Duration, error) {
	if strings.TrimSpace(text) == "" {
		return 0, fmt.Errorf("want a duration between 1d and 90d, e.g. 30d or 720h")
	}
	d, err := time.ParseDuration(daysAsHours(text))
	if err != nil {
		return 0, fmt.Errorf("want a Go-style duration between 1d and 90d, e.g. 30d or 720h: %w", err)
	}
	return d, nil
}

// daysAsHours rewrites every "<number>d" component of s to hours and leaves
// the rest for time.ParseDuration, which has no day unit. The unit is textual:
// d is not a unit Go parses, so there is nothing else it could mean.
func daysAsHours(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); {
		start := i
		if s[i] == '+' || s[i] == '-' {
			i++
		}
		digits := i
		for i < len(s) && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.') {
			i++
		}
		if i == digits || i >= len(s) || s[i] != 'd' {
			b.WriteString(s[start:i])
			if i < len(s) {
				b.WriteByte(s[i])
				i++
			}
			continue
		}
		days, err := strconv.ParseFloat(s[digits:i], 64)
		if err != nil {
			// Leave the text alone; time.ParseDuration reports the bad number.
			b.WriteString(s[start : i+1])
			i++
			continue
		}
		b.WriteString(s[start:digits]) // the sign, if any
		b.WriteString(strconv.FormatFloat(days*24, 'f', -1, 64))
		b.WriteByte('h')
		i++
	}
	return b.String()
}
