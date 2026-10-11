package alertin

import (
	"strings"

	"piperoom/internal/owners"
)

// matchOwners finds the one pipeline whose connector, consumer group or
// table names appear in the alert. values are exact names (label and tag
// values); text is free text scanned for whole names. A schema-less table
// name ("orders" for "public.orders") counts only as an exact value, because
// a short word in free text is too likely to be a coincidence. It returns ""
// when nothing or more than one pipeline matches: a wrong guess would send
// the agent to investigate the wrong pipeline.
func matchOwners(o *owners.Owners, values, text []string) string {
	if o == nil {
		return ""
	}
	exact := map[string]bool{}
	for _, v := range values {
		if v = strings.ToLower(strings.TrimSpace(v)); v != "" {
			exact[v] = true
		}
	}
	words := map[string]bool{}
	for _, t := range text {
		for _, w := range strings.FieldsFunc(strings.ToLower(t), notNameRune) {
			words[strings.Trim(w, ".-")] = true
		}
	}

	match := ""
	for _, p := range o.Pipelines {
		if !pipelineMentioned(p, exact, words) {
			continue
		}
		if match != "" && match != p.ID {
			return ""
		}
		match = p.ID
	}
	return match
}

func pipelineMentioned(p owners.Pipeline, exact, words map[string]bool) bool {
	has := func(name string) bool {
		n := strings.ToLower(name)
		return n != "" && (exact[n] || words[n])
	}
	for _, names := range [][]string{p.Connectors, p.ConsumerGroups, p.SourceTables, p.SinkTables} {
		for _, n := range names {
			if has(n) {
				return true
			}
		}
	}
	for _, tables := range [][]string{p.SourceTables, p.SinkTables} {
		for _, t := range tables {
			if i := strings.LastIndexByte(t, '.'); i >= 0 && exact[strings.ToLower(t[i+1:])] {
				return true
			}
		}
	}
	return false
}

// notNameRune splits free text into candidate names: connector, group and
// table names use letters, digits, '_', '-' and '.'.
func notNameRune(r rune) bool {
	return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.')
}
