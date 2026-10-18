package alertin

import (
	"slices"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// MatchDAG returns the one batch pipeline in o whose dags list the known ids, or "" when none or several do.
//
// The ids are the alert's dag_id values, compared exactly (trimmed,
// case-sensitive): an Airflow dag_id is case-sensitive, and the batch tools check a dag_id against the room
// pipeline's dags the same way, so the pipeline found here can read the DAG.
// An id no pipeline lists is ignored, as the owners scan ignores names it does
// not know, so an alert whose ids are all unknown resolves exactly as if it had
// none. Of the ids that are known, the pipeline must list every one, and only
// one pipeline may do so: a DAG that two pipelines list, or a group whose alerts
// name DAGs of different pipelines, gives "" rather than a guess.
//
// The ids come from the payload and are untrusted; they are only compared
// with owners.yaml entries, never logged or shown. Only a batch pipeline can
// match: owners.Load refuses dags on a cdc pipeline, but an Owners built in
// code is not checked, and a dag_id must never send a room to a CDC pipeline.
// ids is variadic so a caller with one dag_id (the dev webhook in
// internal/room) passes it as is.
func MatchDAG(o *owners.Owners, ids ...string) string {
	if o == nil || len(ids) == 0 {
		return ""
	}
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			want[id] = true
		}
	}
	var known []string
	for _, p := range o.Pipelines {
		for _, d := range p.DAGs {
			if want[d] && !slices.Contains(known, d) {
				known = append(known, d)
			}
		}
	}
	if len(known) == 0 {
		return ""
	}
	match := ""
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		if p.KindOrDefault() != events.PipelineKindBatch || !listsAll(p.DAGs, known) {
			continue
		}
		if match != "" && match != p.ID {
			return ""
		}
		match = p.ID
	}
	return match
}

// listsAll reports whether every id in ids is in dags.
func listsAll(dags, ids []string) bool {
	for _, id := range ids {
		if !slices.Contains(dags, id) {
			return false
		}
	}
	return true
}

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
