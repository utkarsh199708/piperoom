package runbooks

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

const (
	// maxVersions caps the versions input; a pipeline has a handful of parts.
	maxVersions = 16
	// relevantShare is the share of the best score an entry needs for the
	// version boost.
	relevantShare = 0.5
)

// RunningVersion is one entry of runbook_lookup's versions input.
type RunningVersion struct {
	Component string `json:"component"`
	Version   string `json:"version"`
}

// issueIndex is BM25 over the known issues, with the same parameters as the
// runbook sections. Symptom keywords and renamed keys say most about an entry.
type issueIndex struct {
	list  []KnownIssue
	tf    []map[string]float64
	lens  []float64
	df    map[string]int
	avgDL float64
}

func newIssueIndex(ki *KnownIssues, p params) issueIndex {
	ix := issueIndex{df: map[string]int{}}
	if ki == nil {
		return ix
	}
	ix.list = ki.list
	var total float64
	for _, e := range ix.list {
		tf := map[string]float64{}
		strong := []string{e.ID, e.Component}
		strong = append(strong, e.Symptoms...)
		for _, r := range append(slices.Clone(e.RenamedKeys), e.RenamedValues...) {
			strong = append(strong, strings.TrimSuffix(r.Old, "*"), strings.TrimSuffix(r.New, "*"))
		}
		for _, w := range terms(strings.Join(strong, " ")) {
			tf[w] += p.title
		}
		for _, w := range terms(e.Title) {
			tf[w] += p.heading
		}
		for _, w := range terms(e.Workaround) {
			tf[w] += p.body
		}
		var n float64
		for w, f := range tf {
			n += f
			ix.df[w]++
		}
		total += n
		ix.tf = append(ix.tf, tf)
		ix.lens = append(ix.lens, n)
	}
	if len(ix.list) > 0 {
		ix.avgDL = total / float64(len(ix.list))
	}
	return ix
}

// issueHit is one known issue in Result.Data.
type issueHit struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Component   string   `json:"component"`
	Theme       string   `json:"theme"`
	Affected    string   `json:"affected"`
	FixedIn     string   `json:"fixed_in,omitempty"`
	Workaround  string   `json:"workaround"`
	RenamedKeys []Rename `json:"renamed_keys,omitempty"`
	Source      string   `json:"source"`
	Score       int      `json:"score"`
	// Running is the version given for this component, if any; Affects says
	// whether it is in the affected range: "yes", "no", "unknown" when no
	// version was given for the component, or "any" for an entry that holds
	// every version (a limit or a misconfiguration, not a release problem).
	Running string `json:"running,omitempty"`
	Affects string `json:"affects"`
}

// runningFor finds the version given for a component. A connector entry
// (debezium-postgres) also takes a plain "debezium" version, and a
// "debezium" entry takes any connector's version.
func runningFor(comp string, vs map[string]string) (string, bool) {
	if v, ok := vs[comp]; ok {
		return v, true
	}
	if strings.HasPrefix(comp, "debezium-") {
		v, ok := vs["debezium"]
		return v, ok
	}
	if comp == "debezium" {
		keys := make([]string, 0, len(vs))
		for k := range vs {
			if strings.HasPrefix(k, "debezium-") {
				keys = append(keys, k)
			}
		}
		if len(keys) > 0 {
			slices.Sort(keys) // deterministic when several connectors are given
			return vs[keys[0]], true
		}
	}
	return "", false
}

// search ranks matching entries: those whose range holds the running version
// first, then by score; file order breaks ties.
func (ix issueIndex) search(q []weighted, vs map[string]string, p params, n int) []issueHit {
	type scored struct {
		i        int
		score    float64
		affected bool // the range holds the running version
		boost    bool // affected and relevant enough to rank first
	}
	var res []scored
	nDocs := float64(len(ix.list))
	for i, tf := range ix.tf {
		var score float64
		for _, qt := range q {
			f := tf[qt.term]
			if f == 0 {
				continue
			}
			df := float64(ix.df[qt.term])
			idf := math.Log(1 + (nDocs-df+0.5)/(df+0.5))
			norm := p.k1 * (1 - p.b + p.b*ix.lens[i]/ix.avgDL)
			score += qt.weight * idf * f * (p.k1 + 1) / (f + norm)
		}
		if score <= 0 {
			continue
		}
		// Only a bounded range is version knowledge: a "*" entry holds every
		// version, so it would otherwise outrank every release-specific match.
		e := ix.list[i]
		v, ok := runningFor(e.Component, vs)
		res = append(res, scored{i: i, score: score, affected: ok && e.bounded() && e.Holds(v)})
	}
	// The version boost is for relevant entries only: an entry that matched a
	// stray query word must not jump a strong match just because its range
	// holds the version.
	var top float64
	for _, r := range res {
		top = max(top, r.score)
	}
	for i := range res {
		res[i].boost = res[i].affected && res[i].score >= relevantShare*top
	}
	slices.SortStableFunc(res, func(a, b scored) int {
		switch {
		case a.boost != b.boost:
			if a.boost {
				return -1
			}
			return 1
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		}
		return 0
	})
	hits := []issueHit{}
	for _, r := range res[:min(n, len(res))] {
		e := ix.list[r.i]
		h := issueHit{ID: e.ID, Title: e.Title, Component: e.Component, Theme: e.Theme, Affected: e.Affected,
			FixedIn: e.FixedIn, Workaround: e.Workaround, RenamedKeys: e.RenamedKeys, Source: e.Source,
			Score: max(1, int(math.Round(r.score*10))), Affects: "unknown"}
		if !e.bounded() {
			h.Affects = "any"
		} else if v, ok := runningFor(e.Component, vs); ok {
			h.Running = v
			h.Affects = "no"
			if r.affected {
				h.Affects = "yes"
			}
		}
		hits = append(hits, h)
	}
	return hits
}

// line is the summary text of one hit; it always says the fixed-in release
// (or that none exists) so the agent can name it when it recommends an upgrade.
func (h issueHit) line(i int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[K%d] %s: %s (%s %s", i+1, h.ID, h.Title, h.Component, h.Affected)
	if h.Affected == "*" {
		sb.WriteString(", all versions")
	}
	switch h.Affects {
	case "yes":
		fmt.Fprintf(&sb, "; running %s is affected", h.Running)
	case "no":
		fmt.Fprintf(&sb, "; running %s is not affected", h.Running)
	}
	sb.WriteString(")\n")
	if h.FixedIn != "" {
		fmt.Fprintf(&sb, "Fixed in %s. ", h.FixedIn)
	} else {
		sb.WriteString("No release fixes it. ")
	}
	fmt.Fprintf(&sb, "Workaround: %s", cutBytes(strings.TrimSpace(h.Workaround), maxHitTextBytes/2))
	if len(h.RenamedKeys) > 0 {
		pairs := make([]string, len(h.RenamedKeys))
		for j, r := range h.RenamedKeys {
			pairs[j] = r.Old + " -> " + r.New
		}
		fmt.Fprintf(&sb, "\nRenamed: %s", strings.Join(pairs, ", "))
	}
	fmt.Fprintf(&sb, "\nSource: %s", h.Source)
	return sb.String()
}
