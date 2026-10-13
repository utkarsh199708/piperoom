package topology

import (
	"fmt"
	"sort"
	"strings"
)

// renderLegend tells the model how to read the listing. It is fixed text, so
// the rendering stays deterministic.
const renderLegend = `legend: listed upstream first. "from:" = data inputs. "depends on:" = needed to work, not a data stage. ` +
	`(unconfirmed) = assumed, not proven by config or metadata; verify it with tools. ` +
	`First failing component and root cause can differ; check upstream dependencies of the first failing component.`

// Render returns the canonical dependency listing of g for the model's first
// prompt: one line per node in data-flow order (upstream first), each
// followed by a "from:" line naming its data inputs and a "depends on:" line
// naming what it needs to work, with inferred edges marked "(unconfirmed)".
// Facts are sorted, so the same graph always renders the same text.
func (b *builder) Render(g Graph) string {
	order := flowOrder(g)
	pos := make(map[string]int, len(order))
	for i, idx := range order {
		if _, dup := pos[g.Nodes[idx].ID]; !dup {
			pos[g.Nodes[idx].ID] = i
		}
	}
	inputs := map[string][]Edge{}
	needs := map[string][]Edge{}
	for _, e := range g.Edges {
		if e.IsDependency() {
			needs[e.From] = append(needs[e.From], e)
		} else {
			inputs[e.To] = append(inputs[e.To], e)
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "pipeline %s\n", g.PipelineID)
	sb.WriteString(renderLegend)
	sb.WriteByte('\n')
	for i, idx := range order {
		n := g.Nodes[idx]
		sb.WriteString(renderNode(i+1, n))
		sb.WriteByte('\n')
		if in := inputs[n.ID]; len(in) > 0 {
			sb.WriteString("  from: ")
			sb.WriteString(renderLinks(in, func(e Edge) string { return e.From }, pos))
			sb.WriteByte('\n')
		}
		if dep := needs[n.ID]; len(dep) > 0 {
			sb.WriteString("  depends on: ")
			sb.WriteString(renderLinks(dep, func(e Edge) string { return e.To }, pos))
			sb.WriteByte('\n')
		}
	}
	return sb.String()
}

// flowOrder returns node indexes in topological order of the data edges,
// taking the earliest node in g.Nodes whenever several are ready, so a
// builder graph keeps its table -> connector -> topic -> group -> sink order.
// depends_on edges do not order nodes: a schema subject is not a stage. A
// node that only depends_on edges reach (a subject) is listed after the whole
// flow, wherever it sits in g.Nodes. A cycle cannot stop the listing: when no
// node is ready, the earliest remaining one is taken.
func flowOrder(g Graph) []int {
	idx := make(map[string]int, len(g.Nodes))
	for i, n := range g.Nodes {
		if _, dup := idx[n.ID]; !dup {
			idx[n.ID] = i
		}
	}
	indeg := make([]int, len(g.Nodes))
	next := make([][]int, len(g.Nodes))
	inFlow := make([]bool, len(g.Nodes))
	needed := make([]bool, len(g.Nodes))
	seen := map[[2]int]bool{}
	for _, e := range g.Edges {
		f, okF := idx[e.From]
		t, okT := idx[e.To]
		if e.IsDependency() {
			if okT {
				needed[t] = true
			}
			continue
		}
		if okF {
			inFlow[f] = true
		}
		if okT {
			inFlow[t] = true
		}
		if !okF || !okT || f == t || seen[[2]int{f, t}] {
			continue
		}
		seen[[2]int{f, t}] = true
		next[f] = append(next[f], t)
		indeg[t]++
	}
	side := func(i int) bool { return needed[i] && !inFlow[i] }

	done := make([]bool, len(g.Nodes))
	order := make([]int, 0, len(g.Nodes))
	pickFrom := func(ok func(int) bool) int {
		for i := range g.Nodes {
			if !done[i] && ok(i) {
				return i
			}
		}
		return -1
	}
	for {
		pick := pickFrom(func(i int) bool { return !side(i) && indeg[i] <= 0 })
		if pick < 0 {
			pick = pickFrom(func(i int) bool { return !side(i) })
		}
		if pick < 0 {
			break
		}
		done[pick] = true
		order = append(order, pick)
		for _, t := range next[pick] {
			indeg[t]--
		}
	}
	for i := range g.Nodes {
		if !done[i] {
			order = append(order, i)
		}
	}
	return order
}

// renderLinks lists the other end of each edge once, in listing order (ids not
// in the graph last, by id). A link is "(unconfirmed)" only when every edge
// behind it is inferred.
func renderLinks(edges []Edge, end func(Edge) string, pos map[string]int) string {
	unconfirmed := map[string]bool{}
	var ids []string
	for _, e := range edges {
		id := end(e)
		prev, ok := unconfirmed[id]
		if !ok {
			ids = append(ids, id)
			unconfirmed[id] = e.IsInferred()
			continue
		}
		unconfirmed[id] = prev && e.IsInferred()
	}
	sort.SliceStable(ids, func(i, j int) bool {
		pi, iok := pos[ids[i]]
		pj, jok := pos[ids[j]]
		switch {
		case iok && jok:
			return pi < pj
		case iok != jok:
			return iok
		default:
			return ids[i] < ids[j]
		}
	})
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id
		if unconfirmed[id] {
			parts[i] += " (unconfirmed)"
		}
	}
	return strings.Join(parts, ", ")
}

func renderNode(i int, n Node) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d %s [%s, owner %s] %s", i, n.ID, n.Layer, n.Owner, strings.ToUpper(string(n.Health)))
	keys := make([]string, 0, len(n.Facts))
	for k := range n.Facts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&sb, " %s=%s", k, n.Facts[k])
	}
	return sb.String()
}
