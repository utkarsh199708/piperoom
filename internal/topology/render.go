package topology

import (
	"fmt"
	"sort"
	"strings"
)

// Render returns a compact one-line-per-node text form of g for the model's
// first prompt. Nodes appear in flow order and facts are sorted for
// determinism.
func (b *builder) Render(g Graph) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "pipeline %s\n", g.PipelineID)
	for i, n := range g.Nodes {
		sb.WriteString(renderNode(i+1, n))
		sb.WriteByte('\n')
	}
	return sb.String()
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
