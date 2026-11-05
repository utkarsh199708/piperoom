package topology

import (
	"strconv"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
)

// PII tags (X7c, docs/design/lineage.md section 3): each source_table and
// sink_table node whose catalog was read gets Tags.pii, the columns the
// shared classifier (redact.ClassifyColumn: names and catalog types only)
// classes as personal data, corrected by the pipeline's owners.yaml pii:
// include/exclude. The column list comes from the same ColumnReader call as
// the column lineage, so a table whose catalog is not read gets no tag; no
// row or value is ever read. The tag is a label: code that must never read
// a PII column classifies the column list itself (internal/watch).

// classifyColumns returns the PII class of each column of table that is
// personal data: the classifier's answer, then owners.yaml pii:.
func classifyColumns(p owners.Pipeline, table string, cols []clients.ColumnInfo) map[string]PIIClass {
	out := map[string]PIIClass{}
	for _, c := range cols {
		class, ok := redact.ClassifyColumn(c.Name, c.Type)
		switch o, cl := p.PIIOverrideFor(table, c.Name); o {
		case owners.PIIIncluded:
			class, ok = cl, true
		case owners.PIIExcluded:
			ok = false
		}
		if ok {
			out[c.Name] = PIIClass(class)
		}
	}
	return out
}

// addPIITags sets TagPII (and TagPIIOmitted when the cap or an unusable name
// cut pairs) on the table nodes lr classified.
func addPIITags(g *Graph, lr *columnReads) {
	if lr == nil {
		return
	}
	for i := range g.Nodes {
		n := &g.Nodes[i]
		var cols map[string]PIIClass
		switch n.Kind {
		case KindSourceTable:
			cols = lr.srcPII[sourceTableOf(n.ID)]
		case KindSinkTable:
			cols = lr.sinkPII[strings.TrimPrefix(n.ID, "wh:")]
		}
		if len(cols) == 0 {
			continue
		}
		v, omitted := FormatPIITag(cols)
		setTag(n, TagPII, v)
		if omitted > 0 {
			setTag(n, TagPIIOmitted, strconv.Itoa(omitted))
		}
	}
}

// setTag adds a non-empty tag that passes ValidTag, within MaxNodeTags.
func setTag(n *Node, key, value string) {
	if value == "" || ValidTag(key, value) != nil {
		return
	}
	if _, ok := n.Tags[key]; !ok && len(n.Tags) >= MaxNodeTags {
		return
	}
	if n.Tags == nil {
		n.Tags = map[string]string{}
	}
	n.Tags[key] = value
}
