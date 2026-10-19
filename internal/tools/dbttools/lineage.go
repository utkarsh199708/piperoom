package dbttools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"piperoom/internal/tools"
)

// maxLineageIDs caps the parents and the children listed, each, in the
// summary and in Data.
const maxLineageIDs = 100

type modelLineage struct{ d Deps }

func (t *modelLineage) Name() string     { return LineageTool }
func (t *modelLineage) Kind() tools.Kind { return tools.KindRead }
func (t *modelLineage) Description() string {
	return "Reads one dbt model's direct parents and children from manifest.json, as unique_ids (models, sources, seeds, tests). " +
		"Use it to see what feeds a broken model and what it breaks downstream. When the name is ambiguous it lists the " +
		"candidate unique_ids; call again with one of them."
}
func (t *modelLineage) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"model": map[string]any{"type": "string", "description": modelInputDoc},
	}, "model")
}

type lineageData struct {
	Model    string   `json:"model"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
	// ParentsNotShown and ChildrenNotShown count ids past maxLineageIDs.
	ParentsNotShown  int `json:"parents_not_shown,omitempty"`
	ChildrenNotShown int `json:"children_not_shown,omitempty"`
}

func (t *modelLineage) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	var p struct {
		Model string `json:"model"`
	}
	if err := decode(in, &p); err != nil {
		return tools.Result{}, err
	}
	ref, err := parseModel(p.Model)
	if err != nil {
		return tools.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sc, err := checkModel(ctx, t.d, t.Name(), ref)
	if err != nil {
		return tools.Result{}, err
	}
	if t.d.Transform == nil {
		return tools.Result{}, notConfigured(t.Name())
	}
	l, err := t.d.Transform.ModelLineage(ctx, ref.Ref)
	if res, ok := ambiguous(t.Name(), ref, err); ok {
		return res, nil
	}
	if err != nil {
		return tools.Result{}, fmt.Errorf("dbttools: %s: %w", t.Name(), err)
	}

	parents, pMore := capIDs(l.Parents)
	children, cMore := capIDs(l.Children)
	data := lineageData{Model: cleanID(l.Model), Parents: parents, Children: children, ParentsNotShown: pMore, ChildrenNotShown: cMore}

	var sb strings.Builder
	fmt.Fprintf(&sb, "dbt lineage of %s (manifest.json): %d %s, %d %s.",
		data.Model, len(l.Parents), tools.Plural(len(l.Parents), "parent", "parents"),
		len(l.Children), tools.Plural(len(l.Children), "child", "children"))
	writeIDs(&sb, "Parents", parents, pMore, sc)
	writeIDs(&sb, "Children", children, cMore, sc)
	summary, cut := tools.Truncate(sb.String())
	return tools.Result{Summary: summary, Data: data, Truncated: cut || pMore > 0 || cMore > 0}, nil
}

// capIDs cleans ids and keeps at most maxLineageIDs; never nil.
func capIDs(ids []string) ([]string, int) {
	more := 0
	if len(ids) > maxLineageIDs {
		more = len(ids) - maxLineageIDs
		ids = ids[:maxLineageIDs]
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, cleanID(id))
	}
	return out, more
}

func writeIDs(sb *strings.Builder, label string, ids []string, more int, sc tools.Scope) {
	if len(ids) == 0 {
		fmt.Fprintf(sb, "\n%s: none", label)
		return
	}
	fmt.Fprintf(sb, "\n%s:", label)
	for _, id := range ids {
		mark := ""
		if isPipelineModel(sc, id) {
			mark = " (pipeline model)"
		}
		sb.WriteString("\n- " + id + mark)
	}
	if more > 0 {
		fmt.Fprintf(sb, "\n(%d more not shown)", more)
	}
}
