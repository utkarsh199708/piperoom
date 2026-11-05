package runbooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/tools"
)

// ToolName is the name the model calls.
const ToolName = "runbook_lookup"

const (
	maxQueryBytes   = 1000
	defaultResults  = 3
	maxResults      = 5
	maxQueryTerms   = 32
	maxHitTextBytes = 1500
)

// Tool is runbook_lookup over a fixed set of runbooks. It is safe for
// concurrent use: everything is built in New and only read in Run.
type Tool struct {
	rbs   []Runbook
	units []unit
	syn   *synonyms
	df    map[string]int // number of sections containing the term in any field
	avgDL float64        // mean weighted section length
	p     params
	ki    issueIndex
}

var _ tools.Tool = (*Tool)(nil)

// BM25 parameters and field boosts. A runbook title/keyword hit says more
// about the topic than a heading hit, which says more than a body hit.
type params struct{ k1, b, title, heading, body float64 }

var defaultParams = params{k1: 1.2, b: 0.75, title: 3, heading: 2, body: 1}

// unit is one searchable section. tf is the boost-weighted term frequency over
// the runbook title and keywords, the section heading and the section text.
type unit struct {
	rb      int // index into Tool.rbs, which keeps file order
	section int // index into Runbook.Sections
	tf      map[string]float64
	length  float64 // sum of tf
}

// New builds the tool. The runbooks are parsed already (Load/Embedded).
func New(rbs []Runbook) *Tool { return newWithParams(rbs, defaultParams) }

// NewWithKnownIssues builds the tool over runbooks and the known-issues index
// (LoadKnownIssues/EmbeddedKnownIssues); a nil index means none.
func NewWithKnownIssues(rbs []Runbook, ki *KnownIssues) *Tool {
	t := newWithParams(rbs, defaultParams)
	t.ki = newIssueIndex(ki, t.p)
	return t
}

func newWithParams(rbs []Runbook, p params) *Tool {
	t := &Tool{rbs: rbs, syn: newSynonyms(), df: map[string]int{}, p: p}
	var total float64
	for i, rb := range rbs {
		title := terms(rb.Title + " " + strings.Join(rb.Keywords, " "))
		for j, s := range rb.Sections {
			u := unit{rb: i, section: j, tf: map[string]float64{}}
			for _, w := range title {
				u.tf[w] += t.p.title
			}
			for _, w := range terms(s.Heading) {
				u.tf[w] += t.p.heading
			}
			for _, w := range terms(s.Text) {
				u.tf[w] += t.p.body
			}
			for w, f := range u.tf {
				u.length += f
				t.df[w]++
			}
			total += u.length
			t.units = append(t.units, u)
		}
	}
	if len(t.units) > 0 {
		t.avgDL = total / float64(len(t.units))
	}
	return t
}

// Register adds runbook_lookup over the embedded library and known-issues
// index to reg. A bad library file fails here, at start-up.
func Register(reg *tools.Registry) error {
	rbs, err := Embedded()
	if err != nil {
		return err
	}
	ki, err := EmbeddedKnownIssues()
	if err != nil {
		return err
	}
	if err := reg.Register(NewWithKnownIssues(rbs, ki)); err != nil {
		return fmt.Errorf("runbooks: register: %w", err)
	}
	return nil
}

// Name implements tools.Tool.
func (t *Tool) Name() string { return ToolName }

// Kind implements tools.Tool: the tool reads only embedded text.
func (t *Tool) Kind() tools.Kind { return tools.KindRead }

// Description implements tools.Tool.
func (t *Tool) Description() string {
	return "Looks up sourced runbooks for 7 CDC/Kafka failure modes (slot/WAL growth, failed connector, schema " +
		"incompatibility, lag, poison pill, retention loss, source DDL) and known version issues (renamed keys, " +
		"removed plugins, fixed bugs). Pass versions to rank issues that hit them first with the fixed-in " +
		"release. Reference text, not live data."
}

// InputSchema implements tools.Tool. Strict tool use rejects numeric and
// length bounds (minimum, maxLength, …), so limits are enforced in Run.
func (t *Tool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query":       map[string]any{"type": "string", "description": "Symptoms, error text, config or tool names to look up."},
			"max_results": map[string]any{"type": "integer", "description": "How many sections and known issues, 1-5. Default 3."},
			"versions": map[string]any{
				"type":        "array",
				"description": "Running versions, e.g. {component: debezium-postgres, version: 2.1.4.Final}.",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"component": map[string]any{"type": "string", "enum": issueComponents},
						"version":   map[string]any{"type": "string"},
					},
					"required":             []string{"component", "version"},
					"additionalProperties": false,
				},
			},
		},
		"additionalProperties": false,
		"required":             []string{"query"},
	}
}

// hit is one result in Result.Data.
type hit struct {
	Runbook string   `json:"runbook"`
	Title   string   `json:"title"`
	Section string   `json:"section"`
	Score   int      `json:"score"`
	Text    string   `json:"text"`
	Sources []string `json:"sources"`
	// Items are the section's "- " list items, in order; the room UI shows them
	// as a checklist and checklist.step_done addresses them by index.
	Items []string `json:"items"`
}

// Run implements tools.Tool. It does no I/O.
func (t *Tool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if err := ctx.Err(); err != nil {
		return tools.Result{}, err
	}
	var p struct {
		Query      string           `json:"query"`
		MaxResults int              `json:"max_results"`
		Versions   []RunningVersion `json:"versions"`
	}
	if len(bytes.TrimSpace(input)) == 0 {
		return tools.Result{}, fmt.Errorf("%w: empty input", tools.ErrBadInput)
	}
	if !utf8.Valid(input) {
		return tools.Result{}, fmt.Errorf("%w: input is not valid UTF-8", tools.ErrBadInput)
	}
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	switch {
	case strings.TrimSpace(p.Query) == "":
		return tools.Result{}, fmt.Errorf("%w: query is required", tools.ErrBadInput)
	case len(p.Query) > maxQueryBytes:
		return tools.Result{}, fmt.Errorf("%w: query is %d bytes, more than %d", tools.ErrBadInput, len(p.Query), maxQueryBytes)
	case p.MaxResults < 0:
		return tools.Result{}, fmt.Errorf("%w: max_results must be 1-5", tools.ErrBadInput)
	}
	vs, err := runningVersions(p.Versions)
	if err != nil {
		return tools.Result{}, err
	}
	n := p.MaxResults
	if n == 0 {
		n = defaultResults
	}
	n = min(n, maxResults)

	hits := t.search(p.Query, n)
	data := map[string]any{"query": p.Query, "hits": hits}
	var issues []issueHit
	if len(t.ki.list) > 0 {
		issues = t.ki.search(t.queryTerms(p.Query), vs, t.p, n)
		data["known_issues"] = issues
	}
	if len(hits) == 0 && len(issues) == 0 {
		titles := make([]string, len(t.rbs))
		for i, rb := range t.rbs {
			titles[i] = rb.Title
		}
		s, tr := tools.Truncate(fmt.Sprintf("No runbook section matched %q. Runbooks: %s", p.Query, strings.Join(titles, "; ")))
		return tools.Result{Summary: s, Data: data, Truncated: tr}, nil
	}
	var rb, ki strings.Builder
	for i, h := range hits {
		if i > 0 {
			rb.WriteString("\n\n")
		}
		fmt.Fprintf(&rb, "[%d] %s › %s (score %d)\n%s\nSources: %s",
			i+1, h.Title, h.Section, h.Score, cutBytes(h.Text, maxHitTextBytes), strings.Join(h.Sources, "; "))
	}
	if len(issues) > 0 {
		ki.WriteString("Known issues:")
		for i, h := range issues {
			ki.WriteString("\n" + h.line(i))
		}
	}
	// An issue that hits the running version is the most specific answer, so it
	// leads (truncation cuts the end); otherwise the runbooks lead.
	parts := []string{rb.String(), ki.String()}
	if len(issues) > 0 && issues[0].Affects == "yes" {
		parts[0], parts[1] = parts[1], parts[0]
	}
	if len(hits) == 0 {
		parts = []string{fmt.Sprintf("No runbook section matched %q.", p.Query), ki.String()}
	}
	s, tr := tools.Truncate(strings.Join(slices.DeleteFunc(parts, func(x string) bool { return x == "" }), "\n\n"))
	return tools.Result{Summary: s, Data: data, Truncated: tr}, nil
}

// weighted is a query term with its weight.
type weighted struct {
	term   string
	weight float64
}

// queryTerms tokenises the query, drops repeats, caps the typed terms and adds
// synonym terms at a lower weight. The order is fixed so scores are
// reproducible.
func (t *Tool) queryTerms(query string) []weighted {
	q := dedupe(terms(query))
	if len(q) > maxQueryTerms {
		q = q[:maxQueryTerms]
	}
	out := make([]weighted, 0, len(q))
	for _, w := range q {
		out = append(out, weighted{w, 1})
	}
	for _, w := range t.syn.expand(q) {
		out = append(out, weighted{w, synonymWeight})
	}
	return out
}

// search ranks sections with BM25 over the boost-weighted fields. Units are in
// file then section order, so the stable sort keeps ties in that order.
func (t *Tool) search(query string, n int) []hit {
	q := t.queryTerms(query)
	type scored struct {
		u     unit
		score float64
	}
	var res []scored
	nDocs := float64(len(t.units))
	for _, u := range t.units {
		var score float64
		for _, qt := range q {
			f := u.tf[qt.term]
			if f == 0 {
				continue
			}
			df := float64(t.df[qt.term])
			idf := math.Log(1 + (nDocs-df+0.5)/(df+0.5))
			norm := t.p.k1 * (1 - t.p.b + t.p.b*u.length/t.avgDL)
			score += qt.weight * idf * f * (t.p.k1 + 1) / (f + norm)
		}
		if score > 0 {
			res = append(res, scored{u, score})
		}
	}
	slices.SortStableFunc(res, func(a, b scored) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		}
		return 0
	})
	hits := []hit{}
	for _, r := range res[:min(n, len(res))] {
		rb := t.rbs[r.u.rb]
		s := rb.Sections[r.u.section]
		// Score is BM25 x 10, rounded; at least 1 for any match.
		hits = append(hits, hit{Runbook: rb.ID, Title: rb.Title, Section: s.Heading, Score: max(1, int(math.Round(r.score*10))), Text: s.Text, Sources: rb.Sources, Items: listItems(s.Text)})
	}
	return hits
}

func dedupe(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func set(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// cutBytes shortens s to at most n bytes without splitting a rune.
func cutBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

// runningVersions checks the versions input and keys it by component. An
// unknown component or a version that does not parse is the model's mistake,
// so it is refused rather than ignored.
func runningVersions(in []RunningVersion) (map[string]string, error) {
	if len(in) > maxVersions {
		return nil, fmt.Errorf("%w: %d versions, at most %d", tools.ErrBadInput, len(in), maxVersions)
	}
	vs := make(map[string]string, len(in))
	for _, v := range in {
		if !slices.Contains(issueComponents, v.Component) {
			return nil, fmt.Errorf("%w: versions: component %q is not one of %s", tools.ErrBadInput, v.Component, strings.Join(issueComponents, ", "))
		}
		if _, err := parseVersion(v.Version); err != nil {
			return nil, fmt.Errorf("%w: versions: %s: %v", tools.ErrBadInput, v.Component, err)
		}
		if _, dup := vs[v.Component]; dup {
			return nil, fmt.Errorf("%w: versions: %s given twice", tools.ErrBadInput, v.Component)
		}
		vs[v.Component] = strings.TrimSpace(v.Version)
	}
	return vs, nil
}
