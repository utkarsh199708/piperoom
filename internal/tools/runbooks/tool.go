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

// Register adds runbook_lookup over the embedded library to reg.
func Register(reg *tools.Registry) error {
	rbs, err := Embedded()
	if err != nil {
		return err
	}
	if err := reg.Register(New(rbs)); err != nil {
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
	return "Looks up sourced runbook sections for 7 CDC/Kafka failure modes: replication slot/WAL growth, " +
		"failed Debezium connector, schema incompatibility, consumer lag, poison pill, retention data loss and " +
		"source DDL. Returns matching sections with official doc links. Reference text, not live data: " +
		"confirm with the read tools."
}

// InputSchema implements tools.Tool. Strict tool use rejects numeric and
// length bounds (minimum, maxLength, …), so limits are enforced in Run.
func (t *Tool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query":       map[string]any{"type": "string", "description": "Symptoms, error text, config or tool names to look up."},
			"max_results": map[string]any{"type": "integer", "description": "How many sections, 1-5. Default 3."},
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
}

// Run implements tools.Tool. It does no I/O.
func (t *Tool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if err := ctx.Err(); err != nil {
		return tools.Result{}, err
	}
	var p struct {
		Query      string `json:"query"`
		MaxResults int    `json:"max_results"`
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
	case !utf8.ValidString(p.Query) || strings.ContainsRune(p.Query, utf8.RuneError):
		// encoding/json turns invalid sequences and lone surrogates into U+FFFD.
		return tools.Result{}, fmt.Errorf("%w: query is not valid UTF-8", tools.ErrBadInput)
	case p.MaxResults < 0:
		return tools.Result{}, fmt.Errorf("%w: max_results must be 1-5", tools.ErrBadInput)
	}
	n := p.MaxResults
	if n == 0 {
		n = defaultResults
	}
	n = min(n, maxResults)

	hits := t.search(p.Query, n)
	data := map[string]any{"query": p.Query, "hits": hits}
	if len(hits) == 0 {
		titles := make([]string, len(t.rbs))
		for i, rb := range t.rbs {
			titles[i] = rb.Title
		}
		s, tr := tools.Truncate(fmt.Sprintf("No runbook section matched %q. Runbooks: %s", p.Query, strings.Join(titles, "; ")))
		return tools.Result{Summary: s, Data: data, Truncated: tr}, nil
	}
	var sb strings.Builder
	for i, h := range hits {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		fmt.Fprintf(&sb, "[%d] %s › %s (score %d)\n%s\nSources: %s",
			i+1, h.Title, h.Section, h.Score, cutBytes(h.Text, maxHitTextBytes), strings.Join(h.Sources, "; "))
	}
	s, tr := tools.Truncate(sb.String())
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
		hits = append(hits, hit{Runbook: rb.ID, Title: rb.Title, Section: s.Heading, Score: max(1, int(math.Round(r.score*10))), Text: s.Text, Sources: rb.Sources})
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
