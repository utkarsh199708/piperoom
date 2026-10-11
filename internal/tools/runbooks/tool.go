package runbooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"
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
}

var _ tools.Tool = (*Tool)(nil)

// unit is one searchable section, with its tokens counted once in New.
type unit struct {
	rb       int // index into Tool.rbs, which keeps file order
	section  int // index into Runbook.Sections
	runbook  map[string]bool
	heading  map[string]bool
	textFreq map[string]int
}

// New builds the tool. The runbooks are parsed already (Load/Embedded).
func New(rbs []Runbook) *Tool {
	t := &Tool{rbs: rbs}
	for i, rb := range rbs {
		rbTerms := set(terms(rb.Title + " " + strings.Join(rb.Keywords, " ")))
		for j, s := range rb.Sections {
			freq := map[string]int{}
			for _, w := range terms(s.Text) {
				freq[w]++
			}
			t.units = append(t.units, unit{rb: i, section: j, runbook: rbTerms, heading: set(terms(s.Heading)), textFreq: freq})
		}
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

func (t *Tool) search(query string, n int) []hit {
	q := terms(query)
	q = dedupe(q)
	if len(q) > maxQueryTerms {
		q = q[:maxQueryTerms]
	}
	type scored struct {
		u     unit
		score int
	}
	var res []scored
	for _, u := range t.units {
		score := 0
		for _, w := range q {
			if u.runbook[w] {
				score += 3
			}
			if u.heading[w] {
				score += 2
			}
			score += min(u.textFreq[w], 3)
		}
		if score > 0 {
			res = append(res, scored{u, score})
		}
	}
	// Units are in file then section order, so a stable sort by score keeps
	// ties in that order.
	slices.SortStableFunc(res, func(a, b scored) int { return b.score - a.score })
	hits := []hit{}
	for _, r := range res[:min(n, len(res))] {
		rb := t.rbs[r.u.rb]
		s := rb.Sections[r.u.section]
		hits = append(hits, hit{Runbook: rb.ID, Title: rb.Title, Section: s.Heading, Score: r.score, Text: s.Text, Sources: rb.Sources})
	}
	return hits
}

// stopwords carry no meaning for lookup.
var stopwords = set([]string{
	"a", "an", "and", "are", "as", "at", "be", "by", "can", "do", "does", "for", "from", "has", "how", "if",
	"in", "is", "it", "its", "my", "no", "not", "of", "on", "or", "so", "the", "then", "there", "this",
	"to", "was", "what", "when", "why", "with",
})

// terms tokenizes text: lower case, split on anything but letters, digits,
// '_' and '.', trim dots, drop short tokens and stopwords; dotted or
// underscored tokens also contribute their parts.
func terms(text string) []string {
	var out []string
	add := func(w string) {
		if len(w) >= 2 && !stopwords[w] {
			out = append(out, w)
		}
	}
	for _, tok := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '.'
	}) {
		tok = strings.Trim(tok, ".")
		add(tok)
		if strings.ContainsAny(tok, "_.") {
			for _, part := range strings.FieldsFunc(tok, func(r rune) bool { return r == '_' || r == '.' }) {
				add(part)
			}
		}
	}
	return out
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
