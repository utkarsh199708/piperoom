package evals

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/events"
)

// Agreement methods: "terms" is deterministic, "judge" asked an LLM judge.
const (
	AgreementTerms = "terms"
	AgreementJudge = "judge"
)

// Label is the human's blind label of a shadow room (shadow.labelled): the
// layer that really failed and 1-20 words that name the real root cause.
// Terms are ONE OR group: the root cause agrees when any one of them is
// stated. Pick distinctive words; a term every hypothesis mentions (the
// table name) makes every run agree.
type Label struct {
	Layer          string   `json:"actual_layer"`
	RootCauseTerms []string `json:"root_cause_terms"`
}

// AgreementResult says whether the agent's final hypothesis agrees with the
// human label. It is a check of the agent against the label, never a grade:
// the human grade stays the source of truth.
type AgreementResult struct {
	// AgentLayer is the layer the hypothesis blames; "" when it names none.
	AgentLayer  string `json:"agent_layer"`
	LayerAgree  bool   `json:"layer_agree"`
	CauseAgree  bool   `json:"cause_agree"`
	Method      string `json:"method"`
	Reason      string `json:"reason"`
	MatchedTerm string `json:"matched_term,omitempty"`
}

// AgreementOption tunes Agreement.
type AgreementOption func(*agreementCfg)

type agreementCfg struct{ names map[string]string }

// WithPipelineMap teaches Agreement the pipeline's own names from the run's
// pipeline_map text ("4 group:order-sync [consumer, owner kabir]"): a
// hypothesis that says "order-sync is stuck" then names the consumer layer
// although it never writes the word "consumer". Only consumer-group and
// connector names are used; topic and table names appear in every
// hypothesis and would name every layer.
func WithPipelineMap(pipelineMap string) AgreementOption {
	return func(c *agreementCfg) {
		for name, layer := range pipelineNames(pipelineMap) {
			c.names[name] = layer
		}
	}
}

var mapLine = regexp.MustCompile(`(?m)^\s*\d+\s+(?:group|connect):(\S+)\s+\[([a-z-]+)`)

// pipelineNames reads name -> layer from pipeline_map lines.
func pipelineNames(m string) map[string]string {
	out := map[string]string{}
	for _, sub := range mapLine.FindAllStringSubmatch(m, -1) {
		out[sub[1]] = sub[2]
	}
	return out
}

// Validate checks a label the way the server does (shadow.md 3.5).
func (l Label) Validate() error {
	ok := false
	for _, x := range events.Layers {
		ok = ok || x == l.Layer
	}
	if !ok {
		return fmt.Errorf("evals: label layer %q is not one of %s", l.Layer, strings.Join(events.Layers, ", "))
	}
	if n := len(l.RootCauseTerms); n < 1 || n > 20 {
		return fmt.Errorf("evals: label has %d root_cause_terms, want 1-20", n)
	}
	for _, t := range l.RootCauseTerms {
		if _, _, ok := termTokens(t); !ok {
			return fmt.Errorf("evals: label term %q has no words", t)
		}
	}
	return nil
}

// Agreement compares the final hypothesis text with the human label using
// the deterministic matcher of score.go. The hypothesis is untrusted data:
// it is only tokenised and compared, never executed or interpreted.
//
// Layer: the layer of the first clause that reports a failure (fail*,
// stuck, rejected, stopped ...) and names a layer by keyword; the keyword
// that appears first in that clause wins ("Connect task failed: the Schema
// Registry rejected it" blames kafka-connect). With no failure clause, the
// first clause naming any layer. Clauses and words that deny or clear a
// layer ("Connect is healthy", "not the importer") are skipped.
// Cause: any label term stated, outside a negation ("not the bulk import,
// it is the poll timeout" does not agree with the term "bulk import").
func Agreement(hypothesis string, label Label, opts ...AgreementOption) (AgreementResult, error) {
	if err := label.Validate(); err != nil {
		return AgreementResult{}, err
	}
	cfg := agreementCfg{names: map[string]string{}}
	for _, o := range opts {
		o(&cfg)
	}
	res := AgreementResult{Method: AgreementTerms}
	if strings.TrimSpace(hypothesis) == "" {
		res.Reason = "no hypothesis text"
		return res, nil
	}
	layer, why := agentLayer(hypothesis, cfg.names)
	res.AgentLayer = layer
	res.LayerAgree = layer == label.Layer
	var parts []string
	switch {
	case layer == "":
		parts = append(parts, "layer: hypothesis names no layer (label "+label.Layer+")")
	case res.LayerAgree:
		parts = append(parts, "layer: "+why)
	default:
		parts = append(parts, fmt.Sprintf("layer: hypothesis blames %s (%s), label says %s", layer, why, label.Layer))
	}

	toks := clauseTokens(hypothesis)
	var negatedOnly string
	for _, t := range label.RootCauseTerms {
		hit, neg := termHits(toks, t)
		if hit {
			res.CauseAgree, res.MatchedTerm = true, t
			break
		}
		if neg && negatedOnly == "" {
			negatedOnly = t
		}
	}
	switch {
	case res.CauseAgree:
		parts = append(parts, fmt.Sprintf("cause: states %q", res.MatchedTerm))
	case negatedOnly != "":
		parts = append(parts, fmt.Sprintf("cause: %q appears only in a negated clause", negatedOnly))
	default:
		parts = append(parts, "cause: no label term is stated")
	}
	res.Reason = strings.Join(parts, "; ")
	return res, nil
}

// AgreementJudged asks an LLM judge (judge.go; the hypothesis goes in the
// untrusted wrapper) whether the hypothesis states the labelled root cause
// and layer. The label plays the scenario's ground truth. An empty
// hypothesis never reaches the model.
func AgreementJudged(ctx context.Context, j HypothesisJudge, hypothesis string, label Label) (AgreementResult, error) {
	if j == nil {
		return AgreementResult{}, errors.New("evals: no judge")
	}
	if err := label.Validate(); err != nil {
		return AgreementResult{}, err
	}
	if strings.TrimSpace(hypothesis) == "" {
		return AgreementResult{Method: AgreementJudge, Reason: "no hypothesis text"}, nil
	}
	sc := Scenario{
		Name:        "shadow-label",
		Description: "A pipeline engineer labelled this incident after it was resolved. The root cause is given by the words below.",
		Expect:      Expect{FirstFailingLayer: label.Layer, RootCause: [][]string{label.RootCauseTerms}},
	}
	v, err := j.JudgeHypothesis(ctx, sc, hypothesis)
	if err != nil {
		return AgreementResult{}, fmt.Errorf("evals: judging shadow hypothesis: %w", err)
	}
	return AgreementResult{
		Method: AgreementJudge, LayerAgree: v.LayerCorrect, CauseAgree: v.RootCauseCorrect,
		Reason: v.Reason,
	}, nil
}

// --- deterministic internals ---

// clauseSplitFine adds commas and dashes to score.go's clause split, so a
// denial stays in its own clause: "not the importer, it is the poll timeout".
var clauseSplitFine = regexp.MustCompile(`[,—–]`)

// clauseTokens tokenises text clause by clause.
func clauseTokens(text string) [][]string {
	var out [][]string
	for _, c := range clauseSplit.Split(text, -1) {
		for _, f := range clauseSplitFine.Split(c, -1) {
			if t := tokenize(f); len(t) > 0 {
				out = append(out, t)
			}
		}
	}
	return out
}

var (
	// contrastWords end a negation's reach: "not X but Y" leaves Y positive.
	contrastWords = map[string]bool{"but": true, "however": true, "actually": true, "although": true, "though": true, "whereas": true, "yet": true}
	// clearedWords after a term clear it: "Connect is healthy".
	clearedWords = map[string]bool{"healthy": true, "fine": true, "ok": true, "okay": true, "normal": true, "unaffected": true,
		"unrelated": true, "irrelevant": true, "excluded": true, "ruled": true, "innocent": true, "stable": true}
)

const negWindow = 5

// negatorAt reports whether toks[i] is a negator in its context.
func negatorAt(toks []string, i int) bool {
	next := ""
	if i+1 < len(toks) {
		next = toks[i+1]
	}
	switch t := toks[i]; {
	case t == "no":
		return next != "longer" // "no longer carry amount" states a fact
	case t == "not", t == "never", t == "t", t == "without", t == "against", t == "neither", t == "nor", strings.HasPrefix(t, "avoid"):
		return true
	case t == "instead":
		return next == "of"
	case t == "rather":
		return next == "than"
	}
	return false
}

// occurrence is one place a term occurs and whether a negation reaches it.
type occurrence struct {
	start, end int
	negated    bool
}

// findTerm lists the occurrences of term in one clause. A term is negated
// by a negator in the negWindow words before it (not past a contrast word)
// or a clearing word in the negWindow words after it.
func findTerm(toks []string, term string) []occurrence {
	want, prefix, ok := termTokens(term)
	if !ok {
		return nil
	}
	var out []occurrence
	for i := 0; i+len(want) <= len(toks); i++ {
		hit := true
		for j, w := range want {
			if j == len(want)-1 && prefix {
				hit = strings.HasPrefix(toks[i+j], w)
			} else {
				hit = toks[i+j] == w
			}
			if !hit {
				break
			}
		}
		if !hit {
			continue
		}
		o := occurrence{start: i, end: i + len(want)}
		for k := i - 1; k >= 0 && k >= i-negWindow; k-- {
			if contrastWords[toks[k]] {
				break
			}
			if negatorAt(toks, k) {
				o.negated = true
				break
			}
		}
		for k := o.end; !o.negated && k < len(toks) && k < o.end+negWindow; k++ {
			if contrastWords[toks[k]] {
				break
			}
			o.negated = clearedWords[toks[k]]
		}
		out = append(out, o)
	}
	return out
}

// termHits reports whether term occurs un-negated in any clause (hit) and
// whether it occurs at all only negated (neg).
func termHits(clauses [][]string, term string) (hit, neg bool) {
	for _, c := range clauses {
		for _, o := range findTerm(c, term) {
			if o.negated {
				neg = true
			} else {
				return true, false
			}
		}
	}
	return false, neg
}

// failureMarkers say a clause reports something broken.
var failureMarkers = []string{"fail*", "stuck", "cannot", "reject*", "denied", "stopp*", "drop*", "delet*", "lost", "loss",
	"error*", "crash*", "broken", "unreachable", "expired", "incompatible", "inactive", "paused", "refus*", "timeout*",
	"timed out", "exceed*", "unable", "poison*", "retain*", "skipp*"}

// agreementLayerTerms is score.go's layerTerms plus words an agent uses when
// it describes a cause rather than names a component.
func agreementLayerTerms() map[string][]string {
	extra := map[string][]string{
		"postgres":        {"wal", "migration*", "ddl"},
		"kafka-connect":   {"task*"},
		"schema-registry": {"schema compatibility"},
		"kafka":           {"retention", "log start", "offset*"},
		"consumer":        {"poison pill", "deserializ*", "group*"},
		"warehouse":       {"analytics"},
	}
	out := map[string][]string{}
	for l, ts := range layerTerms {
		out[l] = append(append([]string{}, ts...), extra[l]...)
	}
	return out
}

// causeStart is the index of the first word after "because", "due to" or
// "caused by", or 0 when the clause has none.
func causeStart(toks []string) int {
	for i, t := range toks {
		switch {
		case t == "because":
			return i + 1
		case t == "due" && i+1 < len(toks) && toks[i+1] == "to":
			return i + 2
		case t == "caused" && i+1 < len(toks) && toks[i+1] == "by":
			return i + 2
		}
	}
	return 0
}

// agentLayer finds the layer the hypothesis blames and says why.
func agentLayer(text string, names map[string]string) (layer, why string) {
	terms := agreementLayerTerms()
	for name, l := range names {
		terms[l] = append(terms[l], name)
	}
	type hit struct {
		pos         int
		layer, word string
	}
	clauses := clauseTokens(text)
	best := func(wantFailure bool) (hit, bool) {
		for _, toks := range clauses {
			if wantFailure {
				live := false
				for _, m := range failureMarkers {
					for _, o := range findTerm(toks, m) {
						live = live || !o.negated
					}
				}
				if !live {
					continue
				}
			}
			var hits []hit
			for l, ts := range terms {
				for _, t := range ts {
					for _, o := range findTerm(toks, t) {
						if !o.negated {
							hits = append(hits, hit{o.start, l, t})
						}
					}
				}
			}
			if len(hits) == 0 {
				continue
			}
			// "X is stale because the connector stopped": the words after
			// the connective carry the cause, the ones before the symptom.
			if at := causeStart(toks); at > 0 {
				var after []hit
				for _, h := range hits {
					if h.pos >= at {
						after = append(after, h)
					}
				}
				if len(after) > 0 {
					hits = after
				}
			}
			// Earliest word wins; ties go to the layer name so the verdict
			// does not depend on map order.
			sort.Slice(hits, func(a, b int) bool {
				if hits[a].pos != hits[b].pos {
					return hits[a].pos < hits[b].pos
				}
				return hits[a].layer < hits[b].layer
			})
			return hits[0], true
		}
		return hit{}, false
	}
	if h, ok := best(true); ok {
		return h.layer, fmt.Sprintf("failure clause names %q", h.word)
	}
	if h, ok := best(false); ok {
		return h.layer, fmt.Sprintf("first layer named is %q", h.word)
	}
	return "", ""
}
