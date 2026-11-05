package evals

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
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
	if !contains(incidentLayers(), l.Layer) {
		return fmt.Errorf("evals: label layer %q is not one of %s", l.Layer, strings.Join(incidentLayers(), ", "))
	}
	if n := len(l.RootCauseTerms); n < 1 || n > 20 {
		return fmt.Errorf("evals: label has %d root_cause_terms, want 1-20", n)
	}
	for _, t := range l.RootCauseTerms {
		if len(t) > 200 {
			return fmt.Errorf("evals: label term is %d characters, want 1-200", len(t))
		}
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
	layer, why, also := agentLayer(hypothesis, cfg.names)
	if len(also) > 0 {
		why += "; also names " + strings.Join(also, ", ")
	}
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

// --- layer detection ---

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

// agentLayer finds the layer the hypothesis blames and says why. also lists
// the other layers the same clause names, so a reader sees what was
// ambiguous.
func agentLayer(text string, names map[string]string) (layer, why string, also []string) {
	terms := agreementLayerTerms()
	for name, l := range names {
		terms[l] = append(terms[l], name)
	}
	type hit struct {
		pos         int
		layer, word string
	}
	clauses := clauseTokens(text)
	best := func(wantFailure bool) (hit, []string, bool) {
		for _, c := range clauses {
			toks := c.toks
			if wantFailure {
				live := false
				for _, m := range failureMarkers {
					for _, o := range findAgreeTerm(c, m) {
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
					for _, o := range findAgreeTerm(c, t) {
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
			var others []string
			seen := map[string]bool{hits[0].layer: true}
			for _, h := range hits[1:] {
				if !seen[h.layer] {
					seen[h.layer] = true
					others = append(others, h.layer)
				}
			}
			return hits[0], others, true
		}
		return hit{}, nil, false
	}
	if h, o, ok := best(true); ok {
		return h.layer, fmt.Sprintf("failure clause names %q", h.word), o
	}
	if h, o, ok := best(false); ok {
		return h.layer, fmt.Sprintf("first layer named is %q", h.word), o
	}
	return "", "", nil
}
