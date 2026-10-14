package textsearch

import "math"

// BM25 parameters. Same values as the runbook ranker: short ops documents,
// so a moderate length normalisation.
const (
	k1 = 1.2
	b  = 0.75
)

// Field is one weighted piece of a document. A term found in a field counts
// Weight times in the document's term frequency, so a title word outranks the
// same word deep in a trace.
type Field struct {
	Text   string
	Weight float64
}

// Weighted is a query term with its weight.
type Weighted struct {
	Term   string
	Weight float64
}

// Index is an immutable BM25 index over a fixed list of documents. It is safe
// for concurrent use after New.
type Index struct {
	docs  []docStat
	df    map[string]int
	avgDL float64
}

type docStat struct {
	tf     map[string]float64
	length float64
}

// New builds an index. Document i of the index is fields[i].
func New(fields [][]Field) *Index {
	ix := &Index{docs: make([]docStat, len(fields)), df: map[string]int{}}
	var total float64
	for i, fs := range fields {
		d := docStat{tf: map[string]float64{}}
		for _, f := range fs {
			for _, t := range Terms(f.Text) {
				d.tf[t] += f.Weight
				d.length += f.Weight
			}
		}
		for t := range d.tf {
			ix.df[t]++
		}
		total += d.length
		ix.docs[i] = d
	}
	if len(fields) > 0 {
		ix.avgDL = total / float64(len(fields))
	}
	return ix
}

// Len is the number of documents.
func (ix *Index) Len() int { return len(ix.docs) }

// QueryTerms tokenises a query into unit-weight terms, dropping repeats and
// keeping at most max terms (0 means no cap), in text order.
func QueryTerms(query string, max int) []Weighted {
	seen := map[string]bool{}
	var out []Weighted
	for _, t := range Terms(query) {
		if seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, Weighted{Term: t, Weight: 1})
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

// Score is the BM25 score of document i for q. A document with no query term
// scores 0.
func (ix *Index) Score(i int, q []Weighted) float64 {
	d := ix.docs[i]
	n := float64(len(ix.docs))
	var score float64
	for _, qt := range q {
		f := d.tf[qt.Term]
		if f == 0 {
			continue
		}
		df := float64(ix.df[qt.Term])
		idf := math.Log(1 + (n-df+0.5)/(df+0.5))
		norm := k1
		if ix.avgDL > 0 {
			norm = k1 * (1 - b + b*d.length/ix.avgDL)
		}
		score += qt.Weight * idf * f * (k1 + 1) / (f + norm)
	}
	return score
}
