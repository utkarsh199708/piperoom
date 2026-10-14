package runbooks

import (
	"regexp"
	"strings"
	"unicode"
)

// stopwords carry no meaning for lookup.
var stopwords = set([]string{
	"a", "an", "and", "are", "as", "at", "be", "by", "can", "do", "does", "for", "from", "has", "how", "if",
	"in", "is", "it", "its", "my", "no", "not", "of", "on", "or", "so", "the", "then", "there", "this",
	"to", "was", "what", "when", "why", "with",
})

// lsnRe matches a Postgres LSN such as 0/16B3748 or 1A/2B000028.
var lsnRe = regexp.MustCompile(`^[0-9a-f]{1,8}/[0-9a-f]{1,8}$`)

// terms turns ops text (alert titles, stack traces, config names, SQL errors)
// into index terms, in text order and with repeats so callers can count them.
//
//   - '.', '_' and camelCase/PascalCase are split, and the unsplit token is kept
//     too: org.apache.kafka.connect.errors.DataException gives org, apache,
//     kafka, connect, errors, data, exception, dataexception and the full
//     lower-case class name.
//   - A token with a digit is a code (40403, 22P02, 57P01) or an LSN (0/16B3748)
//     and is kept whole, never split or stemmed.
//   - Everything is lower case; plain words get a light stem.
func terms(text string) []string {
	var out []string
	add := func(w string) {
		if len(w) >= 2 && !stopwords[w] {
			out = append(out, w)
		}
	}
	isWord := func(r rune) bool {
		return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.' || r == '/'
	}
	for _, run := range strings.FieldsFunc(text, func(r rune) bool { return !isWord(r) }) {
		run = strings.Trim(run, "./_")
		if lsn := strings.ToLower(run); lsnRe.MatchString(lsn) && strings.ContainsAny(lsn, "0123456789") {
			add(lsn)
			continue
		}
		for _, tok := range strings.Split(run, "/") {
			tokenTerms(tok, add)
		}
	}
	return out
}

// tokenTerms emits the terms of one '/'-free token.
func tokenTerms(tok string, add func(string)) {
	tok = strings.Trim(tok, "._")
	if tok == "" {
		return
	}
	parts := strings.FieldsFunc(tok, func(r rune) bool { return r == '.' || r == '_' })
	if len(parts) > 1 {
		add(strings.ToLower(tok)) // the full class name, restart_lsn, max.poll.interval.ms
	}
	for _, p := range parts {
		words := splitCamel(p)
		if len(words) > 1 {
			add(strings.ToLower(p)) // recorddeserializationexception
		}
		for _, w := range words {
			add(stem(strings.ToLower(w)))
		}
	}
}

// splitCamel splits at lower→Upper ("dataException") and before the last
// capital of an acronym ("SQLState" → SQL, State). A part with a digit is a
// code and stays whole.
func splitCamel(s string) []string {
	rs := []rune(s)
	if strings.ContainsAny(s, "0123456789") {
		return []string{s}
	}
	var out []string
	start := 0
	for i := 1; i < len(rs); i++ {
		lowerToUpper := unicode.IsLower(rs[i-1]) && unicode.IsUpper(rs[i])
		acronymEnd := unicode.IsUpper(rs[i-1]) && unicode.IsUpper(rs[i]) && i+1 < len(rs) && unicode.IsLower(rs[i+1])
		if lowerToUpper || acronymEnd {
			out = append(out, string(rs[start:i]))
			start = i
		}
	}
	return append(out, string(rs[start:]))
}

// stem strips common English suffixes so rebalance, rebalancing and
// rebalanced meet. It is deliberately crude and only has to be consistent
// between documents and queries. Words with a digit, '_' or '.' are left alone.
func stem(w string) string {
	if len(w) < 4 || strings.ContainsAny(w, "0123456789_.") {
		return w
	}
	switch {
	case strings.HasSuffix(w, "sses"):
		w = strings.TrimSuffix(w, "es")
	case strings.HasSuffix(w, "ies") && len(w) > 4:
		w = strings.TrimSuffix(w, "ies") + "y"
	case strings.HasSuffix(w, "s") && !strings.HasSuffix(w, "ss") && !strings.HasSuffix(w, "us") && !strings.HasSuffix(w, "is"):
		w = w[:len(w)-1]
	}
	// serialization/serialize/deserialization meet; "ation" in general would
	// make publication and public (the schema name) collide.
	if r, ok := strings.CutSuffix(w, "ization"); ok && len(r) >= 4 {
		w = r + "iz"
	}
	for _, suf := range []string{"ing", "ed"} {
		// The remainder must keep a vowel and at least 3 letters: "bed" and "sing" stay.
		if r, ok := strings.CutSuffix(w, suf); ok && len(r) >= 3 && strings.ContainsAny(r, "aeiouy") {
			w = undouble(r)
			break
		}
	}
	if len(w) > 4 && strings.HasSuffix(w, "e") {
		w = w[:len(w)-1] // rebalance → rebalanc, so it matches rebalanc(ing)
	}
	return w
}

// undouble turns lagg into lag and stopp into stop, but keeps "ll", "ss", "zz"
// (fall, miss, buzz).
func undouble(w string) string {
	n := len(w)
	if n >= 2 && w[n-1] == w[n-2] && !strings.ContainsRune("lsz", rune(w[n-1])) {
		return w[:n-1]
	}
	return w
}

// synonymGroups lists phrases that mean the same thing in CDC ops text. When a
// query contains one phrase, the others are added at synonymWeight. Phrases go
// through terms(), so they match the stemmed index.
var synonymGroups = [][]string{
	{"slot", "replication slot"},
	{"lag", "behind", "falling behind", "backlog"},
	{"rebalance", "rebalancing", "preparingrebalance", "join group", "group coordinator"},
	{"retention", "log start", "start offset", "aged out", "log truncation"},
	{"pause", "paused"},
	{"dlq", "dead letter queue", "dead-letter"},
	{"poison pill", "bad record", "malformed record", "corrupt record", "unknown magic byte", "wire format"},
	{"wal", "write ahead log", "pg_wal", "disk usage"},
	{"ddl", "alter table", "migration", "schema migration"},
	{"permission", "permission denied", "must be owner", "insufficient privilege", "not authorized"},
	{"incompatible", "schema being registered", "compatibility", "409"},
	{"crash loop", "crashloopbackoff", "restarted", "restart loop", "crashing", "keeps restarting"},
	{"offset out of range", "offsetoutofrange", "out of range", "auto.offset.reset"},
	{"dropped", "drop column", "removed column", "column missing"},
}

// synonymWeight is the query weight of an expanded term relative to a typed one.
const synonymWeight = 0.4

// synonyms maps a normalised phrase (terms joined by space) to the term lists
// of the other phrases of its group.
type synonyms struct {
	phrases []synPhrase
}

type synPhrase struct {
	terms string   // space-joined terms of the phrase
	group int      // index into synonymGroups
	words []string // the phrase's terms
}

func newSynonyms() *synonyms {
	s := &synonyms{}
	for g, grp := range synonymGroups {
		for _, p := range grp {
			ws := terms(p)
			if len(ws) == 0 {
				continue
			}
			s.phrases = append(s.phrases, synPhrase{terms: " " + strings.Join(ws, " ") + " ", group: g, words: ws})
		}
	}
	return s
}

// expand returns the extra terms for a tokenised query, in a fixed order and
// without terms the query already has.
func (s *synonyms) expand(q []string) []string {
	joined := " " + strings.Join(q, " ") + " "
	have := set(q)
	hit := map[int]bool{}
	for _, p := range s.phrases {
		if strings.Contains(joined, p.terms) {
			hit[p.group] = true
		}
	}
	var out []string
	for _, p := range s.phrases { // phrase order is group order, so output is deterministic
		if !hit[p.group] || strings.Contains(joined, p.terms) {
			continue
		}
		for _, w := range p.words {
			if !have[w] {
				have[w] = true
				out = append(out, w)
			}
		}
	}
	return out
}
