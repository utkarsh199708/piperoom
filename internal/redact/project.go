package redact

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Some secrets can only be read in the text before any rule ran: once the
// #372 flag rule has masked "Zx9\" of "--password Zx9\ hunter2", or the URL
// rule has eaten "password={Br@" of a JDBC URL, the rest of the value no
// longer reads as one. The passes for #511 and #534 therefore find their
// spans in the original text, and project maps each span onto the output:
// every output byte that came from the span, and any mask the span
// overlaps, is replaced by one mask. Text is only ever replaced by a mask,
// so nothing an earlier rule masked can come back.

// span is a range [a, b) of the original text that must not survive.
type span struct{ a, b int }

// piece maps out[os:oe] to orig[gs:ge]: literal text (the same bytes) or a
// mask that replaced the original range.
type piece struct {
	os, oe, gs, ge int
	mask           bool
}

// maskTokens returns the bounds of the masks Text writes in s, with the
// characters a token mask keeps on either side (sk-a…[redacted 48]…9Q).
// Masks with nothing between them come back as one.
func maskTokens(s string) [][2]int {
	var out [][2]int
	for i := 0; i < len(s); {
		k := strings.Index(s[i:], marker)
		if k < 0 {
			break
		}
		a := i + k
		e := a + len(marker)
		for e < len(s) && s[e] != ']' && s[e] != '\n' && s[e] != '[' {
			e++
		}
		if e >= len(s) || s[e] != ']' {
			i = a + len(marker)
			continue
		}
		e++
		if strings.HasSuffix(s[:a], "…") {
			if h := tokenSide(s[:a-len("…")], 4, true); h > 0 {
				a -= len("…") + h
			}
		}
		if strings.HasPrefix(s[e:], "…") {
			if t := tokenSide(s[e+len("…"):], 2, false); t > 0 {
				e += len("…") + t
			}
		}
		if n := len(out); n > 0 && out[n-1][1] >= a {
			out[n-1][1] = e
		} else {
			out = append(out, [2]int{a, e})
		}
		i = e
	}
	return out
}

// tokenSide returns the byte length of up to n runes at the end (atEnd) or
// start of s that a token mask keeps: no blank, '…' or bracket.
func tokenSide(s string, n int, atEnd bool) int {
	l := 0
	for range n {
		var r rune
		var size int
		if atEnd {
			r, size = utf8.DecodeLastRuneInString(s[:len(s)-l])
		} else {
			r, size = utf8.DecodeRuneInString(s[l:])
		}
		if size == 0 || r == '…' || r == '[' || r == ']' || unicode.IsSpace(r) {
			break
		}
		l += size
	}
	return l
}

// align splits out into literal pieces and masks and finds where each came
// from in orig. The literal text around the masks is matched in order, the
// first piece as a prefix of orig and the last as a suffix. It reports
// false when out does not read as orig with masks put in.
func align(orig, out string) ([]piece, bool) {
	// Adjacent masks come back as one: there is no literal text between
	// them to place them by.
	merged := maskTokens(out)
	if len(merged) == 0 {
		return []piece{{0, len(out), 0, len(orig), false}}, orig == out
	}
	last := out[merged[len(merged)-1][1]:]
	if !strings.HasPrefix(orig, out[:merged[0][0]]) || !strings.HasSuffix(orig, last) {
		return nil, false
	}
	tailAt := len(orig) - len(last)
	ps := make([]piece, 0, 2*len(merged)+1)
	ps = append(ps, piece{0, merged[0][0], 0, merged[0][0], false})
	g := merged[0][0]
	for i, m := range merged {
		var lit string
		var at int
		if i+1 < len(merged) {
			lit = out[m[1]:merged[i+1][0]]
			// A mask replaced at least two bytes (apply skips shorter
			// values), so the next literal starts after them.
			k := strings.Index(orig[min(g+2, tailAt):tailAt], lit)
			if k < 0 {
				return nil, false
			}
			at = min(g+2, tailAt) + k
		} else {
			lit, at = last, tailAt
		}
		if at <= g {
			return nil, false
		}
		ps = append(ps, piece{m[0], m[1], g, at, true}, piece{m[1], m[1] + len(lit), at, at + len(lit), false})
		g = at + len(lit)
	}
	return ps, true
}

// project replaces the output text of each span with one mask. A span
// that only overlaps masks is already hidden and is left alone. Spans whose
// output overlaps or touches become one mask.
func project(orig, out string, spans []span, st *Stats) string {
	if len(spans) == 0 {
		return out
	}
	ps, ok := align(orig, out)
	if !ok {
		return out
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].a < spans[j].a })
	// Each hit is an output range and the original range it hides.
	type hit struct{ os, oe, ga, gb int }
	var hits []hit
	pi := 0
	for _, sp := range spans {
		for pi < len(ps) && ps[pi].ge <= sp.a {
			pi++
		}
		if pi >= len(ps) {
			break
		}
		f, l, clear := -1, -1, false
		for k := pi; k < len(ps) && ps[k].gs < sp.b; k++ {
			if ps[k].gs == ps[k].ge {
				continue
			}
			if f < 0 {
				f = k
			}
			l = k
			clear = clear || !ps[k].mask
		}
		if !clear {
			continue
		}
		fp, lp := ps[f], ps[l]
		h := hit{fp.os, lp.oe, fp.gs, lp.ge}
		if !fp.mask {
			h.os, h.ga = fp.os+(sp.a-fp.gs), sp.a
		}
		if !lp.mask {
			h.oe, h.gb = lp.os+(sp.b-lp.gs), sp.b
		}
		if n := len(hits); n > 0 && h.os <= hits[n-1].oe {
			hits[n-1].oe = max(hits[n-1].oe, h.oe)
			hits[n-1].gb = max(hits[n-1].gb, h.gb)
			continue
		}
		hits = append(hits, h)
	}
	if len(hits) == 0 {
		return out
	}
	var b strings.Builder
	b.Grow(len(out))
	last := 0
	for _, h := range hits {
		b.WriteString(out[last:h.os])
		b.WriteString(mask(orig[h.ga:h.gb], styleSecret))
		st.add(KindKeyValue)
		last = h.oe
	}
	b.WriteString(out[last:])
	return b.String()
}
