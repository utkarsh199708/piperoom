package redact

// Range is a byte range [Start, End) of a text that Text masks, and Mask
// the text Text writes in its place.
type Range struct {
	Start, End int
	Mask       string
}

// Spans returns the ranges of s that Text masks, in order, without
// overlaps, with the mask Text writes for each. A caller that combines
// redact with its own detectors can mask the union of their ranges in one
// pass over the original text, so no detector's rewrite hides a secret
// from another.
//
// The ranges are read off Text's own output: Text only ever replaces text
// with masks, and align (project.go) places each mask back on the original
// by the literal text around it, so Spans and Text agree by construction.
// When the output cannot be aligned (never seen), the whole changed middle
// of s, between the longest common prefix and suffix, is one range.
func Spans(s string) []Range { return def.Spans(s) }

// Spans is the package-level Spans for this Redactor's options.
func (r *Redactor) Spans(s string) []Range {
	out, _ := r.Text(s)
	if out == s {
		return nil
	}
	ps, ok := align(s, out)
	if !ok {
		p := 0
		for p < len(s) && p < len(out) && s[p] == out[p] {
			p++
		}
		q := 0
		for q < len(s)-p && q < len(out)-p && s[len(s)-1-q] == out[len(out)-1-q] {
			q++
		}
		return []Range{{p, len(s) - q, out[p : len(out)-q]}}
	}
	var rs []Range
	for _, pc := range ps {
		if pc.mask && pc.ge > pc.gs {
			rs = append(rs, Range{pc.gs, pc.ge, out[pc.os:pc.oe]})
		}
	}
	return rs
}
