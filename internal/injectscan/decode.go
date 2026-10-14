package injectscan

import (
	"encoding/hex"
	"html"
	"net/url"
	"strings"
)

// Cheap decoders for the other encodings an injection hides behind: hex
// (plain runs and \x escapes), %-encoding, HTML character references and
// base64 wrapped over several lines. Each decoded candidate is scanned with the
// plain rules once (no nested decoding), and only text that decodes to
// printable UTF-8 is scanned at all, so hex digests, UUIDs, LSNs and binary
// keys never reach the rules.
//
// Limits: a payload encoded twice, split across fields, encoded in a scheme not
// listed here (rot13, base32, punycode, gzip) or written in another language is
// not found. Wrapped base64 is joined only across whitespace that follows a
// line of at least wrapMinLine characters, the shape of MIME and PEM wrapping.

const (
	maxDecoded  = 8  // candidates decoded per decoder per window
	minHexRun   = 24 // hex characters (12 bytes) worth decoding
	minEscapes  = 4  // %XX or \xXX escapes a token needs before it is decoded
	wrapMinLine = 32 // shortest base64 line that may be continued on the next
)

// scanDecoded runs every decoder over the normalised window.
func scanDecoded(norm string, origin []int) []Finding {
	var out []Finding
	out = append(out, scanHex(norm, origin)...)
	out = append(out, scanPercent(norm, origin)...)
	out = append(out, scanEntities(norm, origin)...)
	out = append(out, scanWrappedBase64(norm, origin)...)
	return out
}

// encodedFinding scans decoded text and returns one KindEncoded finding at off
// when it holds instruction-like text.
func encodedFinding(label, decoded string, off int) (Finding, bool) {
	if !printable([]byte(decoded)) {
		return Finding{}, false
	}
	inner, innerOrigin, _, _ := normalize(decoded, false)
	hits := scanText(inner, innerOrigin)
	if len(hits) == 0 {
		return Finding{}, false
	}
	return Finding{Kind: KindEncoded, Offset: off, Excerpt: excerpt(label + ": " + hits[0].Excerpt)}, true
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

// scanHex decodes plain hex runs ("69676e6f7265...") and \xNN escape runs.
func scanHex(norm string, origin []int) []Finding {
	var out []Finding
	n := 0
	for i := 0; i < len(norm) && n < maxDecoded; {
		start := i
		var raw []byte
		switch {
		case isHex(norm[i]) && (i == 0 || !isB64(norm[i-1])):
			j := i
			for j < len(norm) && isHex(norm[j]) {
				j++
			}
			if j-i >= minHexRun && (j == len(norm) || !isB64(norm[j])) {
				run := norm[i:min(j, i+maxBlobBytes)]
				if b, err := hex.DecodeString(run[:len(run)&^1]); err == nil {
					raw = b
				}
			}
			i = j
		case norm[i] == '\\' && i+3 < len(norm) && (norm[i+1] == 'x' || norm[i+1] == 'X') && isHex(norm[i+2]) && isHex(norm[i+3]):
			j := i
			var b []byte
			for j+3 < len(norm) && norm[j] == '\\' && (norm[j+1] == 'x' || norm[j+1] == 'X') && isHex(norm[j+2]) && isHex(norm[j+3]) && len(b) < maxBlobBytes {
				v, _ := hex.DecodeString(norm[j+2 : j+4])
				b = append(b, v...)
				j += 4
			}
			if len(b) >= minEscapes {
				raw = b
			}
			i = j
		default:
			i++
			continue
		}
		if raw == nil {
			continue
		}
		n++
		if f, ok := encodedFinding("hex", string(raw), origin[start]); ok {
			out = append(out, f)
		}
	}
	return out
}

// scanPercent decodes whitespace-separated tokens holding minEscapes or more
// %XX escapes (or '+' spaces next to at least one %XX).
func scanPercent(norm string, origin []int) []Finding {
	if !strings.Contains(norm, "%") {
		return nil
	}
	var out []Finding
	n := 0
	for i := 0; i < len(norm) && n < maxDecoded; {
		if isSpaceRune(rune(norm[i])) {
			i++
			continue
		}
		j := i
		for j < len(norm) && !isSpaceRune(rune(norm[j])) {
			j++
		}
		tok := norm[i:min(j, i+maxBlobBytes)]
		// Query encoding writes spaces as '+', so they count toward the
		// escapes once there is at least one real %XX.
		if pc := countPercent(tok); pc > 0 && pc+strings.Count(tok, "+") >= minEscapes {
			n++
			if dec, err := url.QueryUnescape(tok); err == nil {
				if f, ok := encodedFinding("url", dec, origin[i]); ok {
					out = append(out, f)
				}
			}
		}
		i = j
	}
	return out
}

func countPercent(s string) int {
	c := 0
	for i := 0; i+2 < len(s); i++ {
		if s[i] == '%' && isHex(s[i+1]) && isHex(s[i+2]) {
			c++
		}
	}
	return c
}

// scanEntities unescapes HTML character references. It reports only findings
// the plain text did not already give, so ordinary text with "&amp;" never
// adds a finding.
func scanEntities(norm string, origin []int) []Finding {
	first := strings.Index(norm, "&")
	if first < 0 || !strings.Contains(norm[first:], ";") {
		return nil
	}
	dec := html.UnescapeString(norm)
	if dec == norm {
		return nil
	}
	inner, innerOrigin, _, _ := normalize(dec, false)
	hits := scanText(inner, innerOrigin)
	if len(hits) == 0 || len(hits) <= len(scanText(norm, origin)) {
		return nil
	}
	return []Finding{{Kind: KindEncoded, Offset: origin[first], Excerpt: excerpt("html: " + hits[0].Excerpt)}}
}

// scanWrappedBase64 joins base64 broken over lines (MIME, PEM-style or
// hand-wrapped) and decodes the joined blob. A single-line blob is left to
// scanBase64.
func scanWrappedBase64(norm string, origin []int) []Finding {
	var out []Finding
	n := 0
	for i := 0; i < len(norm) && n < maxDecoded; {
		if !isB64(norm[i]) || (i > 0 && isB64(norm[i-1])) {
			i++
			continue
		}
		start := i
		var b strings.Builder
		lines := 0
		j := i
		for {
			k := j
			for k < len(norm) && isB64(norm[k]) {
				k++
			}
			for e := 0; e < 2 && k < len(norm) && norm[k] == '='; e++ {
				k++
			}
			seg := k - j
			b.WriteString(norm[j:k])
			lines++
			// Continue only after a full-looking line, across whitespace, onto
			// more base64.
			w := k
			for w < len(norm) && (norm[w] == '\n' || norm[w] == '\r' || norm[w] == ' ' || norm[w] == '\t') {
				w++
			}
			if seg < wrapMinLine || w == k || w >= len(norm) || !isB64(norm[w]) || b.Len() >= maxBlobBytes || norm[k-1] == '=' {
				j = k
				break
			}
			j = w
		}
		i = max(j, i+1)
		if lines < 2 || b.Len() < minBlob {
			continue
		}
		n++
		blob := b.String()
		if len(blob) > maxBlobBytes {
			blob = blob[:maxBlobBytes]
		}
		dec, ok := decodeBase64(blob)
		if !ok {
			continue
		}
		if f, ok := encodedFinding("base64", dec, origin[start]); ok {
			out = append(out, f)
		}
	}
	return out
}
