package strimzi

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/redact"
)

// The reads mask the union of their detectors (round 10). Round 11, F2:
// the check reads the rendered output, not the spans the union was built
// from. For every corpus input (the round 10 corpus and the security
// review's), every byte redact.Text alone masks (redact.Spans), every byte
// the key rule alone masks and every byte the written-value rule alone
// masks must be masked in maskCredentials' output: the output must read
// as the input with masks put in over all of them (coveredBy).
func TestMaskingIsMonotone(t *testing.T) {
	cases := append(corpusCases(corpusPEMs(t)), reviewCases()...)
	for _, c := range cases {
		text := sanitizeText(c.line)
		out := maskCredentials(c.line, c.written)
		need := make([]bool, len(text))
		mark := func(a, b int) {
			for i := a; i < b; i++ {
				need[i] = true
			}
		}
		for _, r := range redact.Spans(text) {
			if m := outputMaskRE.FindStringIndex(r.Mask); m == nil || strings.Contains(text, r.Mask[m[0]:m[1]]) || len(outputMaskRE.FindAllString(r.Mask, 2)) > 1 {
				// The input carries this mask's text itself, so Spans
				// cannot place it by the literal text around it and
				// returns the whole changed middle as one range (its
				// documented fallback): no byte-exact claim to check.
				continue
			}
			// A token mask keeps a few characters on either side
			// (sk-a…[redacted 48]…9Q): those were never masked.
			a, b := r.Start, r.End
			if h, _, ok := strings.Cut(r.Mask, "…["); ok {
				a += len(h)
			}
			if i := strings.LastIndex(r.Mask, "]…"); i >= 0 {
				b -= len(r.Mask) - i - len("]…")
			}
			mark(a, b)
		}
		for _, sp := range credentialKeySpans(text) {
			mark(sp[0], sp[1])
		}
		for _, sp := range credentialValueSpans(c.line, text, c.written) {
			mark(sp[0], sp[1])
		}
		if why := coveredBy(text, out, need); why != "" {
			t.Errorf("%s: %s\n in %q\nout %q", c.name, why, text, out)
		}
	}
}

// outputMaskRE is a mask in a read's output: this package's, or redact's
// (with a token mask's ellipses; the characters it keeps are literal).
var outputMaskRE = regexp.MustCompile(`\[masked:credential\]|…?\[redacted[^\]]*\]…?`)

// coveredBy reports why out does not read as text with masks put in over
// every byte need marks, or "". The literal pieces between out's masks
// must be found in text in order (the first as a prefix, the last as a
// suffix, each mask standing for at least one byte), none of them on a
// needed byte: each piece is put at its leftmost such place, which leaves
// the most room for the pieces after it.
func coveredBy(text, out string, need []bool) string {
	ms := outputMaskRE.FindAllStringIndex(out, -1)
	var lits []string
	at := 0
	for _, m := range ms {
		lits = append(lits, out[at:m[0]])
		at = m[1]
	}
	lits = append(lits, out[at:])
	free := func(a, b int) bool {
		for i := a; i < b; i++ {
			if need[i] {
				return false
			}
		}
		return true
	}
	if len(ms) == 0 {
		if text != out || !free(0, len(text)) {
			return "nothing is masked"
		}
		return ""
	}
	if !strings.HasPrefix(text, lits[0]) || !free(0, len(lits[0])) {
		return "the text before the first mask is not the input's, or holds a byte to mask"
	}
	pos := len(lits[0])
	for n, l := range lits[1 : len(lits)-1] {
		found := false
		for from := pos + 1; from <= len(text); {
			i := strings.Index(text[from:], l)
			if i < 0 {
				break
			}
			if free(from+i, from+i+len(l)) {
				pos, found = from+i+len(l), true
				break
			}
			from += i + 1
		}
		if !found {
			return fmt.Sprintf("the text %q after mask %d is not in the input clear of bytes to mask", l, n+1)
		}
	}
	last := lits[len(lits)-1]
	if end := len(text) - len(last); end <= pos || text[end:] != last || !free(end, len(text)) {
		return "the text after the last mask is not the input's, or holds a byte to mask"
	}
	return ""
}

// Round 11, F4: rendering redact.Spans' ranges with their masks gives
// redact.Text's output on every input of both corpora (sanitized, as the
// reads pass it), not only on redact's own fixtures.
func TestSpansRenderTextCorpus(t *testing.T) {
	for _, c := range append(corpusCases(corpusPEMs(t)), reviewCases()...) {
		in := sanitizeText(c.line)
		want, _ := redact.Text(in)
		var b strings.Builder
		at := 0
		for _, r := range redact.Spans(in) {
			if r.Start < at || r.End <= r.Start || r.End > len(in) {
				t.Fatalf("%s: bad range %+v after %d", c.name, r, at)
			}
			b.WriteString(in[at:r.Start])
			b.WriteString(r.Mask)
			at = r.End
		}
		b.WriteString(in[at:])
		if b.String() != want {
			t.Errorf("%s: Spans renders %q, Text gives %q", c.name, b.String(), want)
		}
	}
}

// Round 10, D: an auth key keeps one mechanism word, and pwd, PWD and
// OLDPWD keep an absolute path; anything more is masked. Round 10, C5:
// "resume token:" keeps only its next token.
func TestKeptValuesRound10(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"httpAuth: SCRAM-SHA-512", ""},
		{"sasl.client.auth=required", ""},
		{"masterauth: kerberos", ""},
		{"proxyAuth=basic", ""},
		{"httpAuth: basic Zq9pLm2x", "httpAuth: [masked:credential]"},
		{"OLDPWD=/opt/kafka", ""},
		{"pwd: /home/kafka", ""},
		{"PWD=/opt/kafka", ""},
		{"pwd: hunter2xyz", "pwd: [masked:credential]"},
		{"OLDPWD=/opt Zq9pLm2x", "OLDPWD=[masked:credential]"},
		{"resume token: 8263A1B2 then Zq9pLm2x", "resume token: 8263A1B2 [masked:credential]"},
		{"presume token: 8263A1B2", "presume token: [masked:credential]"},
		{"unable to resume token=8263A1B2", "unable to resume token=[masked:credential]"},
		{"pass_word=x1 pass.word=x2", "pass_word=[masked:credential]"},
		{"p_wd: x1", "p_wd: [masked:credential]"},
		{"se_cret=x1", "se_cret=[masked:credential]"},
		{"to_ken=x1", "to_ken=[masked:credential]"},
		{"bear_er=x1", "bear_er=[masked:credential]"},
		{"cred_entials=x1", "cred_entials=[masked:credential]"},
		{"auth_orization: x1", "auth_orization: [masked:credential]"},
		{"pw=x1", "pw=[masked:credential]"},
		{"requirepass x1\nrequirepass: x2", "requirepass x1\nrequirepass: [masked:credential]"},
		{"masterauth=x1", "masterauth=[masked:credential]"},
		{`m="{\\\"Authorization\\\":\\\"Bearer x1\\\",\\\"k\\\":\\\"v\\\"}"`, `m="{\\\"Authorization\\\":\\\"[masked:credential]\\\",\\\"k\\\":\\\"v\\\"}"`},
	} {
		want := c.want
		if want == "" {
			want = c.in
		}
		if got := maskOwn(c.in, nil); got != want {
			t.Errorf("maskOwn(%q)\n got %q\nwant %q", c.in, got, want)
		}
	}
}

// maskOwn renders only this package's detectors (written values and the
// key rule), for tests of those rules alone.
func maskOwn(text string, written []string) string {
	t := sanitizeText(text)
	var spans []maskSpan
	for _, sp := range credentialValueSpans(text, t, written) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	for _, sp := range credentialKeySpans(t) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	return renderSpans(t, spans)
}
