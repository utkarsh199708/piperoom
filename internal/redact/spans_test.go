package redact

import (
	"strings"
	"testing"
)

// Rendering Spans' ranges with their masks reproduces Text's output on
// these inputs, and a text Text leaves alone has no ranges. The Strimzi
// reads' corpora (several thousand adversarial inputs) run the same check
// in internal/setup/targets/strimzi (TestSpansRenderTextCorpus).
func TestSpansRenderText(t *testing.T) {
	inputs := []string{
		"password=hunter2 and more",
		"key AKIAIOSFODNN7EXAMPL0 then",
		"-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\nabcdefghijklmnop\n-----END PRIVATE KEY-----\nafter",
		`{"password":"x y z","user":"u"}`,
		"Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123",
		"nothing to see here",
		"a\npassword=\"multi \\\nline\"\nb",
	}
	for _, in := range inputs {
		out, _ := Text(in)
		var b strings.Builder
		at := 0
		for _, r := range Spans(in) {
			if r.Start < at || r.End <= r.Start || r.End > len(in) {
				t.Fatalf("%q: bad range %+v after %d", in, r, at)
			}
			b.WriteString(in[at:r.Start])
			b.WriteString(r.Mask)
			at = r.End
		}
		b.WriteString(in[at:])
		if b.String() != out {
			t.Errorf("Spans(%q) renders %q, Text gives %q", in, b.String(), out)
		}
	}
	if rs := Spans("nothing to see here"); rs != nil {
		t.Errorf("a clean text has ranges: %+v", rs)
	}
}

// Round 11: a bracketed placeholder ([hidden], the Strimzi reads'
// [masked:credential]) is kept whole, never masked up to its "]".
func TestBracketPlaceholderKept(t *testing.T) {
	for _, in := range []string{"password=[hidden]", "x secret=[hidden], y", "password=[masked:credential]", `{"password":"[hidden]"}`} {
		if out, _ := Text(in); out != in {
			t.Errorf("Text(%q) = %q", in, out)
		}
	}
	for _, in := range []string{"password=[hiddenZq9pLm2x]", "password=[masked:Zq9pLm2x]"} {
		if out, _ := Text(in); strings.Contains(out, "Zq9pLm2x") {
			t.Errorf("Text(%q) = %q", in, out)
		}
	}
}
