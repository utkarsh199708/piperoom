package redact

import (
	"strings"
	"testing"
)

// Rendering Spans' ranges with their masks reproduces Text's output, on
// every fixture input Text has a test for that changes, and a text Text
// leaves alone has no ranges.
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
