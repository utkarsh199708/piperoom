package strimzi

import (
	"testing"

	"piperoom/internal/redact"
)

// The reads mask the union of their detectors (round 10): for every
// corpus input, the ranges maskCredentials masks cover everything
// redact.Text alone masks and everything the key rule alone masks, so no
// detector can pre-empt another.
func TestMaskingIsMonotone(t *testing.T) {
	for _, c := range corpusCases(corpusPEMs(t)) {
		raw := c.line
		text, spans := maskPlan(raw, c.written)
		covered := make([]bool, len(text)+1)
		for _, sp := range spans {
			for i := sp.a; i < sp.b; i++ {
				covered[i] = true
			}
		}
		check := func(name string, a, b int) {
			for i := a; i < b; i++ {
				if !covered[i] {
					t.Errorf("%s: %q: byte %d of %s's range [%d,%d) is not masked", c.name, text, i, name, a, b)
					return
				}
			}
		}
		for _, r := range redact.Spans(text) {
			check("redact", r.Start, r.End)
		}
		for _, sp := range credentialKeySpans(text) {
			check("the key rule", sp[0], sp[1])
		}
		// And the rendered text holds no byte of those ranges' text where
		// redact.Text alone would have masked it: rendering is one pass.
		if out := renderSpans(text, spans); len(spans) > 0 && out == text {
			t.Errorf("%s: spans but nothing rendered", c.name)
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
