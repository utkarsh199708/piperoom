package runner

import (
	"strings"
	"testing"
)

// TestScrub: untrusted server text is made inert before printing — control
// characters and Unicode format characters (e.g. bidi overrides) are escaped,
// and the result is capped.
func TestScrub(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"escapes C0 and CR", "\x1b[2K\rpair", `\x1b[2K\x0dpair`},
		{"escapes DEL", "a\x7fb", `a\x7fb`},
		{"escapes C1", "a\u009bb", `a\x9bb`},
		{"escapes bidi override", "abc\u202ehello", `abc\u202ehello`},
		{"caps long text", strings.Repeat("x", 300), strings.Repeat("x", 200) + "…"},
		{"passes plain text", "orders", "orders"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := scrub(c.in, 200); got != c.want {
				t.Errorf("scrub(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
