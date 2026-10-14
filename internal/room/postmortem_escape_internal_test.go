package room

import (
	"strings"
	"testing"
)

// The injection corpus: none of these may come out as a heading, quote, list,
// table cell, link, image, tag or extra line.
func TestMDTextInjectionCorpus(t *testing.T) {
	cases := map[string]string{
		"heading":      "# heading",
		"link":         "see [x](http://x) and ![i](http://y)",
		"script":       "<script>alert(1)</script>",
		"cell":         "|cell|other|",
		"quote":        "> quoted",
		"list dash":    "- item",
		"list plus":    "+ item",
		"list number":  "12. item",
		"list paren":   "3) item",
		"nested lead":  "- 1. # x",
		"newline head": "ok\n# heading\n- item",
		"crlf":         "a\r\nb",
		"u2028":        "a # b - c",
		"nul":          "a\x00b",
		"entity":       "&lt;b&gt;",
		"emphasis":     "**bold** _it_ `code`",
		"long":         strings.Repeat("a", 10_000),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			out := mdText(in)
			if strings.ContainsAny(out, "\n\r  \x00") {
				t.Fatalf("%q contains a line break or NUL: %q", name, out)
			}
			if len(out) > 1100 { // 500 bytes of text, each at most escaped
				t.Fatalf("%d bytes, want a cap", len(out))
			}
			// Every markdown-significant byte is preceded by a backslash.
			for i := 0; i < len(out); i++ {
				if strings.IndexByte(mdSpecials, out[i]) < 0 || out[i] == '\\' {
					continue
				}
				if i == 0 || out[i-1] != '\\' {
					t.Fatalf("%q: unescaped %q at %d in %q", name, out[i], i, out)
				}
			}
			if strings.HasPrefix(out, "-") || strings.HasPrefix(out, "+") || strings.HasPrefix(out, "1.") {
				t.Fatalf("%q can start a list: %q", name, out)
			}
		})
	}
	if got := mdText("# heading"); got != `\# heading` {
		t.Fatalf("heading = %q", got)
	}
	if got := mdText("a](http://x)"); got != `a\](http://x)` {
		t.Fatalf("link = %q", got)
	}
}

func TestMDCodeAndFence(t *testing.T) {
	if got := mdCode("p_1`\n|x"); got != "`p_1x`" {
		t.Fatalf("mdCode = %q", got)
	}
	if got := mdCode(""); got != "–" {
		t.Fatalf("empty mdCode = %q", got)
	}
	f := mdFence("json", "{\"a\":\"````\"}")
	if !strings.HasPrefix(f, "`````json\n") || !strings.HasSuffix(f, "\n`````") {
		t.Fatalf("fence = %q", f)
	}
	if !strings.HasPrefix(mdFence("", "x"), "```\n") {
		t.Fatal("a fence is at least three backticks")
	}
}

func TestMDTextCutsOnRuneBoundary(t *testing.T) {
	out := mdTextN(strings.Repeat("é", 300), 11)
	if !strings.HasSuffix(out, "…") || strings.ContainsRune(out, '�') {
		t.Fatalf("bad cut: %q", out)
	}
}
