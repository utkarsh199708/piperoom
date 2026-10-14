package auth

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Security audit 2026-10 (reviews/security-audit-2026-10.md).

// A-RED-1. Open redirect after login: every return value goes through
// SafeReturn and then http.Redirect, which is what the callback does. The
// Location a browser follows must stay on this site whatever the attacker
// writes.
func TestAttack_ReturnOpenRedirect(t *testing.T) {
	for _, raw := range []string{
		"//evil.test", "///evil.test", "/\\evil.test", "\\\\evil.test", "/%2F%2Fevil.test", "/%5Cevil.test",
		"/.//evil.test", "/./%2F/evil.test", "/%2e%2e//evil.test", "https://evil.test", "https:evil.test",
		"//evil.test@x", "/\t/evil.test", "/%09/evil.test", "/\r\nLocation: https://evil.test",
		"javascript:alert(1)", "data:text/html,x", " //evil.test", "/x/../..//evil.test",
		"http:\\\\evil.test", "/" + strings.Repeat("a", 3000), "//%65vil.test", "/ /evil.test",
	} {
		ret := SafeReturn(raw)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "https://piperoom.test/auth/callback", nil)
		http.Redirect(w, r, ret, http.StatusFound)
		loc := w.Header().Get("Location")
		u, err := url.Parse(loc)
		if err != nil {
			t.Errorf("%q -> %q: %v", raw, loc, err)
			continue
		}
		if u.Scheme != "" || u.Host != "" || u.User != nil || !strings.HasPrefix(loc, "/") ||
			strings.HasPrefix(loc, "//") || strings.HasPrefix(loc, "/\\") || strings.ContainsAny(loc, "\r\n\t\\") {
			t.Errorf("return %q -> SafeReturn %q -> Location %q leaves the site", raw, ret, loc)
		}
		// What a browser resolves it to.
		base, _ := url.Parse("https://piperoom.test/auth/callback")
		if got := base.ResolveReference(u); got.Host != "piperoom.test" {
			t.Errorf("return %q resolves to host %q", raw, got.Host)
		}
	}
}

// A-ID-4 (fixed). The characters that can disguise a display name are dropped;
// the joiners that real names need are kept.
func TestAttack_StripFormat(t *testing.T) {
	// Each stripped character, alone inside a name.
	var stripped []rune
	for r := rune(0x202A); r <= 0x202E; r++ {
		stripped = append(stripped, r)
	}
	for r := rune(0x2066); r <= 0x2069; r++ {
		stripped = append(stripped, r)
	}
	stripped = append(stripped, 0x200E, 0x200F, 0x200B, 0x2060, 0xFEFF,
		0x061C, 0x00AD, 0x2062, 0x2061, 0x2064, 0x206A, 0x206F, 0xFFF9, 0xFFFB, 0x180E, 0xE0001, 0xE0041, 0xE007F,
		// Other_Default_Ignorable_Code_Point: the Hangul fillers (letters that
		// render as nothing) and the combining grapheme joiner.
		0x3164, 0x115F, 0x1160, 0xFFA0, 0x034F)
	for _, r := range stripped {
		if got := stripFormat("Pri" + string(r) + "ya"); got != "Priya" {
			t.Errorf("U+%04X: stripFormat = %q, want Priya", r, got)
		}
	}
	for in, want := range map[string]string{
		"\u202eayirP\u202c":       "ayirP",
		"\u2066Priya\u2069":       "Priya",
		"\ufeffPriya\u200e\u200f": "Priya",
		"Pr\u00adiya":             "Priya",
		"Pri\U000E0041ya":         "Priya",
		"Ana Mar\u00eda":          "Ana Mar\u00eda",
	} {
		if got := stripFormat(in); got != want {
			t.Errorf("stripFormat(%q) = %q, want %q", in, got, want)
		}
	}
}

// Names that need ZWJ (U+200D) or ZWNJ (U+200C) stay byte-identical.
func TestStripFormatKeepsJoiners(t *testing.T) {
	for name, in := range map[string]string{
		// Devanagari conjunct with an explicit ZWJ: k + virama + ZWJ + ssa.
		"hindi ZWJ conjunct": "\u0915\u094d\u200d\u0937",
		// Devanagari virama + ZWNJ (explicit halant), inside a full name.
		"hindi ZWNJ": "\u0930\u093e\u0915\u094d\u200c\u0937 \u0936\u0930\u094d\u092e\u093e",
		// Persian "mikhaham" (I want) is spelled with a ZWNJ after the mim-ya.
		"persian ZWNJ":      "\u0645\u06cc\u200c\u062e\u0648\u0627\u0647\u0645",
		"persian name ZWNJ": "\u0647\u0645\u0627\u06cc\u0648\u0646 \u0646\u06cc\u06a9\u200c\u0646\u0627\u0645",
	} {
		if !strings.ContainsAny(in, "\u200c\u200d") {
			t.Fatalf("%s: fixture has no joiner", name)
		}
		if got := stripFormat(in); got != in {
			t.Errorf("%s: stripFormat(%q) = %q, want it unchanged", name, in, got)
		}
	}
	// A joiner next to a character that is stripped: only that one goes.
	if got := stripFormat("\u0915\u094d\u200d\u0937\u200b\u202e"); got != "\u0915\u094d\u200d\u0937" {
		t.Errorf("mixed = %q", got)
	}
}
