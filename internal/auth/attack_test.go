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

// A-ID-4 (fixed). Format characters are dropped from display names.
func TestAttack_StripFormat(t *testing.T) {
	for in, want := range map[string]string{
		"\u202eayirP\u202c":       "ayirP",
		"Pri\u200bya":             "Priya",
		"\u2066Priya\u2069":       "Priya",
		"Pr\u00adiya":             "Priya",
		"\ufeffPriya\u200e\u200f": "Priya",
		"Ana Mar\u00eda":          "Ana Mar\u00eda",
	} {
		if got := stripFormat(in); got != want {
			t.Errorf("stripFormat(%q) = %q, want %q", in, got, want)
		}
	}
}
