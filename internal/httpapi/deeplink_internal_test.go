package httpapi

import (
	"strings"
	"testing"
)

func TestSafeReturnPath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/rooms/r_1", "/rooms/r_1"},
		{"/rooms/r_1?tab=steps", "/rooms/r_1?tab=steps"},
		{"/", "/"},
		{"/rooms/a%20b", "/rooms/a%20b"},
		// A query may hold anything; only the path decides where the browser goes.
		{"/rooms/r_1?next=//evil.com", "/rooms/r_1?next=//evil.com"},

		{"", "/"},
		{"rooms/r_1", "/"},
		{"evil.com", "/"},
		{"//evil.com", "/"},
		{"///evil.com", "/"},
		{"https://evil.com/x", "/"},
		{"javascript:alert(1)", "/"},
		{`/\evil.com`, "/"},
		{`\\evil.com`, "/"},
		{`/rooms\..\..\\evil.com`, "/"},
		{"/rooms/x\\y", "/"},
		{"/%2F%2Fevil.com", "/"},
		{"/%2f%2fevil.com", "/"},
		{"/%5Cevil.com", "/"},
		{"/%5cevil.com", "/"},
		{"/rooms/%5C", "/"},
		{"/%252F%252Fevil.com", "/"},     // double-encoded
		{"/%25252F%25252Fevil.com", "/"}, // triple-encoded
		{"/%2525252Fevil.com", "/"},      // deeper than maxUnescape
		{"/%255Cevil.com", "/"},          // double-encoded backslash
		{"/x%0d%0aSet-Cookie:a=b", "/"},  // CRLF
		{"/x%00", "/"},                   // NUL
		{"/x\ty", "/"},                   // raw control
		{"/bad%zz", "/"},                 // invalid escape
		{"/" + strings.Repeat("a", 2100), "/"},
	}
	for _, c := range cases {
		if got := safeReturnPath(c.in); got != c.want {
			t.Errorf("safeReturnPath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBadHostPageEscapes(t *testing.T) {
	page := badHostPage(`evil"><script>alert(1)</script>`)
	if strings.Contains(page, "<script") || strings.Contains(page, `evil"`) {
		t.Fatalf("Host is not escaped:\n%s", page)
	}
	if !strings.Contains(page, "evil&#34;&gt;&lt;script&gt;") {
		t.Fatalf("escaped Host missing:\n%s", page)
	}
	long := badHostPage(strings.Repeat("é", 200))
	if strings.Count(long, "é") != maxShownHost/2 || !strings.Contains(long, "é…") {
		t.Fatalf("long Host not clipped on a character boundary")
	}
}
