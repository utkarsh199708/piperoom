package oracle

import (
	"strings"
	"testing"
)

// TestQuoteIdentCopiesFromTheAlphabet: quoteIdent builds its output from
// identChars (CodeQL go/sql-injection on PR #1114), so identChars must hold
// exactly the bytes oracleIdentRe admits after the first, and the output
// must be the double-quoted name.
func TestQuoteIdentCopiesFromTheAlphabet(t *testing.T) {
	for c := 0; c < 256; c++ {
		admitted := oracleIdentRe.MatchString("_" + string([]byte{byte(c)}))
		if in := strings.IndexByte(identChars, byte(c)) >= 0; in != admitted {
			t.Errorf("byte %#x: in identChars %v, admitted by oracleIdentRe %v", c, in, admitted)
		}
	}
	for _, name := range []string{"ORDERS", "C##READ", "A$B_9"} {
		if got, err := quoteIdent(name); err != nil || got != `"`+name+`"` {
			t.Errorf("quoteIdent(%q) = %q, %v", name, got, err)
		}
	}
	for _, name := range []string{`a"b`, "", "9x", "a b", "a;DROP"} {
		if _, err := quoteIdent(name); err == nil {
			t.Errorf("quoteIdent(%q) accepted", name)
		}
	}
}
