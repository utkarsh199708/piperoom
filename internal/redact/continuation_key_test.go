package redact

import (
	"strings"
	"testing"
)

// A line that follows an explicit backslash continuation but starts with a
// credential key of its own ("Authorization:", "password=") is new text: the
// continuation must not mask the key away from the line rules and leave the
// value in the clear (fuzz crasher c48da0e331f31857).
func TestContinuationKeepsCredentialKeyHead(t *testing.T) {
	token := "-WWWWW9WGWGWGW9UFXX-WWW9"
	in := `A00000=0 0 0 0=passA000=0000000\` + "\nAuthorization: Bearer " + token
	out, _ := Text(in)
	if strings.Contains(out, token) {
		t.Errorf("bearer token survived after a continuation line:\n in=%q\nout=%q", in, out)
	}

	in = `token=abcDEF123456\` + "\npassword=hunter2hunter2 rest of line"
	out, _ = Text(in)
	if strings.Contains(out, "hunter2hunter2") {
		t.Errorf("password survived after a continuation line:\n in=%q\nout=%q", in, out)
	}

	// A head that merely contains a separator but no known key name is
	// still masked as the tail of the open value.
	in = `password=frag\` + "\nabc:def rest"
	out, _ = Text(in)
	if strings.Contains(out, "abc:def") {
		t.Errorf("secret fragment head kept:\n in=%q\nout=%q", in, out)
	}
}
