package auth

import "testing"

// A-ID-2 review: "Priya (priya@example.com)" compares as "Priya" once the
// bracketed part is gone, whatever the bracket.
func TestWithoutBrackets(t *testing.T) {
	for in, want := range map[string]string{
		"Priya (priya@example.com)": "Priya ",
		"Priya":                     "Priya",
		"Pri(x)ya":                  "Priya",
		"Priya ((a) b)":             "Priya ",
		"Priya [ops] {x} <y>":       "Priya   ",
		"Priya <priya@example.com>": "Priya ",
		"Priya \u2039x\u203a":       "Priya ",
		"Priya \u00abx\u00bb":       "Priya ",
		"Priya\uff08x\uff09":        "Priya",
		"Priya\ufe59x\ufe5a":        "Priya",
		"Priya \u3008x\u3009":       "Priya ",
		"Priya )":                   "Priya ",
		"(only)":                    "",
	} {
		if got := WithoutBrackets(in); got != want {
			t.Errorf("WithoutBrackets(%q) = %q, want %q", in, got, want)
		}
	}
}

// A name may not carry the shape of the suffix the server adds.
func TestHasSuffixShape(t *testing.T) {
	yes := []string{
		"Priya (priya@example.com)",
		"Priya (arjun@example.com)",
		"Bob (u_0123456789ab)",
		"Bob (U_ABC)",
		"Bob ( u_x )",
		"Bob [ops@example.com]",
		"Bob \uff08x\uff20y\uff09",
		"Priya (priya@example.com",
		"Bob (x@y) and more",
		// LEAD-REVIEW round 1: ASCII angle brackets, guillemets, and an
		// email-shaped word with no brackets at all.
		"Priya <priya@example.com>",
		"Priya \u2039priya@example.com\u203a",
		"Priya \u00abpriya@example.com\u00bb",
		"Bob <u_0123456789ab>",
		"Priya - priya@example.com",
		"Priya priya@example.com",
		"Priya, \"priya@example.com\".",
		"Priya \u2014 ops@vendor.example",
	}
	for _, s := range yes {
		if !HasSuffixShape(s) {
			t.Errorf("HasSuffixShape(%q) = false, want true", s)
		}
	}
	no := []string{"Bob", "Bob (contractor)", "a@b", "Bob (Ops) u_x", "Bob ()", "Bob (uu_x)", "Bob <ops>", "Bob \u00abOps\u00bb", "a@b.", "@example.com", "Bob@"}
	for _, s := range no {
		if HasSuffixShape(s) {
			t.Errorf("HasSuffixShape(%q) = true, want false", s)
		}
	}
}

// LEAD-ARCH: a name that mixes Latin with any other script is suffixed.
// Common and Inherited runes (digits, spaces, punctuation, combining marks)
// count for neither side.
func TestMixesLatin(t *testing.T) {
	for in, want := range map[string]bool{
		"Pri\u0443a":                     true,  // Cyrillic small u
		"\u0391gent":                     true,  // Greek capital alpha
		"Priya\u3164":                    true,  // Hangul filler
		"\u738b Wang":                    true,  // Han and Latin
		"J\u0585hn":                      true,  // Armenian
		"Priya":                          false, // Latin only
		"Jos\u00e9":                      false, // precomposed Latin letter
		"Jose\u0301":                     false, // Inherited combining mark
		"Priya 2 (on-call) \u2713":       false, // Common runes
		"\uff30\uff52\uff49\uff59\uff41": false, // fullwidth Latin is Latin
		"\u738b\u82b3":                   false, // Han only
		"\u041f\u0440\u0438\u044f":       false, // Cyrillic only
		"":                               false,
	} {
		if got := MixesLatin(in); got != want {
			t.Errorf("MixesLatin(%q) = %v, want %v", in, got, want)
		}
	}
}
