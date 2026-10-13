package auth

import "testing"

// A-ID-2: names that render like "Priya" must share its skeleton; names that
// do not must not. Look-alike letters are written as escapes so a reader can
// see which is which.
func TestSkeletonLookalikes(t *testing.T) {
	alike := []struct{ name, why string }{
		{"Priya", "the same name"},
		{"priya", "lower case"},
		{"PRIYA", "upper case"},
		{"pRiYa", "mixed case"},
		{"Pri\u0443a", "Cyrillic u0443 for y"},
		{"Priy\u0430", "Cyrillic u0430 for a"},
		{"\u0420riya", "Cyrillic u0420 for P"},
		{"Pr\u0456ya", "Cyrillic u0456 for i"},
		{"\u0420r\u0456\u0443\u0430", "four Cyrillic letters"},
		{"Pr\u03b9ya", "Greek iota for i"},
		{"\u03a1riya", "Greek Rho for P"},
		{"\uff30\uff52\uff49\uff59\uff41", "fullwidth"},
		{"\uff30RIYA", "fullwidth and upper case"},
		{"\U0001d40f\U0001d42b\U0001d422\U0001d432\U0001d41a", "mathematical bold"},
		{"Pr\u0131ya", "dotless i"},
		{"Prlya", "l for i"},
		{"Pr1ya", "digit 1 for i"},
		{"PrIya", "capital I for i"},
		{"Pri\u0301ya", "a combining accent"},
		{"Pr\u00edya", "a precomposed accent"},
		{"Pri\u200bya", "a zero-width space"},
		{"Pri\u200dya", "a zero-width joiner"},
		{" Priya. ", "spaces and punctuation"},
		{"Priya \u2713", "a check mark"},
		{"P r i y a", "spaced letters"},
	}
	for _, c := range alike {
		if !LooksLike(c.name, "Priya") {
			t.Errorf("%q (%s): skeleton %q, want it to look like Priya (%q)", c.name, c.why, Skeleton(c.name), Skeleton("Priya"))
		}
	}
	pairs := []struct{ a, b, why string }{
		{"Meera", "\u041c\u0435\u0435r\u0430", "Cyrillic capital M, small e, a"},
		{"Kabir", "\u039a\u0391\u0392\u0399r", "Greek capitals K, A, B, I"},
		{"Hana", "\u041d\u0430n\u0430", "Cyrillic capital H (En) and a"},
		{"Nina", "\u03b7\u03b9\u03b7\u03b1", "Greek eta for n"},
		// Capitals whose small letter imitates something else: mapped before
		// case folding, or capital eta would read as an n.
		{"Hana", "\u0397ana", "Greek capital eta for H"},
		{"Nina", "\u039dina", "Greek capital nu for N"},
		{"Mohan", "\u039cohan", "Greek capital mu for M"},
		{"Yusuf", "\u03a5usuf", "Greek capital upsilon for Y"},
		{"Mohan", "rnohan", "rn for m"},
		{"Wendy", "VVendy", "vv for w"},
		{"Kelvin", "\u212aelvin", "Kelvin sign"},
		{"Oscar", "0scar", "zero for O"},
		{"Nora", "\u0274\u1d0f\u0280\u1d00", "small capitals"},
		{"priya@example.com", "PRIYA@EXAMPLE.COM", "an email folds too"},
	}
	for _, p := range pairs {
		if !LooksLike(p.a, p.b) {
			t.Errorf("%q and %q (%s): skeletons %q and %q, want equal", p.a, p.b, p.why, Skeleton(p.a), Skeleton(p.b))
		}
	}
	// Names that only share letters stay apart, and an empty skeleton never
	// matches anything.
	different := [][2]string{
		{"Priya", "Priyanka"},
		{"Priya", "Arjun"},
		{"Priya", "Pria"},
		{"Priya", "Priya (mallory@example.com)"},
		{"Priya", "Priya (u_0123456789ab)"},
		{"Meera", "Mira"},
		{"", ""},
		{"\u2713", "\u2714"},
		{"...", "---"},
	}
	for _, p := range different {
		if LooksLike(p[0], p[1]) {
			t.Errorf("%q and %q look alike (skeleton %q), want different", p[0], p[1], Skeleton(p[0]))
		}
	}
}

// A name in a script the table does not touch keeps its letters: it matches
// itself, two different such names stay apart, and it never has an empty
// skeleton (which would match nothing, not even itself).
func TestSkeletonKeepsOtherScripts(t *testing.T) {
	devanagari := "\u092a\u094d\u0930\u093f\u092f\u093e"
	if !LooksLike(devanagari, devanagari) {
		t.Fatal("a Devanagari name does not match itself")
	}
	han1, han2 := "\u738b\u82b3", "\u674e\u5a1c"
	if LooksLike(han1, han2) {
		t.Fatal("two different Han names look alike")
	}
	if Skeleton(han1) == "" {
		t.Fatal("a Han name has an empty skeleton")
	}
}
