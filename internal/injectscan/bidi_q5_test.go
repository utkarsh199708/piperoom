package injectscan

import (
	"slices"
	"testing"
)

// TestBidiOverride (Q5 T8): an embedding, override or isolate character makes
// text render in another order than it is stored ("orders missing lag" shown
// reversed), so it is flagged wherever it appears; other format characters,
// and the plain marks U+200E/U+200F, are not.
func TestBidiOverride(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want bool
	}{
		{"RLO", "orders ‮missing lag", true},
		{"LRE", "a‪b", true},
		{"PDF", "a‬b", true},
		{"LRI", "a⁦b", true},
		{"PDI", "a⁩b", true},
		{"FSI", "a⁨b", true},
		{"plain", "orders missing lag", false},
		{"LRM mark", "a‎b", false},
		{"zero width space", "a​b", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := Scan(tc.in)
			got := slices.Contains(Kinds(fs), string(KindBidi))
			if got != tc.want {
				t.Fatalf("Scan(%q) kinds = %v, want bidi %v", tc.in, Kinds(fs), tc.want)
			}
			if got && fs[0].Excerpt == "" {
				t.Fatal("finding without an excerpt")
			}
		})
	}
	// One finding per input, not one per character.
	n := 0
	for _, f := range Scan("‮‮‮⁦") {
		if f.Kind == KindBidi {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d bidi findings, want 1", n)
	}
}

func TestHasBidiOverride(t *testing.T) {
	if !HasBidiOverride("x‮y") || HasBidiOverride("xy‏") {
		t.Fatal("HasBidiOverride wrong")
	}
}
