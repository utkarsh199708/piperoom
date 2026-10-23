package gtid

import (
	"errors"
	"math"
	"strings"
	"testing"
)

const (
	u1 = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	u2 = "4f22fb58-82db-22f2-af44-d91bba530673"
)

func mustParse(t *testing.T, s string) Set {
	t.Helper()
	g, err := Parse(s)
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	return g
}

func TestParseCanonical(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"  \n", ""},
		{u1 + ":1-5", u1 + ":1-5"},
		{u1 + ":5", u1 + ":5"},
		{u1 + ":1-3:4-6", u1 + ":1-6"},                     // adjacent merge
		{u1 + ":7-9:1-3", u1 + ":1-3:7-9"},                 // sorted
		{u1 + ":1-5:3-8", u1 + ":1-8"},                     // overlap merge
		{u1 + ":1-5," + u1 + ":6-7", u1 + ":1-7"},          // same uuid twice
		{strings.ToUpper(u1) + ":1", u1 + ":1"},            // lower case
		{u2 + ":1,\n" + u1 + ":2", u1 + ":2," + u2 + ":1"}, // uuid order, newline
		{"3e11fa4771ca11e19e33c80aa9429562:1", u1 + ":1"},  // no dashes
		{u1 + ":1-2:Tag_A:1-3:tag_b:5", u1 + ":1-2:tag_a:1-3:tag_b:5"},
		{u1 + ":tagx:1," + u1 + ":2", u1 + ":2:tagx:1"}, // untagged first
	}
	for _, tt := range tests {
		g := mustParse(t, tt.in)
		if got := g.String(); got != tt.want {
			t.Errorf("Parse(%q).String() = %q, want %q", tt.in, got, tt.want)
		}
		if again := mustParse(t, g.String()); !again.Equal(g) {
			t.Errorf("round trip of %q changed the set", tt.in)
		}
	}
}

func TestParseRefuses(t *testing.T) {
	bad := []string{
		"x",
		u1,                           // no transactions
		u1 + ":",                     // empty part
		u1 + ":0",                    // numbers start at 1
		u1 + ":5-3",                  // reversed
		u1 + ":1-",                   // open range
		u1 + ":-3",                   // no start
		u1 + ":a b",                  // whitespace is removed, then "ab" is a tag without transactions
		u1 + ":1,",                   // empty element
		"," + u1 + ":1",              // empty element
		u1 + ":tag",                  // tag without transactions
		u1 + ":t1:t2:1",              // tag after tag
		u1 + ":9223372036854775807",  // GNO_END is exclusive
		u1 + ":99999999999999999999", // overflow
		u1 + ":1-2x",                 // junk
		u1 + ":1tag:1",               // a tag cannot start with a digit
		u1 + ":" + strings.Repeat("a", 33) + ":1", // tag too long
		"3e11fa47-71ca-11e1-9e33-c80aa942956:1",   // short uuid
		"zz11fa47-71ca-11e1-9e33-c80aa9429562:1",  // not hex
	}
	for _, s := range bad {
		if _, err := Parse(s); !errors.Is(err, ErrSyntax) {
			t.Errorf("Parse(%q) = %v, want ErrSyntax", s, err)
		}
	}
	if _, err := Parse(strings.Repeat(" ", MaxParseBytes+1)); !errors.Is(err, ErrSyntax) {
		t.Errorf("oversized input: %v, want ErrSyntax", err)
	}
}

func TestParseErrorEchoIsShort(t *testing.T) {
	_, err := Parse(u1 + ":" + strings.Repeat("ä", 100))
	if err == nil || len(err.Error()) > 120 {
		t.Fatalf("error %q should be short", err)
	}
}

// The expected values below are what the server's GTID_SUBSET and
// GTID_SUBTRACT answer for the same inputs; the integration test in package
// mysql replays this table against a live server.
func TestContainsAndSubtract(t *testing.T) {
	tests := []struct {
		a, b     string
		contains bool   // GTID_SUBSET(b, a)
		subtract string // GTID_SUBTRACT(a, b)
	}{
		{"", "", true, ""},
		{u1 + ":1-10", "", true, u1 + ":1-10"},
		{"", u1 + ":1", false, ""},
		{u1 + ":1-10", u1 + ":3-5", true, u1 + ":1-2:6-10"},
		{u1 + ":1-10", u1 + ":1-10", true, ""},
		{u1 + ":1-10", u1 + ":5-12", false, u1 + ":1-4"},
		{u1 + ":1-10", u1 + ":11", false, u1 + ":1-10"},
		{u1 + ":1-3:7-9", u1 + ":4-6", false, u1 + ":1-3:7-9"},
		{u1 + ":1-3:7-9", u1 + ":2:8", true, u1 + ":1:3:7:9"},
		{u1 + ":1-3:7-9", u1 + ":1-9", false, ""},
		{u1 + ":1-20", u1 + ":2-3:5:9-30", false, u1 + ":1:4:6-8"},
		{u1 + ":1-5," + u2 + ":1-5", u2 + ":2-3", true, u1 + ":1-5," + u2 + ":1:4-5"},
		{u1 + ":1-5", u2 + ":1", false, u1 + ":1-5"},
		{u1 + ":1-5:t:1-5", u1 + ":t:2", true, u1 + ":1-5:t:1:3-5"},
		{u1 + ":1-5", u1 + ":t:1", false, u1 + ":1-5"}, // a tag is another source id
	}
	for _, tt := range tests {
		a, b := mustParse(t, tt.a), mustParse(t, tt.b)
		if got := a.Contains(b); got != tt.contains {
			t.Errorf("Parse(%q).Contains(%q) = %v, want %v", tt.a, tt.b, got, tt.contains)
		}
		if got := a.Subtract(b).String(); got != tt.subtract {
			t.Errorf("Parse(%q).Subtract(%q) = %q, want %q", tt.a, tt.b, got, tt.subtract)
		}
		// Invariants: (a - b) ∪ (a ∩ b) = a, and a - b shares nothing with b.
		diff := a.Subtract(b)
		if !a.Contains(diff) {
			t.Errorf("%q - %q is not inside %q", tt.a, tt.b, tt.a)
		}
		if !diff.Subtract(b).Equal(diff) {
			t.Errorf("%q - %q still overlaps %q", tt.a, tt.b, tt.b)
		}
		if !diff.Union(a.Subtract(diff)).Equal(a) {
			t.Errorf("(%q - %q) ∪ rest != %q", tt.a, tt.b, tt.a)
		}
	}
}

func TestUnion(t *testing.T) {
	a := mustParse(t, u1+":1-3")
	b := mustParse(t, u1+":4-6,"+u2+":9")
	got := a.Union(b)
	if want := u1 + ":1-6," + u2 + ":9"; got.String() != want {
		t.Fatalf("Union = %q, want %q", got, want)
	}
	if a.String() != u1+":1-3" {
		t.Fatalf("Union changed its receiver: %q", a)
	}
	if !got.Contains(a) || !got.Contains(b) {
		t.Fatal("a union contains both inputs")
	}
}

func TestCountAndAccessors(t *testing.T) {
	g := mustParse(t, u1+":1-10:20,"+u2+":t:5-6")
	if n := g.Count(); n != 13 {
		t.Fatalf("Count = %d, want 13", n)
	}
	if ids := g.SourceIDs(); len(ids) != 2 || ids[0] != u1 || ids[1] != u2+":t" {
		t.Fatalf("SourceIDs = %v", ids)
	}
	ivs := g.Intervals(strings.ToUpper(u1))
	if len(ivs) != 2 || ivs[0] != (Interval{1, 10}) || ivs[1] != (Interval{20, 20}) {
		t.Fatalf("Intervals = %v", ivs)
	}
	ivs[0].End = 99 // a copy
	if g.Intervals(u1)[0].End != 10 {
		t.Fatal("Intervals returned the set's own slice")
	}
	if !(Set{}).IsEmpty() || g.IsEmpty() {
		t.Fatal("IsEmpty")
	}
	big := mustParse(t, u1+":1-9223372036854775806,"+u2+":1-9223372036854775806")
	if big.Count() != math.MaxInt64 {
		t.Fatalf("Count saturates, got %d", big.Count())
	}
}

func TestZeroSetOperations(t *testing.T) {
	var z Set
	a := mustParse(t, u1+":1")
	if !a.Contains(z) || z.Contains(a) || z.String() != "" || !z.Union(a).Equal(a) || !z.Subtract(a).IsEmpty() {
		t.Fatal("zero Set must behave as the empty set")
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{"", u1 + ":1-5", u1 + ":1:t:2," + u2 + ":3-4", "x:1"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		g, err := Parse(s)
		if err != nil {
			return
		}
		again, err := Parse(g.String())
		if err != nil || !again.Equal(g) {
			t.Fatalf("round trip of %q: %v", s, err)
		}
		if !g.Contains(g) || !g.Subtract(g).IsEmpty() {
			t.Fatalf("self ops on %q", s)
		}
	})
}
