package fixpr

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestUnified(t *testing.T) {
	cases := []struct {
		name, old, cur string
		isNew          bool
		want           string
	}{
		{"new file", "", "a\nb\n", true, "--- /dev/null\n+++ b/f\n@@ -0,0 +1,2 @@\n+a\n+b\n"},
		{"one line", "a\n", "b\n", false, "--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n+b\n"},
		{"no newline at end", "a\nb", "a\nc", false, "--- a/f\n+++ b/f\n@@ -1,2 +1,2 @@\n a\n-b\n\\ No newline at end of file\n+c\n\\ No newline at end of file\n"},
		{"newline added", "a", "a\n", false, "--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n\\ No newline at end of file\n+a\n"},
		{"insert only", "1\n2\n3\n4\n5\n6\n7\n8\n", "1\n2\n3\n4\nX\n5\n6\n7\n8\n", false,
			"--- a/f\n+++ b/f\n@@ -2,6 +2,7 @@\n 2\n 3\n 4\n+X\n 5\n 6\n 7\n"},
		{"delete only", "1\n2\n3\n", "1\n3\n", false, "--- a/f\n+++ b/f\n@@ -1,3 +1,2 @@\n 1\n-2\n 3\n"},
		{"two hunks", lines(1, 20), strings.Replace(strings.Replace(lines(1, 20), "2\n", "two\n", 1), "18\n", "eighteen\n", 1), false,
			"--- a/f\n+++ b/f\n@@ -1,5 +1,5 @@\n 1\n-2\n+two\n 3\n 4\n 5\n@@ -15,6 +15,6 @@\n 15\n 16\n 17\n-18\n+eighteen\n 19\n 20\n"},
		{"joined hunks", lines(1, 10), strings.Replace(strings.Replace(lines(1, 10), "2\n", "two\n", 1), "8\n", "eight\n", 1), false,
			"--- a/f\n+++ b/f\n@@ -1,10 +1,10 @@\n 1\n-2\n+two\n 3\n 4\n 5\n 6\n 7\n-8\n+eight\n 9\n 10\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := unified("f", c.old, c.cur, c.isNew)
			if got.Unified != c.want {
				t.Fatalf("got\n%s\nwant\n%s", got.Unified, c.want)
			}
			if got.rawBytes != len(c.want) {
				t.Fatalf("raw bytes %d, want %d", got.rawBytes, len(c.want))
			}
		})
	}
}

func lines(from, to int) string {
	var b strings.Builder
	for i := from; i <= to; i++ {
		b.WriteString(strconv.Itoa(i) + "\n")
	}
	return b.String()
}

// TestMyersRoundTrip checks on random inputs that the script rebuilds both
// sides and is no longer than the trivial one.
func TestMyersRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	alpha := []string{"a\n", "b\n", "c\n", "d\n"}
	gen := func() []string {
		out := make([]string, r.Intn(30))
		for i := range out {
			out[i] = alpha[r.Intn(len(alpha))]
		}
		return out
	}
	for i := 0; i < 500; i++ {
		a, b := gen(), gen()
		ops := diffLines(a, b)
		var ga, gb []string
		changes := 0
		for _, op := range ops {
			if op.kind != opIns {
				ga = append(ga, op.text)
			}
			if op.kind != opDel {
				gb = append(gb, op.text)
			}
			if op.kind != opEq {
				changes++
			}
		}
		if strings.Join(ga, "") != strings.Join(a, "") || strings.Join(gb, "") != strings.Join(b, "") {
			t.Fatalf("round trip failed: %q %q -> %v", a, b, ops)
		}
		if changes > len(a)+len(b) {
			t.Fatalf("script longer than trivial")
		}
	}
}

// TestMyersFallback: past maxEditDistance the middle is replaced whole,
// still exactly.
func TestMyersFallback(t *testing.T) {
	var a, b []string
	for i := 0; i < maxEditDistance; i++ {
		a = append(a, "a"+strconv.Itoa(i)+"\n")
		b = append(b, "b"+strconv.Itoa(i)+"\n")
	}
	ops := myers(a, b)
	if len(ops) != 2*maxEditDistance || ops[0].kind != opDel || ops[len(ops)-1].kind != opIns {
		t.Fatalf("fallback: %d ops", len(ops))
	}
}

func TestMaskerPEM(t *testing.T) {
	m := newMasker()
	in := []string{"a", "-----BEGIN PRIVATE KEY-----", "MIIEvQ", "-----END PRIVATE KEY-----", "b"}
	var out []string
	for _, l := range in {
		out = append(out, m.mask(l))
	}
	if out[0] != "a" || out[4] != "b" || out[1] != out[2] || out[2] != out[3] || strings.Contains(strings.Join(out, ""), "MIIEvQ") {
		t.Fatalf("masked = %q", out)
	}
}
